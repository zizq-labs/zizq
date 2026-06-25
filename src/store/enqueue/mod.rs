// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Enqueue operations: single and bulk job insertion.
//!
//! Both entry points funnel into the server-side `EnqueueBatcher` which
//! coalesces concurrent enqueues across many connections into a single
//! fjall transaction. The disk work splits across the prepare → apply →
//! finalize trio in sibling submodules; the methods here are thin async
//! wrappers that hand off through `spawn_blocking`.

mod apply;
mod batcher;
mod finalize;
mod prepare;

pub(in crate::store) use apply::apply_enqueue;
pub(super) use batcher::EnqueueBatcher;
pub(in crate::store) use finalize::finalize_enqueue;
pub(in crate::store) use prepare::{PreparedEnqueue, prepare_enqueue};

use tokio::task;

use super::options::EnqueueOptions;
use super::results::EnqueueResult;
use super::store::Store;
use super::types::StoreError;

impl Store {
    /// Enqueue a new job.
    ///
    /// Generates a unique job ID and inserts the job into the `jobs`
    /// keyspace plus the appropriate indexes. If `ready_at` is in the
    /// future the job enters the `Scheduled` state and is indexed in
    /// the in-memory scheduled index; otherwise it goes straight to `Ready`
    /// and into the priority indexes.
    ///
    /// Subscribers are notified when a job enters the ready state.
    pub async fn enqueue(
        &self,
        now: u64,
        opts: EnqueueOptions,
    ) -> Result<EnqueueResult, StoreError> {
        // Singleton enqueue is a Vec-of-1 trip through the same
        // batcher path as `enqueue_bulk`. The batcher coalesces this
        // with whatever else is in the channel at mutex-acquire time
        // and commits the whole coalesced batch atomically.
        let batcher = self.enqueue_batcher.clone();

        let reply_rx = task::spawn_blocking(move || -> Result<_, StoreError> {
            let prepared = prepare_enqueue(opts, now)?;
            Ok(batcher.submit(vec![prepared]))
        })
        .await??;

        let mut results = reply_rx
            .await
            .map_err(|_| StoreError::Internal("enqueue auto-batcher channel closed".into()))??;

        // Vec-of-1 contract — the batcher always returns the same number
        // of results as the input length. `unwrap` is safe in
        // principle, but we still guard against batcher-side bugs.
        results.pop().ok_or_else(|| {
            StoreError::Internal("enqueue auto-batcher returned empty result".into())
        })
    }

    /// Enqueue multiple jobs in a single transaction.
    ///
    /// All jobs are serialized and inserted under one write transaction with
    /// a single commit. In-memory indexes are updated after commit succeeds.
    /// Events are broadcast after the sync completes.
    ///
    /// Bulk participates in the same auto-batcher as singular enqueue —
    /// a bulk-of-N counts as one op in the batcher's op-count budget,
    /// and may share a commit with other concurrent enqueue requests.
    /// Atomicity is preserved at the commit boundary: if the coalesced
    /// commit fails, every participating op fails together.
    pub async fn enqueue_bulk(
        &self,
        now: u64,
        batch: Vec<EnqueueOptions>,
    ) -> Result<Vec<EnqueueResult>, StoreError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let batcher = self.enqueue_batcher.clone();

        let reply_rx = task::spawn_blocking(move || -> Result<_, StoreError> {
            let prepared: Vec<PreparedEnqueue> = batch
                .into_iter()
                .map(|opts| prepare_enqueue(opts, now))
                .collect::<Result<_, StoreError>>()?;
            Ok(batcher.submit(prepared))
        })
        .await??;

        reply_rx
            .await
            .map_err(|_| StoreError::Internal("enqueue auto-batcher channel closed".into()))?
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::SystemTime;

    use super::super::options::{EnqueueOptions, FailureOptions};
    use super::super::results::EnqueueResult;
    use super::super::store::StoreEvent;
    use super::super::test_support::{test_store, test_store_with_retention};
    use super::super::types::{BackoffConfig, Job, JobStatus, RetentionConfig, UniqueWhile};
    use crate::time::now_millis;

    #[tokio::test]
    async fn enqueue_returns_job_with_id() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"task": "test"})),
            )
            .await
            .unwrap()
            .into_job();

        assert!(!job.id.is_empty());
        assert_eq!(job.queue, "default");
        assert_eq!(job.priority, 0);
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, Some(serde_json::json!({"task": "test"})));
    }

    #[tokio::test]
    async fn enqueue_generates_unique_ids() {
        let store = test_store();
        let job1 = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let job2 = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        assert_ne!(job1.id, job2.id);
    }

    #[tokio::test]
    async fn enqueue_ids_are_fifo_ordered() {
        let store = test_store();
        let first = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let second = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let third = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        // scru128 IDs sort lexicographically in generation order.
        assert!(first.id < second.id);
        assert!(second.id < third.id);
    }

    #[tokio::test]
    async fn enqueue_broadcasts_job_created() {
        let store = test_store();
        let mut rx = store.subscribe();

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "default"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn enqueue_with_future_ready_at_creates_scheduled_job() {
        let store = test_store();
        let future_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000; // 1 minute from now

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future_ms),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(job.status, u8::from(JobStatus::Scheduled));
        assert_eq!(job.ready_at, future_ms);

        // Scheduled jobs should not be dequeue-able.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_none());
    }

    #[tokio::test]
    async fn enqueue_with_past_ready_at_creates_ready_job() {
        let store = test_store();
        let past_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 1000; // 1 second ago

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(past_ms),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.ready_at, past_ms);

        // Should be dequeue-able immediately.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn enqueue_without_ready_at_defaults_to_now() {
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert!(job.ready_at >= before);
        assert!(job.ready_at <= after);
    }

    #[tokio::test]
    async fn enqueue_scheduled_fires_job_scheduled_event() {
        let store = test_store();
        let mut rx = store.subscribe();
        let future = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        assert!(matches!(
            rx.recv().await.unwrap(),
            StoreEvent::JobScheduled { ready_at, .. } if ready_at == future
        ));
    }

    // --- enqueue_bulk tests ---

    #[tokio::test]
    async fn enqueue_bulk_returns_all_jobs() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("a", "q1", serde_json::json!(1)),
                    EnqueueOptions::new("b", "q2", serde_json::json!(2)),
                    EnqueueOptions::new("c", "q3", serde_json::json!(3)),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].job_type, "a");
        assert_eq!(jobs[1].job_type, "b");
        assert_eq!(jobs[2].job_type, "c");
    }

    #[tokio::test]
    async fn enqueue_bulk_ids_are_monotonic() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert!(jobs[0].id < jobs[1].id);
        assert!(jobs[1].id < jobs[2].id);
    }

    #[tokio::test]
    async fn enqueue_bulk_empty_is_noop() {
        let store = test_store();
        let jobs = store
            .enqueue_bulk(now_millis(), vec![])
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect::<Vec<_>>();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn enqueue_bulk_jobs_are_retrievable() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "q1", serde_json::json!("a")),
                    EnqueueOptions::new("test", "q2", serde_json::json!("b")),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        for job in &jobs {
            let fetched = store.get_job(now, &job.id).await.unwrap().unwrap();
            assert_eq!(fetched.id, job.id);
            assert_eq!(fetched.queue, job.queue);
        }
    }

    #[tokio::test]
    async fn enqueue_bulk_jobs_are_takeable() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                ],
            )
            .await
            .unwrap();

        let taken1 = store.take_next_job(now, &HashSet::new()).await.unwrap();
        let taken2 = store.take_next_job(now, &HashSet::new()).await.unwrap();
        let taken3 = store.take_next_job(now, &HashSet::new()).await.unwrap();

        assert!(taken1.is_some());
        assert!(taken2.is_some());
        assert!(taken3.is_none());
    }

    #[tokio::test]
    async fn enqueue_bulk_mixed_ready_and_scheduled() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .ready_at(future),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert_eq!(jobs[0].status, u8::from(JobStatus::Ready));
        assert_eq!(jobs[1].status, u8::from(JobStatus::Scheduled));
        assert_eq!(jobs[1].ready_at, future);

        // Only the ready job should be takeable.
        let taken = store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert!(taken.is_some());
        let none = store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn enqueue_bulk_respects_priority() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(100),
                    EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
                ],
            )
            .await
            .unwrap();

        // Higher priority (lower number) should be taken first.
        let taken = store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.payload, Some(serde_json::json!("high")));
    }

    #[tokio::test]
    async fn enqueue_bulk_broadcasts_events() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future = now + 60_000;

        store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "q1", serde_json::json!(null)),
                    EnqueueOptions::new("test", "q2", serde_json::json!(null)).ready_at(future),
                ],
            )
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "q1"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
        match rx.recv().await.unwrap() {
            StoreEvent::JobScheduled { .. } => {}
            other => panic!("expected JobScheduled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn enqueue_bulk_preserves_options() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .priority(42)
                        .retry_limit(5)
                        .backoff(BackoffConfig {
                            exponent: 3.0,
                            base_ms: 2000,
                            jitter_ms: 500,
                        })
                        .retention(RetentionConfig {
                            completed_ms: Some(3600_000),
                            dead_ms: Some(7200_000),
                        }),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert_eq!(jobs[0].priority, 42);
        assert_eq!(jobs[0].retry_limit, Some(5));
        let backoff = jobs[0].backoff.as_ref().unwrap();
        assert_eq!(backoff.exponent, 3.0);
        assert_eq!(backoff.base_ms, 2000);
        assert_eq!(backoff.jitter_ms, 500);
        let retention = jobs[0].retention.as_ref().unwrap();
        assert_eq!(retention.completed_ms, Some(3600_000));
        assert_eq!(retention.dead_ms, Some(7200_000));
    }

    // --- Unique jobs ---

    #[tokio::test]
    async fn unique_enqueue_returns_duplicate_on_conflict() {
        let store = test_store();
        let now = now_millis();

        let opts = || EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("key1");

        let r1 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r1.is_duplicate());

        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, r1.job().id);
    }

    #[tokio::test]
    async fn unique_different_keys_do_not_conflict() {
        let store = test_store();
        let now = now_millis();

        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("a"),
            )
            .await
            .unwrap();
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("b"),
            )
            .await
            .unwrap();

        assert!(!r1.is_duplicate());
        assert!(!r2.is_duplicate());
        assert_ne!(r1.job().id, r2.job().id);
    }

    #[tokio::test]
    async fn unique_queued_scope_allows_after_take() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Queued)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r1.is_duplicate());

        // Take the job (Ready -> InFlight) — should release the lock.
        let taken = store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, r1.job().id);

        // Now a duplicate should be allowed.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r2.is_duplicate());
        assert_ne!(r2.job().id, r1.job().id);
    }

    #[tokio::test]
    async fn unique_active_scope_blocks_while_in_flight() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Active)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r1.is_duplicate());

        // Take (Ready -> InFlight).
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Should still conflict — active scope includes InFlight.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, r1.job().id);
    }

    #[tokio::test]
    async fn unique_active_scope_allows_after_complete() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Active)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Should be allowed now — job is completed, not active.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_exists_scope_blocks_while_completed() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Exists)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Should still conflict — exists scope includes Completed.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_exists_scope_allows_after_purge() {
        let store = test_store_with_retention(0, 0);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Exists)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        // Zero retention — completion immediately deletes the job.
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Job is purged — should be allowed now.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_stale_index_for_reaped_job_does_not_block() {
        let store = test_store_with_retention(1, 1);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Exists),
            )
            .await
            .unwrap();

        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Purge the job directly.
        store.purge_job(&r1.job().id).await.unwrap();

        // Unique index entry is stale (points to reaped job) — should not block.
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Exists),
            )
            .await
            .unwrap();
        assert!(!r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_bulk_dedup_within_batch() {
        let store = test_store();
        let now = now_millis();

        let opts = || EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("same");

        let results = store
            .enqueue_bulk(now, vec![opts(), opts(), opts()])
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(!results[0].is_duplicate());
        assert!(results[1].is_duplicate());
        assert!(results[2].is_duplicate());
        assert_eq!(results[1].job().id, results[0].job().id);
        assert_eq!(results[2].job().id, results[0].job().id);
    }

    #[tokio::test]
    async fn unique_queued_scope_restores_on_failure_retry() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Queued)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        let job_id = r1.into_job().id;

        // Take, then fail with retry (Scheduled).
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store
            .record_failure(
                now,
                &job_id,
                FailureOptions {
                    message: "oops".into(),
                    error_type: None,
                    backtrace: None,
                    retry_at: Some(now + 60_000),
                    kill: false,
                },
            )
            .await
            .unwrap();

        // Job is back in Scheduled — unique lock should be restored.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, job_id);
    }

    #[tokio::test]
    async fn unique_concurrent_enqueues_produce_one_job() {
        let store = Arc::new(test_store());
        let now = now_millis();

        let mut handles = Vec::new();
        for _ in 0..20 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.enqueue(
                    now,
                    EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("race"),
                )
                .await
                .unwrap()
            }));
        }

        let results: Vec<EnqueueResult> = futures_util::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let created: Vec<&EnqueueResult> = results.iter().filter(|r| !r.is_duplicate()).collect();
        assert_eq!(created.len(), 1, "exactly one job should be created");

        // All duplicates should reference the same job.
        let expected_id = &created[0].job().id;
        for r in &results {
            assert_eq!(&r.job().id, expected_id);
        }
    }

    #[tokio::test]
    async fn unique_no_key_has_no_overhead() {
        let store = test_store();
        let now = now_millis();

        // Enqueue two identical jobs without unique_key — both should succeed.
        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        assert!(!r1.is_duplicate());
        assert!(!r2.is_duplicate());
        assert_ne!(r1.job().id, r2.job().id);
    }

    #[tokio::test]
    async fn unique_recover_in_flight_restores_queued_lock() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Queued),
            )
            .await
            .unwrap();

        // Take the job (removes unique lock for queued scope).
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Simulate crash recovery — in-flight jobs move back to ready.
        let recovered = store.recover_in_flight().await.unwrap();
        assert_eq!(recovered, 1);

        // Unique lock should be restored — duplicate blocked.
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Queued),
            )
            .await
            .unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, r1.job().id);
    }
}
