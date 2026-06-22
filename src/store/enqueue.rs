// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Enqueue operations: single and bulk job insertion.
//!
//! Both entry points funnel into the server-side `EnqueueBatcher` which
//! coalesces concurrent enqueues across many connections into a single
//! fjall transaction. The disk work (unique-conflict check, key writes,
//! in-memory index update, event broadcast) lives in the prepare → apply
//! → finalize trio below; the methods here are thin async wrappers that
//! hand off through `spawn_blocking`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::broadcast;
use tokio::task;

use super::options::EnqueueOptions;
use super::ready_index::ReadyIndex;
use super::results::EnqueueResult;
use super::scheduled_index::ScheduledIndex;
use super::store::{
    Keyspaces, Store, StoreEvent, make_job_key, make_payload_key, make_queue_key, make_status_key,
    make_type_key, make_unique_key,
};
use super::types::{Job, JobStatus, StoreError, UniqueConstraint, UniqueWhile};

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

/// Pre-computed data for inserting a job into the store.
///
/// Built by `prepare_enqueue` from `EnqueueOptions`, consumed by
/// `apply_enqueue` inside a write transaction. This separates the pure
/// computation (serialization, key building) from the transactional writes,
/// allowing callers to compose enqueue with other operations in the same tx.
pub(super) struct PreparedEnqueue {
    job: Job,
    meta_bytes: Vec<u8>,
    payload_bytes: Vec<u8>,
    job_key: Vec<u8>,
    payload_key: Vec<u8>,
    queue_key: Vec<u8>,
    status_key: Vec<u8>,
    type_key: Vec<u8>,
    unique_idx_key: Option<Vec<u8>>,
}

/// Build a `PreparedEnqueue` from `EnqueueOptions` and the current time.
///
/// Pure computation — no IO, no transaction needed.
pub(super) fn prepare_enqueue(
    opts: EnqueueOptions,
    now: u64,
) -> Result<PreparedEnqueue, StoreError> {
    let unique_while_scope = match (opts.unique_key.as_ref(), opts.unique_while) {
        (Some(_), Some(scope)) => Some(scope),
        (Some(_), None) => Some(UniqueWhile::Queued),
        _ => None,
    };

    let id = scru128::new_string();
    let ready_at = opts.ready_at.unwrap_or(now);
    let scheduled = ready_at > now;
    let status = if scheduled {
        JobStatus::Scheduled
    } else {
        JobStatus::Ready
    };

    let payload_bytes = rmp_serde::to_vec_named(&opts.payload)?;

    let job = Job {
        id: id.clone(),
        job_type: opts.job_type,
        queue: opts.queue,
        priority: opts.priority,
        payload: Some(opts.payload),
        status: status.into(),
        ready_at,
        attempts: 0,
        retry_limit: opts.retry_limit,
        backoff: opts.backoff,
        dequeued_at: None,
        failed_at: None,
        retention: opts.retention,
        purge_at: None,
        completed_at: None,
        unique: opts.unique_key.map(|k| UniqueConstraint {
            key: k,
            scope: unique_while_scope.unwrap_or(UniqueWhile::Queued) as u8,
        }),
    };

    let mut meta = job.clone();
    meta.payload = None;
    let meta_bytes = rmp_serde::to_vec_named(&meta)?;

    Ok(PreparedEnqueue {
        job_key: make_job_key(&id),
        payload_key: make_payload_key(&id),
        queue_key: make_queue_key(&job.queue, &id),
        status_key: make_status_key(status, &id),
        type_key: make_type_key(&job.job_type, &id),
        unique_idx_key: job.unique.as_ref().map(|uc| make_unique_key(&uc.key)),
        job,
        meta_bytes,
        payload_bytes,
    })
}

/// Insert a prepared enqueue into an open write transaction.
///
/// Checks for unique constraint conflicts first. Returns `Duplicate` if a
/// conflict is found (nothing is written), `Created` otherwise.
///
/// Does NOT commit the transaction — the caller is responsible for
/// committing (possibly after adding other writes to the same tx).
pub(super) fn apply_enqueue(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    p: &PreparedEnqueue,
) -> Result<EnqueueResult, StoreError> {
    // Check for a unique conflict.
    if let Some(ref uc) = p.job.unique {
        let scope = uc.unique_while();
        let idx_key = p.unique_idx_key.as_ref().unwrap();

        if let Some(existing_id_bytes) = ks.index.get(idx_key)? {
            let existing_id = std::str::from_utf8(&existing_id_bytes).map_err(|e| {
                StoreError::Corruption(format!("unique index value is not valid UTF-8: {e}"))
            })?;

            if let Some(existing_meta_bytes) = ks.data.get(&make_job_key(existing_id))? {
                let existing_meta: Job = rmp_serde::from_slice(&existing_meta_bytes)?;
                if let Ok(existing_status) = JobStatus::try_from(existing_meta.status) {
                    if scope.conflicts_with(existing_status) {
                        return Ok(EnqueueResult::Duplicate(existing_meta));
                    }
                }
            }
        }
    }

    tx.insert(&ks.data, &p.job_key, &p.meta_bytes);
    tx.insert(&ks.data, &p.payload_key, &p.payload_bytes);
    tx.insert(&ks.index, &p.queue_key, b"");
    tx.insert(&ks.index, &p.status_key, b"");
    tx.insert(&ks.index, &p.type_key, b"");

    if let Some(ref idx_key) = p.unique_idx_key {
        tx.insert(&ks.index, idx_key, p.job.id.as_bytes());
    }

    Ok(EnqueueResult::Created(p.job.clone()))
}

/// Apply a batch of prepared enqueues to an open write transaction with
/// intra-batch unique-key dedup.
///
/// When two prepared jobs in the same batch share a `unique` key, the
/// second one returns `EnqueueResult::Duplicate(...)` referring to the
/// first without performing a tx insert. Cross-batch dedup against
/// already-committed jobs is handled by `apply_enqueue` itself.
///
/// Does NOT commit the transaction — the caller commits and runs
/// `finalize_enqueue` per result post-commit.
pub(super) fn apply_enqueue_batch(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    prepared: &[PreparedEnqueue],
) -> Result<Vec<EnqueueResult>, StoreError> {
    use std::collections::HashMap;

    // Maps unique_key -> index in `results` for intra-batch conflicts.
    let mut batch_unique_keys: HashMap<String, usize> = HashMap::new();
    let mut results: Vec<EnqueueResult> = Vec::with_capacity(prepared.len());

    for p in prepared {
        // Intra-batch unique conflict check (no DB read needed).
        if let Some(ref uc) = p.job.unique {
            if let Some(&existing_idx) = batch_unique_keys.get(&uc.key) {
                results.push(EnqueueResult::Duplicate(
                    results[existing_idx].job().clone(),
                ));
                continue;
            }
        }

        let result = apply_enqueue(tx, ks, p)?;

        // Track unique keys for intra-batch dedup.
        if let EnqueueResult::Created(ref job) = result {
            if let Some(ref uc) = job.unique {
                batch_unique_keys.insert(uc.key.clone(), results.len());
            }
        }

        results.push(result);
    }

    Ok(results)
}

/// Update in-memory indexes for a successfully enqueued job.
///
/// Call after the transaction has been committed. Updates the ready or
/// scheduled index depending on the job's status.
pub(super) fn finalize_enqueue(
    result: &EnqueueResult,
    ready_index: &ReadyIndex,
    scheduled_index: &ScheduledIndex,
    event_tx: &broadcast::Sender<StoreEvent>,
) {
    if let EnqueueResult::Created(job) = result {
        match JobStatus::try_from(job.status) {
            Ok(JobStatus::Ready) => {
                ready_index.insert(&job.queue, job.priority, job.id.clone());
                let _ = event_tx.send(StoreEvent::JobCreated {
                    id: job.id.clone(),
                    queue: job.queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            Ok(JobStatus::Scheduled) => {
                scheduled_index.insert(job.ready_at, job.id.clone());
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: job.id.clone(),
                    ready_at: job.ready_at,
                });
            }
            _ => {
                tracing::warn!(
                    job_id = %job.id,
                    status = job.status,
                    "enqueue produced unexpected status"
                );
            }
        }
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
