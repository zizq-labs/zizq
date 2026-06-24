// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Scheduled-job operations: scanning the chronological scheduled index
//! for due jobs, promoting due jobs to Ready, and scanning the purge-at
//! index for expired jobs the reaper should hard-delete.
//!
//! These are the read/write entry points that the in-process scheduler
//! and reaper background tasks use to drain time-keyed work.

use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fjall::Slice;
use tokio::task;

use super::keys::{IndexKind, make_job_key, make_purge_key, make_status_key};
use super::store::{Store, StoreEvent};
use super::types::{Job, JobStatus, StoreError};

impl Store {
    /// Fetch a batch of due scheduled jobs.
    ///
    /// Scans the chronological scheduled index for jobs with
    /// `ready_at <= now` and loads their full job metadata from the `jobs`
    /// keyspace.
    ///
    /// Returns `(due_jobs, next_ready_at)` where `next_ready_at` is the
    /// `ready_at` of the first future job (if any). The scheduler uses
    /// this to know how long to sleep before the next scan.
    pub async fn next_scheduled(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<(Vec<Job>, Option<u64>), StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || -> Result<(Vec<Job>, Option<u64>), StoreError> {
            let (due_entries, next_ready_at) = scheduled_index.next_due(now, limit);

            let mut result = Vec::with_capacity(due_entries.len());
            for (ready_at, job_id) in &due_entries {
                let job_key = make_job_key(job_id);
                let job_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => {
                        // Job was deleted between next_due() and this read.
                        // Clean up the stale scheduled index entry.
                        tracing::trace!(
                            job_id,
                            "scheduled job missing from data keyspace, skipping"
                        );
                        scheduled_index.remove(*ready_at, job_id);
                        continue;
                    }
                };
                result.push(rmp_serde::from_slice(&job_bytes)?);
            }

            Ok((result, next_ready_at))
        })
        .await?
    }

    /// Promote a single scheduled job to the Ready state.
    ///
    /// Atomically removes the job from the scheduled index, swaps its
    /// status from Scheduled to Ready, and inserts it into the priority
    /// indexes so it becomes takeable.
    ///
    /// Subscribers are notified with `StoreEvent::JobCreated`.
    pub async fn promote_scheduled(&self, job: &Job) -> Result<(), StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();

        let id = job.id.clone();
        let event_id = job.id.clone();
        let event_queue = job.queue.clone();
        let queue = job.queue.clone();
        let priority = job.priority;
        let ready_at = job.ready_at;

        task::spawn_blocking(move || -> Result<_, StoreError> {
            // Remove from the in-memory scheduled index *before* commit to
            // prevent data races (same as take_next_job).
            scheduled_index.remove(ready_at, &id);

            // Retry loop: pre-read and pre-compute updates outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(()), // Already gone.
                };

                let current: Job = rmp_serde::from_slice(&pre_bytes)?;

                if current.status != JobStatus::Scheduled as u8 {
                    return Ok(()); // No longer scheduled.
                }

                let old_status_key = make_status_key(JobStatus::Scheduled, &id);
                let new_status_key = make_status_key(JobStatus::Ready, &id);

                let mut updated = current;
                updated.status = JobStatus::Ready.into();

                let updated_slice: Slice = rmp_serde::to_vec_named(&updated)?.into();

                // ---- inside tx (writes only, plus one compare) ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");

                let _sync = match ks.commit(tx, ks.default_commit_mode) {
                    Ok(sync) => sync,
                    Err(e) => {
                        // Commit failed — re-insert into the scheduled index
                        // if the job is still Scheduled.
                        let _tx = ks.write_tx();
                        if let Ok(Some(bytes)) = ks.data.get(&job_key) {
                            if let Ok(current) = rmp_serde::from_slice::<Job>(&bytes) {
                                if current.status == JobStatus::Scheduled as u8 {
                                    scheduled_index.insert(ready_at, id);
                                }
                            }
                        }
                        return Err(e);
                    }
                };

                // Insert into the in-memory ready index after commit succeeds.
                ready_index.insert(&queue, priority, id);

                return Ok(());
            }
        })
        .await??;

        let _ = self.event_tx.send(StoreEvent::JobCreated {
            id: event_id,
            queue: event_queue,
            token: Arc::new(AtomicBool::new(false)),
        });

        Ok(())
    }

    /// Fetch a batch of jobs whose purge_at has passed.
    ///
    /// Returns `(batch_of_job_ids, has_more)`. The reaper calls this
    /// repeatedly to drain expired jobs.
    pub async fn next_expired(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<(Vec<String>, bool), StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || -> Result<(Vec<String>, bool), StoreError> {
            // Scan from the start of the purge-at index up to `now` (inclusive).
            // Key layout: A\0{purge_at_be_u64}\0{job_id}
            let start_key: Vec<u8> = vec![IndexKind::PurgeAt as u8, 0];
            let end_key = make_purge_key(now + 1, "");
            let range = (Bound::Included(start_key), Bound::Excluded(end_key));

            let mut ids = Vec::new();
            for entry in ks.index.inner().range::<Vec<u8>, _>(range) {
                let (key, _) = entry.into_inner()?;
                // Extract job_id: skip 2 bytes (tag) + 8 bytes (purge_at) + 1 byte (null separator).
                let job_id = String::from_utf8(key[11..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;
                ids.push(job_id);
                if ids.len() > limit {
                    break;
                }
            }

            let has_more = ids.len() > limit;
            if has_more {
                ids.truncate(limit);
            }
            Ok((ids, has_more))
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::SystemTime;

    use super::super::options::EnqueueOptions;
    use super::super::store::StoreEvent;
    use super::super::test_support::test_store;
    use super::super::types::JobStatus;
    use crate::time::now_millis;

    #[tokio::test]
    async fn promote_scheduled_moves_due_job_to_ready() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Enqueue a job scheduled in the future so it enters Scheduled state.
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.status, u8::from(JobStatus::Scheduled));

        // Pretend time has passed: ask for jobs due at ready_at.
        let (batch, _) = store.next_scheduled(now + 60_000, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        store.promote_scheduled(&batch[0]).await.unwrap();

        // It should now be takeable.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn next_scheduled_skips_future_jobs() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future_ms = now + 3_600_000; // 1 hour from now

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future_ms),
            )
            .await
            .unwrap()
            .into_job();

        // Asking with current time returns no due jobs, but reports the
        // next ready_at so the caller knows when to wake up.
        let (batch, next) = store.next_scheduled(now, 10).await.unwrap();
        assert!(batch.is_empty());
        assert_eq!(next, Some(future_ms));

        // Still not takeable.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // But asking with future time returns it.
        let (batch, _) = store.next_scheduled(future_ms, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, job.id);
    }

    #[tokio::test]
    async fn next_scheduled_respects_limit() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future = now + 60_000;

        for i in 0..5u64 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "q", serde_json::json!(i)).ready_at(future + i),
                )
                .await
                .unwrap()
                .into_job();
        }

        // All 5 are due at future+10, ask for only 2.
        let (batch, next) = store.next_scheduled(future + 10, 2).await.unwrap();
        assert_eq!(batch.len(), 2);
        // next_ready_at is None because we hit the limit, not because there
        // are no more scheduled jobs.
        assert_eq!(next, None);

        // Ask for all of them.
        let (batch, _) = store.next_scheduled(future + 10, 10).await.unwrap();
        assert_eq!(batch.len(), 5);
    }

    #[tokio::test]
    async fn promote_scheduled_fires_job_created_event() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future = now + 60_000;

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        let mut rx = store.subscribe();
        let (batch, _) = store.next_scheduled(future, 10).await.unwrap();
        store.promote_scheduled(&batch[0]).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "q"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn next_scheduled_returns_earliest_first() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let base = now + 60_000;

        // Enqueue in reverse order to confirm sorting is by ready_at.
        let later = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("later")).ready_at(base + 5000),
            )
            .await
            .unwrap()
            .into_job();
        let earlier = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("earlier"))
                    .ready_at(base + 1000),
            )
            .await
            .unwrap()
            .into_job();

        let (batch, _) = store.next_scheduled(base + 5000, 10).await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].id, earlier.id);
        assert_eq!(batch[1].id, later.id);
    }

    #[tokio::test]
    async fn next_scheduled_returns_empty_when_no_scheduled_jobs() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Enqueue a ready job (no ready_at).
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let (batch, next) = store.next_scheduled(now, 10).await.unwrap();
        assert!(batch.is_empty());
        // No scheduled jobs at all, so no next_ready_at either.
        assert_eq!(next, None);
    }

    #[tokio::test]
    async fn promote_scheduled_is_idempotent() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future = now + 60_000;

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        let (batch, _) = store.next_scheduled(future, 10).await.unwrap();
        store.promote_scheduled(&batch[0]).await.unwrap();

        // Promoting again should be a no-op (job is already Ready).
        store.promote_scheduled(&job).await.unwrap();

        // Should still be takeable exactly once.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn next_scheduled_returns_next_ready_at_for_mixed_batch() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // One due job and one future job.
        let due = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("due")).ready_at(now + 100),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("future"))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        // Ask as of now + 200: only the first job is due.
        let (batch, next) = store.next_scheduled(now + 200, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, due.id);
        assert_eq!(next, Some(now + 60_000));
    }
}
