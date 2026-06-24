// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Requeue: return an in-flight job to the ready queue.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fjall::Slice;
use tokio::task;

use super::keys::{make_job_key, make_status_key};
use super::store::{Store, StoreEvent};
use super::types::{Job, JobStatus, StoreError};

impl Store {
    /// Requeue a job currently in the in-flight state back to ready.
    ///
    /// Returns `true` if the job was actually requeued, `false` if it was
    /// already acked (no longer in the in-flight state).
    ///
    /// Subscribers are notified.
    pub async fn requeue(&self, id: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let id = id.to_string();
        let event_id = id.clone();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Retry loop: pre-read and pre-compute updates outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok(None);
                }

                let old_status_key = make_status_key(JobStatus::InFlight, &id);
                let new_status_key = make_status_key(JobStatus::Ready, &id);

                let queue = job.queue.clone();
                let priority = job.priority;
                job.status = JobStatus::Ready.into();

                let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                // ---- inside tx (writes only, plus one compare) ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");

                ks.commit(tx, ks.default_commit_mode)?;

                // Insert into the in-memory ready index after commit succeeds.
                ready_index.insert(&queue, priority, id.clone());

                return Ok(Some(queue));
            }
        })
        .await??;

        if let Some(queue) = result {
            let _ = self
                .in_flight_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
            let _ = self.event_tx.send(StoreEvent::JobCreated {
                id: event_id,
                queue,
                token: Arc::new(AtomicBool::new(false)),
            });
            return Ok(true);
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::options::EnqueueOptions;
    use super::super::store::StoreEvent;
    use super::super::test_support::{test_failure_opts, test_store};
    use super::super::types::JobStatus;
    use crate::time::now_millis;

    #[tokio::test]
    async fn requeue_returns_job_to_queue() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, job.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn requeue_preserves_priority() {
        let store = test_store();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, high.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        // Should get the high-priority job again, not the low-priority one.
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, high.id);
        assert_eq!(retaken.priority, 1);
        assert_eq!(retaken.status, u8::from(JobStatus::InFlight));
    }

    #[tokio::test]
    async fn requeue_skips_already_acked_job() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(store.mark_completed(now_millis(), &taken.id).await.unwrap());

        assert!(!store.requeue(&taken.id).await.unwrap());

        // Queue should remain empty.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn requeue_broadcasts_job_created() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let mut rx = store.subscribe();
        store.requeue(&taken.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "default"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
    }

    // --- in_flight_count tests (cross-op: take, complete, fail, requeue) ---

    #[tokio::test]
    async fn in_flight_count_tracks_take_and_complete() {
        let store = test_store();
        let now = now_millis();

        assert_eq!(store.in_flight_count(), 0);

        // Enqueue two jobs and take both.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        store.take_next_job(now, &HashSet::new()).await.unwrap();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.in_flight_count(), 2);

        // Completing a job should decrement.
        store.mark_completed(now, &job.id).await.unwrap();
        assert_eq!(store.in_flight_count(), 1);
    }

    #[tokio::test]
    async fn in_flight_count_tracks_failure_and_requeue() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.in_flight_count(), 1);

        // Failing a job should decrement.
        store
            .record_failure(now, &job.id, test_failure_opts())
            .await
            .unwrap();
        assert_eq!(store.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn in_flight_count_tracks_requeue() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.in_flight_count(), 1);

        // Requeuing should decrement.
        store.requeue(&job.id).await.unwrap();
        assert_eq!(store.in_flight_count(), 0);
    }
}
