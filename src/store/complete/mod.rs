// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job completion operations on `Store`.
//!
//! Both the singular `mark_completed` and bulk `mark_completed_bulk` entry
//! points funnel into the server-side `CompleteBatcher`, which coalesces
//! concurrent acks across many connections into a single fjall transaction.
//! The disk work splits across the pre-read → apply pair in sibling
//! submodules; the methods here are thin async wrappers that hand off
//! through `spawn_blocking`.

mod apply;
mod batcher;
mod pre_read;

pub(super) use batcher::CompleteBatcher;

use tokio::task;

use super::results::BulkCompleteResult;
use super::store::Store;
use super::types::StoreError;

impl Store {
    /// Mark a job as successfully completed.
    ///
    /// Transitions the job to Completed status and sets `purge_at` so the
    /// reaper will hard-delete it after the retention period. Returns `true`
    /// if the job was found in the in-flight state, `false` if it was not.
    ///
    /// Subscribers are notified on success. Routes through the same
    /// auto-batcher as `mark_completed_bulk` — a singular ack is a
    /// Vec-of-1 trip through the channel.
    pub async fn mark_completed(&self, now: u64, id: &str) -> Result<bool, StoreError> {
        let result = self.mark_completed_bulk(now, &[id.to_string()]).await?;
        Ok(!result.completed.is_empty())
    }

    /// Mark multiple jobs as successfully completed in a single transaction.
    ///
    /// Jobs not found in the in-flight set are collected in `not_found` but
    /// do not prevent valid jobs from being committed. Returns a
    /// `BulkCompleteResult` with both lists.
    pub async fn mark_completed_bulk(
        &self,
        now: u64,
        ids: &[String],
    ) -> Result<BulkCompleteResult, StoreError> {
        if ids.is_empty() {
            return Ok(BulkCompleteResult {
                completed: Vec::new(),
                not_found: Vec::new(),
            });
        }

        // Route through the auto-batcher. Per-op dedup happens inside
        // the batcher; ready_index updates, in_flight_count decrement,
        // and JobCompleted event broadcasts are all performed once per
        // unique completed job in the batcher thread.
        let batcher = self.complete_batcher.clone();
        let ids: Vec<String> = ids.to_vec();

        let reply_rx = task::spawn_blocking(move || batcher.submit(ids, now)).await?;

        reply_rx
            .await
            .map_err(|_| StoreError::Internal("complete auto-batcher channel closed".into()))?
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::keys::error_keys;
    use super::super::options::EnqueueOptions;
    use super::super::store::StoreEvent;
    use super::super::test_support::{
        enqueue_and_take, test_failure_opts, test_store, test_store_with_retention,
    };
    use crate::time::now_millis;

    #[tokio::test]
    async fn mark_completed_removes_job() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(store.mark_completed(now_millis(), &job.id).await.unwrap());

        // Job should no longer be dequeue-able even if re-enqueue were attempted
        // via crash recovery — it's gone from the jobs keyspace entirely.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn mark_completed_returns_false_for_unknown_id() {
        let store = test_store();
        assert!(
            !store
                .mark_completed(now_millis(), "nonexistent")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn mark_completed_returns_false_on_double_ack() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(store.mark_completed(now_millis(), &job.id).await.unwrap());
        assert!(!store.mark_completed(now_millis(), &job.id).await.unwrap());
    }

    #[tokio::test]
    async fn mark_completed_broadcasts_job_completed() {
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

        let mut rx = store.subscribe();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCompleted { id } => assert_eq!(id, job.id),
            other => panic!("expected JobCompleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mark_completed_retains_error_records_for_reaper() {
        // Use non-zero retention so mark_completed defers to the reaper
        // instead of synchronously deleting.
        let store = test_store_with_retention(60_000, 60_000);
        let job = enqueue_and_take(&store).await;

        // Fail once (retry), then promote, take, and complete.
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();

        let scheduled = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store
            .mark_completed(now_millis(), &retaken.id)
            .await
            .unwrap();

        // Error records are now retained until the reaper purges the job.
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(!keys.is_empty());

        // purge_job should clean them up.
        store.purge_job(&job.id).await.unwrap();
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(keys.is_empty());
    }
}
