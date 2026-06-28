// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job deletion operations: single-job purge/delete, bulk delete, and
//! reaper-style batch purge.
//!
//! - `purge_job` / `purge_batch` are silent: used by the reaper and by
//!   `record_failure`'s sync-delete path. No events.
//! - `delete_job` / `delete_jobs` are user-initiated: emit `JobDeleted`
//!   events so workers can release capacity for in-flight jobs deleted
//!   out from under them.

use tokio::task;

use super::keys::{
    error_keys, make_job_key, make_payload_key, make_purge_key, make_queue_key, make_status_key,
    make_type_key, make_unique_key,
};
use super::options::BulkDeleteOptions;
use super::scan::{JobStream, apply_filters, build_id_stream, filter_needs_payload};
use super::store::{Keyspaces, Store, StoreEvent};
use super::types::{Job, JobStatus, ScanDirection, StoreError};

impl Store {
    /// Hard-delete a job and all associated data.
    ///
    /// Called by the reaper for each expired job. Returns `true` if the
    /// job was found and deleted, `false` if already purged.
    pub async fn purge_job(&self, id: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let in_flight_index = self.in_flight_index.clone();
        let id = id.to_string();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // ---- outside tx ----
            let job_key = make_job_key(&id);
            let job_bytes = match ks.data.get(&job_key)? {
                Some(bytes) => bytes,
                None => return Ok(false), // Already purged.
            };

            let job: Job = rmp_serde::from_slice(&job_bytes)?;
            let status = JobStatus::try_from(job.status).map_err(|v| {
                StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
            })?;

            let del = prepare_job_deletion(&job, status, &ks);

            // ---- inside tx (writes only) ----
            let mut tx = ks.write_tx();
            apply_job_deletion(&mut tx, &del, &ks);
            ks.commit(tx, ks.default_commit_mode)?;

            // Remove from in-memory indexes based on the job's status.
            match status {
                JobStatus::Ready => {
                    ready_index.remove(&job.queue, job.priority, &id);
                }
                JobStatus::Scheduled => {
                    scheduled_index.remove(job.ready_at, &id);
                }
                JobStatus::InFlight => {
                    in_flight_index.remove(job.dequeued_at.unwrap_or(0), &id);
                }
                _ => {}
            }

            Ok(true)
        })
        .await??;

        Ok(result)
    }

    /// Delete a job by ID via the API.
    ///
    /// Delegates to `purge_job` for the actual deletion, then emits a
    /// `JobDeleted` event so that workers can release capacity for
    /// in-flight jobs that were deleted externally.
    pub async fn delete_job(&self, id: &str) -> Result<bool, StoreError> {
        let deleted = self.purge_job(id).await?;
        if deleted {
            let _ = self
                .event_tx
                .send(StoreEvent::JobDeleted { id: id.to_string() });
        }
        Ok(deleted)
    }

    /// Bulk-delete jobs matching the given filters.
    ///
    /// Acquires the write lock, scans matching jobs under a consistent
    /// snapshot, deletes each one, and commits in a single transaction.
    /// Returns the count of deleted jobs.
    ///
    /// Emits `StoreEvent::JobDeleted` for each deleted job.
    pub async fn delete_jobs(&self, opts: BulkDeleteOptions) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let in_flight_index = self.in_flight_index.clone();
        let event_tx = self.event_tx.clone();

        let deleted_ids = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Acquire the write lock first, then open a snapshot.
            // The snapshot sees the latest committed state, and since we
            // hold the writer lock, nothing can commit while we scan + delete.
            let mut tx = ks.write_tx();
            let snapshot = ks.db.read_tx();

            let needs_payload = filter_needs_payload(&opts.filter);

            // Build the job stream: filtered by indexes, or full scan.
            // Then apply range and payload filters as post-hydration checks.
            let jobs =
                match build_id_stream(&snapshot, &ks, &opts.filter, &None, ScanDirection::Asc) {
                    Some((id_stream, source, _)) => apply_filters(
                        JobStream::by_id(
                            id_stream,
                            &snapshot,
                            &ks.data,
                            None, // no expiry filtering for deletion
                            needs_payload,
                            source,
                        ),
                        &opts.filter,
                    ),
                    None => apply_filters(
                        JobStream::full_scan(
                            &snapshot,
                            &ks,
                            &None,
                            ScanDirection::Asc,
                            None,
                            needs_payload,
                        ),
                        &opts.filter,
                    ),
                };

            // Track deleted jobs for post-commit cleanup.
            struct Deleted {
                id: String,
                queue: String,
                status: JobStatus,
                priority: u16,
                ready_at: u64,
                dequeued_at: u64,
            }

            let mut deleted: Vec<Deleted> = Vec::new();

            for result in jobs {
                let job = result?;
                let status = JobStatus::try_from(job.status).map_err(|v| {
                    StoreError::Corruption(format!(
                        "job {} has unrecognized status byte: {v}",
                        job.id
                    ))
                })?;

                let del = prepare_job_deletion(&job, status, &ks);
                apply_job_deletion(&mut tx, &del, &ks);

                deleted.push(Deleted {
                    id: job.id,
                    queue: job.queue,
                    status,
                    priority: job.priority,
                    ready_at: job.ready_at,
                    dequeued_at: job.dequeued_at.unwrap_or(0),
                });
            }

            if deleted.is_empty() {
                return Ok(Vec::new());
            }

            ks.commit(tx, ks.default_commit_mode)?;

            // Post-commit: clean up in-memory indexes.
            for d in &deleted {
                match d.status {
                    JobStatus::Ready => {
                        ready_index.remove(&d.queue, d.priority, &d.id);
                    }
                    JobStatus::Scheduled => {
                        scheduled_index.remove(d.ready_at, &d.id);
                    }
                    JobStatus::InFlight => {
                        in_flight_index.remove(d.dequeued_at, &d.id);
                    }
                    _ => {}
                }
            }

            let deleted_ids: Vec<String> = deleted.into_iter().map(|d| d.id).collect();
            Ok(deleted_ids)
        })
        .await??;

        for id in &deleted_ids {
            let _ = event_tx.send(StoreEvent::JobDeleted { id: id.clone() });
        }

        // Force a full compaction when we've written enough tombstones that
        // leveled compaction would otherwise leave them sitting in upper
        // levels on a quiet database.
        let threshold = self.config.auto_compact_threshold;
        if threshold > 0 && deleted_ids.len() as u64 >= threshold {
            self.compact_all().await?;
        }

        Ok(deleted_ids.len())
    }

    /// Hard-delete a batch of jobs in a single transaction (no events).
    ///
    /// Opens one write transaction, deletes all found jobs, and commits once.
    /// Returns the count of actually deleted jobs. Missing jobs are silently
    /// skipped.
    pub async fn purge_batch(&self, ids: &[String]) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let ids = ids.to_vec();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // ---- outside tx: prepare all deletions ----
            let mut deletions: Vec<(JobDeletion, String, u16)> = Vec::new();

            for id in &ids {
                let job_key = make_job_key(id);
                let job_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => continue, // Already purged.
                };

                let job: Job = rmp_serde::from_slice(&job_bytes)?;
                let status = JobStatus::try_from(job.status).map_err(|v| {
                    StoreError::Corruption(format!(
                        "job {} has unrecognized status byte: {v}",
                        job.id
                    ))
                })?;

                let priority = job.priority;
                let queue = job.queue.clone();
                deletions.push((prepare_job_deletion(&job, status, &ks), queue, priority));
            }

            // ---- inside tx (writes only) ----
            let count = deletions.len();
            let mut tx = ks.write_tx();
            for (del, _, _) in &deletions {
                apply_job_deletion(&mut tx, del, &ks);
            }
            ks.commit(tx, ks.default_commit_mode)?;

            // Defensive: remove from ready_index in case of inconsistency.
            for (del, queue, priority) in deletions {
                ready_index.remove(&queue, priority, &del.id);
            }

            Ok(count)
        })
        .await??;

        Ok(result)
    }
}

/// Pre-computed keys for deleting all data associated with a job.
///
/// Built by `prepare_job_deletion` (no tx required), applied by
/// `apply_job_deletion` (write-only inside an open tx).
pub(super) struct JobDeletion {
    id: String,
    status_key: Vec<u8>,
    queue_key: Vec<u8>,
    type_key: Vec<u8>,
    purge_key: Option<Vec<u8>>,
    error_keys: Vec<Vec<u8>>,
    unique_idx_key: Option<Vec<u8>>,
}

/// Collect all keys needed to delete a job. No tx required.
pub(super) fn prepare_job_deletion(job: &Job, status: JobStatus, ks: &Keyspaces) -> JobDeletion {
    let id = &job.id;
    JobDeletion {
        id: id.clone(),
        status_key: make_status_key(status, id),
        queue_key: make_queue_key(&job.queue, id),
        type_key: make_type_key(&job.job_type, id),
        purge_key: job.purge_at.map(|purge_at| make_purge_key(purge_at, id)),
        error_keys: error_keys(ks, id).collect(),
        unique_idx_key: job.unique.as_ref().map(|uc| make_unique_key(&uc.key)),
    }
}

/// Apply pre-computed deletion inside an open tx.
pub(super) fn apply_job_deletion(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    del: &JobDeletion,
    ks: &Keyspaces,
) {
    tx.remove(&ks.data, &make_job_key(&del.id));
    tx.remove_weak(&ks.data, &make_payload_key(&del.id));
    tx.remove(&ks.index, &del.status_key);
    tx.remove_weak(&ks.index, &del.queue_key);
    tx.remove_weak(&ks.index, &del.type_key);

    if let Some(ref purge_key) = del.purge_key {
        tx.remove(&ks.index, purge_key);
    }

    // Only remove the unique index entry if it still belongs to this job.
    // Another job may have already claimed the same key.
    if let Some(ref unique_key) = del.unique_idx_key {
        let job_id = del.id.as_bytes();
        let _ = tx.fetch_update(&ks.index, unique_key, |v| match v {
            Some(v) if v.as_ref() == job_id => None,
            other => other.cloned(),
        });
    }

    for key in &del.error_keys {
        tx.remove_weak(&ks.data, key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::filter::PayloadFilter;

    use super::super::options::{BulkDeleteOptions, EnqueueOptions, ListJobsOptions};
    use super::super::storage_config::StorageConfig;
    use super::super::store::{Store, StoreEvent};
    use super::super::test_support::{enqueue_and_take, test_store, test_store_with_retention};
    use super::super::types::JobStatus;
    use crate::time::now_millis;

    // --- delete_job ---

    #[tokio::test]
    async fn delete_job_removes_ready_job() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        assert!(store.delete_job(&job.id).await.unwrap());
        assert!(
            store
                .get_job(now_millis(), &job.id)
                .await
                .unwrap()
                .is_none()
        );

        // Should not be takeable.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_none());
    }

    #[tokio::test]
    async fn delete_job_removes_scheduled_job() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let future = now_millis() + 60_000;
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        assert!(store.delete_job(&job.id).await.unwrap());
        assert!(
            store
                .get_job(now_millis(), &job.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_job_removes_in_flight_job() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = enqueue_and_take(&store).await;

        assert!(store.delete_job(&job.id).await.unwrap());
        assert!(
            store
                .get_job(now_millis(), &job.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_job_returns_false_for_missing_job() {
        let store = test_store();
        assert!(!store.delete_job("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn delete_job_emits_job_deleted_event() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let mut rx = store.subscribe();
        store.delete_job(&job.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobDeleted { id } => assert_eq!(id, job.id),
            other => panic!("expected JobDeleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_job_does_not_emit_event_for_missing_job() {
        let store = test_store();
        let mut rx = store.subscribe();

        store.delete_job("nonexistent").await.unwrap();

        // No event should be emitted. Use try_recv to check.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn purge_job_does_not_emit_job_deleted_event() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let mut rx = store.subscribe();
        store.purge_job(&job.id).await.unwrap();

        // purge_job should NOT emit events.
        assert!(rx.try_recv().is_err());
    }

    // --- delete_jobs (bulk) ---

    #[tokio::test]
    async fn delete_jobs_by_status() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        // Create 3 jobs: take and complete one, leave two ready.
        for _ in 0..3 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }
        let taken = store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &taken.id).await.unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.filter.statuses = [JobStatus::Ready].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        // The completed job should still exist.
        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].status, JobStatus::Completed as u8);
    }

    #[tokio::test]
    async fn delete_jobs_by_queue() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "keep", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "delete_me", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "delete_me", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.filter.queues = ["delete_me".into()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].queue, "keep");
    }

    #[tokio::test]
    async fn delete_jobs_by_ids() {
        let store = test_store();
        let now = now_millis();

        let j1 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j3 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let mut opts = BulkDeleteOptions::new();
        opts.filter.ids = [j1.id.clone(), j3.id.clone()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j2.id);
    }

    #[tokio::test]
    async fn delete_jobs_by_payload_filter() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"x": 2})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"x": 3})),
            )
            .await
            .unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.filter.payload_filter = Some(Arc::new(PayloadFilter::compile(".x > 1").unwrap()));
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!({"x": 1})));
    }

    #[tokio::test]
    async fn delete_jobs_with_no_filters_deletes_all() {
        let store = test_store();
        let now = now_millis();

        for _ in 0..5 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }

        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 5);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn delete_jobs_combined_filters() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        // Two jobs in queue "a", one taken (in-flight), one ready.
        store
            .enqueue(now, EnqueueOptions::new("t", "a", serde_json::json!(null)))
            .await
            .unwrap();
        store
            .enqueue(now, EnqueueOptions::new("t", "a", serde_json::json!(null)))
            .await
            .unwrap();
        // One job in queue "b", ready.
        store
            .enqueue(now, EnqueueOptions::new("t", "b", serde_json::json!(null)))
            .await
            .unwrap();

        // Take one from "a".
        store
            .take_next_job(now, &["a".into()].into())
            .await
            .unwrap();

        // Delete ready jobs in queue "a" only.
        let mut opts = BulkDeleteOptions::new();
        opts.filter.queues = ["a".into()].into();
        opts.filter.statuses = [JobStatus::Ready].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 1);

        // In-flight "a" and ready "b" should remain.
        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
    }

    #[tokio::test]
    async fn delete_jobs_emits_events() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap();
        store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap();

        let mut rx = store.subscribe();
        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 2);

        // Should receive 2 JobDeleted events.
        for _ in 0..2 {
            match rx.recv().await.unwrap() {
                StoreEvent::JobDeleted { .. } => {}
                other => panic!("expected JobDeleted, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn delete_jobs_returns_zero_for_no_matches() {
        let store = test_store();
        let mut opts = BulkDeleteOptions::new();
        opts.filter.queues = ["nonexistent".into()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_jobs_skips_nonexistent_ids() {
        let store = test_store();
        let now = now_millis();

        let j1 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let mut opts = BulkDeleteOptions::new();
        opts.filter.ids = [j1.id, "0000000000000000000000000".into()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn delete_jobs_triggers_auto_compact_above_threshold() {
        // Threshold = 5 means a 5-job delete should pass the trigger path.
        // We can't easily observe major_compact ran from the public API,
        // so the assertion is that delete_jobs still succeeds and returns
        // the correct count when the auto-compact branch fires.
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.auto_compact_threshold = 5;
        let store = Store::open(dir.path().join("data"), config).unwrap();
        std::mem::forget(dir);

        let now = now_millis();
        for _ in 0..5 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }
        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn delete_jobs_skips_auto_compact_when_disabled() {
        // threshold = 0 disables auto-compact entirely.
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.auto_compact_threshold = 0;
        let store = Store::open(dir.path().join("data"), config).unwrap();
        std::mem::forget(dir);

        let now = now_millis();
        for _ in 0..3 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }
        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn delete_jobs_by_priority_range() {
        let store = test_store();
        let now = now_millis();

        for p in [0u16, 5, 50, 200] {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("t", "q", serde_json::json!(null)).priority(p),
                )
                .await
                .unwrap();
        }

        let mut opts = BulkDeleteOptions::new();
        opts.filter.priority = Some(5..=100);
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        let remaining: Vec<u16> = page.jobs.iter().map(|j| j.priority).collect();
        assert_eq!(remaining, vec![0, 200]);
    }

    #[tokio::test]
    async fn delete_jobs_by_ready_at_range_intersected_with_queue() {
        let store = test_store();
        let now = now_millis();

        // Queue "a" — two scheduled, one in range.
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "a", serde_json::json!(null)).ready_at(now + 10_000),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "a", serde_json::json!(null)).ready_at(now + 60_000),
            )
            .await
            .unwrap();
        // Queue "b" — one scheduled in range, must not be deleted.
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "b", serde_json::json!(null)).ready_at(now + 10_000),
            )
            .await
            .unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.filter.queues = ["a".into()].into();
        opts.filter.ready_at = Some((now + 5_000)..=(now + 20_000));
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 1);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
    }
}
