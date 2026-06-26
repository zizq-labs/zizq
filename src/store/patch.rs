// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Single-job and bulk-job patch operations.
//!
//! Both flows share a `PatchDiff` that captures the before/after values
//! of mutated fields. The diff drives the three side-effects each patch
//! needs: on-disk index updates inside the open transaction, in-memory
//! index updates after commit, and event emission outside spawn_blocking.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fjall::Slice;
use tokio::sync::broadcast;
use tokio::task;

use super::keys::{make_job_key, make_queue_key, make_status_key};
use super::options::{BulkPatchOptions, PatchJobOptions};
use super::ready_index::ReadyIndex;
use super::scan::{JobStream, apply_filters, build_id_stream, filter_needs_payload};
use super::scheduled_index::ScheduledIndex;
use super::store::{Keyspaces, Store, StoreEvent};
use super::types::{Job, JobStatus, ScanDirection, StoreError};

impl PatchJobOptions {
    /// Apply all patches to a mutable job, including status transitions
    /// arising from `ready_at` changes relative to `now`.
    ///
    /// Returns a `PatchDiff` capturing the before/after values needed for
    /// index updates, keyspace maintenance, and event emission.
    pub(crate) fn apply(&self, job: &mut Job, now: u64) -> PatchDiff {
        let old_status = JobStatus::try_from(job.status).unwrap();
        let old_queue = job.queue.clone();
        let old_priority = job.priority;
        let old_ready_at = job.ready_at;

        if let Some(ref queue) = self.queue {
            job.queue = queue.clone();
        }
        if let Some(priority) = self.priority {
            job.priority = priority;
        }
        if let Some(ref retry_limit) = self.retry_limit {
            job.retry_limit = *retry_limit;
        }
        if let Some(ref backoff) = self.backoff {
            job.backoff = backoff.clone();
        }
        match &self.retention {
            Some(None) => job.retention = None,
            Some(Some(patch)) => {
                job.retention = patch.clone().apply(job.retention.take());
            }
            None => {}
        }

        // Apply ready_at and any resulting status transition.
        match self.ready_at {
            Some(None) => {
                // "Make it ready now" — only meaningful for Scheduled jobs.
                // For Ready/InFlight, leave ready_at unchanged.
                if old_status == JobStatus::Scheduled {
                    job.ready_at = now;
                    job.status = JobStatus::Ready as u8;
                }
            }
            Some(Some(ts)) => {
                job.ready_at = ts;
                if ts > now && old_status == JobStatus::Ready {
                    job.status = JobStatus::Scheduled as u8;
                } else if ts <= now && old_status == JobStatus::Scheduled {
                    job.status = JobStatus::Ready as u8;
                }
            }
            None => {}
        }

        PatchDiff {
            id: job.id.clone(),
            old_status,
            new_status: JobStatus::try_from(job.status).unwrap(),
            old_queue,
            new_queue: job.queue.clone(),
            old_priority,
            new_priority: job.priority,
            old_ready_at,
            new_ready_at: job.ready_at,
        }
    }
}

/// Lightweight diff capturing the before/after values of a patched job.
///
/// Used by both single-job and bulk patch to drive keyspace updates,
/// in-memory index updates, and event emission without retaining full
/// `Job` copies.
pub(crate) struct PatchDiff {
    id: String,
    old_status: JobStatus,
    new_status: JobStatus,
    old_queue: String,
    new_queue: String,
    old_priority: u16,
    new_priority: u16,
    old_ready_at: u64,
    new_ready_at: u64,
}

impl PatchDiff {
    /// Update on-disk index keyspaces for status and queue changes.
    ///
    /// Must be called before `commit()`.
    fn update_keyspaces(&self, tx: &mut fjall::SingleWriterWriteTx<'_>, ks: &Keyspaces) {
        if self.old_status != self.new_status {
            tx.remove(&ks.index, &make_status_key(self.old_status, &self.id));
            tx.insert(&ks.index, &make_status_key(self.new_status, &self.id), b"");
        }
        if self.old_queue != self.new_queue {
            tx.remove(&ks.index, &make_queue_key(&self.old_queue, &self.id));
            tx.insert(&ks.index, &make_queue_key(&self.new_queue, &self.id), b"");
        }
    }

    /// Update in-memory ready and scheduled indexes.
    ///
    /// Must be called after a successful `commit()`.
    fn update_indexes(&self, ready_index: &ReadyIndex, scheduled_index: &ScheduledIndex) {
        match (self.old_status, self.new_status) {
            (JobStatus::Ready, JobStatus::Ready) => {
                if self.old_queue != self.new_queue || self.old_priority != self.new_priority {
                    ready_index.remove(&self.old_queue, self.old_priority, &self.id);
                    ready_index.insert(&self.new_queue, self.new_priority, self.id.clone());
                }
            }
            (JobStatus::Ready, JobStatus::Scheduled) => {
                ready_index.remove(&self.old_queue, self.old_priority, &self.id);
                scheduled_index.insert(self.new_ready_at, self.id.clone());
            }
            (JobStatus::Scheduled, JobStatus::Ready) => {
                scheduled_index.remove(self.old_ready_at, &self.id);
                ready_index.insert(&self.new_queue, self.new_priority, self.id.clone());
            }
            (JobStatus::Scheduled, JobStatus::Scheduled) => {
                if self.old_ready_at != self.new_ready_at {
                    scheduled_index.remove(self.old_ready_at, &self.id);
                    scheduled_index.insert(self.new_ready_at, self.id.clone());
                }
            }
            _ => {} // InFlight — no index changes.
        }
    }

    /// Emit store events for status transitions and the patch itself.
    ///
    /// Must be called after sync.
    fn emit_events(&self, event_tx: &broadcast::Sender<StoreEvent>) {
        match (self.old_status, self.new_status) {
            (JobStatus::Ready, JobStatus::Scheduled) => {
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: self.id.clone(),
                    ready_at: self.new_ready_at,
                });
            }
            (JobStatus::Scheduled, JobStatus::Ready) => {
                let _ = event_tx.send(StoreEvent::JobCreated {
                    id: self.id.clone(),
                    queue: self.new_queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            (JobStatus::Scheduled, JobStatus::Scheduled)
                if self.old_ready_at != self.new_ready_at =>
            {
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: self.id.clone(),
                    ready_at: self.new_ready_at,
                });
            }
            _ => {}
        }

        let _ = event_tx.send(StoreEvent::JobPatched {
            id: self.id.clone(),
        });
    }
}

impl Store {
    /// Patch a single job's mutable fields.
    ///
    /// Returns `Ok(Some(job))` with the updated job on success,
    /// `Ok(None)` if the job does not exist, or `Err` on failure.
    ///
    /// Returns `Err(StoreError::InvalidOperation)` if the job is in a
    /// terminal state (Completed or Dead).
    pub async fn patch_job(
        &self,
        now: u64,
        id: &str,
        patch: PatchJobOptions,
    ) -> Result<Option<Job>, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();
        let id = id.to_string();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            let job_key = make_job_key(&id);

            let mut tx = ks.write_tx();

            // Read, patch, and write in a single fetch_update call.
            // Errors are captured in patch_err since the closure can only
            // return Option<Slice>.
            let mut patched_job: Option<Job> = None;
            let mut diff: Option<PatchDiff> = None;
            let mut patch_err: Option<StoreError> = None;

            let prev = tx.fetch_update(&ks.data, &job_key, |existing| {
                let bytes = existing?;

                let mut job: Job = match rmp_serde::from_slice(bytes) {
                    Ok(j) => j,
                    Err(e) => {
                        patch_err = Some(e.into());
                        return Some(bytes.clone());
                    }
                };

                let status = match JobStatus::try_from(job.status) {
                    Ok(s) => s,
                    Err(v) => {
                        patch_err = Some(StoreError::Corruption(format!(
                            "job {} has unrecognized status byte: {v}",
                            job.id
                        )));
                        return Some(bytes.clone());
                    }
                };

                // Reject patches on terminal jobs.
                if matches!(status, JobStatus::Completed | JobStatus::Dead) {
                    patch_err = Some(StoreError::InvalidOperation(format!(
                        "cannot patch job {} in {} status",
                        job.id,
                        match status {
                            JobStatus::Completed => "completed",
                            JobStatus::Dead => "dead",
                            _ => unreachable!(),
                        }
                    )));
                    return Some(bytes.clone());
                }

                diff = Some(patch.apply(&mut job, now));

                match rmp_serde::to_vec_named(&job) {
                    Ok(updated) => {
                        patched_job = Some(job);
                        Some(updated.into())
                    }
                    Err(e) => {
                        patch_err = Some(e.into());
                        Some(bytes.clone())
                    }
                }
            })?;

            // Propagate any error from inside the closure.
            if let Some(e) = patch_err {
                return Err(e);
            }

            // If prev was None, the job doesn't exist.
            if prev.is_none() {
                return Ok(None);
            }

            let job = match patched_job {
                Some(j) => j,
                None => return Ok(None),
            };
            let diff = diff.unwrap();

            diff.update_keyspaces(&mut tx, &ks);

            ks.commit(tx, ks.default_commit_mode)?;

            diff.update_indexes(&ready_index, &scheduled_index);
            diff.emit_events(&event_tx);

            Ok(Some(job))
        })
        .await??;

        Ok(result)
    }

    /// Patch all jobs matching the given filters in a single transaction.
    ///
    /// Scans for matching jobs, applies the patch to each, commits once,
    /// then updates in-memory indexes and emits events. Terminal jobs
    /// (Completed/Dead) are silently skipped.
    ///
    /// Returns the count of actually patched jobs.
    pub async fn patch_jobs(&self, opts: BulkPatchOptions) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();

        let diffs = task::spawn_blocking(move || -> Result<_, StoreError> {
            let mut tx = ks.write_tx();
            let snapshot = ks.db.read_tx();

            let needs_payload = filter_needs_payload(&opts.filter);
            let now = crate::time::now_millis();

            let jobs =
                match build_id_stream(&snapshot, &ks, &opts.filter, &None, ScanDirection::Asc) {
                    Some((id_stream, source, _)) => apply_filters(
                        JobStream::by_id(
                            id_stream,
                            &snapshot,
                            &ks.data,
                            None,
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

            let mut diffs: Vec<PatchDiff> = Vec::new();

            for result in jobs {
                let mut job = result?;
                let status = match JobStatus::try_from(job.status) {
                    Ok(s) => s,
                    Err(_) => continue, // Skip corrupt jobs.
                };

                // Skip terminal jobs.
                if matches!(status, JobStatus::Completed | JobStatus::Dead) {
                    continue;
                }

                let diff = opts.patch.apply(&mut job, now);
                diff.update_keyspaces(&mut tx, &ks);

                let updated: Slice = rmp_serde::to_vec_named(&job)?.into();
                let job_key = make_job_key(&job.id);
                tx.fetch_update(&ks.data, &job_key, |_| Some(updated.clone()))?;

                diffs.push(diff);
            }

            if diffs.is_empty() {
                return Ok(Vec::new());
            }

            ks.commit(tx, ks.default_commit_mode)?;

            for diff in &diffs {
                diff.update_indexes(&ready_index, &scheduled_index);
            }

            Ok(diffs)
        })
        .await??;

        for diff in &diffs {
            diff.emit_events(&event_tx);
        }

        // See the equivalent block in `delete_jobs` — bulk patches that
        // change indexed fields write tombstones to the index keyspace,
        // and we want to reclaim those once the batch is large enough.
        let threshold = self.config.auto_compact_threshold;
        if threshold > 0 && diffs.len() as u64 >= threshold {
            self.compact_all().await?;
        }

        Ok(diffs.len())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::options::{
        BulkPatchOptions, EnqueueOptions, JobFilter, ListJobsOptions, PatchJobOptions,
        RetentionConfigPatch,
    };
    use super::super::store::StoreEvent;
    use super::super::test_support::{test_store, test_store_with_retention};
    use super::super::types::{BackoffConfig, JobStatus, RetentionConfig, StoreError};
    use crate::time::now_millis;

    // --- patch_job ---

    #[tokio::test]
    async fn patch_job_updates_retry_limit() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            retry_limit: Some(Some(10)),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.retry_limit, Some(10));
    }

    #[tokio::test]
    async fn patch_job_clears_retry_limit_with_null() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retry_limit(5),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.retry_limit, Some(5));

        let patch = PatchJobOptions {
            retry_limit: Some(None), // clear
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.retry_limit, None);
    }

    #[tokio::test]
    async fn patch_job_updates_backoff() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let new_backoff = BackoffConfig {
            exponent: 3.0,
            base_ms: 5000,
            jitter_ms: 1000,
        };

        let patch = PatchJobOptions {
            backoff: Some(Some(new_backoff.clone())),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let b = result.backoff.unwrap();
        assert_eq!(b.exponent, 3.0);
        assert_eq!(b.base_ms, 5000);
        assert_eq!(b.jitter_ms, 1000);
    }

    #[tokio::test]
    async fn patch_job_updates_retention() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: Some(Some(60_000)),
                dead_ms: Some(Some(120_000)),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, Some(60_000));
        assert_eq!(r.dead_ms, Some(120_000));
    }

    #[tokio::test]
    async fn patch_job_merges_retention_fields() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retention(RetentionConfig {
                    completed_ms: Some(60_000),
                    dead_ms: Some(120_000),
                }),
            )
            .await
            .unwrap()
            .into_job();

        // Patch only dead_ms — completed_ms should be preserved.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: None,
                dead_ms: Some(Some(300_000)),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, Some(60_000));
        assert_eq!(r.dead_ms, Some(300_000));
    }

    #[tokio::test]
    async fn patch_job_retention_merge_into_none() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Job has no retention — patching dead_ms should create retention with only dead_ms.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: None,
                dead_ms: Some(Some(300_000)),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, None);
        assert_eq!(r.dead_ms, Some(300_000));
    }

    #[tokio::test]
    async fn patch_job_clearing_both_retention_fields_clears_retention() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retention(RetentionConfig {
                    completed_ms: Some(60_000),
                    dead_ms: Some(120_000),
                }),
            )
            .await
            .unwrap()
            .into_job();

        // Clear both sub-fields — retention itself should become None.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: Some(None),
                dead_ms: Some(None),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert!(result.retention.is_none());
    }

    #[tokio::test]
    async fn patch_job_clearing_one_retention_field_preserves_other() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retention(RetentionConfig {
                    completed_ms: Some(60_000),
                    dead_ms: Some(120_000),
                }),
            )
            .await
            .unwrap()
            .into_job();

        // Clear completed_ms, leave dead_ms untouched.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: Some(None),
                dead_ms: None,
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, None);
        assert_eq!(r.dead_ms, Some(120_000));
    }

    #[tokio::test]
    async fn patch_job_leaves_unmentioned_fields_unchanged() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retry_limit(5),
            )
            .await
            .unwrap()
            .into_job();

        // Patch only backoff — retry_limit should stay.
        let patch = PatchJobOptions {
            backoff: Some(Some(BackoffConfig {
                exponent: 2.0,
                base_ms: 1000,
                jitter_ms: 500,
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.retry_limit, Some(5));
        assert!(result.backoff.is_some());
    }

    #[tokio::test]
    async fn patch_job_rejects_completed_job() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        store.mark_completed(now, &job.id).await.unwrap();

        let patch = PatchJobOptions {
            retry_limit: Some(Some(10)),
            ..Default::default()
        };

        let err = store.patch_job(now, &job.id, patch).await.unwrap_err();
        assert!(matches!(err, StoreError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn patch_job_returns_none_for_missing_job() {
        let store = test_store();
        let patch = PatchJobOptions::default();
        let result = store
            .patch_job(now_millis(), "nonexistent", patch)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn patch_job_updates_queue() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.queue, "q2");

        // Verify persisted.
        let fetched = store.get_job(now, &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.queue, "q2");
    }

    #[tokio::test]
    async fn patch_job_updates_priority() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            priority: Some(100),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.priority, 100);
    }

    #[tokio::test]
    async fn patch_job_queue_updates_ready_index() {
        let store = test_store();
        let now = now_millis();

        // Enqueue a job on q1.
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Move it to q2.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        // Taking from q1 should yield nothing.
        let queues: HashSet<String> = ["q1".into()].into();
        let taken = store.take_next_n_jobs(now, &queues, 1).await.unwrap();
        assert!(taken.is_empty());

        // Taking from q2 should yield the job.
        let queues: HashSet<String> = ["q2".into()].into();
        let taken = store.take_next_n_jobs(now, &queues, 1).await.unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].id, job.id);
    }

    #[tokio::test]
    async fn patch_job_priority_updates_ready_index() {
        let store = test_store();
        let now = now_millis();

        // Enqueue two jobs at different priorities.
        // job_a at priority 100, job_b at priority 200.
        let job_a = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).priority(100),
            )
            .await
            .unwrap()
            .into_job();
        let job_b = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).priority(200),
            )
            .await
            .unwrap()
            .into_job();

        // Before patch: job_a (100) should be taken first.
        // Patch job_b to priority 50, making it higher priority.
        let patch = PatchJobOptions {
            priority: Some(50),
            ..Default::default()
        };
        store.patch_job(now, &job_b.id, patch).await.unwrap();

        // Taking should yield job_b first (priority 50 < 100).
        let taken = store
            .take_next_n_jobs(now, &HashSet::<String>::new(), 2)
            .await
            .unwrap();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].id, job_b.id);
        assert_eq!(taken[1].id, job_a.id);
    }

    #[tokio::test]
    async fn patch_job_queue_on_scheduled_job() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        // Enqueue a scheduled job.
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q1", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        // Move it to q2 while still scheduled.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.queue, "q2");

        // Verify persisted — when it becomes ready it should be on q2.
        let fetched = store.get_job(now, &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.queue, "q2");
    }

    #[tokio::test]
    async fn patch_job_queue_on_inflight_job() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take the job to put it in-flight.
        store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();

        // Move it to q2 while in-flight.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.queue, "q2");
    }

    #[tokio::test]
    async fn patch_job_ready_at_future_moves_ready_to_scheduled() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.status, JobStatus::Ready as u8);

        // Set ready_at to the future → should become Scheduled.
        let patch = PatchJobOptions {
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, future);

        // Should no longer be takeable.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_none());
    }

    #[tokio::test]
    async fn patch_job_clear_ready_at_moves_scheduled_to_ready() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.status, JobStatus::Scheduled as u8);

        // Clear ready_at → should become Ready.
        let patch = PatchJobOptions {
            ready_at: Some(None),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Ready as u8);

        // Should now be takeable.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn patch_job_past_ready_at_moves_scheduled_to_ready() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        // Set ready_at to the past → should become Ready.
        let patch = PatchJobOptions {
            ready_at: Some(Some(now - 1000)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Ready as u8);
    }

    #[tokio::test]
    async fn patch_job_updates_scheduled_index_time() {
        let store = test_store();
        let now = now_millis();
        let future1 = now + 60_000;
        let future2 = now + 120_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future1),
            )
            .await
            .unwrap()
            .into_job();

        // Move scheduled time further out.
        let patch = PatchJobOptions {
            ready_at: Some(Some(future2)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, future2);

        // Verify the scheduled count is still 1 (old entry removed, new inserted).
        assert_eq!(store.scheduled_count(), 1);
    }

    #[tokio::test]
    async fn patch_job_ready_at_on_inflight_is_data_only() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take to put in-flight.
        store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();

        // Set ready_at while in-flight — should update data but not change status.
        let patch = PatchJobOptions {
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::InFlight as u8);
        assert_eq!(result.ready_at, future);
    }

    #[tokio::test]
    async fn patch_job_ready_at_and_queue_together() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Move to q2 and schedule for the future in one patch.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.queue, "q2");
        assert_eq!(result.ready_at, future);

        // Not takeable now.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_none());

        // Make it ready again via clearing ready_at.
        let patch = PatchJobOptions {
            ready_at: Some(None),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        // Should be on q2.
        let queues: HashSet<String> = ["q2".into()].into();
        let taken = store.take_next_job(now, &queues).await.unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn patch_job_emits_scheduled_when_ready_becomes_scheduled() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        let patch = PatchJobOptions {
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobScheduled { id, ready_at }) => {
                assert_eq!(id, job.id);
                assert_eq!(ready_at, future);
            }
            other => panic!("expected JobScheduled, got {:?}", other),
        }
        assert!(matches!(rx.try_recv(), Ok(StoreEvent::JobPatched { .. })));
    }

    #[tokio::test]
    async fn patch_job_emits_created_when_scheduled_becomes_ready() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        let patch = PatchJobOptions {
            ready_at: Some(None),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobCreated { id, queue, .. }) => {
                assert_eq!(id, job.id);
                assert_eq!(queue, "q");
            }
            other => panic!("expected JobCreated, got {:?}", other),
        }
        assert!(matches!(rx.try_recv(), Ok(StoreEvent::JobPatched { .. })));
    }

    #[tokio::test]
    async fn patch_job_emits_scheduled_when_scheduled_time_changes() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future1 = now + 60_000;
        let future2 = now + 120_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future1),
            )
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        let patch = PatchJobOptions {
            ready_at: Some(Some(future2)),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobScheduled { id, ready_at }) => {
                assert_eq!(id, job.id);
                assert_eq!(ready_at, future2);
            }
            other => panic!("expected JobScheduled, got {:?}", other),
        }
        assert!(matches!(rx.try_recv(), Ok(StoreEvent::JobPatched { .. })));
    }

    #[tokio::test]
    async fn patch_job_no_status_event_when_status_unchanged() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        // Patch only priority — no status change, only JobPatched.
        let patch = PatchJobOptions {
            priority: Some(100),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobPatched { id }) => assert_eq!(id, job.id),
            other => panic!("expected JobPatched, got {:?}", other),
        }
        assert!(rx.try_recv().is_err(), "expected no further events");
    }

    // --- bulk patch_jobs tests ---

    #[tokio::test]
    async fn patch_jobs_updates_matching_jobs() {
        let store = test_store();
        let now = now_millis();

        for i in 0..3 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("t", "q1", serde_json::json!({"i": i})),
                )
                .await
                .unwrap();
        }

        let opts = BulkPatchOptions {
            filter: JobFilter {
                queues: ["q1".into()].into(),
                ..Default::default()
            },
            patch: PatchJobOptions {
                queue: Some("q2".into()),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 3);

        // Verify all jobs are on q2 now.
        let page = store
            .list_jobs(ListJobsOptions::new().queues(["q2".into()].into()))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
    }

    #[tokio::test]
    async fn patch_jobs_skips_terminal_jobs() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take and complete the job.
        store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        store.mark_completed(now, &job.id).await.unwrap();

        let opts = BulkPatchOptions {
            filter: JobFilter {
                ids: [job.id.clone()].into(),
                ..Default::default()
            },
            patch: PatchJobOptions {
                priority: Some(99),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn patch_jobs_status_transitions() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        // Enqueue 2 ready jobs.
        for _ in 0..2 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }

        // Patch them all to scheduled.
        let opts = BulkPatchOptions {
            filter: JobFilter::default(),
            patch: PatchJobOptions {
                ready_at: Some(Some(future)),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        // Nothing should be takeable.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_none());

        assert_eq!(store.scheduled_count(), 2);
    }

    #[tokio::test]
    async fn patch_jobs_with_id_filter() {
        let store = test_store();
        let now = now_millis();

        let job_a = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let _job_b = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Only patch job_a.
        let opts = BulkPatchOptions {
            filter: JobFilter {
                ids: [job_a.id.clone()].into(),
                ..Default::default()
            },
            patch: PatchJobOptions {
                priority: Some(99),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 1);

        let fetched = store.get_job(now, &job_a.id).await.unwrap().unwrap();
        assert_eq!(fetched.priority, 99);
    }

    #[tokio::test]
    async fn patch_jobs_returns_zero_for_no_matches() {
        let store = test_store();

        let opts = BulkPatchOptions {
            filter: JobFilter {
                queues: ["nonexistent".into()].into(),
                ..Default::default()
            },
            patch: PatchJobOptions {
                priority: Some(99),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn patch_jobs_by_priority_range() {
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

        let opts = BulkPatchOptions {
            filter: JobFilter {
                priority: Some(5..=100),
                ..Default::default()
            },
            patch: PatchJobOptions {
                priority: Some(7),
                ..Default::default()
            },
        };
        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        let mut got: Vec<u16> = page.jobs.iter().map(|j| j.priority).collect();
        got.sort();
        assert_eq!(got, vec![0, 7, 7, 200]);
    }

    #[tokio::test]
    async fn patch_jobs_by_ready_at_range_intersected_with_queue() {
        let store = test_store();
        let now = now_millis();

        // Queue "a" — two scheduled jobs, one inside the window.
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
        // Queue "b" — one scheduled job in the window, must not be touched.
        let untouched = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "b", serde_json::json!(null)).ready_at(now + 10_000),
            )
            .await
            .unwrap()
            .into_job();

        let opts = BulkPatchOptions {
            filter: JobFilter {
                queues: ["a".into()].into(),
                ready_at: Some((now + 5_000)..=(now + 20_000)),
                ..Default::default()
            },
            patch: PatchJobOptions {
                priority: Some(42),
                ..Default::default()
            },
        };
        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 1);

        let untouched_after = store.get_job(now, &untouched.id).await.unwrap().unwrap();
        assert_eq!(untouched_after.priority, 0);
    }
}
