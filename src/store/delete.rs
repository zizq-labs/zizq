// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job deletion operations: single-job purge/delete, bulk delete, and
//! reaper-style batch purge.
//!
//! - `purge_job` / `purge_batch` are silent: used by the reaper and by
//!   `record_failure`'s sync-delete path. No events.
//! - `delete_job` / `delete_jobs` are user-initiated: emit `JobDeleted`
//!   events so workers can release capacity for in-flight jobs deleted
//!   out from under them.

use fjall::{Readable, Slice};
use tokio::task;

use super::budget::BudgetRef;
use super::dispatch::Placement;
use super::keys::{
    error_keys, make_batch_key, make_job_key, make_payload_key, make_purge_key, make_queue_key,
    make_status_key, make_type_key, make_unique_key,
};
use super::options::BulkDeleteOptions;
use super::scan::{JobStream, apply_filters, build_id_stream, filter_needs_payload};
use super::store::{Keyspaces, Store, StoreEvent};
use super::types::{Job, JobStatus, ScanDirection, StoreError};
use super::writer::{Applied, JobOutcome, JobReply, JobWrite, WriterContext};

impl Store {
    /// Hard-delete a job and all associated data.
    ///
    /// Called by the reaper for each expired job. Returns `true` if the
    /// job was found and deleted, `false` if already purged.
    pub async fn purge_job(&self, id: &str) -> Result<bool, StoreError> {
        // Prepared here, on a blocking thread, so the writer only checks
        // and writes. A job changed between this pre-read and the
        // writer's check comes back as a conflict: pre-read again, so the
        // deletion removes the keys of the state the job is really in.
        loop {
            let ks = self.ks.clone();
            let writer = self.writer.clone();
            let id = id.to_string();

            let reply_rx = task::spawn_blocking(move || -> Result<_, StoreError> {
                let (reply, rx) = tokio::sync::oneshot::channel();
                let Some(op) = PurgeOp::prepare(&ks, &id, reply)? else {
                    return Ok(None);
                };
                writer.submit_job(op.into());
                Ok(Some(rx))
            })
            .await??;

            let Some(reply_rx) = reply_rx else {
                return Ok(false); // Already purged.
            };
            let outcome = reply_rx
                .await
                .map_err(|_| StoreError::Internal("writer channel closed".into()))??;
            match outcome {
                JobOutcome::Done(()) => return Ok(true),
                JobOutcome::Conflict => continue,
            }
        }
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
    /// Scans matching jobs and deletes each one in a single transaction,
    /// in a writer pass of its own. Returns the count of deleted jobs.
    ///
    /// Emits `StoreEvent::JobDeleted` for each deleted job.
    pub async fn delete_jobs(&self, opts: BulkDeleteOptions) -> Result<usize, StoreError> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = BulkDeleteOp {
            opts,
            deleted: Vec::new(),
            reply,
        };
        let writer = self.writer.clone();
        task::spawn_blocking(move || writer.submit_job(op.into())).await?;

        let deleted = match rx
            .await
            .map_err(|_| StoreError::Internal("writer channel closed".into()))??
        {
            JobOutcome::Done(deleted) => deleted,
            JobOutcome::Conflict => {
                return Err(StoreError::Internal(
                    "bulk delete reported a conflict".into(),
                ));
            }
        };

        // Force a full compaction when we've written enough tombstones that
        // leveled compaction would otherwise leave them sitting in upper
        // levels on a quiet database.
        let threshold = self.config.auto_compact_threshold;
        if threshold > 0 && deleted as u64 >= threshold {
            self.compact_all().await?;
        }

        Ok(deleted)
    }

    /// Hard-delete a batch of jobs in a single transaction (no events).
    ///
    /// Used by the reaper. Returns the count of actually deleted jobs.
    /// Missing jobs are silently skipped, as are jobs that changed after
    /// they were read: a job still expired is reaped on the next scan.
    pub async fn purge_batch(&self, ids: &[String]) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let writer = self.writer.clone();
        let ids = ids.to_vec();

        let reply_rx = task::spawn_blocking(move || -> Result<_, StoreError> {
            let (reply, rx) = tokio::sync::oneshot::channel();
            let op = ReapOp::prepare(&ks, &ids, reply)?;
            if op.reaps.is_empty() {
                return Ok(None);
            }
            writer.submit_job(op.into());
            Ok(Some(rx))
        })
        .await??;

        let Some(reply_rx) = reply_rx else {
            return Ok(0);
        };
        match reply_rx
            .await
            .map_err(|_| StoreError::Internal("writer channel closed".into()))??
        {
            JobOutcome::Done(count) => Ok(count),
            JobOutcome::Conflict => Err(StoreError::Internal("reaping reported a conflict".into())),
        }
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
    payload_key: Vec<u8>,
    purge_key: Option<Vec<u8>>,
    error_keys: Vec<Vec<u8>>,
    unique_idx_key: Option<Vec<u8>>,
    batch_idx_key: Option<Vec<u8>>,
}

/// Collect all keys needed to delete a job. No tx required.
pub(super) fn prepare_job_deletion(job: &Job, status: JobStatus, ks: &Keyspaces) -> JobDeletion {
    let id = &job.id;
    JobDeletion {
        id: id.clone(),
        status_key: make_status_key(status, id),
        queue_key: make_queue_key(&job.queue, id),
        type_key: make_type_key(&job.job_type, id),
        payload_key: make_payload_key(job.payload_key()),
        purge_key: job.purge_at.map(|purge_at| make_purge_key(purge_at, id)),
        error_keys: error_keys(ks, id).collect(),
        unique_idx_key: job.unique.as_ref().map(|uc| make_unique_key(&uc.key)),
        batch_idx_key: job.batch.as_ref().map(|bc| make_batch_key(&bc.key)),
    }
}

/// Apply pre-computed deletion inside an open tx.
pub(super) fn apply_job_deletion(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    del: &JobDeletion,
    ks: &Keyspaces,
) {
    tx.remove(&ks.data, &make_job_key(&del.id));
    tx.remove_weak(&ks.data, &del.payload_key);
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

    // Same pattern for the batch index: only remove if it still points
    // at this job. A subsequent seal may have already replaced it.
    if let Some(ref batch_key) = del.batch_idx_key {
        let job_id = del.id.as_bytes();
        let _ = tx.fetch_update(&ks.index, batch_key, |v| match v {
            Some(v) if v.as_ref() == job_id => None,
            other => other.cloned(),
        });
    }

    for key in &del.error_keys {
        tx.remove_weak(&ks.data, key);
    }
}

/// A hard delete of one job, prepared by its caller.
pub(in crate::store) struct PurgeOp {
    job_key: Vec<u8>,
    pre_bytes: Slice,
    job: Job,
    status: JobStatus,
    del: JobDeletion,
    reply: JobReply<()>,
}

impl PurgeOp {
    /// Pre-read the job and gather every key it has. `None` if it is
    /// already gone.
    fn prepare(ks: &Keyspaces, id: &str, reply: JobReply<()>) -> Result<Option<Self>, StoreError> {
        let job_key = make_job_key(id);
        let Some(pre_bytes) = ks.data.get(&job_key)? else {
            return Ok(None);
        };

        let job: Job = rmp_serde::from_slice(&pre_bytes)?;
        let status = JobStatus::try_from(job.status).map_err(|v| {
            StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
        })?;
        let del = prepare_job_deletion(&job, status, ks);

        Ok(Some(Self {
            job_key,
            pre_bytes,
            job,
            status,
            del,
            reply,
        }))
    }
}

impl JobWrite for PurgeOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        if tx.get(&ks.data, &self.job_key)?.as_deref() != Some(&*self.pre_bytes) {
            return Ok(Applied::Conflict);
        }

        apply_job_deletion(tx, &self.del, ks);
        Ok(Applied::Wrote)
    }

    fn finish(self, cx: &WriterContext) {
        let job = &self.job;

        // Remove from in-memory indexes based on the job's status.
        match self.status {
            JobStatus::Ready => {
                cx.dispatch.remove(Placement::of(job));
            }
            JobStatus::Scheduled => {
                cx.scheduled_index.remove(job.ready_at, &job.id);
            }
            JobStatus::InFlight => {
                cx.in_flight_index
                    .remove(job.dequeued_at.unwrap_or(0), &job.id);
                // Only an in-flight job is holding capacity. A ready or
                // scheduled one was never dispatched, so it has no slot
                // to give back and crediting one would invent capacity
                // out of nothing.
                cx.dispatch.release_concurrency(&job.budgets);
            }
            _ => {}
        }

        // Only a job that was still unfinished is still counted. One
        // deleted after completing or dying stopped counting when it got
        // there, and untracking it again would take the budget below the
        // work it actually holds — which reads as capacity that is not
        // there.
        if !self.status.is_terminal() {
            cx.dispatch.untrack(&job.budgets);
        }

        let _ = self.reply.send(Ok(JobOutcome::Done(())));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

/// A delete of every job a filter matches, all or nothing.
///
/// Exclusive: it runs in a pass of its own, so the scan sees exactly the
/// state its writes go into, with nothing earlier in the transaction
/// hidden from it.
pub(in crate::store) struct BulkDeleteOp {
    opts: BulkDeleteOptions,
    /// Filled in by `apply`, for `finish`.
    deleted: Vec<Deleted>,
    reply: JobReply<usize>,
}

/// What `finish` needs of a deleted job, read during the scan so it never
/// reads the job again.
struct Deleted {
    id: String,
    queue: String,
    status: JobStatus,
    priority: u16,
    ready_at: u64,
    dequeued_at: u64,
    budgets: Vec<BudgetRef>,
}

impl JobWrite for BulkDeleteOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        self.deleted.clear();
        let snapshot = ks.db.read_tx();
        let filter = &self.opts.filter;
        let needs_payload = filter_needs_payload(filter);

        // Build the job stream: filtered by indexes, or full scan. Then
        // apply range and payload filters as post-hydration checks.
        let jobs = match build_id_stream(&snapshot, ks, filter, &None, ScanDirection::Asc) {
            Some((id_stream, source, _)) => apply_filters(
                JobStream::by_id(
                    id_stream,
                    &snapshot,
                    &ks.data,
                    None, // no expiry filtering for deletion
                    needs_payload,
                    source,
                ),
                filter,
            ),
            None => apply_filters(
                JobStream::full_scan(
                    &snapshot,
                    ks,
                    &None,
                    ScanDirection::Asc,
                    None,
                    needs_payload,
                ),
                filter,
            ),
        };

        for result in jobs {
            let job = result?;
            let status = JobStatus::try_from(job.status).map_err(|v| {
                StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
            })?;

            let del = prepare_job_deletion(&job, status, ks);
            apply_job_deletion(tx, &del, ks);

            self.deleted.push(Deleted {
                id: job.id,
                queue: job.queue,
                status,
                priority: job.priority,
                ready_at: job.ready_at,
                dequeued_at: job.dequeued_at.unwrap_or(0),
                budgets: job.budgets,
            });
        }

        Ok(if self.deleted.is_empty() {
            Applied::Nothing
        } else {
            Applied::Wrote
        })
    }

    fn exclusive(&self) -> bool {
        true
    }

    fn finish(self, cx: &WriterContext) {
        for d in &self.deleted {
            match d.status {
                JobStatus::Ready => {
                    cx.dispatch.remove(Placement {
                        queue: &d.queue,
                        priority: d.priority,
                        id: &d.id,
                        budgets: &d.budgets,
                    });
                }
                JobStatus::Scheduled => {
                    cx.scheduled_index.remove(d.ready_at, &d.id);
                }
                JobStatus::InFlight => {
                    cx.in_flight_index.remove(d.dequeued_at, &d.id);
                    // See `PurgeOp`: only in-flight holds capacity.
                    cx.dispatch.release_concurrency(&d.budgets);
                }
                _ => {}
            }

            // See `PurgeOp`: terminal jobs are already uncounted.
            if !d.status.is_terminal() {
                cx.dispatch.untrack(&d.budgets);
            }
        }

        for d in &self.deleted {
            let _ = cx
                .event_tx
                .send(StoreEvent::JobDeleted { id: d.id.clone() });
        }
        let _ = self.reply.send(Ok(JobOutcome::Done(self.deleted.len())));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

/// The reaper's hard delete of a batch of expired jobs, prepared by its
/// caller. Each job is checked on its own: one that changed since it was
/// read is left for a later scan, and the rest are still reaped.
pub(in crate::store) struct ReapOp {
    reaps: Vec<Reap>,
    /// Per job, once applied: whether it was deleted.
    deleted: Vec<bool>,
    reply: JobReply<usize>,
}

struct Reap {
    job_key: Vec<u8>,
    pre_bytes: Slice,
    del: JobDeletion,
    queue: String,
    priority: u16,
    budgets: Vec<BudgetRef>,
}

impl ReapOp {
    /// Pre-read every job still there and gather its keys.
    fn prepare(ks: &Keyspaces, ids: &[String], reply: JobReply<usize>) -> Result<Self, StoreError> {
        let mut reaps = Vec::with_capacity(ids.len());
        for id in ids {
            let job_key = make_job_key(id);
            let Some(pre_bytes) = ks.data.get(&job_key)? else {
                continue; // Already purged.
            };

            let job: Job = rmp_serde::from_slice(&pre_bytes)?;
            let status = JobStatus::try_from(job.status).map_err(|v| {
                StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
            })?;

            reaps.push(Reap {
                job_key,
                pre_bytes,
                del: prepare_job_deletion(&job, status, ks),
                queue: job.queue,
                priority: job.priority,
                budgets: job.budgets,
            });
        }
        Ok(Self {
            reaps,
            deleted: Vec::new(),
            reply,
        })
    }
}

impl JobWrite for ReapOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        self.deleted.clear();
        for reap in &self.reaps {
            let unchanged = tx.get(&ks.data, &reap.job_key)?.as_deref() == Some(&*reap.pre_bytes);
            if unchanged {
                apply_job_deletion(tx, &reap.del, ks);
            }
            self.deleted.push(unchanged);
        }

        Ok(if self.deleted.contains(&true) {
            Applied::Wrote
        } else {
            Applied::Nothing
        })
    }

    fn jobs(&self) -> usize {
        self.reaps.len()
    }

    fn finish(self, cx: &WriterContext) {
        let mut count = 0;
        for (reap, deleted) in self.reaps.iter().zip(&self.deleted) {
            if !deleted {
                continue;
            }
            count += 1;

            // Defensive: remove from dispatch in case of inconsistency.
            //
            // Deliberately no `untrack` to match it. The reaper only
            // reaches jobs past their retention, which means they went
            // terminal — and stopped counting against their budgets —
            // some time ago. Untracking defensively here would
            // double-decrement every reaped job, and unlike a stray
            // dispatch entry, an under-count is not self-correcting: it
            // reads as capacity the budget does not have.
            cx.dispatch.remove(Placement {
                queue: &reap.queue,
                priority: reap.priority,
                id: &reap.del.id,
                budgets: &reap.budgets,
            });
        }
        let _ = self.reply.send(Ok(JobOutcome::Done(count)));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::filter::PayloadFilter;

    use super::super::budget::{BudgetBinding, BudgetStrategy};
    use super::super::keys::{make_job_key, make_status_key};
    use super::super::options::{BulkDeleteOptions, EnqueueOptions, ListJobsOptions};
    use super::super::storage_config::StorageConfig;
    use super::super::store::{Store, StoreEvent};
    use super::super::test_support::{
        enqueue_and_take, run_job_ops, test_store, test_store_with_retention,
    };
    use super::super::types::JobStatus;
    use super::super::writer::JobOutcome;
    use crate::time::now_millis;

    /// The reaper skips a job that changed since it read it, leaving it
    /// for a later scan, and still reaps the rest of the batch.
    #[tokio::test]
    async fn the_reaper_skips_a_job_changed_since_it_was_read() {
        let store = test_store();
        let mut ids = Vec::new();
        for i in 0..2 {
            let job = store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("t", "q", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
            ids.push(job.id);
        }

        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = super::ReapOp::prepare(&store.ks, &ids, reply).unwrap();

        let ks = store.ks.clone();
        let key = make_job_key(&ids[1]);
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            tx.insert(&ks.data, key, b"changed");
            ks.commit(tx, ks.default_commit_mode).unwrap();
        })
        .await
        .unwrap();

        run_job_ops(&store, vec![op.into()]).await;
        assert_eq!(rx.await.unwrap().unwrap(), JobOutcome::Done(1));
        assert!(store.ks.data.get(make_job_key(&ids[0])).unwrap().is_none());
        assert!(store.ks.data.get(make_job_key(&ids[1])).unwrap().is_some());
    }

    /// A delete prepared while a job is ready, of a job then taken, is
    /// told to retry. Written regardless, it would remove the ready
    /// status key the job no longer has and leave its in-flight one
    /// behind, listing a job that doesn't exist.
    #[tokio::test]
    async fn a_delete_of_a_job_taken_since_its_pre_read_conflicts() {
        let store = test_store();
        let id = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job()
            .id;

        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = super::PurgeOp::prepare(&store.ks, &id, reply)
            .unwrap()
            .unwrap();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        run_job_ops(&store, vec![op.into()]).await;
        assert_eq!(rx.await.unwrap().unwrap(), JobOutcome::Conflict);

        assert!(store.delete_job(&id).await.unwrap());
        for status in [JobStatus::Ready, JobStatus::InFlight] {
            let key = make_status_key(status, &id);
            assert!(store.ks.index.get(key).unwrap().is_none());
        }
        assert_eq!(store.in_flight_count(), 0);
    }

    /// Enqueue a job drawing `cost` from a budget created for the test.
    async fn budgeted(store: &Store, cost: u32) -> String {
        let now = now_millis();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("a"))
                    .budget(BudgetBinding::new("stripe").cost(cost)),
            )
            .await
            .unwrap()
            .into_job()
            .id
    }

    /// Deleting unfinished work releases the claim it had on the budget.
    #[tokio::test]
    async fn deleting_a_queued_job_untracks_it() {
        let store = test_store();
        let id = budgeted(&store, 5).await;

        assert_eq!(store.budgets.tracked("stripe"), 1);

        assert!(store.delete_job(&id).await.unwrap());

        assert_eq!(store.budgets.tracked("stripe"), 0);
    }

    /// A job deleted after it finished was uncounted when it finished.
    /// Decrementing a second time would take the budget below the work
    /// it actually holds, which reads as capacity that is not there —
    /// and in a debug build trips the assertion in `CostCounts::remove`.
    ///
    /// Needs a retention window: under the default of zero, completion
    /// hard-deletes the job and there is nothing left to delete twice.
    #[tokio::test]
    async fn deleting_a_finished_job_does_not_untrack_twice() {
        let store = test_store_with_retention(60_000, 60_000);
        let id = budgeted(&store, 5).await;

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, id);
        store.mark_completed(now_millis(), &id).await.unwrap();
        assert_eq!(store.budgets.tracked("stripe"), 0);

        assert!(store.delete_job(&id).await.unwrap());

        assert_eq!(store.budgets.tracked("stripe"), 0);
    }

    /// Deleting a job out from under a worker is the one exit from
    /// in-flight the worker never gets to report, so nothing else will
    /// ever hand the slot back.
    #[tokio::test]
    async fn deleting_an_in_flight_job_returns_its_slot() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("solo", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();
        for payload in ["a", "b"] {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(payload))
                        .budget(BudgetBinding::new("solo")),
                )
                .await
                .unwrap();
        }

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        assert!(store.delete_job(&taken.id).await.unwrap());

        let next = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(next.id, taken.id);
    }

    /// A queued job never acquired anything, so deleting it must not
    /// credit a slot — that would invent capacity and let the budget
    /// run two jobs at once on an allocation of one.
    #[tokio::test]
    async fn deleting_a_queued_job_does_not_invent_a_slot() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("solo", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();
        for payload in ["a", "b", "c"] {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(payload))
                        .budget(BudgetBinding::new("solo")),
                )
                .await
                .unwrap();
        }

        let running = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Delete one of the two still waiting.
        let waiting = store
            .list_jobs(ListJobsOptions::new().statuses([JobStatus::Ready].into()))
            .await
            .unwrap()
            .jobs;
        assert_eq!(waiting.len(), 2);
        assert!(store.delete_job(&waiting[0].id).await.unwrap());

        // The one still running holds the only slot.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // And it still works normally once that one finishes.
        store
            .mark_completed(now_millis(), &running.id)
            .await
            .unwrap();
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_some()
        );
    }

    /// The bulk path counts down per job, on the same terminal-state
    /// rule as the single-job one.
    #[tokio::test]
    async fn bulk_delete_untracks_each_unfinished_job() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();

        for payload in ["a", "b", "c"] {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(payload))
                        .budget(BudgetBinding::new("stripe").cost(2)),
                )
                .await
                .unwrap();
        }
        assert_eq!(store.budgets.tracked("stripe"), 3);

        let deleted = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(deleted, 3);

        assert_eq!(store.budgets.tracked("stripe"), 0);
        assert_eq!(store.budgets.max_cost("stripe"), None);
    }

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
        let mut config = StorageConfig::default();
        config.auto_compact_threshold = 5;
        let store = Store::open_temp(config);

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
        let mut config = StorageConfig::default();
        config.auto_compact_threshold = 0;
        let store = Store::open_temp(config);

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
