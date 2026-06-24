// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job completion operations on `Store`.
//!
//! Both the singular `mark_completed` and bulk `mark_completed_bulk` entry
//! points funnel into the server-side `CompleteBatcher`, which coalesces
//! concurrent acks across many connections into a single fjall transaction.
//! The actual disk work (CAS retry, ready-index update, in-flight count
//! decrement, `JobCompleted` broadcast) lives in `batcher.rs`; the
//! methods here are thin async wrappers that hand off through
//! `spawn_blocking`.

mod batcher;

pub(super) use batcher::CompleteBatcher;

use fjall::Slice;
use tokio::task;

use super::delete::{JobDeletion, apply_job_deletion, prepare_job_deletion};
use super::results::BulkCompleteResult;
use super::store::{
    Keyspaces, Store, make_job_key, make_purge_key, make_status_key, make_unique_key,
};
use super::types::{Job, JobStatus, StoreError, UniqueConstraint, UniqueWhile};

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

/// A job that has been pre-read and prepared for completion. Built by
/// `pre_read_completes` (no tx required), applied by
/// `apply_complete_batch` inside a write transaction.
///
/// Carries the original `pre_bytes` for CAS verification at apply time.
/// If the job's stored bytes changed between pre-read and tx, the apply
/// reports a CAS conflict and the caller must retry the pre-read.
pub(super) struct PreparedComplete {
    pub id: String,
    pub queue: String,
    pub pre_bytes: Slice,
    pub priority: u16,
    /// `None` for zero-retention completions (which delete the job).
    pub updated_bytes: Option<Slice>,
    /// `Some` for zero-retention completions (delete-paths).
    pub deletion: Option<JobDeletion>,
    /// Pre-computed index key updates for non-zero-retention paths.
    pub index_keys: Option<CompletionRetentionKeys>,
    /// Unique constraint snapshot for cleaning up the unique index.
    pub unique: Option<UniqueConstraint>,
}

/// Pre-computed index keys for a non-zero-retention completion.
pub(super) struct CompletionRetentionKeys {
    pub old_status: Vec<u8>,
    pub new_status: Vec<u8>,
    pub purge: Vec<u8>,
}

/// Pre-read the given job ids and build the list of `PreparedComplete`
/// values for those that are still in `InFlight` state. Jobs that are
/// missing or in a different status are pushed into `not_found`.
///
/// Does NOT open a transaction — reads go directly through the
/// keyspace, so the returned `pre_bytes` may be invalidated by
/// concurrent writers. `apply_complete_batch` verifies via CAS and
/// surfaces conflicts to the caller.
pub(super) fn pre_read_completes(
    ids: &[String],
    now: u64,
    ks: &Keyspaces,
    default_completed_retention_ms: u64,
) -> Result<(Vec<PreparedComplete>, Vec<String>), StoreError> {
    let mut prepared: Vec<PreparedComplete> = Vec::with_capacity(ids.len());
    let mut not_found: Vec<String> = Vec::new();

    for id in ids {
        let job_key = make_job_key(id);
        let pre_bytes = match ks.data.get(&job_key)? {
            Some(bytes) => bytes,
            None => {
                not_found.push(id.clone());
                continue;
            }
        };

        let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

        if job.status != JobStatus::InFlight as u8 {
            not_found.push(id.clone());
            continue;
        }

        let retention_ms = job
            .retention
            .as_ref()
            .and_then(|r| r.completed_ms)
            .unwrap_or(default_completed_retention_ms);

        if retention_ms == 0 {
            let del = prepare_job_deletion(&job, JobStatus::InFlight, ks);
            prepared.push(PreparedComplete {
                id: id.clone(),
                queue: job.queue.clone(),
                pre_bytes,
                priority: job.priority,
                updated_bytes: None,
                deletion: Some(del),
                index_keys: None,
                unique: None,
            });
        } else {
            let purge_at = now + retention_ms;
            let old_status_key = make_status_key(JobStatus::InFlight, id);
            let new_status_key = make_status_key(JobStatus::Completed, id);
            let purge_key = make_purge_key(purge_at, id);

            let unique = job.unique.clone();

            let queue = job.queue.clone();
            job.status = JobStatus::Completed.into();
            job.purge_at = Some(purge_at);
            job.completed_at = Some(now);

            let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

            prepared.push(PreparedComplete {
                id: id.clone(),
                queue,
                pre_bytes,
                priority: job.priority,
                updated_bytes: Some(updated_slice),
                deletion: None,
                index_keys: Some(CompletionRetentionKeys {
                    old_status: old_status_key,
                    new_status: new_status_key,
                    purge: purge_key,
                }),
                unique,
            });
        }
    }

    Ok((prepared, not_found))
}

/// Apply a batch of prepared completions to an open write transaction
/// with optimistic-concurrency CAS verification.
///
/// On any CAS mismatch, returns `Ok(false)` immediately without writing
/// further items. The caller must drop the tx and retry the
/// pre-read+apply sequence — the returned tx is left in a stale state
/// (some items may have been written before the conflict was detected).
///
/// On success, every item has been applied to the tx; caller commits.
/// Does NOT commit the transaction.
pub(super) fn apply_complete_batch(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    prepared: &[PreparedComplete],
) -> Result<bool, StoreError> {
    for p in prepared {
        let job_key = make_job_key(&p.id);

        if let Some(ref del) = p.deletion {
            // Zero-retention: delete via CAS.
            let prev = tx.take(&ks.data, &job_key)?;
            if prev.as_deref() != Some(&*p.pre_bytes) {
                return Ok(false);
            }
            apply_job_deletion(tx, del, ks);
        } else if let Some(ref updated) = p.updated_bytes {
            // Non-zero retention: update via CAS.
            let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated.clone()))?;
            if prev.as_deref() != Some(&*p.pre_bytes) {
                return Ok(false);
            }
            let keys = p.index_keys.as_ref().unwrap();
            tx.remove(&ks.index, &keys.old_status);
            tx.insert(&ks.index, &keys.new_status, b"");
            tx.insert(&ks.index, &keys.purge, b"");

            // Remove unique index for Queued or Active scope on
            // completion, but only if it still belongs to this job.
            if let Some(ref uc) = p.unique {
                let scope = uc.unique_while();
                if scope == UniqueWhile::Queued || scope == UniqueWhile::Active {
                    let job_id = p.id.as_bytes();
                    tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                        Some(v) if v.as_ref() == job_id => None,
                        other => other.cloned(),
                    })?;
                }
            }
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::options::EnqueueOptions;
    use super::super::store::{StoreEvent, error_keys};
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
