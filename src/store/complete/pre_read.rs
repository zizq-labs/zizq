// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Pre-tx phase of completion: read each candidate job from the data
//! keyspace, classify it (zero-retention → delete, non-zero → status
//! update + purge), and build a `PreparedComplete` for the apply phase.

use fjall::Slice;

use super::super::delete::{JobDeletion, prepare_job_deletion};
use super::super::keys::{make_job_key, make_purge_key, make_status_key};
use super::super::store::Keyspaces;
use super::super::types::{Job, JobStatus, StoreError, UniqueConstraint};

/// A job that has been pre-read and prepared for completion. Built by
/// `pre_read_completes` (no tx required), applied by
/// `apply_complete_batch` inside a write transaction.
///
/// Carries the original `pre_bytes` for CAS verification at apply time.
/// If the job's stored bytes changed between pre-read and tx, the apply
/// reports a CAS conflict and the caller must retry the pre-read.
pub(super) struct PreparedComplete {
    pub(super) id: String,
    pub(super) queue: String,
    pub(super) pre_bytes: Slice,
    pub(super) priority: u16,
    /// Dequeue timestamp from the in-flight record, used to remove the
    /// matching entry from the `InFlightIndex` after commit.
    pub(super) dequeued_at: u64,
    /// `None` for zero-retention completions (which delete the job).
    pub(super) updated_bytes: Option<Slice>,
    /// `Some` for zero-retention completions (delete-paths).
    pub(super) deletion: Option<JobDeletion>,
    /// Pre-computed index key updates for non-zero-retention paths.
    pub(super) index_keys: Option<CompletionRetentionKeys>,
    /// Unique constraint snapshot for cleaning up the unique index.
    pub(super) unique: Option<UniqueConstraint>,
}

/// Pre-computed index keys for a non-zero-retention completion.
pub(super) struct CompletionRetentionKeys {
    pub(super) old_status: Vec<u8>,
    pub(super) new_status: Vec<u8>,
    pub(super) purge: Vec<u8>,
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

        let dequeued_at = job.dequeued_at.unwrap_or(0);

        if retention_ms == 0 {
            let del = prepare_job_deletion(&job, JobStatus::InFlight, ks);
            prepared.push(PreparedComplete {
                id: id.clone(),
                queue: job.queue.clone(),
                pre_bytes,
                priority: job.priority,
                dequeued_at,
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
                dequeued_at,
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
