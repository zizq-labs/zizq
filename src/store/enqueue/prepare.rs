// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Pure-computation phase of enqueue: assemble a `PreparedEnqueue` from
//! `EnqueueOptions` without touching the database. Used both by the
//! enqueue auto-batcher and by `cron::promote_cron_entry`.

use super::super::keys::{
    make_job_key, make_payload_key, make_queue_key, make_status_key, make_type_key, make_unique_key,
};
use super::super::options::EnqueueOptions;
use super::super::types::{Job, JobStatus, StoreError, UniqueConstraint, UniqueWhile};

/// Pre-computed data for inserting a job into the store.
///
/// Built by `prepare_enqueue` from `EnqueueOptions`, consumed by
/// `apply_enqueue` inside a write transaction. This separates the pure
/// computation (serialization, key building) from the transactional writes,
/// allowing callers to compose enqueue with other operations in the same tx.
pub(in crate::store) struct PreparedEnqueue {
    pub(super) job: Job,
    pub(super) meta_bytes: Vec<u8>,
    pub(super) payload_bytes: Vec<u8>,
    pub(super) job_key: Vec<u8>,
    pub(super) payload_key: Vec<u8>,
    pub(super) queue_key: Vec<u8>,
    pub(super) status_key: Vec<u8>,
    pub(super) type_key: Vec<u8>,
    pub(super) unique_idx_key: Option<Vec<u8>>,
}

/// Build a `PreparedEnqueue` from `EnqueueOptions` and the current time.
///
/// Pure computation — no IO, no transaction needed.
pub(in crate::store) fn prepare_enqueue(
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
