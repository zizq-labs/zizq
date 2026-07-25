// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Pure-computation phase of enqueue: assemble a `PreparedEnqueue` from
//! `EnqueueOptions` without touching the database. Used both by the
//! enqueue auto-batcher and by `cron::promote_cron_entry`.

use crate::batch::BatchExpr;

use super::super::keys::{
    make_batch_key, make_job_key, make_payload_key, make_queue_key, make_status_key, make_type_key,
    make_unique_key,
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
    /// `B\0<batch.key>` for batched enqueues (`Some`). When the fold path
    /// is not taken, this is inserted pointing at the newly created job.
    pub(super) batch_idx_key: Option<Vec<u8>>,
    /// Pre-computed scru128 for the merged payload key if this enqueue
    /// ends up folding into an existing batched job. Discarded (i.e.
    /// unused) if the enqueue creates a new job instead, so first-time
    /// batched jobs stay on-disk identical to normal jobs.
    pub(super) fold_payload_key_id: Option<String>,
}

/// Build a `PreparedEnqueue` from `EnqueueOptions` and the current time.
///
/// Pure computation — no IO, no transaction needed.
pub(in crate::store) fn prepare_enqueue(
    opts: EnqueueOptions,
    now: u64,
) -> Result<PreparedEnqueue, StoreError> {
    if opts.unique_key.is_some() && opts.batch.is_some() {
        return Err(StoreError::InvalidOperation(
            "unique_key and batch cannot be combined".into(),
        ));
    }

    if let Some(ref cfg) = opts.batch {
        BatchExpr::compile(&cfg.when, &cfg.fold)
            .map_err(|e| StoreError::InvalidOperation(format!("batch expression: {e}")))?;
    }

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

    // Pre-compute a fresh payload key id for the merged-payload target
    // if this enqueue ends up folding. Discarded when the enqueue
    // instead creates a new job, so the created job's on-disk shape
    // matches a non-batched job (payload at `P\0<job_id>`, no
    // `payload_key` field on the metadata).
    let fold_payload_key_id = if opts.batch.is_some() {
        Some(scru128::new_string())
    } else {
        None
    };
    let batch_idx_key = opts.batch.as_ref().map(|cfg| make_batch_key(&cfg.key));

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
        payload_key: None,
        batch: opts.batch,
    };

    let mut meta = job.clone();
    meta.payload = None;
    let meta_bytes = rmp_serde::to_vec_named(&meta)?;

    Ok(PreparedEnqueue {
        job_key: make_job_key(&id),
        payload_key: make_payload_key(job.payload_key()),
        queue_key: make_queue_key(&job.queue, &id),
        status_key: make_status_key(status, &id),
        type_key: make_type_key(&job.job_type, &id),
        unique_idx_key: job.unique.as_ref().map(|uc| make_unique_key(&uc.key)),
        batch_idx_key,
        fold_payload_key_id,
        job,
        meta_bytes,
        payload_bytes,
    })
}
