// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Write-tx phase of enqueue: apply pre-built `PreparedEnqueue` values to
//! an open transaction, with unique-conflict detection (cross-batch via
//! the index, intra-batch via a `HashMap`) and batched-job fold/seal
//! logic.

use std::collections::HashMap;

use fjall::Readable;

use crate::batch::BatchExpr;

use super::super::keys::{make_job_key, make_payload_key};
use super::super::results::EnqueueResult;
use super::super::store::Keyspaces;
use super::super::types::{Job, JobStatus, StoreError};
use super::prepare::PreparedEnqueue;

/// Insert a prepared enqueue into an open write transaction.
///
/// Checks for unique constraint conflicts first. Returns `Duplicate` if a
/// conflict is found (nothing is written), `Created` otherwise.
///
/// Does NOT commit the transaction — the caller is responsible for
/// committing (possibly after adding other writes to the same tx).
pub(in crate::store) fn apply_enqueue(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    p: &PreparedEnqueue,
) -> Result<EnqueueResult, StoreError> {
    // Check for a unique conflict.
    if let Some(ref uc) = p.job.unique {
        let scope = uc.unique_while();
        let idx_key = p.unique_idx_key.as_ref().unwrap();

        if let Some(existing_id_bytes) = ks.index.get(idx_key)? {
            let existing_id = std::str::from_utf8(&existing_id_bytes).map_err(|e| {
                StoreError::Corruption(format!("unique index value is not valid UTF-8: {e}"))
            })?;

            if let Some(existing_meta_bytes) = ks.data.get(&make_job_key(existing_id))? {
                let existing_meta: Job = rmp_serde::from_slice(&existing_meta_bytes)?;
                if let Ok(existing_status) = JobStatus::try_from(existing_meta.status) {
                    if scope.conflicts_with(existing_status) {
                        return Ok(EnqueueResult::Duplicate(existing_meta));
                    }
                }
            }
        }
    }

    // Try to fold this enqueue into an existing pending batched job.
    // A returned `Folded(_)` short-circuits the normal insert; a
    // returned `None` means "fall through to create a new job" — either
    // no existing batch, or the predicate failed and the existing batch
    // was sealed in this tx.
    //
    // Gated on `batch_idx_key` rather than `job.batch` because
    // scheduled batched enqueues carry `batch` metadata but no index
    // key (they opt out of folding — see `prepare_enqueue`).
    if p.batch_idx_key.is_some() {
        if let Some(folded) = try_apply_fold(tx, ks, p)? {
            return Ok(folded);
        }
    }

    tx.insert(&ks.data, &p.job_key, &p.meta_bytes);
    tx.insert(&ks.data, &p.payload_key, &p.payload_bytes);
    tx.insert(&ks.index, &p.queue_key, b"");
    tx.insert(&ks.index, &p.status_key, b"");
    tx.insert(&ks.index, &p.type_key, b"");

    if let Some(ref idx_key) = p.unique_idx_key {
        tx.insert(&ks.index, idx_key, p.job.id.as_bytes());
    }

    if let Some(ref idx_key) = p.batch_idx_key {
        tx.insert(&ks.index, idx_key, p.job.id.as_bytes());
    }

    Ok(EnqueueResult::Created(p.job.clone()))
}

/// Attempt to fold `p` into an existing pending batched job.
///
/// Returns `Ok(Some(Folded(_)))` when the incoming payload was merged
/// into an existing pending job (its payload is rewritten in place at a
/// fresh payload key, keeping FIFO position).
///
/// Returns `Ok(None)` when the caller should fall through to normal
/// insertion. That covers three cases: no existing entry for this
/// batch key, an existing entry pointing at a job that's no longer
/// pending (stale — cleaned up here), or an existing pending batch
/// whose `when` predicate returned false (sealed here so a fresh job
/// takes over).
fn try_apply_fold(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    p: &PreparedEnqueue,
) -> Result<Option<EnqueueResult>, StoreError> {
    let batch_idx_key = p
        .batch_idx_key
        .as_ref()
        .expect("batched enqueue must carry batch_idx_key");

    // Read-your-writes: sees inserts from earlier ops in this same tx.
    let Some(existing_id_bytes) = tx.get(&ks.index, batch_idx_key)? else {
        return Ok(None);
    };

    let existing_id = std::str::from_utf8(&existing_id_bytes).map_err(|e| {
        StoreError::Corruption(format!("batch index value is not valid UTF-8: {e}"))
    })?;
    let existing_job_key = make_job_key(existing_id);

    let Some(existing_meta_bytes) = tx.get(&ks.data, &existing_job_key)? else {
        // Stale index entry pointing at a job that's been deleted.
        tx.remove(&ks.index, batch_idx_key);
        return Ok(None);
    };

    let existing_meta: Job = rmp_serde::from_slice(&existing_meta_bytes)?;
    let existing_status = JobStatus::try_from(existing_meta.status).map_err(|_| {
        StoreError::Corruption(format!(
            "job {} has unrecognized status byte",
            existing_meta.id
        ))
    })?;

    if existing_status != JobStatus::Ready {
        // Only immediate-ready jobs are foldable. Scheduled jobs opt
        // out of batching so that folds never cross a `ready_at`
        // boundary — otherwise the fold would silently discard one
        // side's schedule. Other statuses (in-flight, completed,
        // dead) indicate a stale index entry that we clean up.
        tx.remove(&ks.index, batch_idx_key);
        return Ok(None);
    }

    let existing_cfg = existing_meta.batch.as_ref().ok_or_else(|| {
        StoreError::Corruption(format!(
            "batched-index target job {} has no batch config",
            existing_meta.id
        ))
    })?;

    // Compile using the *existing* job's stored expressions (first-wins).
    let expr = BatchExpr::compile(&existing_cfg.when, &existing_cfg.fold).map_err(|e| {
        StoreError::Corruption(format!(
            "job {} has invalid stored batch expression: {e}",
            existing_meta.id
        ))
    })?;

    // Hydrate the existing payload from wherever it currently lives.
    let existing_payload_key = make_payload_key(existing_meta.payload_key());
    let existing_payload_bytes = tx.get(&ks.data, &existing_payload_key)?.ok_or_else(|| {
        StoreError::Corruption(format!(
            "batched job {} has no payload record",
            existing_meta.id
        ))
    })?;
    let existing_payload: serde_json::Value = rmp_serde::from_slice(&existing_payload_bytes)?;

    let new_payload =
        p.job.payload.as_ref().ok_or_else(|| {
            StoreError::Internal("prepared enqueue is missing its payload".into())
        })?;

    if !expr.eval_when(&existing_payload, new_payload) {
        // Seal: the current pending job becomes a normal pending job
        // (no more folds against it). Remove the index entry so the
        // caller creates a fresh batched job that takes over.
        tx.remove(&ks.index, batch_idx_key);
        return Ok(None);
    }

    let merged = expr
        .eval_fold(&existing_payload, new_payload)
        .map_err(|e| StoreError::InvalidOperation(format!("batch fold failed: {e}")))?;

    // Rewrite the existing job's payload at a fresh key, then update
    // its metadata to point at the new key, then remove the old
    // payload — all in this tx. `remove_weak` keeps each payload
    // key write-once/delete-once from the perspective of its own
    // lifecycle.
    let new_payload_id = p
        .fold_payload_key_id
        .as_ref()
        .expect("batched enqueue must carry a fold_payload_key_id");
    let new_payload_key = make_payload_key(new_payload_id);
    let new_payload_bytes = rmp_serde::to_vec_named(&merged)?;

    let mut updated_meta = existing_meta.clone();
    updated_meta.payload_key = Some(new_payload_id.clone());
    updated_meta.payload = None;
    let updated_meta_bytes = rmp_serde::to_vec_named(&updated_meta)?;

    tx.insert(&ks.data, &new_payload_key, &new_payload_bytes);
    tx.insert(&ks.data, &existing_job_key, &updated_meta_bytes);
    tx.remove_weak(&ks.data, &existing_payload_key);

    // Return the updated metadata with the merged payload hydrated for
    // the caller (matches the shape of `Created(_)` responses which
    // carry the payload).
    updated_meta.payload = Some(merged);
    Ok(Some(EnqueueResult::Folded(updated_meta)))
}

/// Apply a batch of prepared enqueues to an open write transaction with
/// intra-batch unique-key dedup.
///
/// When two prepared jobs in the same batch share a `unique` key, the
/// second one returns `EnqueueResult::Duplicate(...)` referring to the
/// first without performing a tx insert. Cross-batch dedup against
/// already-committed jobs is handled by `apply_enqueue` itself.
///
/// Does NOT commit the transaction — the caller commits and runs
/// `finalize_enqueue` per result post-commit.
pub(super) fn apply_enqueue_batch(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    prepared: &[PreparedEnqueue],
) -> Result<Vec<EnqueueResult>, StoreError> {
    // Maps unique_key -> index in `results` for intra-batch conflicts.
    let mut batch_unique_keys: HashMap<String, usize> = HashMap::new();
    let mut results: Vec<EnqueueResult> = Vec::with_capacity(prepared.len());

    for p in prepared {
        // Intra-batch unique conflict check (no DB read needed).
        if let Some(ref uc) = p.job.unique {
            if let Some(&existing_idx) = batch_unique_keys.get(&uc.key) {
                results.push(EnqueueResult::Duplicate(
                    results[existing_idx].job().clone(),
                ));
                continue;
            }
        }

        let result = apply_enqueue(tx, ks, p)?;

        // Track unique keys for intra-batch dedup.
        if let EnqueueResult::Created(ref job) = result {
            if let Some(ref uc) = job.unique {
                batch_unique_keys.insert(uc.key.clone(), results.len());
            }
        }

        results.push(result);
    }

    Ok(results)
}
