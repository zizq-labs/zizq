// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Write-tx phase of enqueue: apply pre-built `PreparedEnqueue` values to
//! an open transaction, with unique-conflict detection (cross-batch via
//! the index, intra-batch via a `HashMap`).

use std::collections::HashMap;

use super::super::keys::make_job_key;
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

    tx.insert(&ks.data, &p.job_key, &p.meta_bytes);
    tx.insert(&ks.data, &p.payload_key, &p.payload_bytes);
    tx.insert(&ks.index, &p.queue_key, b"");
    tx.insert(&ks.index, &p.status_key, b"");
    tx.insert(&ks.index, &p.type_key, b"");

    if let Some(ref idx_key) = p.unique_idx_key {
        tx.insert(&ks.index, idx_key, p.job.id.as_bytes());
    }

    Ok(EnqueueResult::Created(p.job.clone()))
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
