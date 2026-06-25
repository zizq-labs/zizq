// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Write-tx phase of completion: apply pre-built `PreparedComplete`
//! values to an open transaction, with optimistic-concurrency CAS
//! verification per item. Zero-retention completions delete the job;
//! non-zero retention updates the status/purge keys.

use super::super::delete::apply_job_deletion;
use super::super::keys::{make_job_key, make_unique_key};
use super::super::store::Keyspaces;
use super::super::types::{StoreError, UniqueWhile};
use super::pre_read::PreparedComplete;

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
