// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Write-tx phase of enqueue: apply pre-built `PreparedEnqueue` values to
//! an open transaction, with unique-conflict detection (cross-batch via
//! the index, intra-batch via a `HashMap`) and batched-job fold/seal
//! logic.

use std::collections::HashMap;

use fjall::Readable;

use crate::batch::BatchExpr;

use super::super::budget::{Budget, check_budget_capacity, make_budget_key};
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

/// Result of resolving one op's budget references.
pub(in crate::store) enum BudgetPrePass {
    /// Every reference resolved. These budget records must be written
    /// before the op's jobs, and are the ones `create_with` asked for.
    Proceed(Vec<(Vec<u8>, Vec<u8>)>),

    /// The op is rejected. Nothing has been written for it.
    Reject(StoreError),
}

/// Resolve every budget an op's jobs reference, planning any that
/// `create_with` asks to bring into existence.
///
/// Runs before a single one of the op's jobs is written, and writes
/// nothing itself — a rejected op must leave no trace, and a shared
/// transaction cannot be partially rolled back. The caller applies the
/// planned creations only once the whole op has resolved.
///
/// The outer `Err` is a database failure, fatal to the batch. A
/// rejection of this op alone comes back as `Reject`.
pub(in crate::store) fn plan_op_budgets(
    tx: &fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    op: &[PreparedEnqueue],
) -> Result<BudgetPrePass, StoreError> {
    // Policies planned by earlier jobs in this same op, so that two
    // jobs naming the same new budget agree on one creation rather
    // than racing to define it. First writer wins, matching how a
    // `create_with` loses to an already-stored budget.
    let mut planned: HashMap<String, Budget> = HashMap::new();

    for p in op {
        for reference in &p.job.budgets {
            let binding = p
                .budgets
                .iter()
                .find(|b| b.key == reference.key)
                .expect("every job budget ref comes from a binding");

            // Reads through the tx, so a budget created by an earlier
            // op in this same batch is already visible.
            let stored: Option<Budget> = match tx.get(&ks.data, make_budget_key(&reference.key))? {
                Some(bytes) => Some(rmp_serde::from_slice(&bytes)?),
                None => None,
            };

            let budget = match stored.or_else(|| planned.get(&reference.key).cloned()) {
                Some(budget) => budget,
                None => match binding.create_with {
                    Some(policy) => {
                        match Budget::new(policy.allocation, policy.strategy, p.now) {
                            Ok(budget) => {
                                // Counted against the cap like any
                                // other creation — enqueue must not be
                                // a back door around it.
                                if let Err(e) =
                                    check_budget_capacity(tx, ks, &reference.key, planned.len())
                                {
                                    return Ok(BudgetPrePass::Reject(e));
                                }
                                planned.insert(reference.key.clone(), budget.clone());
                                budget
                            }
                            Err(e) => return Ok(BudgetPrePass::Reject(e)),
                        }
                    }
                    None => {
                        return Ok(BudgetPrePass::Reject(StoreError::InvalidOperation(
                            format!(
                                "budget '{}' does not exist and the enqueue supplied no \
                             create_with policy to create it with",
                                reference.key
                            ),
                        )));
                    }
                },
            };

            // A job costing more than the budget can ever hold would
            // never dispatch, so it is rejected rather than accepted
            // into a permanent stall.
            if reference.cost > budget.allocation {
                return Ok(BudgetPrePass::Reject(StoreError::InvalidOperation(
                    format!(
                        "job costs {} of budget '{}', which only allocates {}",
                        reference.cost, reference.key, budget.allocation
                    ),
                )));
            }
        }
    }

    Ok(BudgetPrePass::Proceed(
        planned
            .into_iter()
            .map(|(key, budget)| Ok((make_budget_key(&key), rmp_serde::to_vec_named(&budget)?)))
            .collect::<Result<Vec<_>, StoreError>>()?,
    ))
}

/// Per-op outcome: every job's result, or the one error that failed the
/// op as a whole.
///
/// An op is a single request — one `enqueue`, or one `enqueue_bulk` of
/// any size. Failing an op whole is what preserves bulk's
/// all-or-nothing contract while letting unrelated ops sharing the
/// coalesced transaction commit regardless.
pub(super) type OpOutcome = Result<Vec<EnqueueResult>, StoreError>;

/// Apply a batch of coalesced enqueue ops to an open write transaction
/// with intra-batch unique-key dedup.
///
/// Each element of `ops` is one request's prepared jobs. Results come
/// back per op, in the same order.
///
/// # Two kinds of failure
///
/// - The **outer** `Err` is fatal to the batch: a database or
///   corruption error, where no waiter can be told anything useful.
/// - An **inner** `Err` fails one op and leaves the rest to commit.
///
/// A shared transaction cannot be partially rolled back (see the note
/// in `take_next_n_jobs` about `fetch_update` writing into the tx
/// buffer). So an op that fails must not have written anything first:
/// any check that can reject an op belongs in a pre-pass over that op's
/// jobs, before the first of them is applied.
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
    ops: &[Vec<PreparedEnqueue>],
) -> Result<Vec<OpOutcome>, StoreError> {
    // Maps unique_key -> (op index, job index) for conflicts against ops
    // already applied in this batch. Only successful ops are registered:
    // a failed op writes nothing, so its keys must not shadow a later
    // op's legitimate enqueue.
    let mut batch_unique_keys: HashMap<String, (usize, usize)> = HashMap::new();
    let mut outcomes: Vec<OpOutcome> = Vec::with_capacity(ops.len());

    for (op_idx, prepared) in ops.iter().enumerate() {
        // Resolve budgets before writing anything for this op, so a
        // rejection leaves the transaction untouched.
        match plan_op_budgets(tx, ks, prepared)? {
            BudgetPrePass::Proceed(creations) => {
                for (key, bytes) in creations {
                    tx.insert(&ks.data, key, bytes);
                }
            }
            BudgetPrePass::Reject(e) => {
                outcomes.push(Err(e));
                continue;
            }
        }

        // Maps unique_key -> index within this op's own results.
        let mut op_unique_keys: HashMap<String, usize> = HashMap::new();
        let mut results: Vec<EnqueueResult> = Vec::with_capacity(prepared.len());

        for p in prepared {
            // Intra-batch unique conflict check (no DB read needed).
            // Check this op's own jobs first, then earlier ops'.
            if let Some(ref uc) = p.job.unique {
                let existing = op_unique_keys
                    .get(&uc.key)
                    .map(|&job_idx| results[job_idx].job().clone())
                    .or_else(|| {
                        batch_unique_keys.get(&uc.key).map(|&(prev_op, job_idx)| {
                            // Registered only for successful ops, so the
                            // outcome is always `Ok` here.
                            outcomes[prev_op].as_ref().expect("registered op succeeded")[job_idx]
                                .job()
                                .clone()
                        })
                    });

                if let Some(job) = existing {
                    results.push(EnqueueResult::Duplicate(job));
                    continue;
                }
            }

            // A `StoreError` from `apply_enqueue` is a database or
            // corruption failure, not a rejection of this op — it stays
            // fatal to the whole batch.
            let result = apply_enqueue(tx, ks, p)?;

            // Track unique keys for intra-batch dedup.
            if let EnqueueResult::Created(ref job) = result
                && let Some(ref uc) = job.unique
            {
                op_unique_keys.insert(uc.key.clone(), results.len());
            }

            results.push(result);
        }

        for (key, job_idx) in op_unique_keys {
            batch_unique_keys.insert(key, (op_idx, job_idx));
        }
        outcomes.push(Ok(results));
    }

    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::super::super::options::EnqueueOptions;
    use super::super::super::test_support::test_store;
    use super::super::prepare::prepare_enqueue;
    use super::*;

    /// Two *separate* ops in one coalesced batch sharing a unique key.
    ///
    /// Driving this through `enqueue`/`enqueue_bulk` would depend on
    /// two concurrent requests happening to land in the same batch, so
    /// it is exercised directly. The within-one-bulk case is covered by
    /// `unique_bulk_dedup_within_batch`; this is the cross-op branch,
    /// which resolves the winner out of an earlier op's outcome.
    #[tokio::test]
    async fn dedups_a_unique_key_across_two_ops_in_one_batch() {
        let store = test_store();
        let now = crate::time::now_millis();

        let opts = || EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("same");
        let ops = vec![
            vec![prepare_enqueue(opts(), now).unwrap()],
            vec![prepare_enqueue(opts(), now).unwrap()],
        ];

        let ks = store.ks.clone();
        let outcomes = tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            let outcomes = apply_enqueue_batch(&mut tx, &ks, &ops).unwrap();
            drop(tx);
            outcomes
        })
        .await
        .unwrap();

        let first = outcomes[0].as_ref().unwrap();
        let second = outcomes[1].as_ref().unwrap();

        assert!(!first[0].is_duplicate());
        assert!(second[0].is_duplicate());
        // The second op is told about the job the first one created.
        assert_eq!(second[0].job().id, first[0].job().id);
    }

    /// Each op gets its own result list, in submission order, rather
    /// than one flat list the caller has to slice.
    #[tokio::test]
    async fn returns_one_outcome_per_op() {
        let store = test_store();
        let now = crate::time::now_millis();

        let opts = |t: &str| EnqueueOptions::new(t, "q", serde_json::json!(null));
        let ops = vec![
            vec![prepare_enqueue(opts("a"), now).unwrap()],
            vec![
                prepare_enqueue(opts("b"), now).unwrap(),
                prepare_enqueue(opts("c"), now).unwrap(),
            ],
        ];

        let ks = store.ks.clone();
        let outcomes = tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            let outcomes = apply_enqueue_batch(&mut tx, &ks, &ops).unwrap();
            drop(tx);
            outcomes
        })
        .await
        .unwrap();

        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].as_ref().unwrap().len(), 1);
        assert_eq!(outcomes[1].as_ref().unwrap().len(), 2);
        assert_eq!(outcomes[0].as_ref().unwrap()[0].job().job_type, "a");
        assert_eq!(outcomes[1].as_ref().unwrap()[1].job().job_type, "c");
    }
}
