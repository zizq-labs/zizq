// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Budget CRUD against the data keyspace.
//!
//! Lives in a submodule of `budget/` so the key builder stays
//! package-private to the budget subsystem, as the cron module does.
//!
//! Four write verbs differ only in what they do to a budget that
//! already exists:
//!
//! | Operation        | Absent    | Exists                 |
//! |------------------|-----------|------------------------|
//! | `create_budget`  | create    | `Conflict`             |
//! | `put_budget`     | create    | replace policy whole   |
//! | `patch_budget`   | `None`    | merge named fields     |
//! | `delete_budget`  | `false`   | delete                 |
//!
//! `create_budget` exists because `put`/`patch` inherently overwrite
//! whatever an operator adjusted. An application declaring "this budget
//! should exist" on every boot needs a call that will not clobber an
//! allocation someone tightened during an incident.

use std::collections::HashMap;
use std::ops::Bound;

use fjall::Readable;
use tokio::task;

use super::super::cron::cron_budget_usage;
use super::super::keys::RecordKind;
use super::super::options::PatchBudgetOptions;
use super::super::store::{Keyspaces, Store};
use super::super::types::{Job, StoreError};
use super::registry::Budgets;
use super::{Budget, BudgetBinding, BudgetStrategy};

impl Store {
    /// Create a budget, failing if the key is taken.
    ///
    /// Returns [`StoreError::Conflict`] if a budget already exists,
    /// leaving the stored policy untouched.
    pub async fn create_budget(
        &self,
        key: &str,
        allocation: u32,
        strategy: BudgetStrategy,
        now: u64,
    ) -> Result<Budget, StoreError> {
        let ks = self.ks.clone();
        let live = self.budgets.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Budget, StoreError> {
            // Validate before taking the write lock.
            let budget = Budget::new(allocation, strategy, now)?;
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            // Checked under the single-writer lock, so no concurrent
            // creator can slip between the read and the insert.
            if ks.data.get(&budget_key)?.is_some() {
                return Err(StoreError::Conflict(format!(
                    "budget '{key}' already exists"
                )));
            }

            check_budget_capacity(&tx, &ks, &key, NOTHING_PLANNED)?;

            tx.insert(&ks.data, &budget_key, &rmp_serde::to_vec_named(&budget)?);
            ks.commit(tx, ks.default_commit_mode)?;

            // Only after the record is durable: a group built from a
            // policy that failed to commit would throttle against
            // something that does not exist.
            live.sync(&key, &budget, now);

            Ok(budget)
        })
        .await?
    }

    /// Create a budget, or replace an existing one's policy.
    ///
    /// `created_at` is preserved when the budget already exists — a
    /// replace changes the policy, not the budget's identity.
    pub async fn put_budget(
        &self,
        key: &str,
        allocation: u32,
        strategy: BudgetStrategy,
        now: u64,
    ) -> Result<Budget, StoreError> {
        let ks = self.ks.clone();
        let live = self.budgets.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Budget, StoreError> {
            let mut budget = Budget::new(allocation, strategy, now)?;
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            match ks.data.get(&budget_key)? {
                Some(bytes) => {
                    let existing: Budget = rmp_serde::from_slice(&bytes)?;
                    budget.created_at = existing.created_at;
                    check_costs_fit(&tx, &ks, &live, &key, budget.allocation)?;
                }
                // Replacing a budget cannot push the server over its
                // cap; only creating one can.
                None => check_budget_capacity(&tx, &ks, &key, NOTHING_PLANNED)?,
            }

            tx.insert(&ks.data, &budget_key, &rmp_serde::to_vec_named(&budget)?);
            ks.commit(tx, ks.default_commit_mode)?;

            live.sync(&key, &budget, now);

            Ok(budget)
        })
        .await?
    }

    /// Update named fields of an existing budget's policy.
    ///
    /// Absent fields are left alone. Returns `None` if the budget does
    /// not exist — unlike `put_budget`, a patch has nothing to merge
    /// into and does not create one.
    pub async fn patch_budget(
        &self,
        key: &str,
        opts: PatchBudgetOptions,
        now: u64,
    ) -> Result<Option<Budget>, StoreError> {
        let ks = self.ks.clone();
        let live = self.budgets.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Option<Budget>, StoreError> {
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            let existing: Budget = match ks.data.get(&budget_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(None),
            };

            // Re-validated through `Budget::new`, since a patch can
            // reach a combination neither the stored policy nor the
            // patch alone would have been rejected for.
            let mut budget = Budget::new(
                opts.allocation.unwrap_or(existing.allocation),
                opts.strategy.unwrap_or(existing.strategy),
                now,
            )?;
            budget.created_at = existing.created_at;

            check_costs_fit(&tx, &ks, &live, &key, budget.allocation)?;

            tx.insert(&ks.data, &budget_key, &rmp_serde::to_vec_named(&budget)?);
            ks.commit(tx, ks.default_commit_mode)?;

            live.sync(&key, &budget, now);

            Ok(Some(budget))
        })
        .await?
    }

    /// Load a budget's policy, or `None` if it does not exist.
    pub async fn get_budget(&self, key: &str) -> Result<Option<Budget>, StoreError> {
        let ks = self.ks.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Option<Budget>, StoreError> {
            let snapshot = ks.db.read_tx();

            match snapshot.get(&ks.data, make_budget_key(&key))? {
                Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
        .await?
    }

    /// List every budget with its policy, in lexicographic key order.
    ///
    /// A plain range scan, unlike `list_cron_groups` — budget keys have
    /// nothing nested beneath them, so there is nothing to step over.
    ///
    /// The policy comes back with the key because the scan has already
    /// read it; discarding it here would only force the caller into a
    /// read per budget. A budget's policy is a handful of scalars, so
    /// the whole list stays small at any sane number of budgets.
    pub async fn list_budgets(&self) -> Result<Vec<(String, Budget)>, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || -> Result<Vec<(String, Budget)>, StoreError> {
            let snapshot = ks.db.read_tx();

            // Every budget key is `B\0{key}`, so `[B\0, B\1)` is exactly
            // the set of them.
            let start = vec![RecordKind::Budget as u8, 0];
            let end = vec![RecordKind::Budget as u8, 1];

            snapshot
                .range::<Vec<u8>, _>(&ks.data, (Bound::Included(start), Bound::Excluded(end)))
                .map(|guard| {
                    let (key, value) = guard.into_inner()?;
                    // Skip the `B\0` prefix to recover the budget key.
                    let key = String::from_utf8(key[2..].to_vec()).map_err(|e| {
                        StoreError::Corruption(format!("budget key is not valid UTF-8: {e}"))
                    })?;
                    Ok((key, rmp_serde::from_slice(&value)?))
                })
                .collect()
        })
        .await?
    }

    /// Delete a budget.
    ///
    /// Returns `true` if it existed, `false` otherwise.
    ///
    /// Refused with [`StoreError::Conflict`] while anything still
    /// references it, which is two separate questions:
    ///
    /// - **A cron entry's job template.** An entry is a standing claim
    ///   that never drains, so deleting out from under one would strand
    ///   a schedule with no remedy but editing the entry.
    /// - **An unfinished job.** These do drain, so the operator has the
    ///   extra option of simply waiting.
    ///
    /// Reported separately rather than as one count, because the two
    /// have different ways out.
    pub async fn delete_budget(&self, key: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let live = self.budgets.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<bool, StoreError> {
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            if ks.data.get(&budget_key)?.is_none() {
                return Ok(false);
            }

            if let Some(usage) = cron_budget_usage(&tx, &ks, &key)? {
                let plural = if usage.entries == 1 {
                    "entry"
                } else {
                    "entries"
                };
                return Err(StoreError::Conflict(format!(
                    "budget '{key}' is referenced by {} cron {plural}, including '{}'. \
                     Remove them before deleting it.",
                    usage.entries, usage.example
                )));
            }

            // Unlike cron entries, jobs go away on their own, so waiting
            // is a remedy — and the deletion this refuses would be worse
            // than it looks. `forget` discards the group along with the
            // budget, so every job waiting on it leaves dispatch and
            // does not come back: a restart cannot recover them, because
            // the budget they park on genuinely no longer exists.
            let tracked = live.tracked(&key);
            if tracked > 0 {
                let plural = if tracked == 1 { "job" } else { "jobs" };
                return Err(StoreError::Conflict(format!(
                    "budget '{key}' is referenced by {tracked} unfinished {plural}. \
                     Delete them or wait for them to finish before deleting it."
                )));
            }

            tx.remove(&ks.data, &budget_key);
            ks.commit(tx, ks.default_commit_mode)?;

            live.forget(&key);

            Ok(true)
        })
        .await?
    }
}

/// Reject a create that would take the server past `max_budgets`.
///
/// Counts by scanning the budget prefix, stopping as soon as the cap is
/// reached — so the work is bounded by the cap rather than by however
/// many budgets exist. Only creates pay for this, and creating a budget
/// is an operator-scale event, not a per-job one.
///
/// `reader` is the open write transaction, so the count sees budgets
/// created earlier in the same transaction. `planned` covers creations
/// this operation has decided on but not yet written, which the scan
/// therefore cannot see — without it, one request creating several
/// budgets at the boundary would check each against the same stale
/// count and overshoot by all but one. Callers that write immediately
/// after checking pass [`NOTHING_PLANNED`].
///
/// Call under the write lock, so the count cannot shift underneath the
/// decision.
/// No creations are staged behind this one — the caller writes the
/// budget as soon as the check passes.
const NOTHING_PLANNED: usize = 0;

pub(in crate::store) fn check_budget_capacity(
    reader: &impl Readable,
    ks: &Keyspaces,
    key: &str,
    planned: usize,
) -> Result<(), StoreError> {
    let too_many = || {
        StoreError::InvalidOperation(format!(
            "cannot create budget '{key}': the server already holds its maximum of \
             {} budgets (--max-budgets / ZIZQ_MAX_BUDGETS). Raise the limit, or \
             express the throttle as one shared budget rather than one per caller.",
            ks.max_budgets
        ))
    };

    let remaining = ks.max_budgets.saturating_sub(planned);
    if remaining == 0 {
        return Err(too_many());
    }

    let start = vec![RecordKind::Budget as u8, 0];
    let end = vec![RecordKind::Budget as u8, 1];
    let mut count = 0usize;

    for guard in
        reader.range::<Vec<u8>, _>(&ks.data, (Bound::Included(start), Bound::Excluded(end)))
    {
        // Surface a read error rather than undercounting into an
        // allow decision.
        guard.into_inner()?;
        count += 1;
        if count >= remaining {
            return Err(too_many());
        }
    }

    Ok(())
}

/// Reject an allocation that something already committed to the budget
/// could never fit inside.
///
/// A job or entry costing more than the whole allocation is not merely
/// delayed — no amount of waiting makes it affordable, so it stalls
/// silently until an operator notices. Enqueue rejects that at the door;
/// this is the other direction, where the allocation moves instead of
/// the cost.
///
/// Consults both halves of the tracked set, which are counted by
/// different machinery: cron entries by a scan of the keyspace inside
/// `reader`, jobs by the in-memory aggregate. Cron is checked first
/// because its complaint can name the entry at fault, where the jobs one
/// can only give a number.
///
/// Call under the write lock. The job side reads the live registry, and
/// enqueue stages its accounting inside that same lock, so holding it is
/// what stops a job appearing between this check and the write it
/// guards.
fn check_costs_fit(
    reader: &impl Readable,
    ks: &Keyspaces,
    live: &Budgets,
    key: &str,
    allocation: u32,
) -> Result<(), StoreError> {
    // A cron entry is a standing claim: unlike a job it never drains,
    // so there is no "wait for it to finish" remedy. Shrinking below
    // what a template costs would leave an entry that is installed,
    // valid on its face, and permanently unable to fire.
    if let Some(usage) = cron_budget_usage(reader, ks, key)?
        && allocation < usage.max_cost
    {
        return Err(StoreError::InvalidOperation(format!(
            "budget '{key}' cannot allocate {allocation}: cron entry '{}' draws {} from it. \
             Lower the entry's cost or remove it first.",
            usage.example, usage.max_cost
        )));
    }

    // Jobs do drain, so waiting is a real remedy here and worth saying.
    if let Some(max) = live.max_cost(key)
        && allocation < max
    {
        let tracked = live.tracked(key);
        let plural = if tracked == 1 { "job" } else { "jobs" };
        return Err(StoreError::InvalidOperation(format!(
            "budget '{key}' cannot allocate {allocation}: {tracked} unfinished {plural} \
             draw up to {max} from it. Raise the allocation, delete them, or wait for \
             them to drain."
        )));
    }

    Ok(())
}

/// The outcome of planning a set of budget bindings.
pub(in crate::store) enum BudgetPlan {
    /// Every binding resolved. Any budgets listed here must be written
    /// before whatever referenced them, and synced into the live
    /// registry once that write is durable — a job parking on a budget
    /// with no group would be silently dropped from dispatch.
    Proceed(Vec<(String, Budget)>),

    /// A binding could not be honoured. Nothing has been written.
    Reject(StoreError),
}

/// Resolve a set of budget bindings, planning any that `create_with`
/// asks to bring into existence.
///
/// The single place the binding rules live, so that enqueueing a job,
/// installing a cron entry, and firing one cannot drift apart on what
/// they accept. Writes nothing — the caller applies the returned
/// records once whatever it is validating has resolved as a whole,
/// because a shared transaction cannot be partially rolled back.
///
/// `reader` should be the open write transaction, so budgets created
/// earlier in it are already visible.
///
/// The outer `Err` is a database failure. A rejection of the bindings
/// themselves comes back as `Reject`.
pub(in crate::store) fn plan_budgets<'a, I>(
    reader: &impl Readable,
    ks: &Keyspaces,
    bindings: I,
    now: u64,
) -> Result<BudgetPlan, StoreError>
where
    I: IntoIterator<Item = &'a BudgetBinding>,
{
    // Budgets an earlier binding in this same set already planned, so
    // that two references to one new budget agree on a single creation
    // rather than racing to define it. First writer wins, matching how
    // a `create_with` loses to an already-stored budget.
    let mut planned: HashMap<String, Budget> = HashMap::new();

    for binding in bindings {
        let stored: Option<Budget> = match reader.get(&ks.data, make_budget_key(&binding.key))? {
            Some(bytes) => Some(rmp_serde::from_slice(&bytes)?),
            None => None,
        };

        let budget = match stored.or_else(|| planned.get(&binding.key).cloned()) {
            Some(budget) => budget,
            None => {
                let Some(policy) = binding.create_with else {
                    return Ok(BudgetPlan::Reject(StoreError::InvalidOperation(format!(
                        "budget '{}' does not exist and no create_with policy was \
                         supplied to create it with",
                        binding.key
                    ))));
                };

                let budget = match Budget::new(policy.allocation, policy.strategy, now) {
                    Ok(budget) => budget,
                    Err(e) => return Ok(BudgetPlan::Reject(e)),
                };

                // Counted against the cap like any other creation —
                // creating one as a side effect must not be a back door
                // around it.
                if let Err(e) = check_budget_capacity(reader, ks, &binding.key, planned.len()) {
                    return Ok(BudgetPlan::Reject(e));
                }

                planned.insert(binding.key.clone(), budget.clone());
                budget
            }
        };

        // Costing more than the budget can ever hold would never
        // dispatch, so it is refused rather than accepted into a
        // permanent stall.
        if binding.cost > budget.allocation {
            return Ok(BudgetPlan::Reject(StoreError::InvalidOperation(format!(
                "cost {} exceeds budget '{}', which only allocates {}",
                binding.cost, binding.key, budget.allocation
            ))));
        }
    }

    Ok(BudgetPlan::Proceed(planned.into_iter().collect()))
}

/// Write planned budget creations into a transaction.
///
/// Paired with [`stage_budgets`], which brings the same creations into
/// the live registry.
pub(in crate::store) fn write_created_budgets(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    created: &[(String, Budget)],
) -> Result<(), StoreError> {
    for (key, budget) in created {
        tx.insert(
            &ks.data,
            make_budget_key(key),
            rmp_serde::to_vec_named(budget)?,
        );
    }

    Ok(())
}

/// Bring newly written budgets into the live registry, **after** the
/// transaction that wrote them has committed.
///
/// For paths that create budgets without enqueuing jobs — installing a
/// cron group, whose entries reference budgets but are not themselves
/// tracked jobs. Waiting for durability is right here, and is the
/// safer half of the trade: a group built from a policy whose write
/// then failed would throttle jobs against a budget that does not
/// exist.
///
/// There is no accounting window to close, because the guard protecting
/// a budget referenced by a cron entry reads the *keyspace* inside the
/// deleting transaction (`cron_budget_usage`) rather than the registry.
/// A concurrent delete is therefore serialised against the install by
/// the write lock, with no in-memory state in the middle. Paths that
/// enqueue jobs have no such luxury and must use [`stage_budgets`].
pub(in crate::store) fn sync_created_budgets(
    live: &Budgets,
    created: &[(String, Budget)],
    now: u64,
) {
    for (key, budget) in created {
        live.sync(key, budget, now);
    }
}

/// Bring the in-memory budget state in line with a transaction that is
/// written but **not yet committed**.
///
/// Two things are staged: groups for the budgets this transaction
/// creates, and cost accounting for the jobs it enqueues.
///
/// **Call while the write lock is still held**, which is what makes the
/// accounting safe. The guards that refuse to delete or shrink a budget
/// read the live registry rather than the keyspace, so if this ran after
/// the commit there would be a window — the records durable, the
/// accounting not yet applied — in which a concurrent `DELETE
/// /budgets/{key}` sees the budget as unreferenced and removes it. The
/// job it stranded would then park into a group that no longer exists,
/// which drops it from dispatch permanently: a restart does not recover
/// it, because the budget really is gone. Staging under the lock means a
/// delete either runs entirely before this transaction — in which case
/// the enqueue's own in-transaction validation rejects the reference —
/// or entirely after, and sees the jobs.
///
/// The earlier ordering, which synced only after a durable commit, was
/// guarding against the opposite failure: a group built from a policy
/// whose write then failed would throttle jobs against a budget that
/// does not exist. That risk is real, and is why [`unstage_budgets`]
/// exists rather than the ordering simply being reversed.
pub(in crate::store) fn stage_budgets<'a>(
    live: &Budgets,
    created: &[(String, Budget)],
    jobs: impl IntoIterator<Item = &'a Job>,
    now: u64,
) {
    for (key, budget) in created {
        live.sync(key, budget, now);
    }

    // After the groups exist — a job tracked against a budget with no
    // group is silently skipped, and a batch that creates a budget and
    // enqueues against it in one transaction would lose the accounting
    // for exactly the jobs that need it most.
    for job in jobs {
        live.track(&job.budgets);
    }
}

/// Undo [`stage_budgets`] after a commit that did not happen.
///
/// Takes the same `created` and `jobs` the staging call was given, so
/// the two stay symmetrical by construction rather than by a caller
/// remembering what it staged.
///
/// This runs after the write lock has been released, so there is a brief
/// window in which the registry describes work that no longer exists.
/// The consequence is a delete or shrink refused that would in fact have
/// been safe — transient, self-correcting, and in the conservative
/// direction. The reverse ordering has no such harmless failure.
pub(in crate::store) fn unstage_budgets<'a>(
    live: &Budgets,
    created: &[(String, Budget)],
    jobs: impl IntoIterator<Item = &'a Job>,
) {
    for job in jobs {
        live.untrack(&job.budgets);
    }

    // After the untracking, so the counts a group is discarded with are
    // the ones it should have had.
    for (key, _) in created {
        live.forget(key);
    }
}

/// Build a budget key: `B\0{key}`.
pub(in crate::store) fn make_budget_key(key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + key.len());
    bytes.push(RecordKind::Budget as u8);
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::super::super::options::PatchBudgetOptions;
    use super::super::super::test_support::{test_store, test_store_with_max_budgets};
    use super::super::BudgetStrategy;
    use crate::store::Store;
    use crate::store::StoreError;

    const NOW: u64 = 1_700_000_000_000;

    fn patch(allocation: Option<u32>, strategy: Option<BudgetStrategy>) -> PatchBudgetOptions {
        PatchBudgetOptions {
            allocation,
            strategy,
        }
    }

    #[tokio::test]
    async fn create_then_get_round_trips() {
        let store = test_store();

        let created = store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        assert_eq!(created.allocation, 100);

        let loaded = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(loaded.allocation, 100);
        assert_eq!(loaded.strategy, BudgetStrategy::WhileInFlight);
        assert_eq!(loaded.created_at, NOW);
    }

    #[tokio::test]
    async fn get_returns_none_for_a_missing_budget() {
        let store = test_store();
        assert!(store.get_budget("absent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_rejects_an_existing_key_without_touching_it() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let result = store
            .create_budget("stripe", 5, BudgetStrategy::WhileInFlight, NOW + 1_000)
            .await;
        assert!(matches!(result, Err(StoreError::Conflict(_))));

        // The operator's policy survived the second caller.
        let loaded = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(loaded.allocation, 100);
    }

    #[tokio::test]
    async fn create_validates_the_policy() {
        let store = test_store();

        let result = store
            .create_budget("stripe", 0, BudgetStrategy::WhileInFlight, NOW)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
        assert!(store.get_budget("stripe").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_creates_when_absent() {
        let store = test_store();

        let budget = store
            .put_budget(
                "stripe",
                50,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                },
                NOW,
            )
            .await
            .unwrap();

        assert_eq!(budget.allocation, 50);
        assert_eq!(budget.created_at, NOW);
        assert_eq!(budget.updated_at, NOW);
    }

    #[tokio::test]
    async fn put_replaces_the_policy_but_keeps_created_at() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let replaced = store
            .put_budget(
                "stripe",
                10,
                BudgetStrategy::TimeBased { duration_ms: 1_000 },
                NOW + 5_000,
            )
            .await
            .unwrap();

        assert_eq!(replaced.allocation, 10);
        assert_eq!(
            replaced.strategy,
            BudgetStrategy::TimeBased { duration_ms: 1_000 }
        );
        // Identity is unchanged; only the policy moved.
        assert_eq!(replaced.created_at, NOW);
        assert_eq!(replaced.updated_at, NOW + 5_000);
    }

    #[tokio::test]
    async fn patch_changes_only_the_named_field() {
        let store = test_store();
        store
            .create_budget(
                "stripe",
                100,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                },
                NOW,
            )
            .await
            .unwrap();

        let patched = store
            .patch_budget("stripe", patch(Some(25), None), NOW + 5_000)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(patched.allocation, 25);
        // Untouched.
        assert_eq!(
            patched.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000
            }
        );
        assert_eq!(patched.created_at, NOW);
        assert_eq!(patched.updated_at, NOW + 5_000);
    }

    #[tokio::test]
    async fn patch_can_change_the_strategy_alone() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let patched = store
            .patch_budget(
                "stripe",
                patch(
                    None,
                    Some(BudgetStrategy::TimeBased {
                        duration_ms: 30_000,
                    }),
                ),
                NOW + 1,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(patched.allocation, 100);
        assert_eq!(
            patched.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 30_000
            }
        );
    }

    #[tokio::test]
    async fn patch_returns_none_for_a_missing_budget() {
        let store = test_store();

        let result = store
            .patch_budget("absent", patch(Some(10), None), NOW)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn patch_validates_the_merged_policy() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let result = store
            .patch_budget("stripe", patch(Some(0), None), NOW + 1)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        // Rejected patches leave the stored policy alone.
        let loaded = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(loaded.allocation, 100);
    }

    #[tokio::test]
    async fn create_rejects_a_budget_past_the_cap() {
        let store = test_store_with_max_budgets(2);

        for key in ["a", "b"] {
            store
                .create_budget(key, 10, BudgetStrategy::WhileInFlight, NOW)
                .await
                .unwrap();
        }

        let result = store
            .create_budget("c", 10, BudgetStrategy::WhileInFlight, NOW)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
        assert!(store.get_budget("c").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_rejects_a_new_budget_past_the_cap() {
        let store = test_store_with_max_budgets(1);
        store
            .create_budget("a", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let result = store
            .put_budget("b", 10, BudgetStrategy::WhileInFlight, NOW)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// The cap bounds how many budgets exist, so replacing one at the
    /// cap has to keep working — otherwise an operator at the limit
    /// could not adjust an allocation without deleting something first.
    #[tokio::test]
    async fn put_replaces_an_existing_budget_at_the_cap() {
        let store = test_store_with_max_budgets(1);
        store
            .create_budget("a", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let replaced = store
            .put_budget("a", 50, BudgetStrategy::WhileInFlight, NOW + 1)
            .await
            .unwrap();
        assert_eq!(replaced.allocation, 50);
    }

    /// Likewise a patch, which never creates.
    #[tokio::test]
    async fn patch_works_at_the_cap() {
        let store = test_store_with_max_budgets(1);
        store
            .create_budget("a", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let patched = store
            .patch_budget("a", patch(Some(50), None), NOW + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(patched.allocation, 50);
    }

    /// Deleting frees a slot, so the cap is a live ceiling rather than
    /// a high-water mark.
    #[tokio::test]
    async fn deleting_frees_capacity_under_the_cap() {
        let store = test_store_with_max_budgets(1);
        store
            .create_budget("a", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        assert!(
            store
                .create_budget("b", 10, BudgetStrategy::WhileInFlight, NOW)
                .await
                .is_err()
        );

        store.delete_budget("a").await.unwrap();
        store
            .create_budget("b", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_removes_the_budget() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        assert!(store.delete_budget("stripe").await.unwrap());
        assert!(store.get_budget("stripe").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_reports_a_missing_budget() {
        let store = test_store();
        assert!(!store.delete_budget("absent").await.unwrap());
    }

    #[tokio::test]
    async fn list_returns_keys_in_order() {
        let store = test_store();
        for key in ["stripe", "ses", "algolia"] {
            store
                .create_budget(key, 10, BudgetStrategy::WhileInFlight, NOW)
                .await
                .unwrap();
        }

        let keys: Vec<String> = store
            .list_budgets()
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, ["algolia", "ses", "stripe"]);
    }

    /// The listing scan has already read each record, so it hands the
    /// policy back rather than making the caller re-read it.
    #[tokio::test]
    async fn list_returns_each_policy_alongside_its_key() {
        let store = test_store();
        store
            .create_budget(
                "stripe",
                100,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                },
                NOW,
            )
            .await
            .unwrap();

        let listed = store.list_budgets().await.unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0, "stripe");
        assert_eq!(listed[0].1.allocation, 100);
        assert_eq!(
            listed[0].1.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000
            }
        );
        assert_eq!(listed[0].1.created_at, NOW);
    }

    #[tokio::test]
    async fn list_is_empty_when_there_are_no_budgets() {
        let store = test_store();
        assert!(store.list_budgets().await.unwrap().is_empty());
    }

    /// Budget keys sit next to cron, job and payload records in the
    /// same keyspace, so the scan has to stop at its own prefix.
    #[tokio::test]
    async fn list_ignores_records_of_other_kinds() {
        use crate::store::options::{CronEntryOptions, EnqueueOptions, ReplaceCronGroupOptions};

        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        store
            .enqueue(NOW, EnqueueOptions::new("test", "q", serde_json::json!({})))
            .await
            .unwrap();

        store
            .replace_cron_group(
                "nightly",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![CronEntryOptions {
                        name: "cleanup".to_string(),
                        expression: "0 0 * * *".to_string(),
                        timezone: None,
                        paused: None,
                        job: EnqueueOptions::new("cleanup", "q", serde_json::json!({})),
                    }],
                },
                NOW,
            )
            .await
            .unwrap();

        let keys: Vec<String> = store
            .list_budgets()
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, ["stripe"]);
    }

    // --- Protection against breaking an installed schedule ---

    /// Install a one-entry schedule whose template draws `cost` from
    /// `budget`, which must already exist.
    async fn install_cron_drawing(store: &Store, budget: &str, cost: u32) {
        use super::super::super::options::{
            CronEntryOptions, EnqueueOptions, ReplaceCronGroupOptions,
        };
        use super::super::BudgetBinding;

        store
            .replace_cron_group(
                "g",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![CronEntryOptions {
                        name: "nightly".to_string(),
                        expression: "0 3 * * *".to_string(),
                        timezone: Some("UTC".to_string()),
                        paused: None,
                        job: EnqueueOptions::new("t", "q", serde_json::json!({}))
                            .budget(BudgetBinding::new(budget).cost(cost)),
                    }],
                },
                NOW,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_is_refused_while_a_cron_entry_references_it() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 1).await;

        let result = store.delete_budget("stripe").await;
        assert!(matches!(result, Err(StoreError::Conflict(_))));

        // Still there — a refused delete changes nothing.
        assert!(store.get_budget("stripe").await.unwrap().is_some());
    }

    /// The refusal has to name something the operator can act on.
    #[tokio::test]
    async fn a_refused_delete_names_the_offending_entry() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 1).await;

        let Err(StoreError::Conflict(msg)) = store.delete_budget("stripe").await else {
            panic!("expected a conflict");
        };
        assert!(msg.contains("g/nightly"), "unhelpful message: {msg}");
    }

    #[tokio::test]
    async fn delete_is_allowed_once_the_entry_is_gone() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 1).await;

        store.delete_cron_group("g").await.unwrap();
        assert!(store.delete_budget("stripe").await.unwrap());
    }

    /// A budget nothing references deletes as before.
    #[tokio::test]
    async fn delete_is_unaffected_by_unrelated_cron_entries() {
        let store = test_store();
        for key in ["stripe", "ses"] {
            store
                .create_budget(key, 10, BudgetStrategy::WhileInFlight, NOW)
                .await
                .unwrap();
        }
        install_cron_drawing(&store, "ses", 1).await;

        assert!(store.delete_budget("stripe").await.unwrap());
    }

    #[tokio::test]
    async fn patch_cannot_shrink_below_a_cron_template_cost() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 5).await;

        let result = store
            .patch_budget("stripe", patch(Some(4), None), NOW + 1)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        assert_eq!(
            store
                .get_budget("stripe")
                .await
                .unwrap()
                .unwrap()
                .allocation,
            10
        );
    }

    /// Shrinking *to* the template's cost still fits, so it is allowed —
    /// the entry can still fire, just with nothing to spare.
    #[tokio::test]
    async fn patch_may_shrink_exactly_to_the_cron_template_cost() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 5).await;

        let patched = store
            .patch_budget("stripe", patch(Some(5), None), NOW + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(patched.allocation, 5);
    }

    #[tokio::test]
    async fn put_cannot_shrink_below_a_cron_template_cost() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 5).await;

        let result = store
            .put_budget("stripe", 4, BudgetStrategy::WhileInFlight, NOW + 1)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// Growing is always fine, and so is any change that keeps the
    /// template fitting.
    #[tokio::test]
    async fn patch_may_grow_a_referenced_budget() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 5).await;

        let patched = store
            .patch_budget("stripe", patch(Some(100), None), NOW + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(patched.allocation, 100);
    }

    // --- the same protections, for jobs rather than cron entries ---

    /// Enqueue a job drawing `cost` from `budget`, which must exist.
    async fn enqueue_drawing(store: &Store, budget: &str, cost: u32) -> String {
        use super::super::super::options::EnqueueOptions;
        use super::super::BudgetBinding;

        store
            .enqueue(
                NOW,
                EnqueueOptions::new("t", "q", serde_json::json!({}))
                    .budget(BudgetBinding::new(budget).cost(cost)),
            )
            .await
            .unwrap()
            .into_job()
            .id
    }

    /// Deleting here is worse than it sounds: dropping the budget drops
    /// the group with it, and every job waiting on that group leaves
    /// dispatch for good — a restart cannot bring them back, because the
    /// budget they would park on no longer exists.
    #[tokio::test]
    async fn delete_is_refused_while_a_job_references_it() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        enqueue_drawing(&store, "stripe", 1).await;

        let result = store.delete_budget("stripe").await;
        assert!(matches!(result, Err(StoreError::Conflict(_))));

        assert!(store.get_budget("stripe").await.unwrap().is_some());
    }

    /// Jobs drain, so the refusal should point at waiting as a way out —
    /// which is the one remedy the cron message must never offer.
    #[tokio::test]
    async fn a_job_refusal_offers_waiting_as_a_remedy() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        enqueue_drawing(&store, "stripe", 1).await;

        let Err(StoreError::Conflict(msg)) = store.delete_budget("stripe").await else {
            panic!("expected a conflict");
        };
        assert!(msg.contains("1 unfinished job"), "unhelpful message: {msg}");
        assert!(msg.contains("wait"), "unhelpful message: {msg}");
    }

    #[tokio::test]
    async fn delete_is_allowed_once_the_job_is_gone() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        let id = enqueue_drawing(&store, "stripe", 1).await;

        assert!(store.delete_job(&id).await.unwrap());

        assert!(store.delete_budget("stripe").await.unwrap());
    }

    #[tokio::test]
    async fn delete_is_unaffected_by_jobs_on_other_budgets() {
        let store = test_store();
        for key in ["stripe", "mailgun"] {
            store
                .create_budget(key, 10, BudgetStrategy::WhileInFlight, NOW)
                .await
                .unwrap();
        }
        enqueue_drawing(&store, "mailgun", 1).await;

        assert!(store.delete_budget("stripe").await.unwrap());
    }

    #[tokio::test]
    async fn patch_cannot_shrink_below_a_queued_job_cost() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        enqueue_drawing(&store, "stripe", 5).await;

        let result = store
            .patch_budget("stripe", patch(Some(4), None), NOW + 1)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// A job costing exactly the allocation can still run, so the shrink
    /// that lands on it is allowed. Only going below strands anything.
    #[tokio::test]
    async fn patch_may_shrink_exactly_to_a_queued_job_cost() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        enqueue_drawing(&store, "stripe", 5).await;

        let patched = store
            .patch_budget("stripe", patch(Some(5), None), NOW + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(patched.allocation, 5);
    }

    #[tokio::test]
    async fn put_cannot_shrink_below_a_queued_job_cost() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        enqueue_drawing(&store, "stripe", 5).await;

        let result = store
            .put_budget("stripe", 4, BudgetStrategy::WhileInFlight, NOW + 1)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// Both halves of the tracked set are consulted, and the tighter one
    /// wins regardless of which it is. Here the job is dearer than the
    /// cron template, so a shrink that the cron check alone would wave
    /// through still has to fail.
    #[tokio::test]
    async fn the_dearer_of_a_job_and_a_cron_entry_decides() {
        let store = test_store();
        store
            .create_budget("stripe", 20, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install_cron_drawing(&store, "stripe", 3).await;
        enqueue_drawing(&store, "stripe", 9).await;

        // Fits the cron template, but not the job.
        let result = store
            .patch_budget("stripe", patch(Some(5), None), NOW + 1)
            .await;
        let Err(StoreError::InvalidOperation(msg)) = result else {
            panic!("expected the job to block the shrink");
        };
        assert!(msg.contains("9"), "should cite the job's cost: {msg}");

        // Above both, so it is allowed.
        let patched = store
            .patch_budget("stripe", patch(Some(9), None), NOW + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(patched.allocation, 9);
    }
}
