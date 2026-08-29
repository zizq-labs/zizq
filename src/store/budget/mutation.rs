// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Changing which budgets a job draws on.
//!
//! Every route for managing a job's bindings maps onto one variant of
//! [`BudgetMutation`], so the rules about what may replace what live
//! here once rather than per endpoint.
//!
//! # Why a mutation rather than a new set of bindings
//!
//! The obvious alternative — have the caller read a job's bindings,
//! change them, and hand back the whole set — cannot be made atomic
//! without holding a lock across the read and the write. Describing the
//! *change* lets the store apply it inside the transaction that reads
//! the job, so two callers adding different budgets to one job cannot
//! lose each other's work.
//!
//! It is also what makes the bulk routes possible at all. "Add this
//! budget to everything matching" is one mutation applied to N jobs;
//! expressed as whole sets it would be N different documents the caller
//! would have to compute from N reads.
//!
//! # Nothing here touches the keyspace
//!
//! Deliberately pure: given the bindings a job has, work out the ones it
//! should have. Whether those budgets exist, whether their costs fit,
//! where the job then sits in the dispatch groups, and what that does to
//! the cost accounting are all the transaction's problem, and are tested
//! separately from the question of what the caller asked for.

// Everything here is consumed by the store operation that applies a
// mutation inside a transaction, and by the routes over it. Staged as
// one attribute rather than six, since it comes off in one go.
#![allow(
    dead_code,
    reason = "consumed by the job-budget store operation and its routes"
)]

use super::{BudgetBinding, BudgetRef};

/// A change to the set of budgets a job draws on.
///
/// Names the *operation*, not the resulting state, so that the same
/// value applies correctly to any job regardless of what it is bound to
/// already.
#[derive(Debug, Clone)]
pub(in crate::store) enum BudgetMutation {
    /// Replace every binding with these.
    ///
    /// The one whole-set operation, and the reason it is confined to a
    /// single addressed job: replacing a set is only a sound thing to
    /// ask for when the caller knows what is being replaced, which over
    /// a filtered selection they do not.
    ReplaceAll(Vec<BudgetBinding>),

    /// Bind a budget the job does not already draw on.
    ///
    /// Refuses rather than overwrites, so a caller adding a binding
    /// cannot silently change a cost someone else set.
    Add(BudgetBinding),

    /// Bind a budget, replacing any existing binding to it.
    Set(BudgetBinding),

    /// Change the cost of a binding the job already has.
    ///
    /// Distinct from [`Self::Set`] in refusing to create one. A caller
    /// adjusting a cost has a job in mind that it believes is already
    /// throttled, and creating the binding instead would silently
    /// throttle a job that was not.
    SetCost { key: String, cost: u32 },

    /// Unbind one budget.
    Remove { key: String },

    /// Unbind every budget, leaving the job unthrottled.
    RemoveAll,
}

/// What applying a mutation did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::store) enum BudgetMutationOutcome {
    /// The bindings changed, and these are the new ones.
    Changed(Vec<BudgetRef>),

    /// The job already satisfied the request, so there is nothing to
    /// write.
    ///
    /// Worth distinguishing from `Changed`: a job whose bindings did not
    /// move does not need its record rewritten, its placement disturbed,
    /// or its cost accounting touched — and a bulk operation wants to
    /// report how many jobs it actually changed rather than how many it
    /// looked at.
    Unchanged,
}

/// Why a mutation could not be applied.
///
/// Reported rather than absorbed, because the two callers want opposite
/// things from them. A route addressing one job turns them into `409`
/// and `404` — the caller specified a job and was wrong about it. A bulk
/// route skips the job and moves on, since "add this budget to
/// everything matching" is not wrong about the ones already bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::store) enum BudgetMutationError {
    /// [`BudgetMutation::Add`] against a budget the job already draws on.
    AlreadyBound(String),

    /// [`BudgetMutation::SetCost`] against one it does not.
    NotBound(String),

    /// The requested set names one budget more than once.
    ///
    /// A job draws on a budget once, at one cost; two entries for the
    /// same key are a contradiction rather than an addition, and
    /// silently keeping the last would be a coin toss over which the
    /// caller meant.
    DuplicateKey(String),
}

impl BudgetMutation {
    /// Work out the bindings a job should have after this change.
    ///
    /// `current` is what it has now. Returns [`Unchanged`] when the two
    /// would be the same, so the caller can skip the write entirely.
    ///
    /// [`Unchanged`]: BudgetMutationOutcome::Unchanged
    pub(in crate::store) fn apply(
        &self,
        current: &[BudgetRef],
    ) -> Result<BudgetMutationOutcome, BudgetMutationError> {
        let next = match self {
            Self::ReplaceAll(bindings) => to_refs(bindings)?,

            Self::Add(binding) => {
                if let Some(existing) = find(current, &binding.key) {
                    return Err(BudgetMutationError::AlreadyBound(existing.key.clone()));
                }
                let mut next = current.to_vec();
                next.push(binding.to_ref());
                next
            }

            Self::Set(binding) => {
                let mut next = current.to_vec();
                match next.iter_mut().find(|r| r.key == binding.key) {
                    // Replaced whole rather than field by field. The key
                    // is already known equal, so this is the same result
                    // today — but `Set` means "this binding is now that
                    // one", and saying it that way keeps it true when
                    // `BudgetRef` grows a field. Assigning `cost` alone
                    // would quietly drop the new one.
                    //
                    // `SetCost` deliberately does the opposite: it names
                    // the one field it changes, because that is what it
                    // means.
                    Some(existing) => *existing = binding.to_ref(),
                    None => next.push(binding.to_ref()),
                }
                next
            }

            Self::SetCost { key, cost } => {
                let mut next = current.to_vec();
                match next.iter_mut().find(|r| &r.key == key) {
                    Some(existing) => existing.cost = *cost,
                    None => return Err(BudgetMutationError::NotBound(key.clone())),
                }
                next
            }

            Self::Remove { key } => current.iter().filter(|r| &r.key != key).cloned().collect(),

            Self::RemoveAll => Vec::new(),
        };

        // Compared rather than assumed, because several of these reach
        // the same state by different routes — setting a cost to what it
        // already is, removing a budget that was never bound, replacing
        // a set with itself. None of them should cost a write.
        if next == current {
            Ok(BudgetMutationOutcome::Unchanged)
        } else {
            Ok(BudgetMutationOutcome::Changed(next))
        }
    }

    /// The bindings this mutation introduces, if any.
    ///
    /// What the transaction has to validate and possibly create: these
    /// carry `create_with`, which the stored form drops. Removals and
    /// cost changes introduce nothing, since a job cannot be bound to a
    /// budget that was never resolved in the first place.
    pub(in crate::store) fn introduced(&self) -> &[BudgetBinding] {
        match self {
            Self::ReplaceAll(bindings) => bindings,
            Self::Add(binding) | Self::Set(binding) => std::slice::from_ref(binding),
            Self::SetCost { .. } | Self::Remove { .. } | Self::RemoveAll => &[],
        }
    }
}

/// Narrow a requested set to its stored form, rejecting repeats.
fn to_refs(bindings: &[BudgetBinding]) -> Result<Vec<BudgetRef>, BudgetMutationError> {
    let mut refs: Vec<BudgetRef> = Vec::with_capacity(bindings.len());

    for binding in bindings {
        if refs.iter().any(|r| r.key == binding.key) {
            return Err(BudgetMutationError::DuplicateKey(binding.key.clone()));
        }
        refs.push(binding.to_ref());
    }

    Ok(refs)
}

/// The binding to `key`, if the job has one.
fn find<'a>(current: &'a [BudgetRef], key: &str) -> Option<&'a BudgetRef> {
    current.iter().find(|r| r.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drawn(key: &str, cost: u32) -> BudgetRef {
        BudgetRef::new(key).cost(cost)
    }

    fn binding(key: &str, cost: u32) -> BudgetBinding {
        BudgetBinding::new(key).cost(cost)
    }

    fn changed(outcome: BudgetMutationOutcome) -> Vec<BudgetRef> {
        match outcome {
            BudgetMutationOutcome::Changed(refs) => refs,
            BudgetMutationOutcome::Unchanged => panic!("expected a change"),
        }
    }

    // --- ReplaceAll ---

    #[test]
    fn replace_all_takes_the_requested_set_whole() {
        let current = [drawn("stripe", 1), drawn("mailgun", 2)];
        let mutation = BudgetMutation::ReplaceAll(vec![binding("ses", 5)]);

        assert_eq!(
            changed(mutation.apply(&current).unwrap()),
            vec![drawn("ses", 5)]
        );
    }

    #[test]
    fn replace_all_with_an_empty_set_unbinds_everything() {
        let current = [drawn("stripe", 1)];
        let mutation = BudgetMutation::ReplaceAll(Vec::new());

        assert_eq!(changed(mutation.apply(&current).unwrap()), Vec::new());
    }

    /// Two entries for one key are a contradiction, not an addition —
    /// keeping the last would be a coin toss over which was meant.
    #[test]
    fn replace_all_rejects_a_repeated_key() {
        let mutation = BudgetMutation::ReplaceAll(vec![binding("stripe", 1), binding("stripe", 9)]);

        assert_eq!(
            mutation.apply(&[]),
            Err(BudgetMutationError::DuplicateKey("stripe".into()))
        );
    }

    // --- Add ---

    #[test]
    fn add_binds_a_budget_the_job_lacks() {
        let current = [drawn("stripe", 1)];
        let mutation = BudgetMutation::Add(binding("mailgun", 3));

        assert_eq!(
            changed(mutation.apply(&current).unwrap()),
            vec![drawn("stripe", 1), drawn("mailgun", 3)]
        );
    }

    /// Refusing rather than overwriting: a caller adding a binding must
    /// not silently change a cost someone else set.
    #[test]
    fn add_refuses_a_budget_already_bound() {
        let current = [drawn("stripe", 1)];
        let mutation = BudgetMutation::Add(binding("stripe", 9));

        assert_eq!(
            mutation.apply(&current),
            Err(BudgetMutationError::AlreadyBound("stripe".into()))
        );
    }

    // --- Set ---

    #[test]
    fn set_binds_when_absent_and_replaces_when_present() {
        let current = [drawn("stripe", 1)];

        assert_eq!(
            changed(
                BudgetMutation::Set(binding("stripe", 9))
                    .apply(&current)
                    .unwrap()
            ),
            vec![drawn("stripe", 9)]
        );
        assert_eq!(
            changed(
                BudgetMutation::Set(binding("ses", 4))
                    .apply(&current)
                    .unwrap()
            ),
            vec![drawn("stripe", 1), drawn("ses", 4)]
        );
    }

    /// Replacing a binding leaves it where it was rather than moving it
    /// to the end. Order is not meaningful to dispatch, but a read-back
    /// that reshuffles for no reason is a poor answer to a cost change.
    #[test]
    fn set_keeps_an_existing_binding_in_place() {
        let current = [drawn("a", 1), drawn("b", 2), drawn("c", 3)];
        let mutation = BudgetMutation::Set(binding("b", 9));

        assert_eq!(
            changed(mutation.apply(&current).unwrap()),
            vec![drawn("a", 1), drawn("b", 9), drawn("c", 3)]
        );
    }

    // --- SetCost ---

    #[test]
    fn set_cost_changes_an_existing_binding() {
        let current = [drawn("stripe", 1)];
        let mutation = BudgetMutation::SetCost {
            key: "stripe".into(),
            cost: 7,
        };

        assert_eq!(
            changed(mutation.apply(&current).unwrap()),
            vec![drawn("stripe", 7)]
        );
    }

    /// Unlike `Set`, this will not create one: a caller adjusting a cost
    /// believes the job is already throttled, and quietly throttling one
    /// that was not is the opposite of what they asked for.
    #[test]
    fn set_cost_refuses_a_budget_not_bound() {
        let mutation = BudgetMutation::SetCost {
            key: "stripe".into(),
            cost: 7,
        };

        assert_eq!(
            mutation.apply(&[]),
            Err(BudgetMutationError::NotBound("stripe".into()))
        );
    }

    // --- Remove ---

    #[test]
    fn remove_unbinds_one_and_leaves_the_rest() {
        let current = [drawn("stripe", 1), drawn("mailgun", 2)];
        let mutation = BudgetMutation::Remove {
            key: "stripe".into(),
        };

        assert_eq!(
            changed(mutation.apply(&current).unwrap()),
            vec![drawn("mailgun", 2)]
        );
    }

    #[test]
    fn remove_all_unbinds_everything() {
        let current = [drawn("stripe", 1), drawn("mailgun", 2)];

        assert_eq!(
            changed(BudgetMutation::RemoveAll.apply(&current).unwrap()),
            Vec::new()
        );
    }

    // --- Unchanged ---

    /// Several of these reach the state the job is already in, and none
    /// of them should cost a write, a placement move, or a change to the
    /// cost accounting.
    #[test]
    fn a_request_the_job_already_satisfies_changes_nothing() {
        let current = [drawn("stripe", 1)];

        for mutation in [
            BudgetMutation::Set(binding("stripe", 1)),
            BudgetMutation::SetCost {
                key: "stripe".into(),
                cost: 1,
            },
            BudgetMutation::Remove {
                key: "mailgun".into(),
            },
            BudgetMutation::ReplaceAll(vec![binding("stripe", 1)]),
        ] {
            assert_eq!(
                mutation.apply(&current).unwrap(),
                BudgetMutationOutcome::Unchanged,
                "{mutation:?} should have been a no-op"
            );
        }
    }

    #[test]
    fn unbinding_an_already_unbound_job_changes_nothing() {
        assert_eq!(
            BudgetMutation::RemoveAll.apply(&[]).unwrap(),
            BudgetMutationOutcome::Unchanged
        );
    }

    // --- introduced ---

    /// The transaction validates and may create only what a mutation
    /// brings in. Removals and cost changes bring in nothing, so they
    /// need no budget resolution at all.
    #[test]
    fn only_binding_mutations_introduce_budgets() {
        assert_eq!(
            BudgetMutation::Add(binding("stripe", 1)).introduced().len(),
            1
        );
        assert_eq!(
            BudgetMutation::Set(binding("stripe", 1)).introduced().len(),
            1
        );
        assert_eq!(
            BudgetMutation::ReplaceAll(vec![binding("a", 1), binding("b", 2)])
                .introduced()
                .len(),
            2
        );

        assert!(
            BudgetMutation::SetCost {
                key: "stripe".into(),
                cost: 2
            }
            .introduced()
            .is_empty()
        );
        assert!(
            BudgetMutation::Remove {
                key: "stripe".into()
            }
            .introduced()
            .is_empty()
        );
        assert!(BudgetMutation::RemoveAll.introduced().is_empty());
    }
}
