// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Where ready jobs live, and how they are handed out.
//!
//! Every transition into and out of the ready state goes through here
//! rather than touching an index directly. That matters because not all
//! ready jobs will live in the same place: a job bound to a budget is
//! dispatchable only while that budget has tokens, so it belongs in a
//! per-budget group outside the priority index — otherwise the
//! dispatcher would have to walk past jobs it cannot dispatch, which is
//! is cost that should not be incurred globally.
//!
//! Routing therefore needs one home, and this is it. Today every job
//! goes to the priority index; the shape is what the budget groups slot
//! into.
//!
//! # Both directions describe the placement
//!
//! [`insert`](Dispatch::insert) and [`remove`](Dispatch::remove) each
//! take a [`Placement`], derived from the job record. Neither consults
//! any stored note about where a job went.
//!
//! An earlier draft had the dispatcher remember placements in a side
//! map so that removal did not need the bindings. That is unsound: the
//! map and the index are separate structures, so a removal racing an
//! insert of the same job can tear up the note before it is written,
//! leaving a job placed with nothing recording where. Once budgeted
//! jobs live in groups, such a job is never removed from its group, and
//! the tokens acquired on its behalf are never returned — a
//! `while_in_flight` budget of one would wedge permanently.
//!
//! The index tolerates races because it is advisory: `take` re-reads
//! each claimed job from disk and discards anything stale. A record of
//! *where a job was put* cannot be advisory, because nothing else knows.
//! So placement is derived from the job on both sides instead, and the
//! two directions cannot disagree.

use std::collections::HashSet;
use std::iter::Peekable;
use std::sync::Arc;

use super::budget::{BudgetRef, Budgets};
use super::ready_index::ReadyIndex;
use super::types::Job;

/// Where a ready job sits, and what governs whether it can dispatch.
///
/// Grouped rather than passed positionally because `queue` and `id` are
/// both strings and sit next to each other — transposing them compiles
/// and then silently fails to find anything.
#[derive(Clone, Copy)]
pub(super) struct Placement<'a> {
    /// Queue the job belongs to.
    pub(super) queue: &'a str,

    /// Priority, lower being higher.
    pub(super) priority: u16,

    /// The job's id.
    pub(super) id: &'a str,

    /// Budgets the job draws on. Empty for an unthrottled job, which
    /// is what decides where it is placed.
    pub(super) budgets: &'a [BudgetRef],
}

impl<'a> Placement<'a> {
    /// The placement a job record describes.
    ///
    /// Not usable for a patch, which moves a job between two
    /// placements: the record holds only where it is going, and the
    /// removal needs where it was.
    pub(super) fn of(job: &'a Job) -> Self {
        Self {
            queue: &job.queue,
            priority: job.priority,
            id: &job.id,
            budgets: &job.budgets,
        }
    }
}

/// Owns the placement of every ready job.
pub(super) struct Dispatch {
    /// Priority index of ready jobs that nothing throttles.
    ready: ReadyIndex,

    /// Live budget state, holding the jobs that something does.
    ///
    /// Shared with the store, which syncs it as budget policies are
    /// created and changed.
    budgets: Arc<Budgets>,
}

impl Dispatch {
    pub(super) fn new(budgets: Arc<Budgets>) -> Self {
        Self {
            ready: ReadyIndex::new(),
            budgets,
        }
    }

    /// Total number of ready jobs, throttled or not.
    ///
    /// A throttled job is still ready — it is waiting on capacity, not
    /// on anything about its own state — so it counts here.
    pub(super) fn len(&self) -> usize {
        self.ready.len() + self.budgets.job_count()
    }

    /// Place a job into the ready state.
    ///
    /// A job bound to budgets goes to their groups rather than the
    /// priority index. That is the whole point of the split: the
    /// dispatcher never has to walk past a job it cannot dispatch, so a
    /// backed-up throttle costs unthrottled work nothing.
    pub(super) fn insert(&self, at: Placement<'_>) {
        if at.budgets.is_empty() {
            self.ready.insert(at.queue, at.priority, at.id.to_string());
        } else {
            self.budgets.park(at.budgets, at.queue, at.priority, at.id);
        }
    }

    /// Take a job out of the ready state.
    pub(super) fn remove(&self, at: Placement<'_>) {
        if at.budgets.is_empty() {
            self.ready.remove(at.queue, at.priority, at.id);
        } else {
            self.budgets
                .unpark(at.budgets, at.queue, at.priority, at.id);
        }
    }

    /// Iterate ready jobs in priority order, yielding `(priority, id)`.
    ///
    /// Callers use this to list what is queued, so it has to cover every
    /// ready job — including ones a budget is currently holding back.
    /// A throttled job is still queued, and omitting it would make it
    /// invisible to `GET /jobs?status=ready`.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u16, String)> + '_ {
        // Both sides are already ordered by priority then id, so this
        // interleaves them rather than collecting and sorting.
        Merge {
            left: self.ready.iter().peekable(),
            right: self.budgets.entries().peekable(),
        }
    }

    /// Claim the highest-priority, oldest dispatchable job.
    ///
    /// Returns `(priority, job_id, queue)`, or `None` when nothing can
    /// be dispatched right now.
    ///
    /// Deliberately not a [`Placement`]: the caller re-reads the job
    /// from disk anyway — the index is advisory, and the record is what
    /// says whether the job is still there and still ready.
    pub(super) fn claim(&self, queues: &HashSet<String>) -> Option<(u16, String, String)> {
        // Budgeted jobs are parked but not yet offered: selecting one
        // means debiting its budgets, which lands with the acquire.
        // Until then they are visible to listings and invisible to
        // workers.
        self.ready.claim(queues)
    }
}

/// Interleaves two already-ordered streams of ready jobs.
///
/// Ready jobs live in two places, and a caller listing them wants one
/// sequence in dispatch order. Merging lazily keeps the priority index
/// side lazy too — a listing that takes ten jobs does not walk a
/// hundred thousand.
struct Merge<L: Iterator<Item = (u16, String)>, R: Iterator<Item = (u16, String)>> {
    left: Peekable<L>,
    right: Peekable<R>,
}

impl<L, R> Iterator for Merge<L, R>
where
    L: Iterator<Item = (u16, String)>,
    R: Iterator<Item = (u16, String)>,
{
    type Item = (u16, String);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.left.peek(), self.right.peek()) {
            (Some(left), Some(right)) => {
                if left <= right {
                    self.left.next()
                } else {
                    self.right.next()
                }
            }
            (Some(_), None) => self.left.next(),
            (None, _) => self.right.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::budget::{Budget, BudgetStrategy};
    use super::*;

    fn placement<'a>(id: &'a str, budgets: &'a [BudgetRef]) -> Placement<'a> {
        Placement {
            queue: "q",
            priority: 0,
            id,
            budgets,
        }
    }

    #[test]
    fn a_job_round_trips() {
        let dispatch = Dispatch::new(Arc::new(Budgets::new()));
        dispatch.insert(placement("a", &[]));

        assert_eq!(dispatch.len(), 1);
        assert_eq!(
            dispatch.claim(&HashSet::new()),
            Some((0, "a".into(), "q".into()))
        );
        assert_eq!(dispatch.len(), 0);
    }

    #[test]
    fn removing_takes_a_job_out() {
        let dispatch = Dispatch::new(Arc::new(Budgets::new()));
        dispatch.insert(placement("a", &[]));

        dispatch.remove(placement("a", &[]));

        assert_eq!(dispatch.len(), 0);
    }

    const NOW: u64 = 1_700_000_000_000;

    /// A dispatcher whose registry already knows about `stripe`, as it
    /// would in the store — nothing parks on a budget the registry has
    /// not been told about.
    fn with_stripe() -> (Dispatch, Arc<Budgets>) {
        let budgets = Arc::new(Budgets::new());
        let policy = Budget::new(10, BudgetStrategy::WhileInFlight, NOW).unwrap();
        budgets.sync("stripe", &policy, NOW);
        (Dispatch::new(budgets.clone()), budgets)
    }

    /// The heart of the split: a budgeted job is ready, and counted and
    /// listed as such, but it is not in the priority index — so a
    /// worker never walks past it, and it cannot be handed out until
    /// its budgets have been debited.
    #[test]
    fn a_budgeted_job_is_parked_rather_than_indexed() {
        let (dispatch, _budgets) = with_stripe();
        let refs = vec![BudgetRef::new("stripe")];

        dispatch.insert(placement("a", &refs));

        assert!(dispatch.claim(&HashSet::new()).is_none());
        assert_eq!(dispatch.len(), 1);
        assert_eq!(dispatch.iter().count(), 1);
    }

    #[test]
    fn removing_a_budgeted_job_takes_it_out_of_its_group() {
        let (dispatch, _budgets) = with_stripe();
        let refs = vec![BudgetRef::new("stripe")];

        dispatch.insert(placement("a", &refs));
        dispatch.remove(placement("a", &refs));

        assert_eq!(dispatch.len(), 0);
        assert_eq!(dispatch.iter().count(), 0);
    }

    /// Listings read from two places now, and have to look like one.
    #[test]
    fn iter_interleaves_both_sources_in_dispatch_order() {
        let (dispatch, _budgets) = with_stripe();
        let refs = vec![BudgetRef::new("stripe")];

        dispatch.insert(Placement {
            queue: "q",
            priority: 5,
            id: "plain-mid",
            budgets: &[],
        });
        dispatch.insert(Placement {
            queue: "q",
            priority: 1,
            id: "throttled-first",
            budgets: &refs,
        });
        dispatch.insert(Placement {
            queue: "q",
            priority: 9,
            id: "throttled-last",
            budgets: &refs,
        });

        let listed: Vec<(u16, String)> = dispatch.iter().collect();
        assert_eq!(
            listed,
            vec![
                (1, "throttled-first".into()),
                (5, "plain-mid".into()),
                (9, "throttled-last".into()),
            ]
        );
    }

    #[test]
    fn len_counts_both_sources() {
        let (dispatch, _budgets) = with_stripe();
        let refs = vec![BudgetRef::new("stripe")];

        dispatch.insert(placement("plain", &[]));
        dispatch.insert(placement("throttled", &refs));

        assert_eq!(dispatch.len(), 2);
    }

    #[test]
    fn removing_a_job_that_was_never_placed_is_harmless() {
        let dispatch = Dispatch::new(Arc::new(Budgets::new()));
        dispatch.remove(placement("absent", &[]));

        assert_eq!(dispatch.len(), 0);
    }

    #[test]
    fn iter_yields_ready_jobs_in_priority_order() {
        let dispatch = Dispatch::new(Arc::new(Budgets::new()));
        dispatch.insert(Placement {
            queue: "q",
            priority: 5,
            id: "b",
            budgets: &[],
        });
        dispatch.insert(Placement {
            queue: "q",
            priority: 1,
            id: "a",
            budgets: &[],
        });

        let entries: Vec<(u16, String)> = dispatch.iter().collect();
        assert_eq!(entries, vec![(1, "a".into()), (5, "b".into())]);
    }
}
