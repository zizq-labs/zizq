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

use super::budget::BudgetRef;
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

    /// Budgets the job draws on. Empty for an unthrottled job.
    ///
    /// Carried but not yet read: routing on it arrives with the budget
    /// groups. It is part of the signature now so that landing those
    /// changes one function body rather than all thirteen call sites
    /// again.
    #[allow(dead_code, reason = "read once budgeted jobs route into groups")]
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
}

impl Dispatch {
    pub(super) fn new() -> Self {
        Self {
            ready: ReadyIndex::new(),
        }
    }

    /// Total number of ready jobs.
    pub(super) fn len(&self) -> usize {
        self.ready.len()
    }

    /// Place a job into the ready state.
    pub(super) fn insert(&self, at: Placement<'_>) {
        self.ready.insert(at.queue, at.priority, at.id.to_string());
    }

    /// Take a job out of the ready state.
    pub(super) fn remove(&self, at: Placement<'_>) {
        self.ready.remove(at.queue, at.priority, at.id);
    }

    /// Iterate ready jobs in priority order, yielding `(priority, id)`.
    ///
    /// Callers use this to list what is queued, so it has to cover every
    /// ready job — including ones a budget is currently holding back.
    /// A throttled job is still queued, and omitting it would make it
    /// invisible to `GET /jobs?status=ready`.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u16, String)> + '_ {
        self.ready.iter()
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
        self.ready.claim(queues)
    }
}

#[cfg(test)]
mod tests {
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
        let dispatch = Dispatch::new();
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
        let dispatch = Dispatch::new();
        dispatch.insert(placement("a", &[]));

        dispatch.remove(placement("a", &[]));

        assert_eq!(dispatch.len(), 0);
    }

    /// Budgets ride along on the placement without changing where an
    /// unthrottled job goes — the routing they will drive lands with
    /// the budget groups.
    #[test]
    fn a_budgeted_job_is_placed_like_any_other_for_now() {
        let dispatch = Dispatch::new();
        let budgets = vec![BudgetRef::new("stripe")];
        dispatch.insert(placement("a", &budgets));

        assert_eq!(dispatch.len(), 1);
        assert!(dispatch.claim(&HashSet::new()).is_some());
    }

    #[test]
    fn removing_a_job_that_was_never_placed_is_harmless() {
        let dispatch = Dispatch::new();
        dispatch.remove(placement("absent", &[]));

        assert_eq!(dispatch.len(), 0);
    }

    #[test]
    fn iter_yields_ready_jobs_in_priority_order() {
        let dispatch = Dispatch::new();
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
