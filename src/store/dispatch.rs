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

/// A job handed to a worker, and what it cost to get it.
///
/// Carries only what the caller cannot get elsewhere. Priority and
/// queue are deliberately absent: the caller re-reads the job record,
/// which is authoritative for both.
pub(super) struct Claimed {
    pub(super) id: String,

    /// Budgets debited to release this job. Empty for an unthrottled
    /// one.
    ///
    /// Carried because a claim is not the end of the story: the caller
    /// re-reads the job and may find it deleted or no longer ready, and
    /// tokens spent on work that never runs have to go back. For a
    /// deleted job the record is gone, so this is the only remaining
    /// record of what was taken.
    pub(super) budgets: Vec<BudgetRef>,
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
    /// be dispatched right now. A budgeted job comes back only if its
    /// budgets could all be debited, and those tokens are already spent
    /// by the time it does.
    ///
    /// Deliberately not a [`Placement`]: the caller re-reads the job
    /// from disk anyway — the index is advisory, and the record is what
    /// says whether the job is still there and still ready.
    ///
    /// # Ordering across the two sources
    ///
    /// Unthrottled and budgeted jobs are compared on the same
    /// `(priority, id)` the priority index already orders by, so the
    /// existing guarantee — priority first, then oldest — holds across
    /// both. What it does *not* mean is that the best job overall
    /// always goes next: a budgeted job at the front can be passed over
    /// for a lower-priority unthrottled one when its budgets are short.
    /// That is the intended behaviour rather than a compromise, since
    /// waiting for it would be letting a throttle it does not apply to
    /// hold up unthrottled work.
    pub(super) fn claim(&self, queues: &HashSet<String>, now: u64) -> Option<Claimed> {
        // Nothing budgeted anywhere: the overwhelmingly common case,
        // and it must not pay for the machinery. One relaxed load, no
        // mutex, straight to the index.
        if self.budgets.waiting() == 0 {
            return self.ready.claim(queues).map(Self::unthrottled);
        }

        // Compare before committing. Claiming from the index is
        // destructive and debiting budgets is too, so whichever is
        // better has to be decided first.
        let plain = self.ready.peek(queues);
        let throttled = self.budgets.head_of_any(queues);
        let throttled_is_first = match (plain, throttled) {
            (Some(plain), Some(throttled)) => throttled < plain,
            (None, Some(_)) => true,
            _ => false,
        };

        if throttled_is_first && let Some(claimed) = self.budgets.claim_next(queues, now) {
            return Some(Self::throttled(claimed));
        }

        // Either the index had the better job, or the budgeted one
        // turned out to be unaffordable. Both end here.
        if let Some(claimed) = self.ready.claim(queues) {
            return Some(Self::unthrottled(claimed));
        }

        // The index emptied under us, or never had anything. A budgeted
        // job may still be affordable — without this a worker would be
        // told there is no work while a job it could have run sits
        // parked.
        if throttled_is_first {
            return None;
        }

        // Throttled is supposed to go second, and ready index was empty.
        self.budgets.claim_next(queues, now).map(Self::throttled)
    }

    /// Wrap a job taken from a budget group.
    fn throttled((_, id, _, budgets): (u16, String, String, Vec<BudgetRef>)) -> Claimed {
        Claimed { id, budgets }
    }

    /// Wrap a job taken straight from the priority index.
    fn unthrottled((_, id, _): (u16, String, String)) -> Claimed {
        Claimed {
            id,
            budgets: Vec::new(),
        }
    }

    /// Give back tokens debited for a job that will not run after all.
    ///
    /// A no-op for an unthrottled job, which took none.
    pub(super) fn refund(&self, budgets: &[BudgetRef]) {
        if !budgets.is_empty() {
            self.budgets.refund(budgets);
        }
    }

    /// Stop counting a job against the budgets it drew from.
    ///
    /// For jobs reaching a terminal state or being deleted outright —
    /// not for dispatch, which leaves a job counted because it is still
    /// unfinished. See `Budgets::track` for how the three pairs differ.
    ///
    /// **Call after the commit that ends the job, never before.** The
    /// aggregate is allowed to read high and never low: counting a
    /// finished job for a moment longer costs at worst a delete or
    /// shrink refused that would have been safe, where dropping the
    /// count before the write is durable would let a budget be deleted
    /// out from under a job that is still very much alive. The staging
    /// on the enqueue side leans the same way for the same reason.
    pub(super) fn untrack(&self, budgets: &[BudgetRef]) {
        self.budgets.untrack(budgets);
    }

    /// Hand back the capacity a job was occupying while it ran.
    ///
    /// For every exit from in-flight — acknowledged, failed into a
    /// retry, killed, deleted, or returned by a worker disconnect. What
    /// they have in common is the only thing that matters here: the job
    /// has stopped running, so a `while_in_flight` budget has a slot
    /// free again. Rate limits keep their tokens, which is the whole
    /// distinction between the two strategies — a `time_based` token
    /// says a dispatch happened, and one did.
    ///
    /// Distinct from [`Dispatch::refund`], which is for a job that never
    /// ran at all and gives back *every* budget's tokens. A disconnect
    /// is the case that makes the difference concrete: the job did
    /// dispatch and did run for some unknown time, so refunding the rate
    /// limit would let a flapping worker re-dispatch it for free.
    ///
    /// **Call after the commit that ends the job's flight.** Releasing
    /// first and then failing to commit would hand out a slot for a job
    /// that is still running; releasing after leaves the slot held a
    /// moment too long, which merely delays a dispatch.
    pub(super) fn release_concurrency(&self, budgets: &[BudgetRef]) {
        if !budgets.is_empty() {
            self.budgets.release_concurrency(budgets);
        }
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
        let claimed = dispatch.claim(&HashSet::new(), NOW).unwrap();
        assert_eq!(claimed.id, "a");
        assert!(claimed.budgets.is_empty());
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

    /// A budgeted job whose budget can pay is handed out like any
    /// other, and comes back carrying what it cost.
    #[test]
    fn a_budgeted_job_is_claimed_when_its_budget_can_pay() {
        let (dispatch, _budgets) = with_stripe();
        let refs = vec![BudgetRef::new("stripe")];

        dispatch.insert(placement("a", &refs));

        let claimed = dispatch.claim(&HashSet::new(), NOW).unwrap();
        assert_eq!(claimed.id, "a");
        // The bindings come back so the caller can refund them if the
        // job turns out not to be dispatchable after all.
        assert_eq!(claimed.budgets.len(), 1);
        assert_eq!(dispatch.len(), 0);
    }

    /// Drained, it stays put — still ready, still listed, just not
    /// offered to a worker.
    #[test]
    fn a_budgeted_job_is_withheld_when_its_budget_cannot_pay() {
        let (dispatch, budgets) = with_stripe();
        let refs = vec![BudgetRef::new("stripe")];

        // Spend the whole allocation before the job is considered.
        assert!(budgets.try_acquire_all(&[BudgetRef::new("stripe").cost(10)], NOW));

        dispatch.insert(placement("a", &refs));

        assert!(dispatch.claim(&HashSet::new(), NOW).is_none());
        assert_eq!(dispatch.len(), 1);
        assert_eq!(dispatch.iter().count(), 1);
    }

    /// The reason budgeted jobs live outside the priority index: a
    /// throttle that cannot pay must not hold up work it does not
    /// apply to, even work of much lower priority.
    #[test]
    fn a_withheld_job_does_not_block_unthrottled_work() {
        let (dispatch, budgets) = with_stripe();
        assert!(budgets.try_acquire_all(&[BudgetRef::new("stripe").cost(10)], NOW));

        dispatch.insert(Placement {
            queue: "q",
            priority: 0,
            id: "throttled",
            budgets: &[BudgetRef::new("stripe")],
        });
        dispatch.insert(Placement {
            queue: "q",
            priority: 9,
            id: "plain",
            budgets: &[],
        });

        let claimed = dispatch.claim(&HashSet::new(), NOW).unwrap();
        assert_eq!(claimed.id, "plain");
    }

    /// And when it can pay, priority still decides.
    #[test]
    fn a_payable_budgeted_job_outranks_a_lower_priority_plain_one() {
        let (dispatch, _budgets) = with_stripe();

        dispatch.insert(Placement {
            queue: "q",
            priority: 0,
            id: "throttled",
            budgets: &[BudgetRef::new("stripe")],
        });
        dispatch.insert(Placement {
            queue: "q",
            priority: 9,
            id: "plain",
            budgets: &[],
        });

        let claimed = dispatch.claim(&HashSet::new(), NOW).unwrap();
        assert_eq!(claimed.id, "throttled");
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
