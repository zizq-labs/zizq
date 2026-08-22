// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Live budget state: which jobs are waiting on what, and how many
//! tokens each budget has left.
//!
//! Stored policies live on disk; this is the running accounting built
//! on top of them. None of it is persisted — token state is rebuilt
//! full on startup, and waiting jobs are re-derived from the job
//! records.
//!
//! # Two separate things are being tracked
//!
//! **Queue membership** — which jobs are waiting on which budget — is
//! [`park`](Budgets::park) and [`unpark`](Budgets::unpark).
//!
//! **Tokens** — whether a budget can afford a job right now — is
//! [`try_acquire_all`](Budgets::try_acquire_all), and the two ways they
//! come back, [`refund`](Budgets::refund) and
//! [`release_concurrency`](Budgets::release_concurrency).
//!
//! They interleave rather than pair up:
//!
//! 1. A job becomes ready and *parks* on each budget it draws from. No
//!    tokens move; it is now merely a candidate.
//! 2. Dispatch takes the head of a group and tries to *acquire* from
//!    every budget that job names. Failing changes nothing and the job
//!    stays parked. Succeeding takes the tokens, and the job is
//!    *unparked* from every group at once.
//! 3. The job runs. If any of its budgets limit concurrency, their
//!    tokens come back when it stops being in flight.
//!
//! So an unpark without an acquire is ordinary: it is a job that left
//! the ready state without dispatching — deleted, patched elsewhere,
//! killed. There is nothing to give back because nothing was taken.
//!
//! # One lock over everything
//!
//! All of it sits behind a single `Mutex` rather than a lock per
//! budget. That makes an all-or-nothing acquire across several budgets
//! trivially atomic: no lock ordering to get right, no deadlock, and no
//! window where another dispatcher sees a partial acquire that is about
//! to be rolled back.
//!
//! Contention is bounded by what the feature *is*. A budget exists to
//! cap a dispatch rate, so successful acquires against it cannot exceed
//! its own allocation. Failed acquires are bounded too: a group with no
//! capacity stops being offered work, so dispatchers do not queue up on
//! the lock to be told "no".
//!
//! Rollback in [`try_acquire_all`](Budgets::try_acquire_all) is kept
//! even though one lock makes it unobservable, because it is what would
//! let this move to per-budget locks later without a redesign.
//!
//! # Jobs that use no budgets never take the lock
//!
//! [`waiting`](Budgets::waiting) is an atomic count of jobs parked on
//! budgets, readable without acquiring anything. Dispatch checks it
//! before reaching for the mutex, so a server with no budgeted work
//! pays one relaxed load — otherwise every claim would serialise on a
//! lock guarding state it has no interest in.
//!
//! The count settles to the truth whenever the lock is not held, but a
//! reader can catch it mid-update, so each path is ordered to make that
//! window harmless. Adding work counts up *first*; removing it counts
//! down *last*. Both leave the count briefly too high, which costs a
//! reader a pointless lock acquisition. The opposite ordering would
//! leave it briefly too low, which costs a dispatch.
//!
//! No ordering removes the need for a wakeup, though: a reader can
//! always load the count in the instant before work arrives and be
//! correct to skip. That is why anything making work available must
//! publish the count before signalling a waiting worker, never after —
//! the signal is what guarantees someone looks again.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::limiter::Limiter;
use super::{Budget, BudgetRef};

/// The jobs waiting on one budget, and its tokens.
struct BudgetGroup {
    /// Tokens, and how they come back.
    limiter: Limiter,

    /// Jobs waiting on this budget, ordered exactly as the ready index
    /// orders its own: priority first, then id, which is time-ordered.
    ///
    /// Holds identity only, not job records. The store is authoritative
    /// and a claimed job is re-read from it anyway, so keeping whole
    /// jobs here would be a second copy to go stale.
    jobs: BTreeSet<(u16, String)>,
}

/// Live accounting for every budget with a group.
pub(super) struct Budgets {
    inner: Mutex<HashMap<String, BudgetGroup>>,

    /// Jobs currently waiting on some budget.
    ///
    /// A job bound to several budgets counts once per budget, since it
    /// waits in each of their queues. Used only as a "is there anything
    /// at all" hint, so the multiplicity does not matter.
    waiting: AtomicUsize,
}

#[allow(dead_code, reason = "wired up when budgeted jobs reach dispatch")]
impl Budgets {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            waiting: AtomicUsize::new(0),
        }
    }

    /// Whether any job is waiting on any budget.
    ///
    /// Deliberately lock-free: this is the check that keeps unbudgeted
    /// dispatch off the mutex entirely.
    pub(super) fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }

    /// Bring a budget's live state in line with its stored policy,
    /// creating the group if this is the first time it has been seen.
    ///
    /// An existing group keeps its tokens and its waiting jobs — a
    /// policy change adjusts a running budget rather than resetting it,
    /// so tightening one does not hand out a fresh allocation.
    pub(super) fn sync(&self, key: &str, budget: &Budget, now: u64) {
        let mut groups = self.inner.lock().unwrap();

        match groups.get_mut(key) {
            Some(group) => group.limiter.adopt(budget, now),
            None => {
                groups.insert(
                    key.to_string(),
                    BudgetGroup {
                        limiter: Limiter::new(budget, now),
                        jobs: BTreeSet::new(),
                    },
                );
            }
        }
    }

    /// Drop a budget's live state.
    ///
    /// Any jobs still waiting on it are dropped with it, so this is
    /// only correct once nothing references the budget — which is what
    /// deletion being refused while references exist is for.
    pub(super) fn forget(&self, key: &str) {
        let mut groups = self.inner.lock().unwrap();

        if let Some(group) = groups.remove(key) {
            self.waiting.fetch_sub(group.jobs.len(), Ordering::Relaxed);
        }
    }

    /// Record that a job is waiting on every budget it draws from.
    ///
    /// Queue membership only — no tokens are touched. The job becomes a
    /// candidate for dispatch from each of these groups, and is
    /// dispatched from whichever one gets to it first.
    ///
    /// A job that draws on several budgets waits in all of their
    /// queues, because any of them could be the one that frees up.
    /// Whichever group offers it, the acquire still has to satisfy
    /// every budget it names.
    pub(super) fn park(&self, budgets: &[BudgetRef], priority: u16, job_id: &str) {
        // Counted up before the jobs land, unlike the removal paths
        // which count down afterwards. Both orderings leave a window
        // where the count disagrees with the queues, and the point is
        // to choose the harmless disagreement in each direction: a
        // count that is briefly too high costs a reader a wasted lock
        // acquisition, where one that is briefly too low costs a
        // dispatch that never happens.
        self.waiting.fetch_add(budgets.len(), Ordering::Relaxed);

        let mut groups = self.inner.lock().unwrap();
        let mut parked = 0;

        for reference in budgets {
            // A missing group means a job referencing a budget that was
            // never synced, which the install-time and deletion
            // protections exist to prevent. Skipped rather than
            // conjured: inventing a group here would invent an
            // allocation to go with it.
            if let Some(group) = groups.get_mut(&reference.key)
                && group.jobs.insert((priority, job_id.to_string()))
            {
                parked += 1;
            }
        }

        // Give back what the optimistic count over-claimed: references
        // with no group, and ones the job was already parked on.
        self.waiting
            .fetch_sub(budgets.len() - parked, Ordering::Relaxed);
    }

    /// Record that a job is no longer waiting.
    ///
    /// Queue membership only, and deliberately says nothing about
    /// tokens. Two quite different things end here: a job that
    /// dispatched (its tokens were taken by an acquire, and come back
    /// later or not at all), and a job that left the ready state
    /// without ever running — deleted, patched to another queue,
    /// killed. The second took nothing and has nothing to return.
    ///
    /// Must clear the job from *every* group it was parked on, or it
    /// would keep being offered from the ones it was missed in.
    pub(super) fn unpark(&self, budgets: &[BudgetRef], priority: u16, job_id: &str) {
        let mut groups = self.inner.lock().unwrap();
        let mut removed = 0;

        for reference in budgets {
            if let Some(group) = groups.get_mut(&reference.key)
                && group.jobs.remove(&(priority, job_id.to_string()))
            {
                removed += 1;
            }
        }

        self.waiting.fetch_sub(removed, Ordering::Relaxed);
    }

    /// The job at the front of a budget's queue, if any.
    pub(super) fn head(&self, key: &str) -> Option<(u16, String)> {
        let groups = self.inner.lock().unwrap();
        groups.get(key)?.jobs.first().cloned()
    }

    /// Take tokens from every budget, or none of them.
    ///
    /// Called when dispatch has chosen a job and needs to know whether
    /// it may run. Success means the tokens are spent and the caller
    /// should now [`unpark`](Self::unpark) the job from every group and
    /// dispatch it. Failure means the job stays exactly where it is.
    ///
    /// All-or-nothing: a short bucket anywhere leaves every bucket
    /// untouched, so a job bound to several budgets never holds part of
    /// what it needs while waiting for the rest. Without that, two jobs
    /// each holding half of what the other wants would deadlock the
    /// pair of budgets.
    ///
    /// A referenced budget with no group fails the acquire. Dispatching
    /// as though it were unlimited would be the one outcome worse than
    /// not dispatching.
    pub(super) fn try_acquire_all(&self, budgets: &[BudgetRef], now: u64) -> bool {
        let mut groups = self.inner.lock().unwrap();
        let mut taken: Vec<&BudgetRef> = Vec::with_capacity(budgets.len());

        for reference in budgets {
            let acquired = groups
                .get_mut(&reference.key)
                .is_some_and(|group| group.limiter.try_acquire(reference.cost, now));

            if acquired {
                taken.push(reference);
            } else {
                // Nothing outside this lock ever sees the partial
                // state, but the rollback is what would keep this
                // correct under per-budget locks.
                for undo in taken {
                    if let Some(group) = groups.get_mut(&undo.key) {
                        group.limiter.release(undo.cost);
                    }
                }
                return false;
            }
        }

        true
    }

    /// Undo an acquire whose dispatch did not happen after all.
    ///
    /// The acquire succeeds before the job is re-read from the store,
    /// and that read can find it deleted or no longer ready. The tokens
    /// were taken for work that will never run, so they go back —
    /// every budget, whatever its strategy, because nothing about the
    /// dispatch occurred.
    ///
    /// Distinct from [`release_concurrency`](Self::release_concurrency),
    /// which is about work that *did* run.
    pub(super) fn refund(&self, budgets: &[BudgetRef]) {
        let mut groups = self.inner.lock().unwrap();

        for reference in budgets {
            if let Some(group) = groups.get_mut(&reference.key) {
                group.limiter.release(reference.cost);
            }
        }
    }

    /// Return the slots a finished job was occupying.
    ///
    /// Called when a job stops being in flight — acknowledged, failed,
    /// or its worker vanished. Only budgets that limit *concurrency*
    /// are credited: their tokens represent a job currently running, so
    /// the token is free again once it stops.
    ///
    /// Budgets that limit a *rate* are deliberately skipped. Their
    /// tokens are spent by dispatching, not held for the duration, and
    /// the drip is already restoring them — crediting here as well
    /// would hand back a token twice and let the budget run over its
    /// rate. That matters for a job bound to one of each, where the
    /// same call covers both.
    pub(super) fn release_concurrency(&self, budgets: &[BudgetRef]) {
        let mut groups = self.inner.lock().unwrap();

        for reference in budgets {
            if let Some(group) = groups.get_mut(&reference.key)
                && group.limiter.returns_on_release()
            {
                group.limiter.release(reference.cost);
            }
        }
    }

    /// Forget every budget and everything waiting on one.
    pub(super) fn clear(&self) {
        let mut groups = self.inner.lock().unwrap();
        groups.clear();
        self.waiting.store(0, Ordering::Relaxed);
    }

    /// How many jobs are waiting on one budget.
    #[cfg(test)]
    fn depth(&self, key: &str) -> usize {
        let groups = self.inner.lock().unwrap();
        groups.get(key).map_or(0, |group| group.jobs.len())
    }
}

#[cfg(test)]
mod tests {
    use super::super::BudgetStrategy;
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    fn concurrency(allocation: u32) -> Budget {
        Budget::new(allocation, BudgetStrategy::WhileInFlight, NOW).unwrap()
    }

    fn per_minute(allocation: u32) -> Budget {
        Budget::new(
            allocation,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
            },
            NOW,
        )
        .unwrap()
    }

    fn draws(key: &str, cost: u32) -> BudgetRef {
        BudgetRef::new(key).cost(cost)
    }

    fn registry_with(pairs: &[(&str, Budget)]) -> Budgets {
        let budgets = Budgets::new();
        for (key, budget) in pairs {
            budgets.sync(key, budget, NOW);
        }
        budgets
    }

    #[test]
    fn a_fresh_registry_has_nothing_waiting() {
        let budgets = Budgets::new();
        assert_eq!(budgets.waiting(), 0);
    }

    #[test]
    fn parking_and_unparking_track_the_waiting_count() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, 0, "a");
        assert_eq!(budgets.waiting(), 1);
        assert_eq!(budgets.depth("stripe"), 1);

        budgets.unpark(&refs, 0, "a");
        assert_eq!(budgets.waiting(), 0);
        assert_eq!(budgets.depth("stripe"), 0);
    }

    /// A job bound to two budgets waits in both queues, so both must
    /// release it — otherwise it would linger in one forever.
    #[test]
    fn a_job_parks_on_every_budget_it_draws_from() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        let refs = vec![draws("stripe", 1), draws("tenant", 1)];

        budgets.park(&refs, 0, "a");
        assert_eq!(budgets.depth("stripe"), 1);
        assert_eq!(budgets.depth("tenant"), 1);

        budgets.unpark(&refs, 0, "a");
        assert_eq!(budgets.depth("stripe"), 0);
        assert_eq!(budgets.depth("tenant"), 0);
        assert_eq!(budgets.waiting(), 0);
    }

    /// Parking the same job twice must not inflate the count, or the
    /// lock-free hint would never fall back to zero.
    #[test]
    fn parking_the_same_job_twice_counts_once() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, 0, "a");
        budgets.park(&refs, 0, "a");
        assert_eq!(budgets.waiting(), 1);

        budgets.unpark(&refs, 0, "a");
        assert_eq!(budgets.waiting(), 0);
    }

    #[test]
    fn unparking_a_job_that_was_never_parked_is_harmless() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);

        budgets.unpark(&[draws("stripe", 1)], 0, "absent");

        assert_eq!(budgets.waiting(), 0);
    }

    #[test]
    fn the_head_is_the_highest_priority_oldest_job() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, 5, "later");
        budgets.park(&refs, 1, "urgent");
        budgets.park(&refs, 5, "earlier");

        assert_eq!(budgets.head("stripe"), Some((1, "urgent".into())));

        budgets.unpark(&refs, 1, "urgent");
        // Same priority falls back to id order, which is time order.
        assert_eq!(budgets.head("stripe"), Some((5, "earlier".into())));
    }

    #[test]
    fn acquiring_takes_from_every_budget() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        let refs = vec![draws("stripe", 2), draws("tenant", 3)];

        assert!(budgets.try_acquire_all(&refs, NOW));

        // Three left on stripe, two on tenant.
        assert!(budgets.try_acquire_all(&[draws("stripe", 3)], NOW));
        assert!(budgets.try_acquire_all(&[draws("tenant", 2)], NOW));
    }

    /// The property multi-budget composition rests on: a job that
    /// cannot have all of what it needs takes none of it, rather than
    /// stalling while holding half.
    #[test]
    fn a_failed_acquire_gives_back_what_it_took() {
        let budgets = registry_with(&[("plenty", concurrency(10)), ("scarce", concurrency(1))]);

        assert!(budgets.try_acquire_all(&[draws("scarce", 1)], NOW));

        // `plenty` is taken first and then rolled back when `scarce`
        // comes up short.
        assert!(!budgets.try_acquire_all(&[draws("plenty", 10), draws("scarce", 1)], NOW));

        // Untouched, so an unrelated job can still use it in full.
        assert!(budgets.try_acquire_all(&[draws("plenty", 10)], NOW));
    }

    #[test]
    fn finishing_returns_concurrency_slots() {
        let budgets = registry_with(&[("stripe", concurrency(2)), ("tenant", concurrency(2))]);
        let refs = vec![draws("stripe", 2), draws("tenant", 2)];

        assert!(budgets.try_acquire_all(&refs, NOW));
        assert!(!budgets.try_acquire_all(&refs, NOW));

        budgets.release_concurrency(&refs);

        assert!(budgets.try_acquire_all(&refs, NOW));
    }

    /// A rate limit's tokens are spent by dispatching, not held for the
    /// duration of the job. The drip restores them, so crediting on
    /// completion as well would hand the same token back twice and let
    /// the budget exceed its rate.
    #[test]
    fn finishing_does_not_credit_a_rate_limit() {
        let budgets = registry_with(&[("stripe", per_minute(60))]);
        let refs = vec![draws("stripe", 60)];

        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.release_concurrency(&refs);

        // Still empty: only the clock refills this one.
        assert!(!budgets.try_acquire_all(&[draws("stripe", 1)], NOW));
    }

    /// The case that makes the distinction load-bearing rather than
    /// academic: one job, one budget of each kind, one completion.
    #[test]
    fn finishing_credits_only_the_concurrency_half_of_a_mixed_job() {
        let budgets = registry_with(&[("rate", per_minute(60)), ("slots", concurrency(1))]);
        let refs = vec![draws("rate", 60), draws("slots", 1)];

        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.release_concurrency(&refs);

        // The slot is free again...
        assert!(budgets.try_acquire_all(&[draws("slots", 1)], NOW));
        // ...but the rate limit is still spent.
        assert!(!budgets.try_acquire_all(&[draws("rate", 1)], NOW));
    }

    /// A dispatch that falls through after acquiring — the job turned
    /// out to be stale — took tokens for work that never ran, so every
    /// budget gets them back regardless of strategy.
    #[test]
    fn refunding_credits_every_budget() {
        let budgets = registry_with(&[("rate", per_minute(60)), ("slots", concurrency(1))]);
        let refs = vec![draws("rate", 60), draws("slots", 1)];

        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.refund(&refs);

        assert!(budgets.try_acquire_all(&refs, NOW));
    }

    /// Dispatching as though an unknown budget were unlimited is the
    /// one outcome worse than not dispatching at all.
    #[test]
    fn acquiring_against_an_unknown_budget_fails() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);

        assert!(!budgets.try_acquire_all(&[draws("absent", 1)], NOW));
    }

    /// And it must not quietly consume from the budgets alongside it.
    #[test]
    fn an_unknown_budget_rolls_back_its_companions() {
        let budgets = registry_with(&[("stripe", concurrency(1))]);

        assert!(!budgets.try_acquire_all(&[draws("stripe", 1), draws("absent", 1)], NOW));

        assert!(budgets.try_acquire_all(&[draws("stripe", 1)], NOW));
    }

    /// A policy change adjusts a running budget rather than resetting
    /// it — otherwise tightening one during an incident would hand out
    /// a fresh allocation on the way through.
    #[test]
    fn syncing_an_existing_budget_keeps_its_tokens() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);
        assert!(budgets.try_acquire_all(&[draws("stripe", 10)], NOW));

        budgets.sync("stripe", &concurrency(10), NOW);

        assert!(!budgets.try_acquire_all(&[draws("stripe", 1)], NOW));
    }

    #[test]
    fn syncing_an_existing_budget_keeps_its_waiting_jobs() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);
        budgets.park(&[draws("stripe", 1)], 0, "a");

        budgets.sync("stripe", &concurrency(20), NOW);

        assert_eq!(budgets.depth("stripe"), 1);
        assert_eq!(budgets.waiting(), 1);
    }

    #[test]
    fn syncing_applies_a_tightened_allocation() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);

        budgets.sync("stripe", &concurrency(2), NOW);

        assert!(!budgets.try_acquire_all(&[draws("stripe", 3)], NOW));
        assert!(budgets.try_acquire_all(&[draws("stripe", 2)], NOW));
    }

    #[test]
    fn a_time_based_budget_recovers_on_the_clock() {
        let budgets = registry_with(&[("stripe", per_minute(60))]);
        assert!(budgets.try_acquire_all(&[draws("stripe", 60)], NOW));

        assert!(!budgets.try_acquire_all(&[draws("stripe", 1)], NOW));
        assert!(budgets.try_acquire_all(&[draws("stripe", 1)], NOW + 1_000));
    }

    #[test]
    fn forgetting_a_budget_drops_its_waiting_jobs_from_the_count() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        budgets.park(&[draws("stripe", 1)], 0, "a");
        budgets.park(&[draws("stripe", 1)], 0, "b");
        assert_eq!(budgets.waiting(), 2);

        budgets.forget("stripe");

        assert_eq!(budgets.waiting(), 0);
        assert_eq!(budgets.depth("stripe"), 0);
    }

    #[test]
    fn clearing_forgets_everything() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        budgets.park(&[draws("stripe", 1), draws("tenant", 1)], 0, "a");

        budgets.clear();

        assert_eq!(budgets.waiting(), 0);
        assert!(!budgets.try_acquire_all(&[draws("stripe", 1)], NOW));
    }
}
