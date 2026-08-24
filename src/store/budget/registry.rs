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

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::ready_index::ReadyIndex;
use super::costs::CostCounts;
use super::limiter::{Availability, Limiter};
use super::{Budget, BudgetRef};

/// Everything the mutex guards.
struct Inner {
    /// Per-budget token state and waiting jobs.
    groups: HashMap<String, BudgetGroup>,

    /// Budgets that currently have at least one job waiting.
    ///
    /// Selection consults this rather than every budget. A server may
    /// hold thousands of budgets while only a handful have work, and
    /// walking the idle ones on every claim would make dequeue cost
    /// scale with how many budgets are *configured* rather than how
    /// many are actually contended.
    occupied: HashSet<String>,

    /// What each waiting job draws on, keyed by job id.
    ///
    /// A group knows a job is waiting on *it*, but dispatching that job
    /// means debiting every budget it names, and the group has no way
    /// to know what the others are. Recorded here on the way in rather
    /// than re-read from the store at selection time, which would put a
    /// disk read inside the lock.
    ///
    /// Unlike the placement map an earlier draft of the dispatcher
    /// kept, this is not a second source of truth racing a first: it
    /// lives under the same mutex as the groups and is written by the
    /// same two functions, so the two cannot disagree.
    refs: HashMap<String, Vec<BudgetRef>>,
}

/// What the budget registry has for a dispatcher, and when to ask again.
///
/// The two fields answer different questions and are not alternatives: a
/// pass can find one budget with work ready to go and another that will
/// not be affordable for an hour.
#[derive(Debug, Default)]
#[allow(dead_code, reason = "consumed by the budget waker task")]
pub(in crate::store) struct BudgetWakeup {
    /// Parked jobs affordable at the time asked about, best first, as
    /// `(job_id, queue)`. The queue comes along because a waker has to
    /// tell workers *which* queue woke up — one filtering on `emails`
    /// should not be roused for a job in `reports`.
    pub(in crate::store) dispatchable: Vec<(String, String)>,

    /// The earliest a still-parked job becomes affordable on the clock
    /// alone.
    ///
    /// `None` means no timer would help: either nothing is parked, or
    /// everything parked is waiting on a release rather than a drip.
    /// Those callers wake on an event instead.
    pub(in crate::store) next_refill: Option<u64>,
}

/// The jobs waiting on one budget, and its tokens.
struct BudgetGroup {
    /// Tokens, and how they come back.
    limiter: Limiter,

    /// Jobs waiting on this budget.
    ///
    /// The same index the unthrottled jobs use, one instance per
    /// budget. It already solves exactly this problem — ordered by
    /// priority then id, with a per-queue view so a worker taking from
    /// named queues does not walk past jobs in queues it did not ask
    /// for.
    ///
    /// Its lock-free internals are redundant under the registry mutex.
    /// That is a deliberate trade: a budget exists to cap a dispatch
    /// rate, so this is by construction not a hot path, and shared
    /// correctness is worth more here than shaving uncontended atomics
    /// off a path the feature exists to slow down.
    ///
    /// Holds identity only, not job records. The store is authoritative
    /// and a claimed job is re-read from it anyway.
    jobs: ReadyIndex,

    /// What the jobs drawing on this budget cost.
    ///
    /// Wider than `jobs`, and deliberately so: a job counts here from
    /// enqueue until it reaches a terminal state, including while it is
    /// scheduled for the future or already in flight — neither of which
    /// is waiting in `jobs`. The guards this feeds have to consider a
    /// job that is not queued *yet* as much as one that is.
    costs: CostCounts,
}

/// Live accounting for every budget with a group.
pub(in crate::store) struct Budgets {
    inner: Mutex<Inner>,

    /// Every parked job, exactly once, whatever number of budgets it
    /// draws from.
    ///
    /// Deliberately outside the mutex. The lock exists so that an
    /// acquire spanning several budgets is atomic, and a reader
    /// counting or listing jobs has no stake in that — it wants a
    /// number, not a consistent view of token state. `ReadyIndex` is
    /// safe to read concurrently, which is why `ready_count` is
    /// lock-free today, and the same applies here: a count read
    /// mid-update is momentarily stale, and the next read corrects it.
    ///
    /// It exists because the per-budget groups cannot answer "how many
    /// jobs" without deduplication — a job waits in one queue per
    /// budget, so summing the groups would report a two-budget job
    /// twice. Feeding an inflated depth to `zizq top` would undermine
    /// every other number on the screen.
    all: ReadyIndex,

    /// Jobs currently waiting on some budget.
    ///
    /// A job bound to several budgets counts once per budget, since it
    /// waits in each of their queues. Used only as a "is there anything
    /// at all" hint, so the multiplicity does not matter.
    waiting: AtomicUsize,
}

impl Budgets {
    pub(in crate::store) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                groups: HashMap::new(),
                occupied: HashSet::new(),
                refs: HashMap::new(),
            }),
            all: ReadyIndex::new(),
            waiting: AtomicUsize::new(0),
        }
    }

    /// Whether any job is waiting on any budget.
    ///
    /// Deliberately lock-free: this is the check that keeps unbudgeted
    /// dispatch off the mutex entirely.
    #[allow(dead_code, reason = "used once dispatch selects from budget groups")]
    pub(in crate::store) fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }

    /// Bring a budget's live state in line with its stored policy,
    /// creating the group if this is the first time it has been seen.
    ///
    /// An existing group keeps its tokens and its waiting jobs — a
    /// policy change adjusts a running budget rather than resetting it,
    /// so tightening one does not hand out a fresh allocation.
    pub(in crate::store) fn sync(&self, key: &str, budget: &Budget, now: u64) {
        let mut inner = self.inner.lock().unwrap();

        match inner.groups.get_mut(key) {
            Some(group) => group.limiter.adopt(budget, now),
            None => {
                inner.groups.insert(
                    key.to_string(),
                    BudgetGroup {
                        limiter: Limiter::new(budget, now),
                        jobs: ReadyIndex::new(),
                        costs: CostCounts::default(),
                    },
                );
            }
        }
    }

    /// Record a job against every budget it draws from.
    ///
    /// This is the *lifecycle* pair, and the registry now has three of
    /// them. They answer different questions and span different periods,
    /// so it is worth being explicit about which is which:
    ///
    /// - `track` / `untrack` — does this budget have any unfinished job
    ///   at all? Spans enqueue to terminal, covering scheduled and
    ///   in-flight jobs that are not queued for dispatch. Feeds the
    ///   guards that refuse to delete or shrink a budget out from under
    ///   work that is already committed to it.
    /// - `park` / `unpark` — is this job waiting to be dispatched right
    ///   now? Spans ready to claimed. Feeds selection.
    /// - `try_acquire_all` / `refund` / `release_concurrency` — tokens.
    ///   Spans the dispatch itself.
    ///
    /// A job is tracked once per budget it names. Bindings are validated
    /// to name each budget at most once, so no job double-counts against
    /// one budget.
    ///
    /// A budget with no group is skipped, as in `park`: enqueue resolves
    /// and creates budgets before writing the job, so a missing group
    /// means state that should not exist, and inventing one here would
    /// invent an allocation with it.
    /// An unbudgeted job returns without taking the lock. Enqueue calls
    /// this for every job it writes, and most jobs draw on nothing —
    /// keeping them off the mutex entirely is the non-negotiable that
    /// says the feature costs nothing to those not using it.
    pub(in crate::store) fn track(&self, budgets: &[BudgetRef]) {
        if budgets.is_empty() {
            return;
        }

        let mut inner = self.inner.lock().unwrap();

        for reference in budgets {
            if let Some(group) = inner.groups.get_mut(&reference.key) {
                group.costs.add(reference.cost);
            }
        }
    }

    /// Drop a job from every budget it draws from.
    ///
    /// Called when a job reaches a terminal state or is deleted — not
    /// when it is merely dispatched. See [`Budgets::track`] for how this
    /// pair differs from the other two, and for why an unbudgeted job
    /// returns without taking the lock.
    pub(in crate::store) fn untrack(&self, budgets: &[BudgetRef]) {
        if budgets.is_empty() {
            return;
        }

        let mut inner = self.inner.lock().unwrap();

        for reference in budgets {
            if let Some(group) = inner.groups.get_mut(&reference.key) {
                group.costs.remove(reference.cost);
            }
        }
    }

    /// The largest cost any unfinished job draws from `key`.
    ///
    /// `None` when the budget has no group, or nothing draws on it. Both
    /// mean the same thing to a caller deciding whether an allocation is
    /// safe to shrink: there is no job to strand.
    #[allow(dead_code, reason = "used once the job-side guards call it")]
    pub(in crate::store) fn max_cost(&self, key: &str) -> Option<u32> {
        let inner = self.inner.lock().unwrap();

        inner.groups.get(key).and_then(|group| group.costs.max())
    }

    /// How many unfinished jobs draw on `key`.
    ///
    /// Zero for a budget with no group. The deletion guard reads this:
    /// a budget nothing references can go.
    #[allow(dead_code, reason = "used once the job-side guards call it")]
    pub(in crate::store) fn tracked(&self, key: &str) -> usize {
        let inner = self.inner.lock().unwrap();

        inner.groups.get(key).map_or(0, |group| group.costs.len())
    }

    /// Drop a budget's live state.
    ///
    /// Any jobs still waiting on it are dropped with it, so this is
    /// only correct once nothing references the budget — which is what
    /// deletion being refused while references exist is for.
    pub(in crate::store) fn forget(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(group) = inner.groups.remove(key) {
            inner.occupied.remove(key);
            self.waiting.fetch_sub(group.jobs.len(), Ordering::Relaxed);

            // Jobs left waiting only on this budget leave the roll-up
            // with it; ones that also draw on a surviving budget stay.
            // Normally an empty loop, since deleting a budget anything
            // references is refused.
            for (priority, id, queue) in group.jobs.iter_with_queue() {
                let elsewhere = inner
                    .groups
                    .values()
                    .any(|other| other.jobs.contains(priority, &id));
                if !elsewhere {
                    self.all.remove(&queue, priority, &id);
                }
            }
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
    pub(in crate::store) fn park(
        &self,
        budgets: &[BudgetRef],
        queue: &str,
        priority: u16,
        job_id: &str,
    ) {
        // Counted up before the jobs land, unlike the removal paths
        // which count down afterwards. Both orderings leave a window
        // where the count disagrees with the queues, and the point is
        // to choose the harmless disagreement in each direction: a
        // count that is briefly too high costs a reader a wasted lock
        // acquisition, where one that is briefly too low costs a
        // dispatch that never happens.
        self.waiting.fetch_add(budgets.len(), Ordering::Relaxed);

        let mut inner = self.inner.lock().unwrap();
        let mut parked = 0;
        let mut newly_occupied: Vec<String> = Vec::new();

        for reference in budgets {
            // A missing group means a job referencing a budget that was
            // never synced, which the install-time and deletion
            // protections exist to prevent. Skipped rather than
            // conjured: inventing a group here would invent an
            // allocation to go with it.
            if let Some(group) = inner.groups.get_mut(&reference.key) {
                // Measured rather than assumed: inserting a job already
                // parked here is a no-op, and counting it anyway would
                // leave the tally permanently above zero, pinning the
                // lock-free hint on forever.
                let before = group.jobs.len();
                group.jobs.insert(queue, priority, job_id.to_string());
                parked += group.jobs.len() - before;

                if before == 0 {
                    newly_occupied.push(reference.key.clone());
                }
            }
        }

        for key in newly_occupied {
            inner.occupied.insert(key);
        }

        if parked > 0 {
            inner.refs.insert(job_id.to_string(), budgets.to_vec());

            // Written under the lock alongside the groups, though read
            // without it. Idempotent, so a job on several budgets lands
            // here once.
            self.all.insert(queue, priority, job_id.to_string());
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
    pub(in crate::store) fn unpark(
        &self,
        budgets: &[BudgetRef],
        queue: &str,
        priority: u16,
        job_id: &str,
    ) {
        let mut inner = self.inner.lock().unwrap();
        let removed = Self::dequeue(&mut inner, budgets, queue, priority, job_id);

        if removed > 0 {
            // The job is leaving the ready state altogether, not just
            // one budget's queue, so it leaves the roll-up too.
            self.all.remove(queue, priority, job_id);
        }

        self.waiting.fetch_sub(removed, Ordering::Relaxed);
    }

    /// Take a job out of every budget queue it sits in, and forget what
    /// it drew on. Returns how many queues it left.
    ///
    /// The shared half of removing a job, whether that is because it
    /// dispatched or because it left the ready state some other way.
    /// Caller holds the lock and owns the roll-up and counter updates.
    fn dequeue(
        inner: &mut Inner,
        budgets: &[BudgetRef],
        queue: &str,
        priority: u16,
        job_id: &str,
    ) -> usize {
        let mut removed = 0;
        let mut emptied: Vec<String> = Vec::new();

        for reference in budgets {
            if let Some(group) = inner.groups.get_mut(&reference.key) {
                let before = group.jobs.len();
                group.jobs.remove(queue, priority, job_id);
                removed += before - group.jobs.len();

                if group.jobs.len() == 0 {
                    emptied.push(reference.key.clone());
                }
            }
        }

        for key in emptied {
            inner.occupied.remove(&key);
        }

        if removed > 0 {
            inner.refs.remove(job_id);
        }

        removed
    }

    /// The head of every budget that has work, best first.
    ///
    /// One candidate per budget: only a budget's own head can be next
    /// from within that budget, so there is nothing to gain from looking
    /// deeper.
    fn candidates(inner: &Inner, queues: &HashSet<String>) -> Vec<(u16, String, String)> {
        let mut candidates: Vec<(u16, String, String)> = inner
            .occupied
            .iter()
            .filter_map(|key| inner.groups.get(key)?.jobs.peek(queues))
            .collect();

        candidates.sort();
        // A job at the head of several budgets is one candidate.
        candidates.dedup();
        candidates
    }

    /// What a job is waiting for, across every budget it draws on.
    ///
    /// A job needs *all* of its budgets, so the answer is the most
    /// blocking of their individual answers. The ordering is
    /// `Never` > `OnRelease` > `At` > `Now`, and two of those pairings
    /// deserve saying out loud:
    ///
    /// - Two `At`s take the **later**. Being affordable on one budget
    ///   early does not help; the job goes when the last one catches up.
    /// - `OnRelease` beats `At`, so a job needing both a release and a
    ///   drip sets no timer. That is deliberate: a timer would fire
    ///   while the release was still outstanding and achieve nothing.
    ///   The release itself provokes a fresh look, and by then the
    ///   answer is a plain `At` that can be timed properly.
    ///
    /// A budget with no group answers `Never` rather than being skipped.
    /// Skipping would quietly treat the job as affordable on the budgets
    /// that *do* exist, and the install-time and deletion guards exist
    /// precisely so this cannot arise.
    fn availability_of(inner: &Inner, refs: &[BudgetRef], now: u64) -> Availability {
        let mut worst = Availability::Now;

        for reference in refs {
            let Some(group) = inner.groups.get(&reference.key) else {
                return Availability::Never;
            };

            worst = match (worst, group.limiter.next_available(reference.cost, now)) {
                (Availability::Never, _) | (_, Availability::Never) => Availability::Never,
                (Availability::OnRelease, _) | (_, Availability::OnRelease) => {
                    Availability::OnRelease
                }
                (Availability::At(a), Availability::At(b)) => Availability::At(a.max(b)),
                (Availability::At(t), Availability::Now)
                | (Availability::Now, Availability::At(t)) => Availability::At(t),
                (Availability::Now, Availability::Now) => Availability::Now,
            };
        }

        worst
    }

    /// What is dispatchable now, and when to look again.
    ///
    /// Answers both halves in one pass under one lock, because they are
    /// two views of the same question and a caller acting on
    /// inconsistent answers would either dispatch nothing or arm a timer
    /// for work it had already been handed.
    ///
    /// Only the head of each occupied budget is considered, matching
    /// `claim_next`: within a budget the order is strict FIFO, so a job
    /// behind the head cannot go first however cheap it is.
    ///
    /// Nothing is acquired. This reports what *could* happen, and the
    /// claim that follows may still lose a race to another worker —
    /// which is fine, since the caller's job is to provoke an attempt,
    /// not to guarantee it succeeds.
    #[allow(dead_code, reason = "consumed by the budget waker task")]
    pub(in crate::store) fn wakeup(&self, now: u64, limit: usize) -> BudgetWakeup {
        let mut out = BudgetWakeup::default();

        // The same lock-free hint the dispatch fast path uses. It reads
        // high rather than low, so a zero here really does mean nothing
        // is parked.
        if self.waiting.load(Ordering::Relaxed) == 0 {
            return out;
        }

        let inner = self.inner.lock().unwrap();
        let anywhere = HashSet::new();

        for (_, id, queue) in Self::candidates(&inner, &anywhere) {
            let Some(refs) = inner.refs.get(&id) else {
                continue;
            };

            match Self::availability_of(&inner, refs, now) {
                Availability::Now => {
                    if out.dispatchable.len() < limit {
                        out.dispatchable.push((id, queue));
                    }
                }
                Availability::At(at) => {
                    out.next_refill = Some(out.next_refill.map_or(at, |soonest| soonest.min(at)));
                }
                // Neither is worth a timer: one waits on an event, the
                // other on nothing at all.
                Availability::OnRelease | Availability::Never => {}
            }
        }

        out
    }

    /// The next job a budget would offer, if any.
    ///
    /// Yields `(priority, job_id, queue)`, matching what the ready
    /// index hands back from a claim. An empty `queues` means the
    /// worker will take from anywhere; otherwise only jobs in those
    /// queues are considered, and the best across them wins.
    #[allow(dead_code, reason = "used once dispatch selects from budget groups")]
    pub(in crate::store) fn head(
        &self,
        key: &str,
        queues: &HashSet<String>,
    ) -> Option<(u16, String, String)> {
        let inner = self.inner.lock().unwrap();
        inner.groups.get(key)?.jobs.peek(queues)
    }

    /// Every job waiting on any budget, in dispatch order, once each.
    ///
    /// Lock-free and lazy — reads the roll-up rather than merging the
    /// per-budget queues, so nothing needs deduplicating and nothing is
    /// collected up front.
    pub(in crate::store) fn entries(&self) -> impl Iterator<Item = (u16, String)> + '_ {
        self.all.iter()
    }

    /// How many distinct jobs are waiting on any budget.
    ///
    /// Lock-free and constant time. Counts a job once however many
    /// budgets it draws from, unlike [`waiting`](Self::waiting), which
    /// tallies queue memberships and exists only as a "is there
    /// anything at all" hint.
    ///
    /// Reached on every admin event while `zizq top` is connected — so
    /// once per job state change — which is why it must not walk the
    /// queues to work the answer out.
    pub(in crate::store) fn job_count(&self) -> usize {
        self.all.len()
    }

    /// The best job any budget is offering, whether or not it can be
    /// afforded.
    ///
    /// Used to decide between a budgeted job and an unthrottled one
    /// before committing to either. Says nothing about capacity — a job
    /// named here may still fail to acquire.
    pub(in crate::store) fn head_of_any(
        &self,
        queues: &HashSet<String>,
    ) -> Option<(u16, String, String)> {
        let inner = self.inner.lock().unwrap();
        Self::candidates(&inner, queues).into_iter().next()
    }

    /// Take the best job any budget can currently afford, debiting it.
    ///
    /// Returns the job that is now the caller's — its tokens spent and
    /// its place in every queue given up — along with the bindings that
    /// were debited. The caller needs those to refund if the job turns
    /// out not to be dispatchable after all, and cannot recover them
    /// from the job record, which by then may be deleted.
    ///
    /// `None` means nothing is affordable right now — either nothing is
    /// waiting, or everything waiting is short.
    ///
    /// # What gets considered
    ///
    /// Only the head of each budget's queue. A job behind the head of
    /// its own budget waits its turn even if it is cheaper and would
    /// fit, which keeps a budget FIFO for the work bound to it.
    ///
    /// Across budgets it is best-first, and a job whose budgets are
    /// short is passed over for one that fits — the design accepts
    /// that so a job wanting a rare combination cannot act as a poison
    /// pill for everything behind it.
    ///
    /// # Cost
    ///
    /// Walks the budgets that have work, in the order their heads would
    /// dispatch, stopping at the first that can pay. A drained budget
    /// is examined and skipped rather than known to be unaffordable in
    /// advance; parking those on a timer is what removes them from
    /// consideration, and lands with the refill tick.
    pub(in crate::store) fn claim_next(
        &self,
        queues: &HashSet<String>,
        now: u64,
    ) -> Option<(u16, String, String, Vec<BudgetRef>)> {
        let mut inner = self.inner.lock().unwrap();

        for (priority, job_id, queue) in Self::candidates(&inner, queues) {
            let Some(refs) = inner.refs.get(&job_id).cloned() else {
                continue;
            };

            if !Self::acquire_all(&mut inner, &refs, now) {
                continue;
            }

            let removed = Self::dequeue(&mut inner, &refs, &queue, priority, &job_id);
            self.all.remove(&queue, priority, &job_id);
            self.waiting.fetch_sub(removed, Ordering::Relaxed);

            return Some((priority, job_id, queue, refs));
        }

        None
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
    #[allow(dead_code, reason = "used once dispatch selects from budget groups")]
    pub(in crate::store) fn try_acquire_all(&self, budgets: &[BudgetRef], now: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        Self::acquire_all(&mut inner, budgets, now)
    }

    /// The all-or-nothing acquire itself. Caller holds the lock.
    fn acquire_all(inner: &mut Inner, budgets: &[BudgetRef], now: u64) -> bool {
        let mut taken: Vec<&BudgetRef> = Vec::with_capacity(budgets.len());

        for reference in budgets {
            let acquired = inner
                .groups
                .get_mut(&reference.key)
                .is_some_and(|group| group.limiter.try_acquire(reference.cost, now));

            if acquired {
                taken.push(reference);
            } else {
                // Nothing outside this lock ever sees the partial
                // state, but the rollback is what would keep this
                // correct under per-budget locks.
                for undo in taken {
                    if let Some(group) = inner.groups.get_mut(&undo.key) {
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
    #[allow(dead_code, reason = "used once dispatch selects from budget groups")]
    pub(in crate::store) fn refund(&self, budgets: &[BudgetRef]) {
        let mut inner = self.inner.lock().unwrap();

        for reference in budgets {
            if let Some(group) = inner.groups.get_mut(&reference.key) {
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
    #[allow(dead_code, reason = "used once dispatch selects from budget groups")]
    pub(in crate::store) fn release_concurrency(&self, budgets: &[BudgetRef]) {
        let mut inner = self.inner.lock().unwrap();

        for reference in budgets {
            if let Some(group) = inner.groups.get_mut(&reference.key)
                && group.limiter.returns_on_release()
            {
                group.limiter.release(reference.cost);
            }
        }
    }

    /// Forget every budget and everything waiting on one.
    ///
    /// Paired with removing the records themselves — the registry alone
    /// would leave live groups holding allocations for budgets that no
    /// longer exist, and the records alone would leave the opposite.
    pub(in crate::store) fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.groups.clear();
        inner.occupied.clear();
        inner.refs.clear();
        self.all.clear();
        self.waiting.store(0, Ordering::Relaxed);
    }

    /// How many budgets currently have work.
    #[cfg(test)]
    fn occupied_count(&self) -> usize {
        self.inner.lock().unwrap().occupied.len()
    }

    /// How many jobs are waiting on one budget.
    #[cfg(test)]
    fn depth(&self, key: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.groups.get(key).map_or(0, |group| group.jobs.len())
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

        budgets.park(&refs, "q", 0, "a");
        assert_eq!(budgets.waiting(), 1);
        assert_eq!(budgets.depth("stripe"), 1);

        budgets.unpark(&refs, "q", 0, "a");
        assert_eq!(budgets.waiting(), 0);
        assert_eq!(budgets.depth("stripe"), 0);
    }

    /// A job bound to two budgets waits in both queues, so both must
    /// release it — otherwise it would linger in one forever.
    #[test]
    fn a_job_parks_on_every_budget_it_draws_from() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        let refs = vec![draws("stripe", 1), draws("tenant", 1)];

        budgets.park(&refs, "q", 0, "a");
        assert_eq!(budgets.depth("stripe"), 1);
        assert_eq!(budgets.depth("tenant"), 1);

        budgets.unpark(&refs, "q", 0, "a");
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

        budgets.park(&refs, "q", 0, "a");
        budgets.park(&refs, "q", 0, "a");
        assert_eq!(budgets.waiting(), 1);

        budgets.unpark(&refs, "q", 0, "a");
        assert_eq!(budgets.waiting(), 0);
    }

    #[test]
    fn unparking_a_job_that_was_never_parked_is_harmless() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);

        budgets.unpark(&[draws("stripe", 1)], "q", 0, "absent");

        assert_eq!(budgets.waiting(), 0);
    }

    #[test]
    fn the_head_is_the_highest_priority_oldest_job() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, "q", 5, "later");
        budgets.park(&refs, "q", 1, "urgent");
        budgets.park(&refs, "q", 5, "earlier");

        assert_eq!(
            budgets.head("stripe", &HashSet::new()),
            Some((1, "urgent".into(), "q".into()))
        );

        budgets.unpark(&refs, "q", 1, "urgent");
        // Same priority falls back to id order, which is time order.
        assert_eq!(
            budgets.head("stripe", &HashSet::new()),
            Some((5, "earlier".into(), "q".into()))
        );
    }

    fn queues(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// The roll-up counts a job once however many budgets it draws
    /// from — summing the per-budget queues would report it twice, and
    /// that number is what `zizq top` shows as the ready depth.
    #[test]
    fn a_multi_budget_job_counts_once() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        let refs = vec![draws("stripe", 1), draws("tenant", 1)];

        budgets.park(&refs, "q", 0, "a");

        assert_eq!(budgets.job_count(), 1);
        // ...while the hint tallies queue memberships, so it sees two.
        assert_eq!(budgets.waiting(), 2);

        let listed: Vec<(u16, String)> = budgets.entries().collect();
        assert_eq!(listed, vec![(0, "a".into())]);
    }

    #[test]
    fn the_roll_up_empties_when_a_job_leaves() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        let refs = vec![draws("stripe", 1), draws("tenant", 1)];

        budgets.park(&refs, "q", 0, "a");
        budgets.unpark(&refs, "q", 0, "a");

        assert_eq!(budgets.job_count(), 0);
        assert_eq!(budgets.entries().count(), 0);
    }

    #[test]
    fn the_roll_up_lists_jobs_in_dispatch_order() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, "q", 9, "low");
        budgets.park(&refs, "q", 1, "high");

        let listed: Vec<(u16, String)> = budgets.entries().collect();
        assert_eq!(listed, vec![(1, "high".into()), (9, "low".into())]);
    }

    /// Dropping one budget must not evict a job that is still waiting
    /// on another.
    #[test]
    fn forgetting_a_budget_keeps_jobs_that_draw_on_another() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        budgets.park(&[draws("stripe", 1), draws("tenant", 1)], "q", 0, "shared");
        budgets.park(&[draws("stripe", 1)], "q", 0, "only-stripe");

        budgets.forget("stripe");

        let listed: Vec<(u16, String)> = budgets.entries().collect();
        assert_eq!(listed, vec![(0, "shared".into())]);
        assert_eq!(budgets.job_count(), 1);
    }

    /// A starved budget must not hold up a healthy one. A job on a
    /// once-a-month budget can sit at the very front — highest
    /// priority, oldest — for weeks, and work behind it on a
    /// thousand-a-minute budget still has to flow.
    #[tokio::test]
    async fn a_starved_budget_does_not_block_a_healthy_one() {
        let budgets = registry_with(&[("monthly", concurrency(1)), ("fast", concurrency(1000))]);

        // The monthly budget's only token is already spent.
        assert!(budgets.try_acquire_all(&[draws("monthly", 1)], NOW));

        // Priority 0 and enqueued first, so it is the best candidate.
        budgets.park(&[draws("monthly", 1)], "q", 0, "aaa-starved");
        budgets.park(&[draws("fast", 1)], "q", 9, "zzz-healthy");

        let claimed = budgets.claim_next(&HashSet::new(), NOW);
        assert_eq!(
            claimed.map(|(_, id, _, _)| id),
            Some("zzz-healthy".to_string())
        );

        // The starved job is still waiting, not lost.
        assert_eq!(budgets.job_count(), 1);
    }

    /// Within one budget it is strict FIFO, deliberately: a cheaper job
    /// does not overtake the head just because it happens to fit.
    /// Otherwise an expensive job could be starved indefinitely by a
    /// stream of small ones behind it.
    #[tokio::test]
    async fn a_cheaper_job_does_not_overtake_its_own_budget_head() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);

        // Five left, but the head wants six.
        assert!(budgets.try_acquire_all(&[draws("stripe", 5)], NOW));

        budgets.park(&[draws("stripe", 6)], "q", 0, "expensive-head");
        budgets.park(&[draws("stripe", 1)], "q", 9, "cheap-behind");

        assert!(budgets.claim_next(&HashSet::new(), NOW).is_none());
    }

    /// Selection walks the budgets that have work, not every budget
    /// that exists — a server holding thousands of idle budgets should
    /// not pay for them on each dequeue.
    #[tokio::test]
    async fn idle_budgets_are_not_consulted() {
        let mut pairs: Vec<(String, Budget)> = (0..64)
            .map(|i| (format!("idle-{i}"), concurrency(1)))
            .collect();
        pairs.push(("busy".to_string(), concurrency(1)));

        let budgets = Budgets::new();
        for (key, budget) in &pairs {
            budgets.sync(key, budget, NOW);
        }
        budgets.park(&[draws("busy", 1)], "q", 0, "a");

        assert_eq!(budgets.occupied_count(), 1);
        assert_eq!(
            budgets
                .claim_next(&HashSet::new(), NOW)
                .map(|(_, id, _, _)| id),
            Some("a".to_string())
        );
        // And it drops back out once its work is gone.
        assert_eq!(budgets.occupied_count(), 0);
    }

    #[test]
    fn a_filtered_head_only_considers_the_named_queues() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, "emails", 1, "urgent-email");
        budgets.park(&refs, "reports", 5, "slow-report");

        assert_eq!(
            budgets.head("stripe", &queues(&["reports"])),
            Some((5, "slow-report".into(), "reports".into()))
        );
    }

    /// A worker asking for a queue the group holds nothing for gets
    /// nothing — without walking the jobs it did not ask about.
    #[test]
    fn a_filtered_head_is_empty_when_no_job_matches() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        budgets.park(&[draws("stripe", 1)], "emails", 0, "a");

        assert_eq!(budgets.head("stripe", &queues(&["reports"])), None);
    }

    #[test]
    fn a_filtered_head_picks_the_best_across_the_named_queues() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, "emails", 9, "low");
        budgets.park(&refs, "reports", 2, "high");
        budgets.park(&refs, "ignored", 0, "highest");

        assert_eq!(
            budgets.head("stripe", &queues(&["emails", "reports"])),
            Some((2, "high".into(), "reports".into()))
        );
    }

    /// The per-queue view has to be maintained on the way out too, or a
    /// filtered head would keep offering a job that has already gone.
    #[test]
    fn unparking_clears_the_job_from_the_queue_view() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);
        let refs = vec![draws("stripe", 1)];

        budgets.park(&refs, "emails", 0, "a");
        budgets.unpark(&refs, "emails", 0, "a");

        assert_eq!(budgets.head("stripe", &queues(&["emails"])), None);
        assert_eq!(budgets.head("stripe", &HashSet::new()), None);
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
        budgets.park(&[draws("stripe", 1)], "q", 0, "a");

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
        budgets.park(&[draws("stripe", 1)], "q", 0, "a");
        budgets.park(&[draws("stripe", 1)], "q", 0, "b");
        assert_eq!(budgets.waiting(), 2);

        budgets.forget("stripe");

        assert_eq!(budgets.waiting(), 0);
        assert_eq!(budgets.depth("stripe"), 0);
    }

    #[test]
    fn clearing_forgets_everything() {
        let budgets = registry_with(&[("stripe", concurrency(5)), ("tenant", concurrency(5))]);
        budgets.park(&[draws("stripe", 1), draws("tenant", 1)], "q", 0, "a");

        budgets.clear();

        assert_eq!(budgets.waiting(), 0);
        assert!(!budgets.try_acquire_all(&[draws("stripe", 1)], NOW));
    }

    #[test]
    fn tracking_records_a_job_against_every_budget_it_draws_from() {
        let budgets = registry_with(&[("stripe", concurrency(10)), ("tenant", per_minute(10))]);

        budgets.track(&[draws("stripe", 3), draws("tenant", 7)]);

        assert_eq!(budgets.tracked("stripe"), 1);
        assert_eq!(budgets.tracked("tenant"), 1);
        assert_eq!(budgets.max_cost("stripe"), Some(3));
        assert_eq!(budgets.max_cost("tenant"), Some(7));
    }

    #[test]
    fn untracking_removes_the_job_from_every_budget() {
        let budgets = registry_with(&[("stripe", concurrency(10)), ("tenant", per_minute(10))]);
        let refs = [draws("stripe", 3), draws("tenant", 7)];

        budgets.track(&refs);
        budgets.untrack(&refs);

        assert_eq!(budgets.tracked("stripe"), 0);
        assert_eq!(budgets.tracked("tenant"), 0);
        assert_eq!(budgets.max_cost("stripe"), None);
        assert_eq!(budgets.max_cost("tenant"), None);
    }

    /// Tracking is lifecycle membership, not queue membership. A job
    /// that has been dispatched is no longer parked but is still
    /// unfinished, and a shrink underneath it would strand its retry.
    #[test]
    fn dispatching_a_job_leaves_it_tracked() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);
        let refs = [draws("stripe", 4)];

        budgets.track(&refs);
        budgets.park(&refs, "q", 0, "a");

        let claimed = budgets.claim_next(&HashSet::new(), NOW);
        assert!(claimed.is_some());

        assert_eq!(budgets.waiting(), 0);
        assert_eq!(budgets.tracked("stripe"), 1);
        assert_eq!(budgets.max_cost("stripe"), Some(4));
    }

    /// A budget nobody has ever mentioned reads as unreferenced rather
    /// than as an error — the guards treat "no group" and "no jobs" the
    /// same way, since neither has a job to strand.
    #[test]
    fn an_unknown_budget_is_untracked_rather_than_missing() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);

        assert_eq!(budgets.tracked("nonexistent"), 0);
        assert_eq!(budgets.max_cost("nonexistent"), None);
    }

    /// Tracking a job against a budget with no group is skipped, not
    /// conjured — the same rule `park` follows, and for the same reason.
    #[test]
    fn tracking_an_unknown_budget_creates_nothing() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);

        budgets.track(&[draws("ghost", 5)]);

        assert_eq!(budgets.tracked("ghost"), 0);
        assert_eq!(budgets.max_cost("ghost"), None);
    }

    /// A policy change adjusts a running budget. Whatever is already
    /// committed to it stays committed — otherwise a shrink could erase
    /// the very accounting that decides whether the shrink is safe.
    #[test]
    fn a_policy_change_leaves_tracked_jobs_alone() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);

        budgets.track(&[draws("stripe", 6)]);
        budgets.sync("stripe", &concurrency(20), NOW);

        assert_eq!(budgets.tracked("stripe"), 1);
        assert_eq!(budgets.max_cost("stripe"), Some(6));
    }

    #[test]
    fn forgetting_a_budget_discards_its_cost_accounting() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);

        budgets.track(&[draws("stripe", 6)]);
        budgets.forget("stripe");

        assert_eq!(budgets.tracked("stripe"), 0);
        assert_eq!(budgets.max_cost("stripe"), None);
    }

    // --- what the waker asks for ---

    const NO_LIMIT: usize = usize::MAX;

    #[test]
    fn an_empty_registry_wants_no_wakeup() {
        let budgets = Budgets::new();
        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert!(wakeup.dispatchable.is_empty());
        assert_eq!(wakeup.next_refill, None);
    }

    #[test]
    fn an_affordable_job_is_reported_with_its_queue() {
        let budgets = registry_with(&[("stripe", concurrency(10))]);
        budgets.park(&[draws("stripe", 1)], "emails", 0, "a");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert_eq!(
            wakeup.dispatchable,
            vec![("a".to_string(), "emails".to_string())]
        );
        assert_eq!(wakeup.next_refill, None);
    }

    /// A drained rate limit is answerable by the clock, so the waker
    /// gets a time to come back at rather than a job.
    #[test]
    fn a_drained_rate_limit_reports_when_it_recovers() {
        let budgets = registry_with(&[("stripe", per_minute(1))]);
        let refs = [draws("stripe", 1)];

        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.park(&refs, "q", 0, "a");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert!(wakeup.dispatchable.is_empty());
        assert_eq!(wakeup.next_refill, Some(NOW + 60_000));
    }

    /// A drained concurrency budget is not answerable by any clock. No
    /// timer would help, so none is asked for — the waker has to be
    /// roused by the release instead.
    #[test]
    fn a_drained_concurrency_budget_asks_for_no_timer() {
        let budgets = registry_with(&[("stripe", concurrency(1))]);
        let refs = [draws("stripe", 1)];

        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.park(&refs, "q", 0, "a");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert!(wakeup.dispatchable.is_empty());
        assert_eq!(wakeup.next_refill, None);
    }

    /// Waiting on two clocks means waiting for the later one. Reporting
    /// the earlier would wake to find the job still unaffordable.
    #[test]
    fn a_job_on_two_rate_limits_waits_for_the_slower() {
        let budgets = Budgets::new();
        budgets.sync("fast", &per_minute(1), NOW);
        budgets.sync(
            "slow",
            &Budget::new(
                1,
                BudgetStrategy::TimeBased {
                    duration_ms: 600_000,
                },
                NOW,
            )
            .unwrap(),
            NOW,
        );

        let refs = [draws("fast", 1), draws("slow", 1)];
        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.park(&refs, "q", 0, "a");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert_eq!(wakeup.next_refill, Some(NOW + 600_000));
    }

    /// A job needing both a release and a drip sets no timer: one that
    /// fired while the release was still outstanding would achieve
    /// nothing. The release provokes a fresh look, and by then the
    /// answer is a plain time.
    #[test]
    fn a_release_outranks_a_drip_and_suppresses_the_timer() {
        let budgets = Budgets::new();
        budgets.sync("rate", &per_minute(1), NOW);
        budgets.sync("slots", &concurrency(1), NOW);

        let refs = [draws("rate", 1), draws("slots", 1)];
        assert!(budgets.try_acquire_all(&refs, NOW));
        budgets.park(&refs, "q", 0, "a");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert!(wakeup.dispatchable.is_empty());
        assert_eq!(wakeup.next_refill, None);

        // Once the slot comes back, the drip is all that is left and it
        // can be timed.
        budgets.release_concurrency(&refs);
        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert_eq!(wakeup.next_refill, Some(NOW + 60_000));
    }

    /// One starved budget must not hide another that is ready, and the
    /// pass has to report both halves rather than stopping at the first
    /// thing it finds.
    #[test]
    fn a_single_pass_reports_the_ready_and_the_waiting() {
        let budgets = Budgets::new();
        budgets.sync("ready", &concurrency(10), NOW);
        budgets.sync("starved", &per_minute(1), NOW);

        let starved = [draws("starved", 1)];
        assert!(budgets.try_acquire_all(&starved, NOW));
        budgets.park(&starved, "q", 0, "waits");
        budgets.park(&[draws("ready", 1)], "q", 9, "goes");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        assert_eq!(
            wakeup.dispatchable,
            vec![("goes".to_string(), "q".to_string())]
        );
        assert_eq!(wakeup.next_refill, Some(NOW + 60_000));
    }

    /// The limit caps how many jobs come back, but must not stop the
    /// scan — the timer is the half that keeps the waker armed, and
    /// losing it would leave a drained budget waiting on nothing.
    #[test]
    fn a_limit_caps_the_jobs_without_losing_the_timer() {
        let budgets = Budgets::new();
        budgets.sync("a", &concurrency(10), NOW);
        budgets.sync("b", &concurrency(10), NOW);
        budgets.sync("starved", &per_minute(1), NOW);

        budgets.park(&[draws("a", 1)], "q", 0, "one");
        budgets.park(&[draws("b", 1)], "q", 1, "two");

        let starved = [draws("starved", 1)];
        assert!(budgets.try_acquire_all(&starved, NOW));
        budgets.park(&starved, "q", 2, "waits");

        let wakeup = budgets.wakeup(NOW, 1);

        assert_eq!(wakeup.dispatchable.len(), 1);
        assert_eq!(wakeup.next_refill, Some(NOW + 60_000));
    }

    /// Only heads are considered, matching `claim_next`. A cheap job
    /// behind a head that cannot currently be paid for is not
    /// dispatchable, because FIFO within a budget is the deliberate
    /// behaviour — otherwise a stream of small jobs could starve a
    /// large one indefinitely.
    #[test]
    fn a_job_behind_an_unaffordable_head_is_not_reported() {
        let budgets = registry_with(&[("stripe", concurrency(5))]);

        // Something already running holds two of the five.
        assert!(budgets.try_acquire_all(&[draws("stripe", 2)], NOW));

        budgets.park(&[draws("stripe", 5)], "q", 0, "dear");
        budgets.park(&[draws("stripe", 1)], "q", 1, "cheap");

        let wakeup = budgets.wakeup(NOW, NO_LIMIT);

        // `cheap` could be paid for, but it is not the head.
        assert!(wakeup.dispatchable.is_empty());
        // Concurrency, so no clock will help.
        assert_eq!(wakeup.next_refill, None);
    }
}
