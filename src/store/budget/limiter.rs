// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! The token bucket behind one budget.
//!
//! Tokens are consumed when a job dispatches and come back either on a
//! clock ([`BudgetStrategy::TimeBased`]) or when the job leaves the
//! in-flight state ([`BudgetStrategy::WhileInFlight`]).
//!
//! # Continuous drip, not fixed windows
//!
//! Tokens accrue steadily rather than being restored in a lump at a
//! period boundary: a budget of 100 per minute grants roughly one every
//! 600ms. A counter that reset to 100 on the minute would let 100 jobs
//! run at 59.9s and another 100 at 60.0s — twice the configured rate
//! across any window straddling the boundary, which is exactly the
//! throttling failure a budget exists to prevent.
//!
//! Refill is computed lazily from elapsed time rather than driven by a
//! timer, so an idle budget costs nothing: a bucket drained an hour ago
//! is simply full the next time anyone asks.
//!
//! # Whole tokens, exact remainders
//!
//! Continuous accrual means a call can land part-way to the next token.
//! That fraction is held as an exact integer remainder in `credit`,
//! measured in token-milliseconds, rather than as a fractional token —
//! so the arithmetic is exact by construction rather than exact within
//! a tolerance. Costs and allocations are integers everywhere else in
//! the subsystem, and this keeps the bucket the same.
//!
//! # Time is a parameter, not a reading
//!
//! Every method takes `now` as milliseconds since the epoch, threaded
//! from whatever clock the caller is using, rather than reading a
//! monotonic `Instant` internally. That is what makes refill behaviour
//! testable without real time passing — otherwise every test of a rate
//! limit would have to sleep, which is exactly how tests become flaky.

use super::{Budget, BudgetStrategy};

/// When a budget could next afford a given cost.
///
/// Four distinct answers rather than an optional timestamp, because
/// each calls for something different from the caller and two of them
/// are easy to conflate. In particular "go now" and "park until t" must
/// not share a representation: a caller that parks on whatever it is
/// handed would set a timer for the current instant, wake immediately,
/// and spin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Availability {
    /// Enough tokens are there. Acquire, do not park.
    Now,

    /// Not yet, but the drip will cover it at this time — always
    /// strictly later than the `now` that was asked about.
    At(u64),

    /// Not on any clock. Only a job finishing and returning its tokens
    /// can help, so the group waits to be woken rather than timed.
    OnRelease,

    /// Not ever: the cost exceeds what the bucket can hold even full,
    /// so neither waiting nor a release will satisfy it. Worth
    /// surfacing rather than parking — a job in this state would sit
    /// undispatched forever.
    Never,
}

/// How fast a budget's tokens come back on a clock.
#[derive(Clone, Copy)]
struct Drip {
    /// Tokens restored per `duration_ms`.
    allocation: u32,

    /// The period those tokens are spread across. Never zero — the
    /// policy is validated before it gets here.
    duration_ms: u64,
}

/// A budget's token bucket.
#[allow(dead_code, reason = "wired up when budgeted jobs reach dispatch")]
pub(super) struct Limiter {
    /// Tokens the bucket holds when full.
    capacity: u32,

    /// Whole tokens currently available.
    tokens: u32,

    /// Progress toward the next whole token, in token-milliseconds.
    ///
    /// Always less than `duration_ms`. Carrying it is what makes the
    /// drip continuous: without it, two 500ms refills of a one-per-
    /// second budget would each round down to nothing and the budget
    /// would never accrue at all.
    credit: u64,

    /// Slots owed back before any release frees capacity again.
    ///
    /// Only a concurrency budget can be over-committed, and only by
    /// being shrunk while jobs are running: six in flight against an
    /// allocation cut to one leaves five more outstanding than the
    /// budget now permits. Those five have to be surrendered before a
    /// completion means a slot is genuinely free — otherwise every
    /// completion hands one straight back and the old concurrency
    /// persists for as long as work keeps arriving.
    ///
    /// Always zero for a `time_based` budget, whose tokens are spent
    /// rather than held: nothing is outstanding to be owed.
    debt: u32,

    /// `None` for a strategy whose tokens return on acknowledgement
    /// rather than on a clock — an absent rate rather than a zero one.
    drip: Option<Drip>,

    /// When the bucket was last brought up to date (ms since epoch).
    last_refill: u64,
}

#[allow(dead_code, reason = "wired up when budgeted jobs reach dispatch")]
impl Limiter {
    /// Build a limiter for a policy, starting full.
    ///
    /// Starting full is deliberate: token state is not persisted, so a
    /// restart grants at most one allocation's worth of burst. The
    /// alternative — persisting the bucket on a hot path — costs every
    /// dispatch to defend against a case that only arises on an
    /// unclean restart.
    pub(super) fn new(budget: &Budget, now: u64) -> Self {
        // Capacity, not allocation. For a burst-capped budget the two
        // differ: the allocation is how fast tokens arrive, the
        // capacity is how many may be banked before the rest are
        // dropped on the floor.
        let capacity = budget.capacity();

        Self {
            capacity,
            tokens: capacity,
            credit: 0,
            debt: 0,
            drip: Self::drip(budget),
            last_refill: now,
        }
    }

    /// The clock-driven refill rate for a policy, if it has one.
    fn drip(budget: &Budget) -> Option<Drip> {
        match budget.strategy {
            // The drip is the *rate*, so it always uses the
            // allocation. A burst only changes how much of that rate
            // can accumulate unspent.
            BudgetStrategy::TimeBased { duration_ms, .. } => Some(Drip {
                allocation: budget.allocation,
                duration_ms,
            }),
            BudgetStrategy::WhileInFlight => None,
        }
    }

    /// Adopt a changed policy.
    ///
    /// Elapsed time is settled at the old rate first, so accrual that
    /// already happened is not silently re-priced. Tokens are then
    /// capped, so tightening a budget bites immediately rather than
    /// after the existing surplus drains — the point of being able to
    /// tighten one during an incident.
    pub(super) fn adopt(&mut self, budget: &Budget, now: u64) {
        self.refill(now);

        let was_concurrency = self.drip.is_none();
        let old_period = self.drip.map(|drip| drip.duration_ms);
        let old_capacity = self.capacity;

        self.capacity = budget.capacity();
        self.drip = Self::drip(budget);

        // A concurrency budget's ceiling has to be re-applied against
        // the work already running, in both directions, because nothing
        // else ever will: it has no drip, so its tokens move only when
        // jobs start and finish.
        //
        // What is running does not change here, so it is the fixed point
        // to reason from. `tokens - debt` is how many slots were spare
        // before (negative when over-committed), and shifting the
        // ceiling by `delta` shifts that by the same amount. A positive
        // result is free slots, handed over at once; a negative one is
        // an over-commitment to be surrendered as jobs finish.
        //
        // Widening without this leaves a budget pinned at its old
        // allocation: it would rise by one token when a job released and
        // be pushed straight back to zero by the next acquire.
        // Narrowing without it is worse — six jobs running against an
        // allocation cut to one would keep six running, since each
        // completion would hand a slot straight to a replacement.
        //
        // Only when it was already a concurrency budget and still is.
        // Across a strategy change the tokens mean something different
        // before and after, so carrying anything between the two would
        // be inventing capacity rather than moving it.
        if was_concurrency && self.drip.is_none() {
            let delta = i64::from(self.capacity) - i64::from(old_capacity);
            let spare = i64::from(self.tokens) - i64::from(self.debt) + delta;

            if spare >= 0 {
                self.tokens = spare.min(i64::from(self.capacity)) as u32;
                self.debt = 0;
            } else {
                self.tokens = 0;
                self.debt = (-spare) as u32;
            }
        }

        self.tokens = self.tokens.min(self.capacity);

        // Banked progress is measured in token-milliseconds against the
        // period, so it survives a change of *rate* but not a change of
        // *period*. Keeping it across a re-rate is worth doing: someone
        // who has waited most of a slow minute should not have that
        // waiting thrown away for speeding the budget up.
        //
        // Discarding it across a period change is not merely tidiness.
        // `credit` is a remainder modulo `duration_ms`, so it is always
        // smaller than the period it was computed against — which is
        // exactly what stops `next_available` underflowing when it
        // subtracts the credit from a shortfall's worth of the new
        // period. Carry a remainder from a minute into a one-second
        // budget and that invariant is gone.
        if old_period != self.drip.map(|drip| drip.duration_ms) {
            self.credit = 0;
        }
    }

    /// What the bucket would hold at `now`, without mutating it.
    ///
    /// Returns whole tokens and the leftover progress toward the next.
    fn accrued(&self, now: u64) -> (u32, u64) {
        let Some(drip) = self.drip else {
            return (self.tokens, self.credit);
        };

        if self.tokens >= self.capacity {
            return (self.capacity, 0);
        }

        // Saturating, so a clock that steps backwards stalls refill
        // rather than draining the bucket. Wall-clock time does move
        // backwards — NTP correction, a VM restored from a snapshot.
        let elapsed = now.saturating_sub(self.last_refill);

        // Widened so the multiplication cannot overflow for any
        // allocation and period the policy permits.
        let banked = u128::from(self.credit) + u128::from(elapsed) * u128::from(drip.allocation);
        let period = u128::from(drip.duration_ms);

        let earned = banked / period;
        let credit = (banked % period) as u64;

        let headroom = u128::from(self.capacity - self.tokens);
        if earned >= headroom {
            // The ordinary idle case, not a guard. Refill is lazy, so
            // `elapsed` can span many periods and `earned` can be a
            // large multiple of the capacity — a bucket one token short
            // and untouched for a year has "earned" a year's worth.
            //
            // This clamp is what "never exceeds capacity" actually is.
            // Without it the addition in the other branch would
            // overflow `u32` long before it ever reached the ceiling.
            //
            // The remainder is dropped along with it: it accrued during
            // time the bucket spent full, and banking it would hand out
            // a free token the moment one was spent.
            (self.capacity, 0)
        } else {
            (self.tokens + earned as u32, credit)
        }
    }

    /// Bring the bucket up to date for the current time.
    fn refill(&mut self, now: u64) {
        let (tokens, credit) = self.accrued(now);
        self.tokens = tokens;
        self.credit = credit;

        // Advanced even when nothing accrued, so a backwards step is
        // absorbed once rather than re-measured on every later call.
        self.last_refill = self.last_refill.max(now);
    }

    /// Take `cost` tokens if they are there.
    ///
    /// Returns `false` without consuming anything when they are not, so
    /// a caller composing several budgets can roll back cleanly.
    pub(super) fn try_acquire(&mut self, cost: u32, now: u64) -> bool {
        self.refill(now);

        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// Whether tokens come back when a job finishes rather than on a
    /// clock.
    ///
    /// The two are mutually exclusive, and callers need to tell them
    /// apart: crediting a dripping bucket when a job completes would
    /// return a token the drip is already restoring.
    pub(super) fn returns_on_release(&self) -> bool {
        self.drip.is_none()
    }

    /// Give `cost` tokens back.
    ///
    /// Pays down any over-commitment first. A budget shrunk while jobs
    /// were running owes those slots back, and until they are, a
    /// completion means "one fewer job over the limit" rather than "a
    /// slot is free" — crediting it would let the old concurrency
    /// continue indefinitely, since each completion would immediately
    /// admit a replacement.
    ///
    /// Whatever is left over is capped at capacity, which covers the
    /// simultaneous case: several jobs finishing at once cannot fill the
    /// bucket past its ceiling.
    pub(super) fn release(&mut self, cost: u32) {
        let owed = cost.min(self.debt);
        self.debt -= owed;
        self.tokens = self.tokens.saturating_add(cost - owed).min(self.capacity);
    }

    /// When `cost` tokens could next be afforded.
    ///
    /// See [`Availability`] for what each answer asks of the caller.
    pub(super) fn next_available(&self, cost: u32, now: u64) -> Availability {
        if cost > self.capacity {
            return Availability::Never;
        }

        let (tokens, credit) = self.accrued(now);
        if tokens >= cost {
            return Availability::Now;
        }

        let Some(drip) = self.drip else {
            return Availability::OnRelease;
        };

        // `credit` is always below one period's worth and the shortfall
        // is at least one token, so this cannot underflow.
        let shortfall = u128::from(cost - tokens);
        let needed = shortfall * u128::from(drip.duration_ms) - u128::from(credit);
        let wait_ms = needed.div_ceil(u128::from(drip.allocation));

        // Rounding up already puts this in the future, but a rate fast
        // enough to cover the shortfall within a millisecond would
        // round to zero. `At` promises a strictly later time, so that
        // a caller cannot set a timer for the current instant, wake
        // straight away, and spin against the same shortfall.
        let wait_ms = wait_ms.max(1).min(u128::from(u64::MAX)) as u64;

        Availability::At(now.saturating_add(wait_ms))
    }

    /// Whole tokens available as of the last refill.
    #[cfg(test)]
    fn tokens(&self) -> u32 {
        self.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    fn per_minute(allocation: u32) -> Budget {
        Budget::new(
            allocation,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: None,
            },
            NOW,
        )
        .unwrap()
    }

    fn concurrency(allocation: u32) -> Budget {
        Budget::new(allocation, BudgetStrategy::WhileInFlight, NOW).unwrap()
    }

    #[test]
    fn starts_full() {
        let limiter = Limiter::new(&per_minute(10), NOW);
        assert_eq!(limiter.tokens(), 10);
    }

    #[test]
    fn acquiring_consumes_tokens() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);

        assert!(limiter.try_acquire(3, NOW));
        assert_eq!(limiter.tokens(), 7);
    }

    #[test]
    fn acquiring_more_than_is_left_consumes_nothing() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);
        assert!(limiter.try_acquire(10, NOW));

        assert!(!limiter.try_acquire(1, NOW));
        // The failed attempt left the bucket alone, so a caller
        // composing budgets can roll back what it did take.
        assert_eq!(limiter.tokens(), 0);
    }

    #[test]
    fn tokens_accrue_continuously_rather_than_in_windows() {
        // 60 per minute is one per second.
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        // Five seconds in, five tokens — not "nothing until the minute
        // is up, then sixty".
        limiter.refill(NOW + 5_000);
        assert_eq!(limiter.tokens(), 5);
    }

    /// The reason the bucket carries a remainder at all. Two half-
    /// second refills of a one-per-second budget must add up to a
    /// token; rounding each down independently would accrue nothing,
    /// ever.
    #[test]
    fn a_partial_token_is_carried_rather_than_dropped() {
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        limiter.refill(NOW + 500);
        assert_eq!(limiter.tokens(), 0);

        limiter.refill(NOW + 1_000);
        assert_eq!(limiter.tokens(), 1);
    }

    /// And the remainder survives arbitrarily many observations, so a
    /// frequently-polled budget accrues at the same rate as an idle one.
    #[test]
    fn frequent_refills_accrue_the_same_as_one_long_one() {
        let mut frequent = Limiter::new(&per_minute(60), NOW);
        let mut idle = Limiter::new(&per_minute(60), NOW);
        assert!(frequent.try_acquire(60, NOW));
        assert!(idle.try_acquire(60, NOW));

        for ms in 1..=1_000 {
            frequent.refill(NOW + ms);
        }
        idle.refill(NOW + 1_000);

        assert_eq!(frequent.tokens(), idle.tokens());
        assert_eq!(frequent.tokens(), 1);
    }

    #[test]
    fn a_drained_bucket_is_full_again_after_one_duration() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);
        assert!(limiter.try_acquire(10, NOW));

        assert!(limiter.try_acquire(10, NOW + 60_000));
    }

    #[test]
    fn refill_never_exceeds_capacity() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);
        assert!(limiter.try_acquire(1, NOW));

        // A year later, still ten — not a year's worth.
        limiter.refill(NOW + 365 * 24 * 60 * 60 * 1_000);
        assert_eq!(limiter.tokens(), 10);
    }

    /// Sitting full must not bank credit that would be released as a
    /// free token the instant one is spent.
    #[test]
    fn time_spent_full_does_not_bank_credit() {
        let mut limiter = Limiter::new(&per_minute(60), NOW);

        limiter.refill(NOW + 60_000);
        assert!(limiter.try_acquire(60, NOW + 60_000));

        // Immediately after draining, nothing has accrued yet.
        limiter.refill(NOW + 60_000);
        assert_eq!(limiter.tokens(), 0);
    }

    #[test]
    fn a_concurrency_budget_does_not_refill_on_a_clock() {
        let mut limiter = Limiter::new(&concurrency(2), NOW);
        assert!(limiter.try_acquire(2, NOW));

        assert!(!limiter.try_acquire(1, NOW + 60 * 60 * 1_000));
    }

    #[test]
    fn releasing_returns_tokens() {
        let mut limiter = Limiter::new(&concurrency(2), NOW);
        assert!(limiter.try_acquire(2, NOW));

        limiter.release(1);

        assert!(limiter.try_acquire(1, NOW));
    }

    #[test]
    fn releasing_never_exceeds_capacity() {
        let mut limiter = Limiter::new(&concurrency(2), NOW);

        limiter.release(5);

        assert_eq!(limiter.tokens(), 2);
    }

    /// Wall-clock time can step backwards — NTP correction, a VM
    /// restored from a snapshot. That must not drain the bucket.
    #[test]
    fn a_backwards_clock_does_not_lose_tokens() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);
        assert!(limiter.try_acquire(5, NOW));

        limiter.refill(NOW - 30_000);

        assert_eq!(limiter.tokens(), 5);
    }

    /// And the step is absorbed once, rather than re-measured against a
    /// stale `last_refill` on every later call.
    #[test]
    fn refill_resumes_normally_after_a_backwards_step() {
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        limiter.refill(NOW - 10_000);
        limiter.refill(NOW + 1_000);

        // One second of real progress from NOW, not eleven.
        assert_eq!(limiter.tokens(), 1);
    }

    /// "Go now" is its own answer, not a timestamp equal to `now` —
    /// so a caller cannot park on it by accident.
    #[test]
    fn next_available_is_now_when_tokens_are_there() {
        let limiter = Limiter::new(&per_minute(10), NOW);
        assert_eq!(limiter.next_available(1, NOW), Availability::Now);
    }

    #[test]
    fn next_available_is_when_the_shortfall_accrues() {
        // One token per second.
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        // Two tokens short, so two seconds out.
        assert_eq!(
            limiter.next_available(2, NOW),
            Availability::At(NOW + 2_000)
        );
    }

    /// Banked progress counts toward the wait, or a frequently polled
    /// budget would be told to wait a full period every time.
    #[test]
    fn next_available_accounts_for_banked_progress() {
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        // Half a second of progress banked; half a second still to go.
        limiter.refill(NOW + 500);
        assert_eq!(
            limiter.next_available(1, NOW + 500),
            Availability::At(NOW + 1_000)
        );
    }

    /// Never `Some(now)` when the bucket is short, or a caller would
    /// park a group on a timer that fires immediately and spin.
    #[test]
    fn next_available_is_always_in_the_future_when_short() {
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        let Availability::At(at) = limiter.next_available(1, NOW) else {
            panic!("a short bucket on a drip should name a later time");
        };
        assert!(at > NOW);
    }

    #[test]
    fn a_concurrency_budget_has_no_next_available() {
        let mut limiter = Limiter::new(&concurrency(1), NOW);
        assert!(limiter.try_acquire(1, NOW));

        // Waiting cannot help; only an acknowledgement can.
        assert_eq!(limiter.next_available(1, NOW), Availability::OnRelease);
    }

    /// A cost the bucket could never hold is not "wait a very long
    /// time", and not "wait for a release" either — those would park
    /// the job forever. It is its own answer.
    #[test]
    fn a_cost_above_capacity_can_never_be_afforded() {
        let limiter = Limiter::new(&per_minute(10), NOW);
        assert_eq!(limiter.next_available(11, NOW), Availability::Never);
    }

    /// Including on a concurrency budget, where the shortfall would
    /// otherwise look like something a release could fix.
    #[test]
    fn a_cost_above_capacity_is_never_rather_than_on_release() {
        let limiter = Limiter::new(&concurrency(2), NOW);
        assert_eq!(limiter.next_available(3, NOW), Availability::Never);
    }

    #[test]
    fn adopting_a_larger_allocation_raises_the_ceiling() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);

        limiter.adopt(&per_minute(100), NOW);
        limiter.refill(NOW + 60_000);

        assert_eq!(limiter.tokens(), 100);
    }

    /// A rate limit gets its increase from the drip, but a concurrency
    /// budget has no drip and its tokens only come back from jobs
    /// finishing. Without handing the extra slots over here, widening
    /// one from one to five would leave it running one job at a time
    /// for the life of the process: the release would take it to one
    /// token and the next acquire straight back to zero.
    #[test]
    fn widening_a_concurrency_budget_hands_over_the_extra_slots() {
        let mut limiter = Limiter::new(&concurrency(1), NOW);

        // A job takes the only slot.
        assert!(limiter.try_acquire(1, NOW));
        assert_eq!(limiter.tokens(), 0);

        limiter.adopt(&concurrency(5), NOW);

        // Four more may run alongside the one still going.
        assert_eq!(limiter.tokens(), 4);
    }

    /// The reported sequence: serialise at one, widen to six, then
    /// narrow back to one. Without paying down the over-commitment the
    /// budget stays at six indefinitely, because each completion hands
    /// its slot straight to a replacement.
    #[test]
    fn narrowing_a_concurrency_budget_surrenders_the_excess() {
        let mut limiter = Limiter::new(&concurrency(1), NOW);

        // One job running.
        assert!(limiter.try_acquire(1, NOW));
        assert!(!limiter.try_acquire(1, NOW));

        // Widen to six: five more may start, so six are running.
        limiter.adopt(&concurrency(6), NOW);
        for _ in 0..5 {
            assert!(limiter.try_acquire(1, NOW));
        }
        assert!(!limiter.try_acquire(1, NOW));

        // Narrow back to one while all six are still in flight.
        limiter.adopt(&concurrency(1), NOW);

        // The first five completions pay down the excess: each means
        // "one fewer over the limit", not "a slot is free".
        for i in 0..5 {
            limiter.release(1);
            assert!(
                !limiter.try_acquire(1, NOW),
                "a replacement started after completion {i}, so the old \
                 concurrency would persist"
            );
        }

        // The sixth leaves nothing running, and the budget is back to
        // admitting exactly one at a time.
        limiter.release(1);
        assert!(limiter.try_acquire(1, NOW));
        assert!(!limiter.try_acquire(1, NOW));
    }

    /// Widening again before the debt is paid should cancel it rather
    /// than leave the budget owing slots it no longer owes.
    #[test]
    fn widening_again_cancels_an_outstanding_debt() {
        let mut limiter = Limiter::new(&concurrency(6), NOW);

        for _ in 0..6 {
            assert!(limiter.try_acquire(1, NOW));
        }

        // Six running, ceiling cut to two: four over.
        limiter.adopt(&concurrency(2), NOW);
        // Back to six with all six still running: square again, and
        // still nothing spare.
        limiter.adopt(&concurrency(6), NOW);

        assert!(!limiter.try_acquire(1, NOW));

        // One finishes, so exactly one slot is free — not five.
        limiter.release(1);
        assert!(limiter.try_acquire(1, NOW));
        assert!(!limiter.try_acquire(1, NOW));
    }

    /// Narrowing with nothing running owes nothing: the ceiling simply
    /// applies.
    #[test]
    fn narrowing_an_idle_concurrency_budget_owes_nothing() {
        let mut limiter = Limiter::new(&concurrency(6), NOW);

        limiter.adopt(&concurrency(2), NOW);

        assert!(limiter.try_acquire(1, NOW));
        assert!(limiter.try_acquire(1, NOW));
        assert!(!limiter.try_acquire(1, NOW));
    }

    /// The tokens either side of a strategy change do not mean the same
    /// thing, so no delta is carried across one.
    #[test]
    fn widening_across_a_strategy_change_hands_over_nothing() {
        let mut limiter = Limiter::new(&per_minute(1), NOW);

        assert!(limiter.try_acquire(1, NOW));
        assert_eq!(limiter.tokens(), 0);

        limiter.adopt(&concurrency(5), NOW);

        assert_eq!(limiter.tokens(), 0);
    }

    /// Speeding a budget up should credit the waiting already done, not
    /// restart it. Half a minute spent waiting on a one-a-minute budget
    /// is half a token's worth of progress, and it means the same thing
    /// under any rate sharing that period.
    #[test]
    fn adopting_a_faster_rate_keeps_banked_progress() {
        let mut limiter = Limiter::new(&per_minute(1), NOW);

        // Spend the token, then wait half the period.
        assert!(limiter.try_acquire(1, NOW));
        limiter.refill(NOW + 30_000);

        limiter.adopt(&per_minute(120), NOW + 30_000);

        // Thirty seconds of the minute are already banked, so the next
        // token needs 30_000 more token-milliseconds at 120 a minute —
        // a quarter of a second, not the half it would be from scratch.
        assert_eq!(
            limiter.next_available(1, NOW + 30_000),
            Availability::At(NOW + 30_250)
        );
    }

    /// A period change makes the remainder meaningless — and unsafe.
    /// `credit` is a remainder modulo the period, which is what keeps
    /// `next_available` from underflowing; carrying a minute's worth
    /// into a one-second budget would break that.
    #[test]
    fn adopting_a_new_period_discards_banked_progress() {
        let mut limiter = Limiter::new(&per_minute(1), NOW);

        assert!(limiter.try_acquire(1, NOW));
        limiter.refill(NOW + 30_000);

        let per_second = Budget::new(
            1,
            BudgetStrategy::TimeBased {
                duration_ms: 1_000,
                burst: None,
            },
            NOW,
        )
        .unwrap();
        limiter.adopt(&per_second, NOW + 30_000);

        // A full second from scratch, not an instant token from a
        // remainder measured against a minute.
        assert_eq!(
            limiter.next_available(1, NOW + 30_000),
            Availability::At(NOW + 31_000)
        );
    }

    /// Tightening a budget applies at once rather than after the
    /// existing surplus drains.
    #[test]
    fn adopting_a_smaller_allocation_caps_tokens_immediately() {
        let mut limiter = Limiter::new(&per_minute(100), NOW);

        limiter.adopt(&per_minute(5), NOW);

        assert_eq!(limiter.tokens(), 5);
    }

    #[test]
    fn adopting_a_different_strategy_changes_the_refill() {
        let mut limiter = Limiter::new(&concurrency(10), NOW);
        assert!(limiter.try_acquire(10, NOW));

        limiter.adopt(&per_minute(10), NOW);
        limiter.refill(NOW + 60_000);

        assert_eq!(limiter.tokens(), 10);
    }

    /// Elapsed time before a policy change accrues at the old rate, not
    /// silently at the new one.
    #[test]
    fn adopting_settles_the_old_rate_first() {
        let mut limiter = Limiter::new(&per_minute(60), NOW);
        assert!(limiter.try_acquire(60, NOW));

        // Ten seconds at one per second, then the rate changes.
        limiter.adopt(&concurrency(60), NOW + 10_000);

        assert_eq!(limiter.tokens(), 10);
    }

    /// An allocation and period at the extremes of what the policy
    /// permits must not overflow the widened arithmetic.
    #[test]
    fn extreme_policies_do_not_overflow() {
        let budget = Budget::new(
            crate::store::MAX_BUDGET_ALLOCATION,
            BudgetStrategy::TimeBased {
                duration_ms: u64::MAX,
                burst: None,
            },
            NOW,
        )
        .unwrap();

        let mut limiter = Limiter::new(&budget, NOW);
        assert!(limiter.try_acquire(crate::store::MAX_BUDGET_ALLOCATION, NOW));

        limiter.refill(u64::MAX);
        assert_ne!(limiter.next_available(1, NOW), Availability::Never);
    }

    // --- burst ---

    fn per_minute_bursting(allocation: u32, burst: u32) -> Budget {
        Budget::new(
            allocation,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: Some(burst),
            },
            NOW,
        )
        .unwrap()
    }

    /// The overshoot the burst field exists to remove. A bucket that
    /// starts full pays out a whole allocation before the drip has
    /// delivered anything, so "ten a minute" really does permit twenty
    /// in the first minute.
    #[test]
    fn an_uncapped_bucket_allows_twice_the_rate_in_the_first_period() {
        let mut limiter = Limiter::new(&per_minute(10), NOW);

        // Ten immediately, from the full bucket.
        for _ in 0..10 {
            assert!(limiter.try_acquire(1, NOW));
        }
        assert!(!limiter.try_acquire(1, NOW));

        // Ten more dripped across the minute: twenty inside [0, 60s].
        let mut dripped = 0;
        for tick in 1..=10 {
            if limiter.try_acquire(1, NOW + tick * 6_000) {
                dripped += 1;
            }
        }

        assert_eq!(dripped, 10, "expected the drip to deliver a second ten");
    }

    /// With the ceiling capped, the same budget cannot front-load
    /// anything: one token is all it ever holds, so dispatches pace out
    /// at the drip rate from the very first one.
    #[test]
    fn a_burst_of_one_paces_evenly_with_no_overshoot() {
        let mut limiter = Limiter::new(&per_minute_bursting(10, 1), NOW);

        // The bucket starts full — but full is one.
        assert!(limiter.try_acquire(1, NOW));
        assert!(!limiter.try_acquire(1, NOW));

        // One per six seconds, and never two.
        for tick in 1..=10 {
            let at = NOW + tick * 6_000;
            assert!(
                limiter.try_acquire(1, at),
                "expected a token at tick {tick}"
            );
            assert!(
                !limiter.try_acquire(1, at),
                "expected only one at tick {tick}"
            );
        }
    }

    /// The long-run rate is unchanged by the cap: the burst governs how
    /// many tokens may be *banked*, not how fast they arrive.
    #[test]
    fn a_burst_does_not_change_the_refill_rate() {
        let mut limiter = Limiter::new(&per_minute_bursting(10, 1), NOW);

        // Drain the single token, then let a full minute pass.
        assert!(limiter.try_acquire(1, NOW));
        limiter.refill(NOW + 60_000);

        // A minute's worth of drip arrived, but only one could be held.
        assert_eq!(limiter.tokens(), 1);
    }

    /// Idling does not bank more than the ceiling, which is the whole
    /// point — an hour of quiet must not become an hour's worth of
    /// dispatches the moment work arrives.
    #[test]
    fn idling_banks_no_more_than_the_burst() {
        let mut limiter = Limiter::new(&per_minute_bursting(10, 3), NOW);

        limiter.refill(NOW + 3_600_000);

        assert_eq!(limiter.tokens(), 3);
    }

    /// A cost above the ceiling can never be paid however long anyone
    /// waits, and says so rather than parking the job forever.
    #[test]
    fn a_cost_above_the_burst_can_never_be_afforded() {
        let limiter = Limiter::new(&per_minute_bursting(100, 5), NOW);

        assert_eq!(limiter.next_available(6, NOW), Availability::Never);
        assert_eq!(limiter.next_available(5, NOW), Availability::Now);
    }
}
