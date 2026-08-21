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
#[allow(dead_code, reason = "wired up when budgeted jobs reach dispatch")]
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
        Self {
            capacity: budget.allocation,
            tokens: budget.allocation,
            credit: 0,
            drip: Self::drip(budget),
            last_refill: now,
        }
    }

    /// The clock-driven refill rate for a policy, if it has one.
    fn drip(budget: &Budget) -> Option<Drip> {
        match budget.strategy {
            BudgetStrategy::TimeBased { duration_ms } => Some(Drip {
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

        self.capacity = budget.allocation;
        self.drip = Self::drip(budget);
        self.tokens = self.tokens.min(self.capacity);

        // Banked progress was measured against the old period, so it
        // means nothing under the new one. At most a fraction of a
        // token is lost.
        self.credit = 0;
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

    /// Give `cost` tokens back.
    ///
    /// Capped at capacity, which matters after a budget is shrunk: jobs
    /// acquired under the old allocation return more than the new
    /// bucket holds, and the excess is simply dropped.
    pub(super) fn release(&mut self, cost: u32) {
        self.tokens = self.tokens.saturating_add(cost).min(self.capacity);
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
            },
            NOW,
        )
        .unwrap();

        let mut limiter = Limiter::new(&budget, NOW);
        assert!(limiter.try_acquire(crate::store::MAX_BUDGET_ALLOCATION, NOW));

        limiter.refill(u64::MAX);
        assert_ne!(limiter.next_available(1, NOW), Availability::Never);
    }
}
