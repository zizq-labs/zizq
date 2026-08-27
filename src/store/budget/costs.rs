// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! What the jobs drawing on a budget currently cost.
//!
//! Answers one question — "what is the largest cost any tracked job
//! draws from this budget?" — which is what stops an operator shrinking
//! an allocation below a cost some already-queued job needs. A job in
//! that state is not merely delayed: nothing it waits for will ever
//! make it affordable, so it stalls until someone notices.

use std::collections::BTreeMap;

/// How many tracked jobs draw each distinct cost from one budget.
///
/// A multiset rather than a running `max: u32`, because costs leave in
/// any order. The job holding the maximum can finish while cheaper ones
/// remain, and a bare maximum has no way to fall back to the next value
/// down — it would stay pinned at a high-water mark that no live job
/// justifies, refusing shrinks that are perfectly safe.
///
/// The map is keyed by cost rather than by job, so its size is bounded
/// by how many *distinct* costs are in play, not by how many jobs are
/// queued. Applications pick a handful of costs and reuse them, so this
/// stays a few entries wide whether ten jobs are waiting or ten million.
///
/// "Tracked" means any non-terminal job: ready, scheduled or in-flight.
/// In-flight counts because a failure sends the job back to scheduled
/// and then to ready without a user ever re-enqueuing it — if the
/// allocation shrank underneath it in the meantime, it would stall on a
/// transition nobody asked for.
#[derive(Debug, Default)]
pub(super) struct CostCounts {
    counts: BTreeMap<u32, usize>,
    total: usize,
}

impl CostCounts {
    /// Record one more job drawing `cost`.
    pub(super) fn add(&mut self, cost: u32) {
        *self.counts.entry(cost).or_insert(0) += 1;
        self.total += 1;
    }

    /// Drop one job drawing `cost`.
    ///
    /// Removing the last job at a cost takes the key with it, so
    /// [`Self::max`] falls back to the next cost down rather than
    /// reporting one no live job draws.
    ///
    /// A cost with no jobs recorded against it is left alone. Reaching
    /// that means a job was untracked twice or never tracked at all,
    /// which is a bug in the caller — but the useful response is to
    /// stay consistent rather than to saturate the total downwards and
    /// have the aggregate lie in the permissive direction for the rest
    /// of the process's life.
    pub(super) fn remove(&mut self, cost: u32) {
        let Some(count) = self.counts.get_mut(&cost) else {
            debug_assert!(false, "untracked cost {cost} removed from a budget");
            return;
        };

        *count -= 1;
        if *count == 0 {
            self.counts.remove(&cost);
        }
        self.total -= 1;
    }

    /// The largest cost any tracked job draws, or `None` when nothing
    /// is tracked.
    pub(super) fn max(&self) -> Option<u32> {
        self.counts.last_key_value().map(|(cost, _)| *cost)
    }

    /// How many jobs are tracked, across every cost.
    ///
    /// Zero is what the deletion guard looks for: a budget nothing
    /// references can go, and one that something references cannot.
    pub(super) fn len(&self) -> usize {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_untouched_budget_has_no_costs() {
        let costs = CostCounts::default();

        assert_eq!(costs.max(), None);
        assert_eq!(costs.len(), 0);
    }

    #[test]
    fn the_maximum_is_the_largest_tracked_cost() {
        let mut costs = CostCounts::default();

        costs.add(5);
        costs.add(50);
        costs.add(1);

        assert_eq!(costs.max(), Some(50));
        assert_eq!(costs.len(), 3);
    }

    /// The reason this is a multiset. A running maximum would stay at
    /// 50 here and refuse a shrink to 10 that nothing justifies.
    #[test]
    fn the_maximum_falls_back_when_the_dearest_job_leaves() {
        let mut costs = CostCounts::default();

        costs.add(5);
        costs.add(50);
        assert_eq!(costs.max(), Some(50));

        costs.remove(50);

        assert_eq!(costs.max(), Some(5));
        assert_eq!(costs.len(), 1);
    }

    /// Jobs sharing a cost are counted, not deduplicated — one of them
    /// finishing must not retire the cost the others still draw.
    #[test]
    fn a_cost_survives_until_its_last_job_leaves() {
        let mut costs = CostCounts::default();

        costs.add(7);
        costs.add(7);
        costs.add(7);

        costs.remove(7);
        assert_eq!(costs.max(), Some(7));
        assert_eq!(costs.len(), 2);

        costs.remove(7);
        costs.remove(7);

        assert_eq!(costs.max(), None);
        assert_eq!(costs.len(), 0);
    }

    #[test]
    fn emptying_and_refilling_starts_over_cleanly() {
        let mut costs = CostCounts::default();

        costs.add(3);
        costs.remove(3);
        assert_eq!(costs.len(), 0);

        costs.add(9);

        assert_eq!(costs.max(), Some(9));
        assert_eq!(costs.len(), 1);
    }

    /// Costs are `u32` and the allocation ceiling is a million, but the
    /// aggregate itself imposes no ordering assumptions worth guessing
    /// at — check the extremes sort the way `BTreeMap` promises.
    #[test]
    fn extreme_costs_still_order_correctly() {
        let mut costs = CostCounts::default();

        costs.add(0);
        costs.add(u32::MAX);
        costs.add(1);

        assert_eq!(costs.max(), Some(u32::MAX));

        costs.remove(u32::MAX);

        assert_eq!(costs.max(), Some(1));
    }
}
