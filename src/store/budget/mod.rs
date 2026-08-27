// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Budgets — named throttles that cap how fast jobs are dispatched.
//!
//! A budget is a token bucket with a name. Jobs reference budgets by
//! key and consume tokens when they dispatch; a job whose budgets have
//! no capacity is not dispatched until they replenish. How tokens
//! replenish is the [`BudgetStrategy`]:
//!
//! - [`BudgetStrategy::TimeBased`] refills on a clock — a rate limit.
//! - [`BudgetStrategy::WhileInFlight`] returns tokens when the job is
//!   acknowledged — a concurrency limit, of which a mutex is the
//!   `allocation: 1` case.
//!
//! This module holds the stored policy only. Token accounting, the
//! per-budget dispatch queues, and the reference a job carries are
//! separate concerns and land alongside dispatch.
//!
//! The `impl Store { ... }` methods for the budget API live in the
//! sibling `ops` submodule.

mod costs;
mod limiter;
mod ops;
mod registry;

pub(in crate::store) use registry::Budgets;

pub(in crate::store) use ops::{
    BudgetPlan, plan_budgets, stage_budgets, sync_created_budgets, unstage_budgets,
    write_created_budgets,
};

/// Exposed for tests that need to construct an inconsistent store on
/// purpose — see the cron defence-in-depth case. No production caller
/// outside this module builds budget keys.
#[cfg(test)]
pub(in crate::store) use ops::make_budget_key;

use serde::{Deserialize, Serialize};

use super::types::StoreError;

/// Largest `allocation` a budget may declare.
///
/// Well beyond any real throttle, and low enough that the token
/// arithmetic stays far from `f64`'s exact-integer range.
pub const MAX_BUDGET_ALLOCATION: u32 = 1_000_000;

/// How a budget's tokens replenish.
///
/// Modelled as an enum rather than a tag plus nullable fields so that
/// a `WhileInFlight` budget cannot carry a refill period. The API layer
/// rejects a request that supplies one; here it is unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "StrategyRepr", try_from = "StrategyRepr")]
pub enum BudgetStrategy {
    /// Refill the whole allocation every `duration_ms`, as a
    /// continuous drip rather than in fixed windows — a fully drained
    /// bucket is full again `duration_ms` later.
    TimeBased {
        /// Period over which the full allocation replenishes.
        duration_ms: u64,

        /// Most tokens the bucket may hold at once, capping how much of
        /// an idle period can be spent in one go. Defaults to
        /// `allocation`.
        ///
        /// A bucket that starts full delivers a whole allocation the
        /// instant work arrives and *then* settles to the drip, so
        /// `10 per minute` really does permit twenty in the first
        /// minute. That is standard behaviour and often the point,
        /// absorbing a spike rather than smearing it. A `burst` of 1
        /// is a constant rate limit with no upfront burst.
        ///
        /// Lives here rather than beside `allocation` because it is
        /// meaningless without a refill period — a `while_in_flight`
        /// budget has no drip to burst ahead of, so the field is
        /// unrepresentable there rather than merely ignored.
        burst: Option<u32>,
    },

    /// Return a token when the job leaves the in-flight state, by
    /// acknowledgement, failure, or worker disconnect.
    WhileInFlight,
}

/// Tag for [`BudgetStrategy::TimeBased`] in the stored form.
const KIND_TIME_BASED: u8 = 0;

/// Tag for [`BudgetStrategy::WhileInFlight`] in the stored form.
const KIND_WHILE_IN_FLIGHT: u8 = 1;

/// Compact stored form of a [`BudgetStrategy`].
///
/// Serde's default enum representation would write the variant *name*
/// into every record. This writes a tag byte plus only the fields that
/// tag implies, matching how the rest of the data keyspace is encoded.
#[derive(Serialize, Deserialize)]
struct StrategyRepr {
    /// Which strategy: `KIND_TIME_BASED` or `KIND_WHILE_IN_FLIGHT`.
    #[serde(rename = "k")]
    kind: u8,

    /// Refill period. Present for `TimeBased` only.
    #[serde(rename = "d")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,

    /// Bucket ceiling. Absent means "the allocation".
    #[serde(rename = "b")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    burst: Option<u32>,
}

impl From<BudgetStrategy> for StrategyRepr {
    fn from(strategy: BudgetStrategy) -> Self {
        match strategy {
            BudgetStrategy::TimeBased { duration_ms, burst } => Self {
                kind: KIND_TIME_BASED,
                duration_ms: Some(duration_ms),
                burst,
            },
            BudgetStrategy::WhileInFlight => Self {
                kind: KIND_WHILE_IN_FLIGHT,
                duration_ms: None,
                burst: None,
            },
        }
    }
}

impl TryFrom<StrategyRepr> for BudgetStrategy {
    type Error = String;

    fn try_from(repr: StrategyRepr) -> Result<Self, Self::Error> {
        match repr.kind {
            KIND_TIME_BASED => match repr.duration_ms {
                Some(duration_ms) => Ok(Self::TimeBased {
                    duration_ms,
                    burst: repr.burst,
                }),
                None => Err("time_based budget is missing duration_ms".to_string()),
            },
            // Rejected rather than dropped. A burst on a strategy with
            // no drip is a request the server cannot honour, and
            // silently ignoring it would let a caller believe a
            // throttle is narrower than it is.
            KIND_WHILE_IN_FLIGHT if repr.burst.is_some() => {
                Err("while_in_flight budget cannot take a burst".to_string())
            }
            KIND_WHILE_IN_FLIGHT => Ok(Self::WhileInFlight),
            other => Err(format!("unknown budget strategy: {other}")),
        }
    }
}

/// Default tokens a job consumes when it names a budget without saying
/// how much of it to draw.
pub const DEFAULT_BUDGET_COST: u32 = 1;

/// What a budget does, without the bookkeeping a stored one carries.
///
/// Separate from [`Budget`] because an enqueue can supply a policy for
/// a budget that does not exist yet, at which point there is no
/// creation time to speak of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    /// Tokens the budget makes available to jobs, whose refill
    /// behaviour varies depending on the strategy.
    #[serde(rename = "a")]
    pub allocation: u32,

    /// How tokens replenish.
    #[serde(rename = "s")]
    pub strategy: BudgetStrategy,
}

/// A job's request to draw on a budget, as supplied at enqueue time.
///
/// Narrows to a [`BudgetRef`] on the stored job — `create_with` is
/// consumed when the enqueue is applied and never persisted per job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetBinding {
    /// Key of the budget to draw from.
    #[serde(rename = "k")]
    pub key: String,

    /// Tokens to consume on dispatch.
    #[serde(rename = "c")]
    pub cost: u32,

    /// Policy to create the budget with when it does not yet exist.
    ///
    /// Ignored when it does — the server is authoritative, so an
    /// enqueue can never quietly restate the throttle an operator
    /// configured. Absent, referencing a budget that does not exist is
    /// an error rather than an unthrottled dispatch.
    #[serde(rename = "w")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_with: Option<BudgetPolicy>,
}

impl BudgetBinding {
    /// Draw one token from the named budget, which must already exist.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            cost: DEFAULT_BUDGET_COST,
            create_with: None,
        }
    }

    /// Set how many tokens this job consumes and return `self`.
    pub fn cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }

    /// Create the budget with this policy if it does not exist, and
    /// return `self`.
    pub fn create_with(mut self, policy: BudgetPolicy) -> Self {
        self.create_with = Some(policy);
        self
    }

    /// The part of this binding that the job carries.
    ///
    /// `to_` rather than `as_`: this clones the key to build an owned
    /// [`BudgetRef`], where `as_` conventionally promises a free borrow
    /// (and would shadow [`AsRef::as_ref`] besides).
    pub(in crate::store) fn to_ref(&self) -> BudgetRef {
        BudgetRef {
            key: self.key.clone(),
            cost: self.cost,
        }
    }
}

/// A job's binding to a budget: which budget, and how much of it the
/// job consumes when it dispatches.
///
/// Kept to the two fields dispatch actually needs, so a job that uses
/// budgets stays as compact as one that does not. The `create_with`
/// policy an enqueue may carry is deliberately absent — it is consumed
/// when the enqueue is applied and never persisted per job, or every
/// job would carry a copy of a policy the server is already
/// authoritative for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetRef {
    /// Key of the budget this job draws from.
    #[serde(rename = "k")]
    pub key: String,

    /// Tokens consumed from that budget on dispatch.
    #[serde(rename = "c")]
    pub cost: u32,
}

impl BudgetRef {
    /// Bind to a budget at the default cost of one token.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            cost: DEFAULT_BUDGET_COST,
        }
    }

    /// Set how many tokens this job consumes and return `self`.
    ///
    /// Not validated here — a cost is only meaningful against the
    /// budget's allocation, which is resolved when the enqueue is
    /// handled.
    pub fn cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }
}

/// A named budget's policy, stored at `B\0{key}` in the data keyspace.
///
/// The key lives in the record's position rather than in the record, as
/// with cron groups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Budget {
    /// Tokens the budget has available when full, though the refill
    /// behaviour varies depending on the strategy.
    #[serde(rename = "a")]
    pub allocation: u32,

    /// How tokens replenish.
    #[serde(rename = "s")]
    pub strategy: BudgetStrategy,

    /// When the budget was created (ms since epoch).
    #[serde(rename = "c")]
    pub created_at: u64,

    /// When the policy was last changed (ms since epoch).
    ///
    /// Equal to `created_at` until something changes it. Stored for
    /// operator auditing purposes (was this budget changed recently?).
    #[serde(rename = "u")]
    pub updated_at: u64,
}

impl Budget {
    /// Build a policy, rejecting one that could never dispatch a job.
    ///
    /// Returns [`StoreError::InvalidOperation`], which the API layer
    /// reports as 422 — these are semantic rejections rather than
    /// malformed input.
    pub fn new(allocation: u32, strategy: BudgetStrategy, now: u64) -> Result<Self, StoreError> {
        // Zero would mean "never dispatch", which is a permanently
        // stuck job dressed up as configuration.
        if allocation == 0 {
            return Err(StoreError::InvalidOperation(
                "budget allocation must be at least 1".to_string(),
            ));
        }

        if allocation > MAX_BUDGET_ALLOCATION {
            return Err(StoreError::InvalidOperation(format!(
                "budget allocation must not exceed {MAX_BUDGET_ALLOCATION}"
            )));
        }

        if let BudgetStrategy::TimeBased { duration_ms, burst } = strategy {
            // A zero-length refill period is an infinite token rate,
            // which is a budget that does not budget.
            if duration_ms == 0 {
                return Err(StoreError::InvalidOperation(
                    "time_based budget duration_ms must be at least 1".to_string(),
                ));
            }

            if let Some(burst) = burst {
                // Zero would hold no tokens at all, so nothing could
                // ever dispatch however long it waited.
                if burst == 0 {
                    return Err(StoreError::InvalidOperation(
                        "time_based budget burst must be at least 1".to_string(),
                    ));
                }

                // Bounded like the allocation, since it is the number
                // that actually sizes the bucket.
                if burst > MAX_BUDGET_ALLOCATION {
                    return Err(StoreError::InvalidOperation(format!(
                        "time_based budget burst must not exceed {MAX_BUDGET_ALLOCATION}"
                    )));
                }
            }

            // Deliberately not rejected: a burst above the allocation.
            // "10 a minute, but tolerate a spike of 50" is a coherent
            // policy — it banks several idle periods rather than one.
        }

        Ok(Self {
            allocation,
            strategy,
            created_at: now,
            updated_at: now,
        })
    }

    /// Most tokens this budget can hold at once.
    ///
    /// The allocation is a *rate* for a `time_based` budget and a
    /// *level* for a `while_in_flight` one; only the second is also the
    /// ceiling. Anything asking "can this job ever be afforded?" wants
    /// this rather than `allocation`, because a burst-capped budget
    /// with a large allocation still cannot pay for a job costing more
    /// than the bucket holds.
    pub fn capacity(&self) -> u32 {
        match self.strategy {
            BudgetStrategy::TimeBased {
                burst: Some(burst), ..
            } => burst,
            _ => self.allocation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    fn round_trip(budget: &Budget) -> Budget {
        let bytes = rmp_serde::to_vec_named(budget).unwrap();
        rmp_serde::from_slice(&bytes).unwrap()
    }

    #[test]
    fn new_sets_both_timestamps_to_now() {
        let budget = Budget::new(10, BudgetStrategy::WhileInFlight, NOW).unwrap();

        assert_eq!(budget.allocation, 10);
        assert_eq!(budget.strategy, BudgetStrategy::WhileInFlight);
        assert_eq!(budget.created_at, NOW);
        assert_eq!(budget.updated_at, NOW);
    }

    #[test]
    fn new_rejects_zero_allocation() {
        let result = Budget::new(0, BudgetStrategy::WhileInFlight, NOW);
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    #[test]
    fn new_accepts_the_maximum_allocation() {
        let budget =
            Budget::new(MAX_BUDGET_ALLOCATION, BudgetStrategy::WhileInFlight, NOW).unwrap();
        assert_eq!(budget.allocation, MAX_BUDGET_ALLOCATION);
    }

    #[test]
    fn new_rejects_an_allocation_above_the_maximum() {
        let result = Budget::new(
            MAX_BUDGET_ALLOCATION + 1,
            BudgetStrategy::WhileInFlight,
            NOW,
        );
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    #[test]
    fn new_rejects_a_zero_duration_time_based_budget() {
        let result = Budget::new(
            10,
            BudgetStrategy::TimeBased {
                duration_ms: 0,
                burst: None,
            },
            NOW,
        );
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    #[test]
    fn time_based_round_trips() {
        let budget = Budget::new(
            100,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: None,
            },
            NOW,
        )
        .unwrap();
        let decoded = round_trip(&budget);

        assert_eq!(decoded.allocation, 100);
        assert_eq!(
            decoded.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: None,
            }
        );
        assert_eq!(decoded.created_at, NOW);
    }

    #[test]
    fn while_in_flight_round_trips() {
        let budget = Budget::new(1, BudgetStrategy::WhileInFlight, NOW).unwrap();
        let decoded = round_trip(&budget);

        assert_eq!(decoded.allocation, 1);
        assert_eq!(decoded.strategy, BudgetStrategy::WhileInFlight);
    }

    /// `while_in_flight` has no refill period, so it should not spend
    /// bytes on an absent one.
    #[test]
    fn while_in_flight_omits_the_duration_field() {
        let budget = Budget::new(1, BudgetStrategy::WhileInFlight, NOW).unwrap();
        let encoded = rmp_serde::to_vec_named(&budget).unwrap();

        assert!(!String::from_utf8_lossy(&encoded).contains('d'));
    }

    #[test]
    fn an_unknown_strategy_tag_is_rejected() {
        let repr = StrategyRepr {
            kind: 99,
            duration_ms: None,
            burst: None,
        };
        assert!(BudgetStrategy::try_from(repr).is_err());
    }

    /// A `time_based` record with no period cannot be turned into a
    /// rate, so decoding fails rather than inventing one.
    #[test]
    fn a_time_based_tag_without_a_duration_is_rejected() {
        let repr = StrategyRepr {
            kind: KIND_TIME_BASED,
            duration_ms: None,
            burst: None,
        };
        assert!(BudgetStrategy::try_from(repr).is_err());
    }

    /// A burst on a strategy with no drip is refused rather than
    /// dropped. Silently ignoring it would let a caller believe a
    /// throttle is narrower than the one actually in force.
    #[test]
    fn a_while_in_flight_tag_with_a_burst_is_rejected() {
        let repr = StrategyRepr {
            kind: KIND_WHILE_IN_FLIGHT,
            duration_ms: None,
            burst: Some(5),
        };
        assert!(BudgetStrategy::try_from(repr).is_err());
    }

    /// Records written before `burst` existed decode to their original
    /// meaning — the ceiling is the allocation — rather than needing a
    /// migration.
    #[test]
    fn a_record_without_a_burst_falls_back_to_the_allocation() {
        let repr = StrategyRepr {
            kind: KIND_TIME_BASED,
            duration_ms: Some(60_000),
            burst: None,
        };
        let strategy = BudgetStrategy::try_from(repr).unwrap();
        let budget = Budget::new(10, strategy, NOW).unwrap();

        assert_eq!(budget.capacity(), 10);
    }

    #[test]
    fn a_burst_caps_the_bucket_below_the_allocation() {
        let budget = Budget::new(
            100,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: Some(5),
            },
            NOW,
        )
        .unwrap();

        assert_eq!(budget.capacity(), 5);
    }

    /// "A hundred a minute, but tolerate a spike of five hundred" is a
    /// coherent policy: it banks several idle periods rather than one.
    #[test]
    fn a_burst_above_the_allocation_is_allowed() {
        let budget = Budget::new(
            100,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: Some(500),
            },
            NOW,
        )
        .unwrap();

        assert_eq!(budget.capacity(), 500);
    }

    #[test]
    fn new_rejects_a_zero_burst() {
        let result = Budget::new(
            10,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: Some(0),
            },
            NOW,
        );
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    #[test]
    fn new_rejects_a_burst_above_the_maximum() {
        let result = Budget::new(
            10,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000,
                burst: Some(MAX_BUDGET_ALLOCATION + 1),
            },
            NOW,
        );
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// A concurrency budget has no burst to speak of, so its capacity
    /// is simply the allocation.
    #[test]
    fn a_concurrency_budget_holds_its_allocation() {
        let budget = Budget::new(7, BudgetStrategy::WhileInFlight, NOW).unwrap();

        assert_eq!(budget.capacity(), 7);
    }
}
