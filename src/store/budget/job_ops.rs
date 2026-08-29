// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Applying a [`BudgetMutation`] to a job.
//!
//! [`BudgetMutation`] works out *what* a job's bindings should become;
//! this applies those bindings, which is much more involved than simply
//! writing the record.
//!
//! # Changing a binding moves the job
//!
//! A budgeted job does not live in the ready index — it is parked in the
//! group of every budget it draws on, and that placement is derived from
//! the bindings themselves. So a re-bind has to unpark from the old
//! groups and park into the new along with the write.
//!
//! It also shifts the cost accounting the delete and shrink guards read,
//! and may create budgets, if the caller supplied a `create_with` for
//! one that does not exist yet. All of it belongs to one transaction: a
//! job whose record says one thing while the registry says another is
//! either throttled by a budget it no longer draws on, or not throttled
//! by one it does.
//!
//! # Queued jobs only
//!
//! A dispatched job holds tokens against its *current* bindings, and by
//! the release rules its slot returns to whatever binding it holds when
//! it finishes. Re-binding it mid-flight would either credit the new
//! budget for a slot it never issued, or strand a token on the old one
//! forever. Nothing here can make that safe, so it is refused instead.
//!
//! The same restriction falls out for terminal jobs, which hold nothing
//! and will never run again.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fjall::Slice;
use tokio::task;

use super::super::dispatch::Placement;
use super::super::keys::make_job_key;
use super::super::store::{Store, StoreEvent};
use super::super::types::{Job, JobStatus, StoreError};
use super::BudgetPlan;
use super::mutation::{BudgetMutation, BudgetMutationError, BudgetMutationOutcome};
use super::ops::{plan_budgets, stage_budgets, unstage_budgets, write_created_budgets};

/// What applying a mutation to one job did.
///
/// Every arm is a distinct answer rather than an error, because the two
/// callers want opposite things from most of them. A route naming one
/// job turns them into status codes; a bulk route over a filtered set
/// treats the last three as "skip this one" — a job already bound is not
/// a failure of "add this budget to everything matching".
#[allow(
    dead_code,
    reason = "consumed by the job-budget routes in the next commit"
)]
#[derive(Debug)]
pub(in crate::store) enum JobBudgetChange {
    /// The bindings changed. Carries the job as it now stands.
    Changed(Job),

    /// The job already satisfied the request, so nothing was written.
    Unchanged(Job),

    /// No job with that id.
    JobNotFound,

    /// The job is in flight or finished, so its bindings may not move.
    NotQueued(JobStatus),

    /// [`BudgetMutation::Add`] against a budget the job already draws on.
    AlreadyBound(String),

    /// [`BudgetMutation::SetCost`] against one it does not.
    NotBound(String),
}

impl Store {
    /// Apply a mutation to one job's budgets.
    ///
    /// Only queued jobs may change — see the module docs for why. A
    /// mutation that leaves the bindings as they were writes nothing at
    /// all.
    ///
    /// Budgets named by a `create_with` are created as part of the same
    /// transaction, exactly as an enqueue would, so binding a job to a
    /// budget that does not exist yet is one call rather than two.
    #[allow(
        dead_code,
        reason = "consumed by the job-budget routes in the next commit"
    )]
    pub(in crate::store) async fn patch_job_budgets(
        &self,
        id: &str,
        mutation: BudgetMutation,
        now: u64,
    ) -> Result<JobBudgetChange, StoreError> {
        let ks = self.ks.clone();
        let dispatch = self.dispatch.clone();
        let live = self.budgets.clone();
        let id = id.to_string();

        let change = task::spawn_blocking(move || -> Result<JobBudgetChange, StoreError> {
            let job_key = make_job_key(&id);

            // Retry loop: pre-read and decide outside the tx, then
            // compare-and-write inside. Retries only when the job
            // changed under us between the two.
            loop {
                // ---- outside tx ----
                let Some(pre_bytes) = ks.data.get(&job_key)? else {
                    return Ok(JobBudgetChange::JobNotFound);
                };

                let job: Job = rmp_serde::from_slice(&pre_bytes)?;
                let status = JobStatus::try_from(job.status).map_err(|v| {
                    StoreError::Corruption(format!("job {id} has unrecognized status byte: {v}"))
                })?;

                if !matches!(status, JobStatus::Ready | JobStatus::Scheduled) {
                    return Ok(JobBudgetChange::NotQueued(status));
                }

                let next = match mutation.apply(&job.budgets) {
                    Ok(BudgetMutationOutcome::Changed(next)) => next,
                    // Nothing to write, nothing to move, nothing to
                    // re-account. Returned as the job already is.
                    Ok(BudgetMutationOutcome::Unchanged) => {
                        return Ok(JobBudgetChange::Unchanged(job));
                    }
                    Err(BudgetMutationError::AlreadyBound(key)) => {
                        return Ok(JobBudgetChange::AlreadyBound(key));
                    }
                    Err(BudgetMutationError::NotBound(key)) => {
                        return Ok(JobBudgetChange::NotBound(key));
                    }
                    // Malformed rather than inapplicable: the request
                    // names one budget twice whatever job it is aimed
                    // at, so it is wrong for all of them.
                    Err(BudgetMutationError::DuplicateKey(key)) => {
                        return Err(StoreError::InvalidOperation(format!(
                            "budget '{key}' named more than once"
                        )));
                    }
                };

                let previous = job.budgets.clone();
                let mut updated = job.clone();
                updated.budgets = next;
                let updated_slice: Slice = rmp_serde::to_vec_named(&updated)?.into();

                // ---- inside tx ----
                let mut tx = ks.write_tx();

                // Same pre-pass an enqueue runs: every budget the
                // mutation introduces must exist or be creatable, and
                // its cost must fit what the bucket can hold. Resolved
                // under the write lock so the answer cannot change
                // between the check and the write it guards.
                let created = match plan_budgets(&tx, &ks, mutation.introduced().iter(), now)? {
                    BudgetPlan::Proceed(created) => created,
                    BudgetPlan::Reject(e) => return Err(e),
                };
                write_created_budgets(&mut tx, &ks, &created)?;

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                // Staged before the commit, while the write lock still
                // excludes every other writer — see `stage_budgets`. The
                // *new* bindings are counted up here; the old ones are
                // counted down after, so the accounting reads high for a
                // moment rather than low.
                stage_budgets(&live, &created, std::iter::once(&updated), now);

                if let Err(e) = ks.commit(tx, ks.default_commit_mode) {
                    unstage_budgets(&live, &created, std::iter::once(&updated));
                    return Err(e);
                }

                live.untrack(&previous);

                // A scheduled job is not placed for dispatch at all, so
                // there is nothing to move; it is re-placed from its new
                // bindings when it is promoted.
                if status == JobStatus::Ready {
                    dispatch.remove(Placement {
                        queue: &updated.queue,
                        priority: updated.priority,
                        id: &updated.id,
                        budgets: &previous,
                    });
                    dispatch.insert(Placement::of(&updated));
                }

                return Ok(JobBudgetChange::Changed(updated));
            }
        })
        .await??;

        // Announced outside the closure, which holds the keyspace and
        // the registry but not the event channel.
        //
        // Worth announcing whichever way the bindings moved. Unbinding a
        // budget can make a job dispatchable that was throttled a moment
        // ago, and rebinding it moves it between groups — in both cases
        // a worker blocked on an empty queue has no other reason to look
        // again.
        if let JobBudgetChange::Changed(job) = &change
            && job.status == JobStatus::Ready as u8
        {
            let _ = self.event_tx.send(StoreEvent::JobDispatchable {
                id: job.id.clone(),
                queue: job.queue.clone(),
                token: Arc::new(AtomicBool::new(false)),
            });
        }

        Ok(change)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::super::options::EnqueueOptions;
    use super::super::super::test_support::test_store;
    use super::super::{BudgetBinding, BudgetStrategy};
    use super::*;
    use crate::time::now_millis;

    const NOW: u64 = 1_700_000_000_000;

    async fn store_with_budgets(keys: &[&str]) -> Store {
        let store = test_store();
        for key in keys {
            store
                .create_budget(key, 100, BudgetStrategy::WhileInFlight, NOW)
                .await
                .unwrap();
        }
        store
    }

    async fn enqueue_bound(store: &Store, bindings: &[(&str, u32)]) -> Job {
        let mut opts = EnqueueOptions::new("t", "q", serde_json::json!({}));
        for (key, cost) in bindings {
            opts = opts.budget(BudgetBinding::new(*key).cost(*cost));
        }
        store.enqueue(now_millis(), opts).await.unwrap().into_job()
    }

    fn changed(change: JobBudgetChange) -> Job {
        match change {
            JobBudgetChange::Changed(job) => job,
            other => panic!("expected a change, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn adding_a_budget_binds_it_and_counts_its_cost() {
        let store = store_with_budgets(&["stripe", "mailgun"]).await;
        let job = enqueue_bound(&store, &[("stripe", 1)]).await;

        let updated = changed(
            store
                .patch_job_budgets(
                    &job.id,
                    BudgetMutation::Add(BudgetBinding::new("mailgun").cost(4)),
                    now_millis(),
                )
                .await
                .unwrap(),
        );

        assert_eq!(updated.budgets.len(), 2);
        assert_eq!(store.budgets.tracked("mailgun"), 1);
        assert_eq!(store.budgets.max_cost("mailgun"), Some(4));
        // The binding it already had is untouched.
        assert_eq!(store.budgets.tracked("stripe"), 1);
    }

    /// Unbinding has to release the cost accounting as well as the
    /// record, or the budget stays undeletable on the strength of a job
    /// that no longer draws on it.
    #[tokio::test]
    async fn removing_a_budget_stops_counting_against_it() {
        let store = store_with_budgets(&["stripe"]).await;
        let job = enqueue_bound(&store, &[("stripe", 5)]).await;
        assert_eq!(store.budgets.tracked("stripe"), 1);

        changed(
            store
                .patch_job_budgets(
                    &job.id,
                    BudgetMutation::Remove {
                        key: "stripe".into(),
                    },
                    now_millis(),
                )
                .await
                .unwrap(),
        );

        assert_eq!(store.budgets.tracked("stripe"), 0);
        assert_eq!(store.budgets.max_cost("stripe"), None);

        // And with nothing drawing on it, the budget deletes cleanly.
        assert!(store.delete_budget("stripe").await.unwrap());
    }

    /// The part a read of the job record cannot show: a budgeted job is
    /// parked in its budgets' groups, so a re-bind has to move it. If it
    /// stayed in the old group it would be dispatched under a throttle
    /// it no longer draws on — and never under the one it does.
    #[tokio::test]
    async fn re_binding_moves_the_job_between_budget_groups() {
        let store = test_store();
        store
            .create_budget("old", 1, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        store
            .create_budget("new", 1, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let blocker = enqueue_bound(&store, &[("old", 1)]).await;
        let moved = enqueue_bound(&store, &[("old", 1)]).await;

        // Drain `old` so anything parked on it is stuck.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, blocker.id);
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // Move the second job onto a budget with capacity to spare.
        changed(
            store
                .patch_job_budgets(
                    &moved.id,
                    BudgetMutation::ReplaceAll(vec![BudgetBinding::new("new")]),
                    now_millis(),
                )
                .await
                .unwrap(),
        );

        // It dispatches now, which it could not have from `old`.
        let dispatched = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dispatched.id, moved.id);
    }

    /// Unbinding entirely returns a job to the plain ready index, so it
    /// dispatches without consulting any budget.
    #[tokio::test]
    async fn unbinding_everything_makes_a_job_unthrottled() {
        let store = store_with_budgets(&["solo"]).await;
        // Allocation of one, already spent by another job.
        let holder = enqueue_bound(&store, &[("solo", 1)]).await;
        let stuck = enqueue_bound(&store, &[("solo", 1)]).await;

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, holder.id);

        store
            .patch_budget(
                "solo",
                crate::store::PatchBudgetOptions {
                    allocation: Some(1),
                    strategy: Default::default(),
                },
                now_millis(),
            )
            .await
            .unwrap();

        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        changed(
            store
                .patch_job_budgets(&stuck.id, BudgetMutation::RemoveAll, now_millis())
                .await
                .unwrap(),
        );

        let dispatched = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dispatched.id, stuck.id);
    }

    /// Binding to a budget that does not exist yet is one call, as it is
    /// on enqueue.
    #[tokio::test]
    async fn create_with_creates_the_budget_it_names() {
        let store = test_store();
        let job = enqueue_bound(&store, &[]).await;

        changed(
            store
                .patch_job_budgets(
                    &job.id,
                    BudgetMutation::Add(BudgetBinding::new("fresh").cost(2).create_with(
                        super::super::BudgetPolicy {
                            allocation: 50,
                            strategy: BudgetStrategy::WhileInFlight,
                        },
                    )),
                    now_millis(),
                )
                .await
                .unwrap(),
        );

        let budget = store.get_budget("fresh").await.unwrap().unwrap();
        assert_eq!(budget.allocation, 50);
        assert_eq!(store.budgets.tracked("fresh"), 1);
    }

    #[tokio::test]
    async fn binding_to_an_unknown_budget_is_refused() {
        let store = test_store();
        let job = enqueue_bound(&store, &[]).await;

        let result = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::Add(BudgetBinding::new("ghost")),
                now_millis(),
            )
            .await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// A cost the bucket could never hold would never dispatch, so it is
    /// refused here exactly as it is on enqueue.
    #[tokio::test]
    async fn a_cost_above_the_capacity_is_refused() {
        let store = test_store();
        store
            .create_budget("small", 5, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        let job = enqueue_bound(&store, &[]).await;

        let result = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::Add(BudgetBinding::new("small").cost(50)),
                now_millis(),
            )
            .await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    /// An in-flight job holds tokens against the bindings it was
    /// dispatched under, so moving them would either invent a slot on
    /// the new budget or strand one on the old.
    #[tokio::test]
    async fn an_in_flight_job_is_refused() {
        let store = store_with_budgets(&["stripe", "mailgun"]).await;
        let job = enqueue_bound(&store, &[("stripe", 1)]).await;

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let change = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::Add(BudgetBinding::new("mailgun")),
                now_millis(),
            )
            .await
            .unwrap();

        assert!(matches!(
            change,
            JobBudgetChange::NotQueued(JobStatus::InFlight)
        ));
        // And nothing moved.
        assert_eq!(store.budgets.tracked("mailgun"), 0);
    }

    #[tokio::test]
    async fn a_missing_job_is_reported_rather_than_erroring() {
        let store = test_store();

        let change = store
            .patch_job_budgets(
                "0000000000000000000000000",
                BudgetMutation::RemoveAll,
                now_millis(),
            )
            .await
            .unwrap();

        assert!(matches!(change, JobBudgetChange::JobNotFound));
    }

    #[tokio::test]
    async fn already_bound_and_not_bound_are_reported_separately() {
        let store = store_with_budgets(&["stripe"]).await;
        let job = enqueue_bound(&store, &[("stripe", 1)]).await;

        let change = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::Add(BudgetBinding::new("stripe")),
                now_millis(),
            )
            .await
            .unwrap();
        assert!(matches!(change, JobBudgetChange::AlreadyBound(k) if k == "stripe"));

        let change = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::SetCost {
                    key: "absent".into(),
                    cost: 2,
                },
                now_millis(),
            )
            .await
            .unwrap();
        assert!(matches!(change, JobBudgetChange::NotBound(k) if k == "absent"));
    }

    /// A request the job already satisfies writes nothing — no record,
    /// no placement move, no change to the accounting.
    #[tokio::test]
    async fn a_no_op_request_leaves_the_job_alone() {
        let store = store_with_budgets(&["stripe"]).await;
        let job = enqueue_bound(&store, &[("stripe", 3)]).await;

        let change = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::SetCost {
                    key: "stripe".into(),
                    cost: 3,
                },
                now_millis(),
            )
            .await
            .unwrap();

        assert!(matches!(change, JobBudgetChange::Unchanged(_)));
        assert_eq!(store.budgets.tracked("stripe"), 1);
        assert_eq!(store.budgets.max_cost("stripe"), Some(3));
    }

    /// Scheduled jobs are not placed for dispatch, but they are counted:
    /// the guards have to see work that is committed but not yet due.
    #[tokio::test]
    async fn a_scheduled_job_can_be_rebound() {
        let store = store_with_budgets(&["stripe"]).await;
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({})).ready_at(now + 600_000),
            )
            .await
            .unwrap()
            .into_job();

        changed(
            store
                .patch_job_budgets(
                    &job.id,
                    BudgetMutation::Add(BudgetBinding::new("stripe").cost(6)),
                    now_millis(),
                )
                .await
                .unwrap(),
        );

        assert_eq!(store.budgets.tracked("stripe"), 1);
        assert_eq!(store.budgets.max_cost("stripe"), Some(6));
    }
}
