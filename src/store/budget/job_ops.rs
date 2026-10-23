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

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fjall::{Readable, Slice};
use tokio::task;

use super::super::dispatch::Placement;
use super::super::keys::make_job_key;
use super::super::options::JobFilter;
use super::super::scan::{JobStream, apply_filters, build_id_stream, filter_needs_payload};
use super::super::store::{Keyspaces, Store, StoreEvent};
use super::super::types::{Job, JobStatus, ScanDirection, StoreError};
use super::super::writer::{Applied, JobOutcome, JobReply, JobWrite, WriterContext};
use super::mutation::{BudgetMutation, BudgetMutationError, BudgetMutationOutcome};
use super::ops::{plan_budgets, stage_budgets, unstage_budgets, write_created_budgets};
use super::registry::Budgets;
use super::{Budget, BudgetBinding, BudgetPlan, BudgetRef};

/// What applying a mutation to one job did.
///
/// Every arm is a distinct answer rather than an error, because the two
/// callers want opposite things from most of them. A route naming one
/// job turns them into status codes; a bulk route over a filtered set
/// treats the last three as "skip this one" — a job already bound is not
/// a failure of "add this budget to everything matching".
#[derive(Debug)]
pub enum JobBudgetChange {
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

/// A job the bulk pass is about to rewrite.
///
/// Carried out of the scan so the placement move and the untracking of
/// the *old* bindings can happen after the commit.
///
/// The two halves of the accounting are deliberately split around it:
/// the new bindings are tracked *before* the commit by `stage_budgets`,
/// the old ones untracked *after*. That brackets the change, so in the
/// window between them both are counted — a cost raised from one to two
/// reads a maximum of two, and one lowered from five to two reads five.
/// High in either direction, which costs at worst a delete or shrink
/// refused that would have been safe. The opposite order would leave the
/// aggregate briefly reporting less work than exists, and a guard
/// reading it then would let a budget be shrunk out from under a job.
struct Rewritten {
    previous: Vec<super::BudgetRef>,
    updated: Job,
    status: JobStatus,
}

/// What applying a mutation across a matched set did.
///
/// Counts what happened and names what was prevented, which are
/// different questions. A caller wants to know how much moved, and then
/// wants to be able to *do something* about whatever did not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkBudgetChange {
    /// Jobs whose bindings moved.
    pub changed: usize,

    /// Jobs the mutation would have changed, but could not, because they
    /// were in flight.
    ///
    /// Named rather than counted, because this is the one outcome a
    /// caller can act on — wait for the jobs to leave the in-flight
    /// state and try those ids again.
    ///
    /// **Jobs the mutation simply did not apply to are absent**, not
    /// counted here or anywhere. A `Remove` against a job that never had
    /// the binding, an `Add` against one already bound, a cost already
    /// at the requested value: these are the documented behaviour of the
    /// bulk verbs rather than a shortfall. Counting them would make the
    /// number roughly "jobs that exist" — an unfiltered unbind would
    /// report the whole store as skipped, which is both useless and
    /// alarming.
    ///
    /// **Terminal jobs are absent too**, deliberately. A finished job's
    /// bindings are inert: it will never dispatch, it is already
    /// untracked from the cost accounting, and it is parked nowhere.
    /// Nothing can be done about it and nothing needs to be, so listing
    /// them would reintroduce the unbounded list this exists to avoid.
    ///
    /// That leaves a set bounded by how much work is in flight at once,
    /// which is small enough to name.
    pub blocked: Vec<String>,
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
    pub async fn patch_job_budgets(
        &self,
        id: &str,
        mutation: BudgetMutation,
        now: u64,
    ) -> Result<JobBudgetChange, StoreError> {
        validate(&mutation)?;
        let mutation = Arc::new(mutation);

        // Prepared here, on a blocking thread, so the writer only checks
        // and writes. A job changed between this pre-read and the
        // writer's check comes back as a conflict: pre-read again.
        let change = loop {
            let ks = self.ks.clone();
            let writer = self.writer.clone();
            let live = self.budgets.clone();
            let id = id.to_string();
            let mutation = mutation.clone();

            let prepared = task::spawn_blocking(move || -> Result<_, StoreError> {
                let (reply, rx) = tokio::sync::oneshot::channel();
                Ok(
                    match JobBudgetOp::prepare(&ks, &id, &mutation, live, now, reply)? {
                        ControlFlow::Continue(op) => {
                            writer.submit_job(op.into());
                            Err(rx)
                        }
                        ControlFlow::Break(change) => Ok(change),
                    },
                )
            })
            .await??;

            let reply_rx = match prepared {
                Ok(change) => break change,
                Err(reply_rx) => reply_rx,
            };
            match reply_rx
                .await
                .map_err(|_| StoreError::Internal("writer channel closed".into()))??
            {
                JobOutcome::Done(change) => break change,
                JobOutcome::Conflict => continue,
            }
        };

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

    /// Apply a mutation to every job a filter matches.
    ///
    /// Skips rather than fails on anything that is true of one job but
    /// not of the request: already bound, not bound, in flight, finished,
    /// or already in the requested state. A per-job `409` has nowhere to
    /// go in an operation over a matched set, and "add this budget to
    /// everything matching" is not wrong about the ones that have it.
    ///
    /// Malformed requests still fail outright — an unknown budget, a
    /// cost the bucket could never hold, a repeated key, a cost of zero.
    /// Those are wrong for every job the filter could match, so failing
    /// half way through would leave the caller worse off than refusing.
    ///
    /// One transaction for the whole set, like the other bulk
    /// operations. The budgets to resolve are the same for every job,
    /// since the mutation is, so the pre-pass runs once rather than per
    /// job.
    pub async fn patch_jobs_budgets(
        &self,
        filter: JobFilter,
        mutation: BudgetMutation,
        now: u64,
    ) -> Result<BulkBudgetChange, StoreError> {
        validate(&mutation)?;

        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = JobsBudgetOp {
            filter,
            mutation,
            now,
            live: self.budgets.clone(),
            created: Vec::new(),
            rewritten: Vec::new(),
            staged: false,
            result: None,
            reply,
        };
        let writer = self.writer.clone();
        task::spawn_blocking(move || writer.submit_job(op.into())).await?;

        match rx
            .await
            .map_err(|_| StoreError::Internal("writer channel closed".into()))??
        {
            JobOutcome::Done(change) => Ok(change),
            JobOutcome::Conflict => Err(StoreError::Internal(
                "bulk budget change reported a conflict".into(),
            )),
        }
    }
}

/// Reject a mutation that is malformed whatever job it meets.
fn validate(mutation: &BudgetMutation) -> Result<(), StoreError> {
    match mutation.validate() {
        Ok(()) => Ok(()),
        Err(BudgetMutationError::DuplicateKey(key)) => Err(StoreError::InvalidOperation(format!(
            "budget '{key}' named more than once"
        ))),
        Err(BudgetMutationError::ZeroCost(key)) => Err(StoreError::InvalidOperation(format!(
            "budget '{key}' cost must be at least 1"
        ))),
        Err(e) => Err(StoreError::Internal(format!(
            "unexpected budget mutation error: {e:?}"
        ))),
    }
}

/// A change to one queued job's budget bindings, prepared by its caller.
pub(in crate::store) struct JobBudgetOp {
    job_key: Vec<u8>,
    pre_bytes: Slice,
    previous: Vec<BudgetRef>,
    /// The job as it will be once re-bound.
    updated: Job,
    updated_slice: Slice,
    status: JobStatus,
    /// Every binding whose cost the mutation sets, checked in the pass.
    costed: Vec<BudgetBinding>,
    now: u64,
    live: Arc<Budgets>,
    /// Filled in by `apply`.
    created: Vec<(String, Budget)>,
    staged: bool,
    rejected: Option<StoreError>,
    reply: JobReply<JobBudgetChange>,
}

impl JobBudgetOp {
    /// Pre-read the job and work out its new bindings, ready for the
    /// writer, or break with the answer when there is nothing to write.
    fn prepare(
        ks: &Keyspaces,
        id: &str,
        mutation: &BudgetMutation,
        live: Arc<Budgets>,
        now: u64,
        reply: JobReply<JobBudgetChange>,
    ) -> Result<ControlFlow<JobBudgetChange, Self>, StoreError> {
        let job_key = make_job_key(id);
        let Some(pre_bytes) = ks.data.get(&job_key)? else {
            return Ok(ControlFlow::Break(JobBudgetChange::JobNotFound));
        };

        let job: Job = rmp_serde::from_slice(&pre_bytes)?;
        let status = JobStatus::try_from(job.status).map_err(|v| {
            StoreError::Corruption(format!("job {id} has unrecognized status byte: {v}"))
        })?;

        if !matches!(status, JobStatus::Ready | JobStatus::Scheduled) {
            return Ok(ControlFlow::Break(JobBudgetChange::NotQueued(status)));
        }

        let next = match mutation.apply(&job.budgets) {
            Ok(BudgetMutationOutcome::Changed(next)) => next,
            // Nothing to write, nothing to move, nothing to re-account.
            // Returned as the job already is.
            Ok(BudgetMutationOutcome::Unchanged) => {
                return Ok(ControlFlow::Break(JobBudgetChange::Unchanged(job)));
            }
            Err(BudgetMutationError::AlreadyBound(key)) => {
                return Ok(ControlFlow::Break(JobBudgetChange::AlreadyBound(key)));
            }
            Err(BudgetMutationError::NotBound(key)) => {
                return Ok(ControlFlow::Break(JobBudgetChange::NotBound(key)));
            }
            // Ruled out by `validate` before any job is read.
            Err(e) => {
                return Err(StoreError::Internal(format!(
                    "unexpected budget mutation error: {e:?}"
                )));
            }
        };

        let previous = job.budgets.clone();
        let mut updated = job;
        updated.budgets = next;
        let updated_slice = rmp_serde::to_vec_named(&updated)?.into();

        Ok(ControlFlow::Continue(Self {
            job_key,
            pre_bytes,
            previous,
            updated,
            updated_slice,
            status,
            costed: mutation.costed(),
            now,
            live,
            created: Vec::new(),
            staged: false,
            rejected: None,
            reply,
        }))
    }
}

impl JobWrite for JobBudgetOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        // Checked before any budget is created for it, so a conflict
        // leaves nothing in the transaction.
        if tx.get(&ks.data, &self.job_key)?.as_deref() != Some(&*self.pre_bytes) {
            return Ok(Applied::Conflict);
        }

        // Same pre-pass an enqueue runs: every budget whose cost this
        // mutation sets must exist or be creatable, and that cost must
        // fit what the bucket can hold. Read through the transaction, so
        // the answer cannot change between the check and the write it
        // guards.
        self.created = match plan_budgets(&*tx, ks, self.costed.iter(), self.now)? {
            BudgetPlan::Proceed(created) => created,
            BudgetPlan::Reject(e) => {
                self.rejected = Some(e);
                return Ok(Applied::Nothing);
            }
        };
        write_created_budgets(tx, ks, &self.created)?;
        tx.insert(&ks.data, &self.job_key, self.updated_slice.clone());

        // Staged before the commit, while no other write can come between
        // — see `stage_budgets`. The *new* bindings are counted up here;
        // the old ones are counted down once durable, so the accounting
        // reads high for a moment rather than low.
        stage_budgets(
            &self.live,
            &self.created,
            std::iter::once(&self.updated),
            self.now,
        );
        self.staged = true;
        Ok(Applied::Wrote)
    }

    fn discard(&mut self) {
        if std::mem::take(&mut self.staged) {
            unstage_budgets(&self.live, &self.created, std::iter::once(&self.updated));
        }
    }

    fn finish(self, cx: &WriterContext) {
        if let Some(e) = self.rejected {
            let _ = self.reply.send(Err(e));
            return;
        }

        self.live.untrack(&self.previous);

        // A scheduled job is not placed for dispatch at all, so there is
        // nothing to move; it is re-placed from its new bindings when it
        // is promoted.
        if self.status == JobStatus::Ready {
            cx.dispatch.remove(Placement {
                queue: &self.updated.queue,
                priority: self.updated.priority,
                id: &self.updated.id,
                budgets: &self.previous,
            });
            cx.dispatch.insert(Placement::of(&self.updated));
        }

        let _ = self
            .reply
            .send(Ok(JobOutcome::Done(JobBudgetChange::Changed(self.updated))));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

/// A change to the budget bindings of every job a filter matches, all or
/// nothing.
///
/// Exclusive: it runs in a pass of its own, so the scan sees exactly the
/// state its writes go into.
pub(in crate::store) struct JobsBudgetOp {
    filter: JobFilter,
    mutation: BudgetMutation,
    now: u64,
    live: Arc<Budgets>,
    /// Filled in by `apply`.
    created: Vec<(String, Budget)>,
    rewritten: Vec<Rewritten>,
    staged: bool,
    result: Option<Result<BulkBudgetChange, StoreError>>,
    reply: JobReply<BulkBudgetChange>,
}

impl JobsBudgetOp {
    fn scan(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Vec<String>, StoreError> {
        let snapshot = ks.db.read_tx();
        let filter = &self.filter;
        let needs_payload = filter_needs_payload(filter);
        let jobs = match build_id_stream(&snapshot, ks, filter, &None, ScanDirection::Asc) {
            Some((id_stream, source, _)) => apply_filters(
                JobStream::by_id(id_stream, &snapshot, &ks.data, None, needs_payload, source),
                filter,
            ),
            None => apply_filters(
                JobStream::full_scan(
                    &snapshot,
                    ks,
                    &None,
                    ScanDirection::Asc,
                    None,
                    needs_payload,
                ),
                filter,
            ),
        };

        let mut blocked = Vec::new();
        for job in jobs {
            let job = job?;
            let status = JobStatus::try_from(job.status).map_err(|v| {
                StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
            })?;

            // Applied before the state is consulted, so that "would have
            // changed but could not" can be told apart from "there was
            // nothing to do". Only the first is worth reporting.
            let next = match self.mutation.apply(&job.budgets) {
                Ok(BudgetMutationOutcome::Changed(next)) => next,
                // Nothing to do. Not a shortfall, and not reported: these
                // are what the bulk verbs promise to pass over.
                Ok(BudgetMutationOutcome::Unchanged)
                | Err(BudgetMutationError::AlreadyBound(_))
                | Err(BudgetMutationError::NotBound(_)) => continue,
                // Ruled out by `validate` before the op was submitted.
                Err(e) => {
                    return Err(StoreError::Internal(format!(
                        "unexpected budget mutation error: {e:?}"
                    )));
                }
            };

            match status {
                JobStatus::Ready | JobStatus::Scheduled => {}
                // Live work whose bindings cannot move while it holds
                // tokens against them. The caller can try again once it
                // has left the in-flight state.
                JobStatus::InFlight => {
                    blocked.push(job.id);
                    continue;
                }
                // Finished, so its bindings are record-keeping: it will
                // never dispatch and is already untracked. Nothing to do
                // and nothing to report.
                JobStatus::Completed | JobStatus::Dead => continue,
            }

            let previous = job.budgets.clone();
            let mut updated = job;
            updated.budgets = next;
            tx.insert(
                &ks.data,
                make_job_key(&updated.id),
                rmp_serde::to_vec_named(&updated)?,
            );
            self.rewritten.push(Rewritten {
                previous,
                updated,
                status,
            });
        }
        Ok(blocked)
    }
}

impl JobWrite for JobsBudgetOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        self.rewritten.clear();

        // Resolved once, before anything is written. Every job takes the
        // same mutation, so the budgets it names and the costs it sets
        // are the same whichever job is being looked at.
        let costed = self.mutation.costed();
        self.created = match plan_budgets(&*tx, ks, costed.iter(), self.now)? {
            BudgetPlan::Proceed(created) => created,
            BudgetPlan::Reject(e) => {
                self.result = Some(Err(e));
                return Ok(Applied::Nothing);
            }
        };
        write_created_budgets(tx, ks, &self.created)?;

        let blocked = self.scan(tx, ks)?;
        self.result = Some(Ok(BulkBudgetChange {
            changed: self.rewritten.len(),
            blocked,
        }));

        // Nothing written and nothing created: nothing to commit.
        if self.rewritten.is_empty() && self.created.is_empty() {
            return Ok(Applied::Nothing);
        }

        stage_budgets(
            &self.live,
            &self.created,
            self.rewritten.iter().map(|r| &r.updated),
            self.now,
        );
        self.staged = true;
        Ok(Applied::Wrote)
    }

    fn exclusive(&self) -> bool {
        true
    }

    fn discard(&mut self) {
        if std::mem::take(&mut self.staged) {
            unstage_budgets(
                &self.live,
                &self.created,
                self.rewritten.iter().map(|r| &r.updated),
            );
        }
    }

    fn finish(self, cx: &WriterContext) {
        let result = self.result.unwrap_or_else(|| {
            Err(StoreError::Internal(
                "bulk budget change never applied".into(),
            ))
        });
        if result.is_ok() {
            for entry in &self.rewritten {
                self.live.untrack(&entry.previous);

                if entry.status == JobStatus::Ready {
                    cx.dispatch.remove(Placement {
                        queue: &entry.updated.queue,
                        priority: entry.updated.priority,
                        id: &entry.updated.id,
                        budgets: &entry.previous,
                    });
                    cx.dispatch.insert(Placement::of(&entry.updated));
                    let _ = cx.event_tx.send(StoreEvent::JobDispatchable {
                        id: entry.updated.id.clone(),
                        queue: entry.updated.queue.clone(),
                        token: Arc::new(AtomicBool::new(false)),
                    });
                }
            }
        }
        let _ = self.reply.send(result.map(JobOutcome::Done));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::super::options::{EnqueueOptions, PatchJobOptions};
    use super::super::super::test_support::{run_job_ops, test_store, test_store_with_retention};
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

    /// Prepare `mutation` of `id` for the writer, as the caller would.
    fn prepared(
        store: &Store,
        id: &str,
        mutation: &BudgetMutation,
    ) -> (
        JobBudgetOp,
        tokio::sync::oneshot::Receiver<Result<JobOutcome<JobBudgetChange>, StoreError>>,
    ) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        match JobBudgetOp::prepare(&store.ks, id, mutation, store.budgets.clone(), NOW, reply)
            .unwrap()
        {
            ControlFlow::Continue(op) => (op, rx),
            ControlFlow::Break(change) => panic!("expected a write, got {change:?}"),
        }
    }

    /// A change prepared before its job changed is told to retry, and
    /// creates nothing: the job is checked before any budget the change
    /// would create is written.
    #[tokio::test]
    async fn a_conflicting_change_creates_no_budget() {
        let store = test_store();
        let job = enqueue_bound(&store, &[]).await;
        let mutation = BudgetMutation::Add(BudgetBinding::new("fresh").cost(2).create_with(
            super::super::BudgetPolicy {
                allocation: 50,
                strategy: BudgetStrategy::WhileInFlight,
            },
        ));
        let (op, rx) = prepared(&store, &job.id, &mutation);

        let patch = PatchJobOptions {
            priority: Some(9),
            ..Default::default()
        };
        store.patch_job(now_millis(), &job.id, patch).await.unwrap();
        run_job_ops(&store, vec![op.into()]).await;

        assert!(matches!(rx.await.unwrap().unwrap(), JobOutcome::Conflict));
        assert!(store.get_budget("fresh").await.unwrap().is_none());
        assert_eq!(store.budgets.tracked("fresh"), 0);
    }

    /// Discarded after it applied, as when another op fails the pass,
    /// a change leaves no accounting behind, so applied again in a later
    /// pass the job is counted once.
    #[tokio::test]
    async fn a_discarded_change_leaves_no_accounting() {
        let store = store_with_budgets(&["stripe"]).await;
        let job = enqueue_bound(&store, &[]).await;
        let mutation = BudgetMutation::Add(BudgetBinding::new("stripe").cost(1));
        let (mut op, rx) = prepared(&store, &job.id, &mutation);

        let ks = store.ks.clone();
        let live = store.budgets.clone();
        let op = tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            assert_eq!(op.apply(&mut tx, &ks).unwrap(), Applied::Wrote);
            assert_eq!(live.tracked("stripe"), 1);
            op.discard();
            drop(tx);
            assert_eq!(live.tracked("stripe"), 0);
            op
        })
        .await
        .unwrap();

        run_job_ops(&store, vec![op.into()]).await;
        assert!(matches!(
            rx.await.unwrap().unwrap(),
            JobOutcome::Done(JobBudgetChange::Changed(_))
        ));
        assert_eq!(store.budgets.tracked("stripe"), 1);
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

    /// The reported bug. A cost change introduces no binding, so an
    /// earlier version handed the pre-pass nothing to check and the
    /// capacity guard never ran — leaving jobs raised past what the
    /// bucket can hold, which never dispatch again.
    #[tokio::test]
    async fn raising_a_cost_above_the_capacity_is_refused() {
        let store = test_store();
        store
            .create_budget(
                "per-minute",
                10,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                    burst: Some(1),
                },
                NOW,
            )
            .await
            .unwrap();
        let job = enqueue_bound(&store, &[("per-minute", 1)]).await;

        let result = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::SetCost {
                    key: "per-minute".into(),
                    cost: 2,
                },
                now_millis(),
            )
            .await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        // And the job still draws what it did, so it still dispatches.
        let stored = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(stored.budgets[0].cost, 1);
    }

    /// The allocation is not the ceiling when a burst is set, so the
    /// burst is what a cost has to fit inside — a cost well within the
    /// allocation is still refused.
    #[tokio::test]
    async fn a_cost_change_is_measured_against_the_burst() {
        let store = test_store();
        store
            .create_budget(
                "bursty",
                1_000,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                    burst: Some(5),
                },
                NOW,
            )
            .await
            .unwrap();
        let job = enqueue_bound(&store, &[("bursty", 1)]).await;

        let result = store
            .patch_job_budgets(
                &job.id,
                BudgetMutation::SetCost {
                    key: "bursty".into(),
                    cost: 50,
                },
                now_millis(),
            )
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        // Up to the burst is fine.
        changed(
            store
                .patch_job_budgets(
                    &job.id,
                    BudgetMutation::SetCost {
                        key: "bursty".into(),
                        cost: 5,
                    },
                    now_millis(),
                )
                .await
                .unwrap(),
        );
    }

    /// The aggregate has to end up describing the *new* cost and not the
    /// old, in both directions. The old binding is untracked after the
    /// commit; forgetting it would leave the previous cost counted
    /// forever, and a budget permanently unshrinkable on the strength of
    /// a cost no job draws any more.
    #[tokio::test]
    async fn a_cost_change_leaves_only_the_new_cost_counted() {
        let store = store_with_budgets(&["stripe"]).await;
        let job = enqueue_bound(&store, &[("stripe", 3)]).await;

        let raise = |cost| BudgetMutation::SetCost {
            key: "stripe".into(),
            cost,
        };

        changed(
            store
                .patch_job_budgets(&job.id, raise(7), now_millis())
                .await
                .unwrap(),
        );
        assert_eq!(store.budgets.tracked("stripe"), 1);
        assert_eq!(store.budgets.max_cost("stripe"), Some(7));

        // Downwards is the direction that exposes a lingering old cost:
        // the maximum would stay at seven.
        changed(
            store
                .patch_job_budgets(&job.id, raise(2), now_millis())
                .await
                .unwrap(),
        );
        assert_eq!(store.budgets.tracked("stripe"), 1);
        assert_eq!(store.budgets.max_cost("stripe"), Some(2));
    }

    /// The same, through the bulk path, which does its untracking in a
    /// separate loop after the commit.
    #[tokio::test]
    async fn a_bulk_cost_change_leaves_only_the_new_cost_counted() {
        let store = store_with_budgets(&["stripe"]).await;
        enqueue_bound(&store, &[("stripe", 9)]).await;
        enqueue_bound(&store, &[("stripe", 9)]).await;

        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::SetCost {
                    key: "stripe".into(),
                    cost: 2,
                },
                now_millis(),
            )
            .await
            .unwrap();
        assert_eq!(result.changed, 2);

        assert_eq!(store.budgets.tracked("stripe"), 2);
        assert_eq!(store.budgets.max_cost("stripe"), Some(2));
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

    // --- bulk ---

    /// The operator case this exists for: move every job off one budget
    /// and onto another, without disturbing bindings it was not asked
    /// about. Two calls, each scoped to one key.
    #[tokio::test]
    async fn a_shared_budget_can_be_split_without_losing_other_bindings() {
        let store = store_with_budgets(&["old", "new", "other"]).await;

        // One job on `old` alone, one on `old` and `other`.
        let alone = enqueue_bound(&store, &[("old", 1)]).await;
        let both = enqueue_bound(&store, &[("old", 1), ("other", 2)]).await;

        let filter = JobFilter::new().budget_keys(HashSet::from(["old".to_string()]));

        let added = store
            .patch_jobs_budgets(
                filter.clone(),
                BudgetMutation::Add(BudgetBinding::new("new")),
                now_millis(),
            )
            .await
            .unwrap();
        assert_eq!(added.changed, 2);
        assert!(added.blocked.is_empty());

        let removed = store
            .patch_jobs_budgets(
                filter,
                BudgetMutation::Remove { key: "old".into() },
                now_millis(),
            )
            .await
            .unwrap();
        assert_eq!(removed.changed, 2);

        let alone = store
            .get_job(now_millis(), &alone.id)
            .await
            .unwrap()
            .unwrap();
        let keys: Vec<_> = alone.budgets.iter().map(|b| b.key.as_str()).collect();
        assert_eq!(keys, vec!["new"]);

        // The binding it was never asked about survived.
        let both = store
            .get_job(now_millis(), &both.id)
            .await
            .unwrap()
            .unwrap();
        let mut keys: Vec<_> = both.budgets.iter().map(|b| b.key.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["new", "other"]);
    }

    /// A job already bound is what `POST` promises to pass over, so it
    /// is not reported at all — reporting it would make the response say
    /// the operation fell short when it did exactly what it says.
    #[tokio::test]
    async fn already_bound_jobs_are_passed_over_silently() {
        let store = store_with_budgets(&["stripe"]).await;
        enqueue_bound(&store, &[("stripe", 1)]).await;
        enqueue_bound(&store, &[]).await;

        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::Add(BudgetBinding::new("stripe")),
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 1);
        assert!(result.blocked.is_empty());
    }

    /// The number this replaced counted every no-op, so an unfiltered
    /// unbind reported the whole store as skipped — a figure that grew
    /// with the database and told the caller nothing.
    #[tokio::test]
    async fn jobs_the_mutation_does_not_touch_are_not_reported() {
        let store = store_with_budgets(&["wanted"]).await;
        for _ in 0..5 {
            enqueue_bound(&store, &[]).await;
        }
        enqueue_bound(&store, &[("wanted", 1)]).await;

        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::Remove {
                    key: "wanted".into(),
                },
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 1);
        assert!(
            result.blocked.is_empty(),
            "five untouched jobs should not be reported: {:?}",
            result.blocked
        );
    }

    /// In-flight jobs hold tokens against their current bindings, so a
    /// filter that sweeps one up passes over it — but this is the one
    /// outcome the caller can act on, so it comes back by id.
    #[tokio::test]
    async fn in_flight_jobs_are_reported_by_id() {
        let store = store_with_budgets(&["stripe", "mailgun"]).await;
        enqueue_bound(&store, &[("stripe", 1)]).await;
        enqueue_bound(&store, &[("stripe", 1)]).await;

        let running = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::Add(BudgetBinding::new("mailgun")),
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 1);
        assert_eq!(result.blocked, vec![running.id.clone()]);
        assert_eq!(store.budgets.tracked("mailgun"), 1);

        // Once it is no longer in flight, the same call lands.
        store
            .mark_completed(now_millis(), &running.id)
            .await
            .unwrap();
    }

    /// An in-flight job the mutation would not have changed anyway is
    /// not blocked by anything — reporting it would send the caller
    /// retrying something that will never do anything.
    #[tokio::test]
    async fn an_in_flight_job_with_nothing_to_change_is_not_reported() {
        let store = store_with_budgets(&["stripe"]).await;
        enqueue_bound(&store, &[("stripe", 1)]).await;

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // It already draws on `stripe`, so `Add` had nothing to do
        // regardless of its state.
        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::Add(BudgetBinding::new("stripe")),
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 0);
        assert!(result.blocked.is_empty());
    }

    /// The other way a mutation can have nothing to do: the job does not
    /// draw on the budget being removed at all. Being in flight is
    /// irrelevant to a job the request was never going to touch, so it
    /// is not reported — otherwise an unfiltered unbind would name every
    /// running job on the server.
    #[tokio::test]
    async fn an_in_flight_job_without_the_budget_is_not_reported() {
        let store = store_with_budgets(&["targeted", "unrelated"]).await;

        // In flight, and bound only to a budget the request ignores.
        enqueue_bound(&store, &[("unrelated", 1)]).await;
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Queued, and actually affected.
        let affected = enqueue_bound(&store, &[("targeted", 1)]).await;

        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::Remove {
                    key: "targeted".into(),
                },
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 1);
        assert!(
            result.blocked.is_empty(),
            "an in-flight job the request never touched should not be \
             reported: {:?}",
            result.blocked
        );

        let stored = store
            .get_job(now_millis(), &affected.id)
            .await
            .unwrap()
            .unwrap();
        assert!(stored.budgets.is_empty());
    }

    /// A finished job's bindings are inert — it will never dispatch and
    /// is already untracked — so it is neither changed nor reported.
    #[tokio::test]
    async fn terminal_jobs_are_not_reported() {
        let store = test_store_with_retention(60_000, 60_000);
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        let job = enqueue_bound(&store, &[("stripe", 1)]).await;

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &job.id).await.unwrap();

        let result = store
            .patch_jobs_budgets(JobFilter::new(), BudgetMutation::RemoveAll, now_millis())
            .await
            .unwrap();

        assert_eq!(result.changed, 0);
        assert!(
            result.blocked.is_empty(),
            "a finished job cannot be acted on: {:?}",
            result.blocked
        );
    }

    /// Wrong for every job the filter could match, so it refuses rather
    /// than getting half way through.
    #[tokio::test]
    async fn a_malformed_request_fails_the_whole_batch() {
        let store = store_with_budgets(&["stripe"]).await;
        let job = enqueue_bound(&store, &[("stripe", 1)]).await;

        let result = store
            .patch_jobs_budgets(
                JobFilter::new(),
                BudgetMutation::Add(BudgetBinding::new("ghost")),
                now_millis(),
            )
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        // Nothing moved.
        let stored = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(stored.budgets.len(), 1);
        assert_eq!(store.budgets.tracked("stripe"), 1);
    }

    /// The filter narrows what is touched, so jobs outside it keep their
    /// bindings.
    #[tokio::test]
    async fn only_matching_jobs_are_changed() {
        let store = store_with_budgets(&["stripe"]).await;
        let inside = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("t", "wanted", serde_json::json!({})),
            )
            .await
            .unwrap()
            .into_job();
        let outside = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("t", "other", serde_json::json!({})),
            )
            .await
            .unwrap()
            .into_job();

        let result = store
            .patch_jobs_budgets(
                JobFilter::new().queues(HashSet::from(["wanted".to_string()])),
                BudgetMutation::Add(BudgetBinding::new("stripe")),
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 1);
        assert!(
            !store
                .get_job(now_millis(), &inside.id)
                .await
                .unwrap()
                .unwrap()
                .budgets
                .is_empty()
        );
        assert!(
            store
                .get_job(now_millis(), &outside.id)
                .await
                .unwrap()
                .unwrap()
                .budgets
                .is_empty()
        );
    }

    /// Matching nothing is a success that did nothing, not an error.
    #[tokio::test]
    async fn matching_no_jobs_changes_nothing() {
        let store = store_with_budgets(&["stripe"]).await;

        let result = store
            .patch_jobs_budgets(
                JobFilter::new().queues(HashSet::from(["absent".to_string()])),
                BudgetMutation::Add(BudgetBinding::new("stripe")),
                now_millis(),
            )
            .await
            .unwrap();

        assert_eq!(result.changed, 0);
        assert!(result.blocked.is_empty());
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
