// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Enqueues within a writer pass.
//!
//! Singular and bulk enqueues each submit an `EnqueueOp` of prepared
//! jobs. The writer gathers them with other writes and applies them in
//! the pass's transaction, so concurrent enqueue requests share one
//! commit. Ops stay separate all the way through: one op failing
//! validation, or hitting a unique-key duplicate, does not disturb the
//! others, and each op gets its own results.

use tokio::sync::broadcast;

use super::super::budget::{Budget, Budgets, stage_budgets, unstage_budgets};
use super::super::dispatch::Dispatch;
use super::super::results::EnqueueResult;
use super::super::scheduled_index::ScheduledIndex;
use super::super::store::{Keyspaces, StoreEvent};
use super::super::types::{Job, StoreError};
use super::apply::{OpOutcome, apply_enqueue_batch};
use super::finalize::finalize_enqueue;
use super::prepare::PreparedEnqueue;

pub(in crate::store) type EnqueueReply =
    tokio::sync::oneshot::Sender<Result<Vec<EnqueueResult>, StoreError>>;

/// An enqueue request: the prepared jobs plus a reply slot for the
/// per-request results. One job for `enqueue`, one or more for
/// `enqueue_bulk`.
pub(in crate::store) struct EnqueueOp {
    pub prepared: Vec<PreparedEnqueue>,
    pub reply: EnqueueReply,
}

/// The enqueues in one pass.
pub(in crate::store) struct EnqueueGroup {
    ops: Vec<Vec<PreparedEnqueue>>,
    replies: Vec<EnqueueReply>,
    outcomes: Vec<OpOutcome>,
    created_budgets: Vec<(String, Budget)>,
}

impl EnqueueGroup {
    pub(in crate::store) fn new(ops: Vec<EnqueueOp>) -> Self {
        let (ops, replies) = ops.into_iter().map(|op| (op.prepared, op.reply)).unzip();
        Self {
            ops,
            replies,
            outcomes: Vec::new(),
            created_budgets: Vec::new(),
        }
    }

    /// Apply every op into the pass's transaction. An error here (a
    /// corrupt unique-index entry, say) is for the whole group.
    pub(in crate::store) fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<(), StoreError> {
        let (outcomes, created_budgets) = apply_enqueue_batch(tx, ks, &self.ops)?;
        self.outcomes = outcomes;
        self.created_budgets = created_budgets;
        Ok(())
    }

    /// How many jobs this pass writes for enqueues. A `Duplicate` writes
    /// nothing; a `Folded` writes to the job it merges into, so counts.
    pub(in crate::store) fn writes(&self) -> usize {
        self.outcomes
            .iter()
            .filter_map(|o| o.as_ref().ok())
            .flatten()
            .filter(|r| !matches!(r, EnqueueResult::Duplicate(_)))
            .count()
    }

    /// Track the jobs this pass creates against their budgets. Called
    /// before the commit, while the write lock still excludes every
    /// other writer: afterwards would leave a window in which the jobs
    /// are durable but unaccounted for, and a concurrent budget delete
    /// would see them as absent.
    pub(in crate::store) fn stage(&self, budgets: &Budgets) {
        if let Some(now) = self.now() {
            stage_budgets(budgets, &self.created_budgets, self.created_jobs(), now);
        }
    }

    /// Undo `stage` after a failed commit: nothing became durable, so
    /// nothing may go on referencing it, including the groups this pass
    /// would have created.
    pub(in crate::store) fn unstage(&self, budgets: &Budgets) {
        unstage_budgets(budgets, &self.created_budgets, self.created_jobs());
    }

    /// After the commit: update the in-memory indexes and announce each
    /// result, then answer each op. A failed op wrote nothing, so it is
    /// just answered with the error that rejected it.
    pub(in crate::store) fn finish(
        self,
        dispatch: &Dispatch,
        scheduled_index: &ScheduledIndex,
        event_tx: &broadcast::Sender<StoreEvent>,
    ) {
        for (outcome, reply) in self.outcomes.into_iter().zip(self.replies) {
            if let Ok(ref results) = outcome {
                for r in results {
                    finalize_enqueue(r, dispatch, scheduled_index, event_tx);
                }
            }
            let _ = reply.send(outcome);
        }
    }

    pub(in crate::store) fn fail(self, msg: &str) {
        for reply in self.replies {
            let _ = reply.send(Err(StoreError::Internal(msg.to_string())));
        }
    }

    /// The ops again, to carry to the next pass when the transaction
    /// they were applied to is dropped for another group's failure.
    pub(in crate::store) fn into_ops(self) -> Vec<EnqueueOp> {
        self.ops
            .into_iter()
            .zip(self.replies)
            .map(|(prepared, reply)| EnqueueOp { prepared, reply })
            .collect()
    }

    /// Every op carries its caller's clock reading; any of them will do
    /// for stamping budgets this pass creates.
    fn now(&self) -> Option<u64> {
        self.ops.iter().flatten().next().map(|p| p.now)
    }

    /// The jobs the pass actually brought into existence.
    ///
    /// Only `Created` counts. A `Duplicate` returns a job that was already
    /// enqueued, and a `Folded` merges a payload into a batched job that
    /// already exists — in both cases the job was tracked when it was
    /// first created, and counting it again would leave the budget
    /// permanently holding a reference to work that has already finished.
    fn created_jobs(&self) -> impl Iterator<Item = &Job> {
        self.outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().ok())
            .flatten()
            .filter_map(|result| match result {
                EnqueueResult::Created(job) => Some(job),
                EnqueueResult::Duplicate(_) | EnqueueResult::Folded(_) => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::keys::make_job_key;
    use crate::store::options::EnqueueOptions;
    use crate::store::store::Store;
    use crate::store::test_support::test_store;
    use crate::time::now_millis;

    fn unique(key: &str) -> PreparedEnqueue {
        let opts = EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key(key);
        super::super::prepare_enqueue(opts, now_millis()).unwrap()
    }

    fn op(
        prepared: Vec<PreparedEnqueue>,
    ) -> (
        EnqueueOp,
        tokio::sync::oneshot::Receiver<Result<Vec<EnqueueResult>, StoreError>>,
    ) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        (EnqueueOp { prepared, reply }, rx)
    }

    /// Apply `group` on a blocking thread and return it, with its writes
    /// discarded: no commit.
    async fn applied(store: &Store, mut group: EnqueueGroup) -> EnqueueGroup {
        let ks = store.ks.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            group.apply(&mut tx, &ks).unwrap();
            drop(tx);
            group
        })
        .await
        .unwrap()
    }

    /// When another group in the pass fails, the enqueues already applied
    /// are discarded with the transaction and carried to the next pass.
    /// Applied again, they must behave as if the first attempt never
    /// happened: a unique key is not a duplicate of its own discarded
    /// write, and the job exists once.
    #[tokio::test]
    async fn a_carried_enqueue_is_created_exactly_once() {
        let store = test_store();
        let (first, rx) = op(vec![unique("k")]);

        let carried = applied(&store, EnqueueGroup::new(vec![first]))
            .await
            .into_ops();

        let ks = store.ks.clone();
        let budgets = store.budgets.clone();
        let dispatch = store.dispatch.clone();
        let scheduled_index = store.scheduled_index.clone();
        let event_tx = store.event_tx.clone();
        tokio::task::spawn_blocking(move || {
            let mut group = EnqueueGroup::new(carried);
            let mut tx = ks.write_tx();
            group.apply(&mut tx, &ks).unwrap();
            assert_eq!(group.writes(), 1);
            group.stage(&budgets);
            ks.commit(tx, ks.default_commit_mode).unwrap();
            group.finish(&dispatch, &scheduled_index, &event_tx);
        })
        .await
        .unwrap();

        let results = rx.await.unwrap().unwrap();
        let [EnqueueResult::Created(job)] = results.as_slice() else {
            panic!("expected one created job, got {} results", results.len());
        };
        assert!(store.ks.data.get(make_job_key(&job.id)).unwrap().is_some());
        assert_eq!(store.dispatch.len(), 1);

        // The key belongs to the job that was created.
        let again = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("k"),
            )
            .await
            .unwrap();
        assert!(again.is_duplicate());
        assert_eq!(again.into_job().id, job.id);
    }

    /// A pass whose only enqueues are duplicates writes nothing, so the
    /// writer skips its commit.
    #[tokio::test]
    async fn duplicates_count_as_no_writes() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("taken"),
            )
            .await
            .unwrap();

        let (duplicate, _rx) = op(vec![unique("taken")]);
        let group = applied(&store, EnqueueGroup::new(vec![duplicate])).await;
        assert_eq!(group.writes(), 0);

        let (mixed, _rx) = op(vec![unique("taken"), unique("new")]);
        let group = applied(&store, EnqueueGroup::new(vec![mixed])).await;
        assert_eq!(group.writes(), 1);
    }
}
