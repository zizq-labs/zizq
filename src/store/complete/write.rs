// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job completions within a writer pass.
//!
//! Worker processes typically take a prefetch batch of jobs, process
//! them concurrently, and ack the completed ones. Each client SDK's
//! `AckProcessor` already batches acks *within* one worker into a single
//! bulk-ack request. The writer adds a second layer: acks from *all*
//! workers waiting at once share one transaction, with the writer's
//! other work.
//!
//! A completion arrives at the writer already prepared: its caller
//! removed duplicate IDs and pre-read every job, so the writer thread
//! only checks and writes. In the pass:
//!
//! - **apply**: each op's pre-read records are checked through the
//!   transaction. An op with a record that changed since (most often a
//!   concurrent `patch_job`) writes nothing and is answered `Conflict`,
//!   for its caller to pre-read again and retry; the other ops still
//!   commit. A job acked by more than one op is written once.
//! - **finish**, once durable: dispatch and in-flight index updates,
//!   `JobCompleted` events, and each op's own `BulkCompleteResult`.

use std::collections::HashSet;

use fjall::Readable;
use tokio::sync::broadcast;

use super::super::dispatch::{Dispatch, Placement};
use super::super::in_flight_index::InFlightIndex;
use super::super::keys::make_job_key;
use super::super::results::BulkCompleteResult;
use super::super::store::{Keyspaces, StoreEvent};
use super::super::types::StoreError;
use super::apply::apply_complete_batch;
use super::pre_read::{PreparedComplete, pre_read_completes};

/// What became of a completion op.
pub(in crate::store) enum CompleteOutcome {
    Done(BulkCompleteResult),

    /// A job changed since it was pre-read. Nothing was written for this
    /// op: pre-read again and resubmit.
    Conflict,
}

pub(in crate::store) type CompleteReply =
    tokio::sync::oneshot::Sender<Result<CompleteOutcome, StoreError>>;

/// A completion request, prepared by its caller.
pub(in crate::store) struct CompleteOp {
    /// The op's IDs, deduplicated, in request order.
    pub ids: Vec<String>,
    prepared: Vec<PreparedComplete>,
    not_found: Vec<String>,
    reply: CompleteReply,
}

impl CompleteOp {
    /// Remove duplicate IDs and pre-read every job, ready to submit. Done
    /// on the caller's thread, so the writer thread doesn't.
    pub(in crate::store) fn prepare(
        ids: &[String],
        now: u64,
        ks: &Keyspaces,
        default_completed_retention_ms: u64,
        reply: CompleteReply,
    ) -> Result<Self, StoreError> {
        let mut seen = HashSet::with_capacity(ids.len());
        let ids: Vec<String> = ids.iter().filter(|id| seen.insert(*id)).cloned().collect();
        let (prepared, not_found) =
            pre_read_completes(&ids, now, ks, default_completed_retention_ms)?;
        Ok(Self {
            ids,
            prepared,
            not_found,
            reply,
        })
    }
}

/// The completions in one pass.
pub(in crate::store) struct CompleteGroup {
    ops: Vec<CompleteOp>,
    conflicted: Vec<bool>,
    /// IDs written this pass, by whichever op wrote them first.
    written: HashSet<String>,
}

impl CompleteGroup {
    pub(in crate::store) fn new(ops: Vec<CompleteOp>) -> Self {
        Self {
            conflicted: vec![false; ops.len()],
            ops,
            written: HashSet::new(),
        }
    }

    /// Check and write each op into the pass's transaction. Reads go
    /// through the transaction, so writes earlier in the pass are seen.
    pub(in crate::store) fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<(), StoreError> {
        for (op, conflicted) in self.ops.iter().zip(self.conflicted.iter_mut()) {
            // A job an earlier op wrote this pass is done, not changed.
            for p in op.prepared.iter().filter(|p| !self.written.contains(&p.id)) {
                let current = tx.get(&ks.data, make_job_key(&p.id))?;
                if current.as_deref() != Some(&*p.pre_bytes) {
                    *conflicted = true;
                    break;
                }
            }
            if *conflicted {
                continue;
            }

            for p in &op.prepared {
                if self.written.insert(p.id.clone())
                    && !apply_complete_batch(tx, ks, std::slice::from_ref(p))?
                {
                    return Err(StoreError::Internal(
                        "completion changed after its check in the same pass".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// How many jobs this pass writes for completions.
    pub(in crate::store) fn writes(&self) -> usize {
        self.written.len()
    }

    /// Once durable: update the in-memory indexes, announce each
    /// completion, and answer each op.
    pub(in crate::store) fn finish(
        self,
        dispatch: &Dispatch,
        in_flight_index: &InFlightIndex,
        event_tx: &broadcast::Sender<StoreEvent>,
    ) {
        let mut done: HashSet<&str> = HashSet::with_capacity(self.written.len());
        let unconflicted = self
            .ops
            .iter()
            .zip(&self.conflicted)
            .filter(|(_, conflicted)| !**conflicted);
        for (op, _) in unconflicted {
            for p in &op.prepared {
                if !self.written.contains(&p.id) || !done.insert(&p.id) {
                    continue;
                }
                dispatch.remove(Placement {
                    queue: &p.queue,
                    priority: p.priority,
                    id: &p.id,
                    budgets: &p.budgets,
                });
                // Two separate things, and a completing job needs both:
                // the slot it occupied while running goes back, and it
                // stops being counted as outstanding work at all.
                dispatch.release_concurrency(&p.budgets);
                dispatch.untrack(&p.budgets);
                in_flight_index.remove(p.dequeued_at, &p.id);
                let _ = event_tx.send(StoreEvent::JobCompleted { id: p.id.clone() });
            }
        }

        for (op, conflicted) in self.ops.into_iter().zip(self.conflicted) {
            let outcome = if conflicted {
                CompleteOutcome::Conflict
            } else {
                CompleteOutcome::Done(BulkCompleteResult {
                    completed: op
                        .ids
                        .into_iter()
                        .filter(|id| self.written.contains(id))
                        .collect(),
                    not_found: op.not_found,
                })
            };
            let _ = op.reply.send(Ok(outcome));
        }
    }

    pub(in crate::store) fn fail(self, msg: &str) {
        for op in self.ops {
            let _ = op.reply.send(Err(StoreError::Internal(msg.to_string())));
        }
    }

    /// The ops again, to carry to the next pass when the transaction they
    /// were applied to is dropped for another group's failure. Their
    /// pre-reads may be stale by then, which the next pass's check turns
    /// into a `Conflict` and a retry.
    pub(in crate::store) fn into_ops(self) -> Vec<CompleteOp> {
        self.ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::store::Store;
    use crate::store::test_support::{enqueue_and_take, test_store};
    use crate::time::now_millis;

    type Reply = tokio::sync::oneshot::Receiver<Result<CompleteOutcome, StoreError>>;

    fn op(store: &Store, ids: &[&str]) -> (CompleteOp, Reply) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let ids: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        let op = CompleteOp::prepare(&ids, now_millis(), &store.ks, 0, reply).unwrap();
        (op, rx)
    }

    fn done(outcome: Result<CompleteOutcome, StoreError>) -> BulkCompleteResult {
        match outcome.unwrap() {
            CompleteOutcome::Done(result) => result,
            CompleteOutcome::Conflict => panic!("expected the op to be done, not to conflict"),
        }
    }

    /// Apply, commit and finish `group` as the writer would. Commits
    /// block on the group committer, so on a blocking thread.
    async fn commit(store: &Store, mut group: CompleteGroup) -> usize {
        let ks = store.ks.clone();
        let dispatch = store.dispatch.clone();
        let in_flight_index = store.in_flight_index.clone();
        let event_tx = store.event_tx.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            group.apply(&mut tx, &ks).unwrap();
            let writes = group.writes();
            ks.commit(tx, ks.default_commit_mode).unwrap();
            group.finish(&dispatch, &in_flight_index, &event_tx);
            writes
        })
        .await
        .unwrap()
    }

    #[test]
    fn prepare_removes_duplicates_and_splits_out_unknown_ids() {
        let store = test_store();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let a = rt.block_on(enqueue_and_take(&store)).id;

        let (op, _rx) = op(&store, &[&a, "unknown", &a]);
        assert_eq!(op.ids, vec![a.clone(), "unknown".to_string()]);
        assert_eq!(op.prepared.len(), 1);
        assert_eq!(op.not_found, vec!["unknown".to_string()]);
    }

    /// An op with a job changed since its pre-read writes none of its
    /// jobs and is told to retry, while the other op in the pass commits.
    #[tokio::test]
    async fn a_conflicting_op_writes_nothing_and_others_commit() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;
        let b = enqueue_and_take(&store).await.id;
        let c = enqueue_and_take(&store).await.id;

        let (first, first_rx) = op(&store, &[&a]);
        let (second, second_rx) = op(&store, &[&b, &c]);
        let b_before = store.ks.data.get(make_job_key(&b)).unwrap();

        // `c` changes after the second op pre-read it.
        let ks = store.ks.clone();
        let key = make_job_key(&c);
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            tx.insert(&ks.data, key, b"changed");
            ks.commit(tx, ks.default_commit_mode).unwrap();
        })
        .await
        .unwrap();

        let writes = commit(&store, CompleteGroup::new(vec![first, second])).await;
        assert_eq!(writes, 1);

        assert_eq!(done(first_rx.await.unwrap()).completed, vec![a.clone()]);
        assert!(matches!(
            second_rx.await.unwrap().unwrap(),
            CompleteOutcome::Conflict
        ));
        assert!(store.ks.data.get(make_job_key(&a)).unwrap().is_none());
        assert_eq!(store.ks.data.get(make_job_key(&b)).unwrap(), b_before);
    }

    /// Two workers acking the same job in one pass complete it once, and
    /// both are told it completed: the second op's check doesn't mistake
    /// the first op's delete for a change.
    #[tokio::test]
    async fn a_job_acked_by_two_ops_completes_once() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;
        let b = enqueue_and_take(&store).await.id;
        let mut events = store.subscribe();

        let (first, first_rx) = op(&store, &[&a, &b]);
        let (second, second_rx) = op(&store, &[&a]);
        let writes = commit(&store, CompleteGroup::new(vec![first, second])).await;
        assert_eq!(writes, 2);

        assert_eq!(done(first_rx.await.unwrap()).completed, vec![a.clone(), b]);
        assert_eq!(done(second_rx.await.unwrap()).completed, vec![a.clone()]);
        assert_eq!(store.in_flight_count(), 0);

        let mut completed_a = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event, StoreEvent::JobCompleted { ref id } if *id == a) {
                completed_a += 1;
            }
        }
        assert_eq!(completed_a, 1);
    }
}
