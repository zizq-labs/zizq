// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job ompletions within a writer pass.
//!
//! Worker processes typically take a prefetch batch of jobs, process
//! them concurrently, and ack the completed ones. Each client SDK's
//! `AckProcessor` already batches acks *within* one worker into a single
//! bulk-ack request. The writer adds a second layer: acks from *all*
//! workers waiting at once share one transaction, with the writer's
//! other work.
//!
//! Both singular `mark_completed` and bulk `mark_completed_bulk` calls
//! submit a `CompleteOp` of job IDs. A pass's completions go through
//! these phases:
//!
//! - **prepare**, before the write lock: dedup per op and across ops,
//!   then pre-read every job.
//! - **check**, under the lock: every pre-read record is unchanged.
//!   If any has changed (most often a concurrent `patch_job`), nothing
//!   is written for this pass's completions and the ops are carried to
//!   the next pass, where they are pre-read afresh.
//! - **apply**, in the pass's transaction.
//! - **finish**, after the commit: dispatch and in-flight index updates,
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

pub(in crate::store) type CompleteReply =
    tokio::sync::oneshot::Sender<Result<BulkCompleteResult, StoreError>>;

/// A completion request: IDs to ack, the caller's `now`, and a reply
/// slot for the per-op `BulkCompleteResult`.
pub(in crate::store) struct CompleteOp {
    pub ids: Vec<String>,
    pub now: u64,
    pub reply: CompleteReply,
}

/// The completions in one pass, pre-read.
pub(in crate::store) struct CompleteGroup {
    /// Each op's IDs (deduped within the op), `now` and reply.
    ops: Vec<(Vec<String>, u64, CompleteReply)>,
    prepared: Vec<PreparedComplete>,
    not_found: HashSet<String>,
}

impl CompleteGroup {
    /// Dedup and pre-read a pass's completions, before the write lock.
    /// On a read error the ops are answered with it and `None` returned.
    pub(in crate::store) fn prepare(
        ops: Vec<CompleteOp>,
        ks: &Keyspaces,
        default_completed_retention_ms: u64,
    ) -> Option<Self> {
        // Per-op dedup, mirroring single-op behaviour, plus a global
        // dedup for the pre-read. `now` is the smallest seen: the most
        // conservative purge_at.
        let mut grouped = Vec::with_capacity(ops.len());
        let mut flat_ids: Vec<String> = Vec::new();
        let mut global_seen: HashSet<String> = HashSet::new();
        let mut now = u64::MAX;

        for op in ops {
            now = now.min(op.now);
            let mut op_seen: HashSet<String> = HashSet::with_capacity(op.ids.len());
            let mut op_ids = Vec::with_capacity(op.ids.len());
            for id in op.ids {
                if !op_seen.insert(id.clone()) {
                    continue;
                }
                if global_seen.insert(id.clone()) {
                    flat_ids.push(id.clone());
                }
                op_ids.push(id);
            }
            grouped.push((op_ids, op.now, op.reply));
        }

        match pre_read_completes(&flat_ids, now, ks, default_completed_retention_ms) {
            Ok((prepared, not_found)) => Some(Self {
                ops: grouped,
                prepared,
                not_found: not_found.into_iter().collect(),
            }),
            Err(e) => {
                tracing::error!(error = %e, "writer: completion pre-read failed");
                let msg = e.to_string();
                for (_, _, reply) in grouped {
                    let _ = reply.send(Err(StoreError::Internal(msg.clone())));
                }
                None
            }
        }
    }

    /// Whether there is anything to write: every ID may have been
    /// already completed, or never in flight.
    pub(in crate::store) fn has_writes(&self) -> bool {
        !self.prepared.is_empty()
    }

    /// Whether every pre-read record is unchanged. Reads through the
    /// pass's transaction, so writes earlier in the pass are seen.
    pub(in crate::store) fn check(
        &self,
        tx: &fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<bool, StoreError> {
        for p in &self.prepared {
            let current = tx.get(&ks.data, make_job_key(&p.id))?;
            if current.as_deref() != Some(&*p.pre_bytes) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Write every completion into the pass's transaction. Call only
    /// after `check`, so no CAS conflict can leave partial writes.
    pub(in crate::store) fn apply(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<(), StoreError> {
        if apply_complete_batch(tx, ks, &self.prepared)? {
            Ok(())
        } else {
            Err(StoreError::Internal(
                "completion changed after its check in the same pass".into(),
            ))
        }
    }

    /// How many jobs this pass writes for completions.
    pub(in crate::store) fn writes(&self) -> usize {
        self.prepared.len()
    }

    /// After the commit: update the in-memory indexes, announce each
    /// completion, and answer each op.
    pub(in crate::store) fn finish(
        self,
        dispatch: &Dispatch,
        in_flight_index: &InFlightIndex,
        event_tx: &broadcast::Sender<StoreEvent>,
    ) {
        // `prepared` is already deduped (the pre-read is of unique IDs).
        let mut completed: HashSet<String> = HashSet::with_capacity(self.prepared.len());
        for p in &self.prepared {
            dispatch.remove(Placement {
                queue: &p.queue,
                priority: p.priority,
                id: &p.id,
                budgets: &p.budgets,
            });
            // Two separate things, and a completing job needs both: the
            // slot it occupied while running goes back, and it stops being
            // counted as outstanding work at all.
            dispatch.release_concurrency(&p.budgets);
            dispatch.untrack(&p.budgets);
            in_flight_index.remove(p.dequeued_at, &p.id);
            completed.insert(p.id.clone());
        }

        for id in &completed {
            let _ = event_tx.send(StoreEvent::JobCompleted { id: id.clone() });
        }

        for (op_ids, _, reply) in self.ops {
            let mut result = BulkCompleteResult {
                completed: Vec::new(),
                not_found: Vec::new(),
            };
            for id in op_ids {
                if completed.contains(&id) {
                    result.completed.push(id);
                } else if self.not_found.contains(&id) {
                    result.not_found.push(id);
                }
                // Otherwise the ID appears in neither set, which the
                // pre-read's partition rules out. Surfacing a third
                // status would break the API contract, so it is dropped.
            }
            let _ = reply.send(Ok(result));
        }
    }

    pub(in crate::store) fn fail(self, msg: &str) {
        for (_, _, reply) in self.ops {
            let _ = reply.send(Err(StoreError::Internal(msg.to_string())));
        }
    }

    /// The ops again, to carry to the next pass after a conflict.
    pub(in crate::store) fn into_ops(self) -> Vec<CompleteOp> {
        self.ops
            .into_iter()
            .map(|(ids, now, reply)| CompleteOp { ids, now, reply })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::store::Store;
    use crate::store::test_support::{enqueue_and_take, test_store};
    use crate::time::now_millis;

    fn op(
        ids: &[&str],
    ) -> (
        CompleteOp,
        tokio::sync::oneshot::Receiver<Result<BulkCompleteResult, StoreError>>,
    ) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = CompleteOp {
            ids: ids.iter().map(|id| id.to_string()).collect(),
            now: now_millis(),
            reply,
        };
        (op, rx)
    }

    fn prepare(store: &Store, ops: Vec<CompleteOp>) -> CompleteGroup {
        CompleteGroup::prepare(ops, &store.ks, 0).unwrap()
    }

    fn check(store: &Store, group: &CompleteGroup) -> bool {
        let tx = store.ks.write_tx();
        group.check(&tx, &store.ks).unwrap()
    }

    /// Commit `group` as the writer would. Commits block on the group
    /// committer, so on a blocking thread.
    async fn commit(store: &Store, group: CompleteGroup) {
        let ks = store.ks.clone();
        let dispatch = store.dispatch.clone();
        let in_flight_index = store.in_flight_index.clone();
        let event_tx = store.event_tx.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            assert!(group.check(&tx, &ks).unwrap());
            group.apply(&mut tx, &ks).unwrap();
            ks.commit(tx, ks.default_commit_mode).unwrap();
            group.finish(&dispatch, &in_flight_index, &event_tx);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn check_passes_while_every_record_is_unchanged() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;
        let b = enqueue_and_take(&store).await.id;

        let (op, _rx) = op(&[&a, &b]);
        assert!(check(&store, &prepare(&store, vec![op])));
    }

    /// The guarantee that lets completions share a transaction: a job
    /// changed between its pre-read and the write fails the check, so
    /// the group writes nothing and is retried.
    #[tokio::test]
    async fn check_fails_once_a_record_changed_after_its_pre_read() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;
        let b = enqueue_and_take(&store).await.id;

        let (op, _rx) = op(&[&a, &b]);
        let group = prepare(&store, vec![op]);

        let ks = store.ks.clone();
        let key = make_job_key(&b);
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            tx.insert(&ks.data, key, b"changed");
            ks.commit(tx, ks.default_commit_mode).unwrap();
        })
        .await
        .unwrap();

        assert!(!check(&store, &group));
    }

    /// Ops carried to the next pass after a conflict keep their IDs and
    /// replies, and complete when prepared again.
    #[tokio::test]
    async fn carried_ops_complete_when_prepared_again() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;

        let (first, first_rx) = op(&[&a, &a]);
        let (second, second_rx) = op(&["unknown"]);
        let carried = prepare(&store, vec![first, second]).into_ops();

        assert_eq!(carried.len(), 2);
        assert_eq!(carried[0].ids, vec![a.clone()]);
        assert_eq!(carried[1].ids, vec!["unknown".to_string()]);

        commit(&store, prepare(&store, carried)).await;

        let first = first_rx.await.unwrap().unwrap();
        assert_eq!(first.completed, vec![a.clone()]);
        let second = second_rx.await.unwrap().unwrap();
        assert_eq!(second.not_found, vec!["unknown".to_string()]);
        assert!(store.ks.data.get(make_job_key(&a)).unwrap().is_none());
    }

    /// Two workers acking the same job in one pass complete it once, and
    /// both are told it completed.
    #[tokio::test]
    async fn a_job_acked_by_two_ops_completes_once() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;
        let b = enqueue_and_take(&store).await.id;
        let mut events = store.subscribe();

        let (first, first_rx) = op(&[&a, &b]);
        let (second, second_rx) = op(&[&a]);
        let group = prepare(&store, vec![first, second]);
        assert_eq!(group.writes(), 2);

        commit(&store, group).await;

        assert_eq!(
            first_rx.await.unwrap().unwrap().completed,
            vec![a.clone(), b]
        );
        assert_eq!(second_rx.await.unwrap().unwrap().completed, vec![a.clone()]);
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
