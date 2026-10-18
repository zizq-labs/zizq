// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Take operations as part of a writer pass.
//!
//! A take writes each of its jobs' Ready -> InFlight transition. The
//! caller claims candidates from the ready index and pre-reads their
//! records before submitting, and reads payloads after, so only the
//! write happens here.
//!
//! Each take succeeds or conflicts on its own. Before writing any of a
//! take's jobs, every one is checked to still hold the bytes the caller
//! pre-read. If any has changed (deleted, patched, folded into by an
//! enqueue earlier in the same pass), none of that take's jobs are
//! written and the caller retries, as after a conflict on its own
//! transaction. Checks read through the pass's transaction, so they see
//! writes made earlier in the same pass.

use fjall::{Readable, Slice};

use super::super::store::Keyspaces;
use super::super::types::StoreError;

/// One job's Ready -> InFlight transition, prepared by the caller.
pub(in crate::store) struct TakeWrite {
    /// The job record's key in the data keyspace.
    pub job_key: Vec<u8>,

    /// The record as the caller pre-read it. The write only happens if
    /// the record is still exactly this.
    pub pre_bytes: Slice,

    /// The record as it should be once in flight.
    pub updated: Slice,

    pub old_status_key: Vec<u8>,
    pub new_status_key: Vec<u8>,

    /// Unique and batch index keys to release, but only while they still
    /// point at this job: a newer job may have replaced them.
    pub release_keys: Vec<Vec<u8>>,

    pub job_id: String,
}

/// What became of one take's writes.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::store) enum TakeOutcome {
    /// Every job in the take is now in flight.
    Committed,

    /// A job changed since it was pre-read. Nothing was written for this
    /// take, and the caller should put its jobs back and retry.
    Conflict,
}

pub(in crate::store) type TakeReply = tokio::sync::oneshot::Sender<Result<TakeOutcome, StoreError>>;

/// A take submitted to the writer.
pub(in crate::store) struct TakeOp {
    pub writes: Vec<TakeWrite>,
    pub reply: TakeReply,
}

/// The takes in one pass.
pub(super) struct TakeGroup {
    ops: Vec<TakeOp>,
    outcomes: Vec<TakeOutcome>,
}

impl TakeGroup {
    pub(super) fn new(ops: Vec<TakeOp>) -> Self {
        Self {
            ops,
            outcomes: Vec::new(),
        }
    }

    /// Check and write every take into the pass's transaction.
    pub(super) fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<(), StoreError> {
        self.outcomes = self
            .ops
            .iter()
            .map(|op| apply_take(tx, ks, &op.writes))
            .collect::<Result<_, _>>()?;
        Ok(())
    }

    /// How many jobs this pass writes for takes.
    pub(super) fn writes(&self) -> usize {
        self.ops
            .iter()
            .zip(&self.outcomes)
            .filter(|(_, outcome)| **outcome == TakeOutcome::Committed)
            .map(|(op, _)| op.writes.len())
            .sum()
    }

    pub(super) fn finish(self) {
        for (op, outcome) in self.ops.into_iter().zip(self.outcomes) {
            let _ = op.reply.send(Ok(outcome));
        }
    }

    pub(super) fn fail(self, msg: &str) {
        for op in self.ops {
            let _ = op.reply.send(Err(StoreError::Internal(msg.to_string())));
        }
    }

    pub(super) fn into_ops(self) -> Vec<TakeOp> {
        self.ops
    }
}

/// Check one take's jobs are all unchanged, then write them. Writes
/// nothing if any has changed.
fn apply_take(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    writes: &[TakeWrite],
) -> Result<TakeOutcome, StoreError> {
    for write in writes {
        let current = tx.get(&ks.data, &write.job_key)?;
        if current.as_deref() != Some(&*write.pre_bytes) {
            return Ok(TakeOutcome::Conflict);
        }
    }

    for write in writes {
        tx.insert(&ks.data, &write.job_key, write.updated.clone());
        tx.remove(&ks.index, &write.old_status_key);
        tx.insert(&ks.index, &write.new_status_key, b"");

        let job_id = write.job_id.as_bytes();
        for key in &write.release_keys {
            tx.fetch_update(&ks.index, key, |v| match v {
                Some(v) if v.as_ref() == job_id => None,
                other => other.cloned(),
            })?;
        }
    }

    Ok(TakeOutcome::Committed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::keys::{make_job_key, make_status_key, make_unique_key};
    use crate::store::options::EnqueueOptions;
    use crate::store::store::Store;
    use crate::store::test_support::test_store;
    use crate::store::types::JobStatus;
    use crate::time::now_millis;

    async fn enqueue(store: &Store) -> String {
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!({})),
            )
            .await
            .unwrap()
            .into_job()
            .id
    }

    /// A take write marking `id` in flight, as pre-read now.
    fn take_write(store: &Store, id: &str, release_keys: Vec<Vec<u8>>) -> TakeWrite {
        TakeWrite {
            job_key: make_job_key(id),
            pre_bytes: store.ks.data.get(make_job_key(id)).unwrap().unwrap(),
            updated: Slice::from(b"in flight".as_slice()),
            old_status_key: make_status_key(JobStatus::Ready, id),
            new_status_key: make_status_key(JobStatus::InFlight, id),
            release_keys,
            job_id: id.to_string(),
        }
    }

    fn take_op(writes: Vec<TakeWrite>) -> TakeOp {
        let (reply, _rx) = tokio::sync::oneshot::channel();
        TakeOp { writes, reply }
    }

    /// Write `entries` and commit, on a blocking thread: commits block on
    /// the group committer.
    async fn commit_raw(
        store: &Store,
        data: Vec<(Vec<u8>, Vec<u8>)>,
        index: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let ks = store.ks.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            for (k, v) in data {
                tx.insert(&ks.data, k, v);
            }
            for (k, v) in index {
                tx.insert(&ks.index, k, v);
            }
            ks.commit(tx, ks.default_commit_mode).unwrap();
        })
        .await
        .unwrap();
    }

    /// Apply `group`, commit if it wrote anything, and return it with how
    /// many jobs it wrote.
    async fn apply(store: &Store, mut group: TakeGroup) -> (TakeGroup, usize) {
        let ks = store.ks.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            group.apply(&mut tx, &ks).unwrap();
            let writes = group.writes();
            if writes > 0 {
                ks.commit(tx, ks.default_commit_mode).unwrap();
            }
            (group, writes)
        })
        .await
        .unwrap()
    }

    /// Unique and batch keys are released only while they still point at
    /// the job being taken. A newer job may already own one, and
    /// releasing it would drop that job's key.
    #[tokio::test]
    async fn a_release_key_now_owned_by_another_job_is_left_alone() {
        let store = test_store();
        let id = enqueue(&store).await;

        let own = make_unique_key("own");
        let taken_over = make_unique_key("taken-over");
        commit_raw(
            &store,
            Vec::new(),
            vec![
                (own.clone(), id.clone().into_bytes()),
                (taken_over.clone(), b"another-job".to_vec()),
            ],
        )
        .await;

        let write = take_write(&store, &id, vec![own.clone(), taken_over.clone()]);
        let (group, writes) = apply(&store, TakeGroup::new(vec![take_op(vec![write])])).await;
        assert_eq!(writes, 1);
        group.finish();

        assert!(store.ks.index.get(&own).unwrap().is_none());
        assert_eq!(
            store.ks.index.get(&taken_over).unwrap().as_deref(),
            Some(b"another-job".as_slice())
        );
    }

    /// Only committed takes count as writes, so a pass whose takes all
    /// conflict writes nothing and its commit is skipped.
    #[tokio::test]
    async fn writes_count_only_committed_takes() {
        let store = test_store();
        let a = enqueue(&store).await;
        let b = enqueue(&store).await;
        let c = enqueue(&store).await;

        let committed = take_op(vec![take_write(&store, &a, Vec::new())]);
        let conflicting = take_op(vec![
            take_write(&store, &b, Vec::new()),
            take_write(&store, &c, Vec::new()),
        ]);
        let only_conflicting = take_op(vec![take_write(&store, &c, Vec::new())]);

        // `c` changes after both takes of it pre-read it.
        commit_raw(
            &store,
            vec![(make_job_key(&c), b"changed".to_vec())],
            Vec::new(),
        )
        .await;

        let (_, writes) = apply(&store, TakeGroup::new(vec![committed, conflicting])).await;
        assert_eq!(writes, 1);

        let (_, writes) = apply(&store, TakeGroup::new(vec![only_conflicting])).await;
        assert_eq!(writes, 0);
    }
}
