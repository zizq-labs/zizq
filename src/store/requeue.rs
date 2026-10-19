// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Requeue: return an in-flight job to the ready queue.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fjall::{Readable, Slice};
use tokio::task;

use super::dispatch::Placement;
use super::keys::{make_job_key, make_status_key};
use super::store::{Keyspaces, Store, StoreEvent};
use super::types::{Job, JobStatus, StoreError};
use super::writer::{JobOutcome, JobReply, JobWrite, WriterContext};

impl Store {
    /// Requeue a job currently in the in-flight state back to ready.
    ///
    /// Returns `true` if the job was actually requeued, `false` if it was
    /// already acked (no longer in the in-flight state).
    ///
    /// Subscribers are notified.
    pub async fn requeue(&self, id: &str) -> Result<bool, StoreError> {
        // Prepared here, on a blocking thread, so the writer only checks
        // and writes. A job changed between this pre-read and the
        // writer's check comes back as a conflict: pre-read again.
        loop {
            let ks = self.ks.clone();
            let writer = self.writer.clone();
            let id = id.to_string();

            let reply_rx = task::spawn_blocking(move || -> Result<_, StoreError> {
                let (reply, rx) = tokio::sync::oneshot::channel();
                let Some(op) = RequeueOp::prepare(&ks, &id, reply)? else {
                    return Ok(None);
                };
                writer.submit_job(op.into());
                Ok(Some(rx))
            })
            .await??;

            let Some(reply_rx) = reply_rx else {
                return Ok(false);
            };
            let outcome = reply_rx
                .await
                .map_err(|_| StoreError::Internal("writer channel closed".into()))??;
            match outcome {
                JobOutcome::Done(()) => return Ok(true),
                JobOutcome::Conflict => continue,
            }
        }
    }
}

/// A requeue of one in-flight job, prepared by its caller.
pub(in crate::store) struct RequeueOp {
    job_key: Vec<u8>,
    pre_bytes: Slice,
    /// The job as it will be once ready again.
    job: Job,
    updated: Slice,
    reply: JobReply<()>,
}

impl RequeueOp {
    /// Pre-read the job and work out its ready record. `None` if the job
    /// is gone or isn't in flight.
    fn prepare(ks: &Keyspaces, id: &str, reply: JobReply<()>) -> Result<Option<Self>, StoreError> {
        let job_key = make_job_key(id);
        let Some(pre_bytes) = ks.data.get(&job_key)? else {
            return Ok(None);
        };

        let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;
        if job.status != JobStatus::InFlight as u8 {
            return Ok(None);
        }

        job.status = JobStatus::Ready.into();
        let updated = rmp_serde::to_vec_named(&job)?.into();

        Ok(Some(Self {
            job_key,
            pre_bytes,
            job,
            updated,
            reply,
        }))
    }
}

impl JobWrite for RequeueOp {
    fn apply(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<bool, StoreError> {
        if tx.get(&ks.data, &self.job_key)?.as_deref() != Some(&*self.pre_bytes) {
            return Ok(false);
        }

        let id = &self.job.id;
        tx.insert(&ks.data, &self.job_key, self.updated.clone());
        tx.remove(&ks.index, make_status_key(JobStatus::InFlight, id));
        tx.insert(&ks.index, make_status_key(JobStatus::Ready, id), b"");
        Ok(true)
    }

    fn finish(self, cx: &WriterContext) {
        let job = self.job;

        // Released, not refunded. The job did dispatch and did run for
        // some unknown amount of time before the worker went away, so a
        // rate limit keeps the token it spent — otherwise a flapping
        // worker could re-dispatch the same job indefinitely at no cost
        // to the limit it exists to enforce. Only the concurrency slot
        // comes back.
        //
        // Before the re-insert, not after. Queueing the job first leaves
        // a window where it is visible while still holding its own slot,
        // so a claim can look at it, fail to acquire, and then the
        // release lands with nothing to provoke another attempt.
        cx.dispatch.release_concurrency(&job.budgets);
        cx.dispatch.insert(Placement::of(&job));

        cx.in_flight_index
            .remove(job.dequeued_at.unwrap_or(0), &job.id);
        let _ = cx.event_tx.send(StoreEvent::JobDispatchable {
            id: job.id.clone(),
            queue: job.queue.clone(),
            token: Arc::new(AtomicBool::new(false)),
        });

        let _ = self.reply.send(Ok(JobOutcome::Done(())));
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

    use super::super::budget::{BudgetBinding, BudgetStrategy};
    use super::super::keys::make_job_key;
    use super::super::options::EnqueueOptions;
    use super::super::store::StoreEvent;
    use super::super::test_support::enqueue_and_take;
    use super::super::test_support::{test_failure_opts, test_store};
    use super::super::types::{JobStatus, StoreError};
    use super::super::writer::{JobOp, JobOutcome, JobWrite, WriterContext};
    use crate::time::now_millis;

    /// Prepare a requeue of `id`, as its caller would.
    fn requeue_op(
        store: &super::Store,
        id: &str,
    ) -> (
        super::RequeueOp,
        tokio::sync::oneshot::Receiver<Result<JobOutcome<()>, StoreError>>,
    ) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = super::RequeueOp::prepare(&store.ks, id, reply)
            .unwrap()
            .unwrap();
        (op, rx)
    }

    /// Apply, commit and finish `ops` as one writer pass would. Commits
    /// block on the group committer, so on a blocking thread.
    async fn one_pass(store: &super::Store, ops: Vec<JobOp>) {
        let ks = store.ks.clone();
        let cx = WriterContext {
            ks: store.ks.clone(),
            dispatch: store.dispatch.clone(),
            budgets: store.budgets.clone(),
            scheduled_index: store.scheduled_index.clone(),
            in_flight_index: store.in_flight_index.clone(),
            event_tx: store.event_tx.clone(),
        };
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            let written: Vec<bool> = ops
                .iter()
                .map(|op| op.apply(&mut tx, &ks).unwrap())
                .collect();
            ks.commit(tx, ks.default_commit_mode).unwrap();
            for (op, written) in ops.into_iter().zip(written) {
                if written {
                    op.finish(&cx);
                } else {
                    op.conflict();
                }
            }
        })
        .await
        .unwrap();
    }

    /// A requeue of a job changed since its pre-read writes nothing and
    /// is told to retry, while another in the same pass commits.
    #[tokio::test]
    async fn a_requeue_of_a_changed_job_conflicts_and_others_commit() {
        let store = test_store();
        let a = enqueue_and_take(&store).await.id;
        let b = enqueue_and_take(&store).await.id;

        let (first, first_rx) = requeue_op(&store, &a);
        let (second, second_rx) = requeue_op(&store, &b);

        // `b` changes after its requeue pre-read it.
        let ks = store.ks.clone();
        let key = make_job_key(&b);
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            tx.insert(&ks.data, key, b"changed");
            ks.commit(tx, ks.default_commit_mode).unwrap();
        })
        .await
        .unwrap();

        one_pass(&store, vec![first.into(), second.into()]).await;

        assert_eq!(first_rx.await.unwrap().unwrap(), JobOutcome::Done(()));
        assert_eq!(second_rx.await.unwrap().unwrap(), JobOutcome::Conflict);
        assert_eq!(store.dispatch.len(), 1);
        assert_eq!(
            store.ks.data.get(make_job_key(&b)).unwrap().as_deref(),
            Some(b"changed".as_slice())
        );
    }

    /// Two writes of one job in the same pass: the second sees the
    /// first's write and conflicts, rather than writing over it.
    #[tokio::test]
    async fn a_second_write_of_a_job_in_one_pass_conflicts() {
        let store = test_store();
        let id = enqueue_and_take(&store).await.id;

        let (first, first_rx) = requeue_op(&store, &id);
        let (second, second_rx) = requeue_op(&store, &id);
        one_pass(&store, vec![first.into(), second.into()]).await;

        assert_eq!(first_rx.await.unwrap().unwrap(), JobOutcome::Done(()));
        assert_eq!(second_rx.await.unwrap().unwrap(), JobOutcome::Conflict);

        // Retried, it finds the job no longer in flight.
        assert!(!store.requeue(&id).await.unwrap());
        assert_eq!(store.dispatch.len(), 1);
    }

    /// A worker that vanished was, from the budget's point of view, a
    /// completion whose outcome never arrived: the job has stopped
    /// running, so the slot is genuinely free. Holding it would let one
    /// disconnect wedge a concurrency budget of one for good.
    #[tokio::test]
    async fn a_disconnect_returns_the_concurrency_slot() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("solo", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("a"))
                    .budget(BudgetBinding::new("solo")),
            )
            .await
            .unwrap();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        assert!(store.requeue(&taken.id).await.unwrap());

        // Takeable again, on the same single slot it just gave back.
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, taken.id);
    }

    /// The other half of the same decision, and the reason a disconnect
    /// releases rather than refunds. The job did dispatch and did run
    /// for some unknown time, so a rate limit keeps the token it spent.
    /// Refunding it would let a flapping worker re-dispatch the same job
    /// indefinitely at no cost to the limit.
    #[tokio::test]
    async fn a_disconnect_does_not_refund_a_rate_limit() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget(
                "hourly",
                1,
                BudgetStrategy::TimeBased {
                    duration_ms: 3_600_000,
                    burst: None,
                },
                now,
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("a"))
                    .budget(BudgetBinding::new("hourly")),
            )
            .await
            .unwrap();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        assert!(store.requeue(&taken.id).await.unwrap());

        // Queued again, but this hour's dispatch has been used.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn requeue_returns_job_to_queue() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, job.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn requeue_preserves_priority() {
        let store = test_store();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, high.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        // Should get the high-priority job again, not the low-priority one.
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, high.id);
        assert_eq!(retaken.priority, 1);
        assert_eq!(retaken.status, u8::from(JobStatus::InFlight));
    }

    #[tokio::test]
    async fn requeue_skips_already_acked_job() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(store.mark_completed(now_millis(), &taken.id).await.unwrap());

        assert!(!store.requeue(&taken.id).await.unwrap());

        // Queue should remain empty.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn requeue_broadcasts_job_dispatchable() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let mut rx = store.subscribe();
        store.requeue(&taken.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobDispatchable { queue, .. } => assert_eq!(queue, "default"),
            other => panic!("expected JobDispatchable, got {other:?}"),
        }
    }

    // --- in_flight_count tests (cross-op: take, complete, fail, requeue) ---

    #[tokio::test]
    async fn in_flight_count_tracks_take_and_complete() {
        let store = test_store();
        let now = now_millis();

        assert_eq!(store.in_flight_count(), 0);

        // Enqueue two jobs and take both.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        store.take_next_job(now, &HashSet::new()).await.unwrap();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.in_flight_count(), 2);

        // Completing a job should decrement.
        store.mark_completed(now, &job.id).await.unwrap();
        assert_eq!(store.in_flight_count(), 1);
    }

    #[tokio::test]
    async fn in_flight_count_tracks_failure_and_requeue() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.in_flight_count(), 1);

        // Failing a job should decrement.
        store
            .record_failure(now, &job.id, test_failure_opts())
            .await
            .unwrap();
        assert_eq!(store.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn in_flight_count_tracks_requeue() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.in_flight_count(), 1);

        // Requeuing should decrement.
        store.requeue(&job.id).await.unwrap();
        assert_eq!(store.in_flight_count(), 0);
    }
}
