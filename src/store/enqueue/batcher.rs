// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Server-side opportunistic batching for concurrent enqueues.
//!
//! Concurrent applications typically enqueue jobs one at a time across
//! each application thread/process, with occasional callers using the
//! explicit `enqueue_bulk` path to submit many jobs together. At the
//! storage layer all of these calls serialize behind fjall's
//! single-writer tx mutex anyway, so coalescing them into one
//! transaction is a pure win: one `tx.commit()` instead of N, one
//! journal write instead of N (which is then further coalesced by the
//! `GroupCommitter` for fsync).
//!
//! Both singular `enqueue` and `enqueue_bulk` calls flow through this
//! batcher. Each op carries a `Vec<PreparedEnqueue>` — Vec-of-1 for
//! singles, Vec-of-N for bulks. The ops stay separate through
//! `apply_enqueue_batch`, which returns one outcome per op, so each
//! waiter's oneshot resolves with exactly its own jobs' results.
//!
//! Atomicity is preserved: a bulk's "all-or-nothing" contract holds
//! because the entire coalesced commit succeeds or fails as a unit,
//! and because an op that is rejected fails whole rather than
//! per-job.
//!
//! An op can be rejected on its own without disturbing the rest of the
//! batch — the other ops still commit. That is what lets validation
//! needing a database read (resolving a job's budgets, say) reject one
//! request without failing unrelated concurrent ones.
//! The channel is op-count bounded — a bulk counts as one op against
//! the channel/batch budget regardless of how many jobs it carries,
//! so a single in-flight bulk can be of any size. Memory pressure
//! under pathological loads (1000 concurrent huge bulks) is in
//! principle possible but practically rare; backpressure still works.
//!
//! Design:
//!
//! - Single dedicated OS thread (`enqueue-batcher`) drains a bounded
//!   `std::sync::mpsc::sync_channel`.
//! - Each request enters the channel with a `oneshot` reply slot.
//! - The batcher blocks on `recv()` for the first op, then opportunistically
//!   drains additional queued ops via `try_recv()` up to the configured
//!   batch size — no time-based windowing, so single-op traffic gets zero
//!   added latency.
//! - One open `write_tx` is shared across the batch; on commit success,
//!   each waiter's oneshot resolves with its individual results.
//! - On commit failure, all waiters in the batch receive the same error
//!   (wrapped as `StoreError::Internal` since the underlying error types
//!   are not `Clone`).
//! - Backpressure: a bounded channel means `send()` blocks when the
//!   channel is full. Combined with the request being made from inside
//!   `spawn_blocking`, this naturally throttles HTTP-handler concurrency.
//! - Shutdown: dropping the `EnqueueBatcher` drops the `SyncSender`,
//!   closing the channel. The batcher's `recv()` returns `Err`, after
//!   draining any remaining queued ops via the inner `try_recv` loop.
//!   In-flight HTTP handlers awaiting their oneshot are kept alive by
//!   axum's `with_graceful_shutdown`, which transitively keeps the
//!   `Arc<Store>` (and therefore this batcher) alive until they finish.

use std::sync::Arc;

use tokio::sync::broadcast;

use super::super::budget::{Budgets, stage_budgets, unstage_budgets};
use super::super::dispatch::Dispatch;
use super::super::results::EnqueueResult;
use super::super::scheduled_index::ScheduledIndex;
use super::super::store::{Keyspaces, StoreEvent};
use super::super::types::{Job, StoreError};
use super::apply::{OpOutcome, apply_enqueue_batch};
use super::finalize::finalize_enqueue;
use super::prepare::PreparedEnqueue;

/// A single enqueue request in flight: the prepared jobs plus a
/// oneshot slot for the per-request results. `prepared.len() == 1`
/// for a singular `enqueue` call; `>= 1` for `enqueue_bulk`.
pub(super) struct EnqueueOp {
    pub prepared: Vec<PreparedEnqueue>,
    pub reply: tokio::sync::oneshot::Sender<Result<Vec<EnqueueResult>, StoreError>>,
}

/// Auto-batcher for enqueue ops (singular + bulk). Owns the channel
/// sender; the batcher thread runs detached and exits naturally when
/// the sender drops.
pub(in crate::store) struct EnqueueBatcher {
    tx: std::sync::mpsc::SyncSender<EnqueueOp>,
}

impl EnqueueBatcher {
    /// Spawn the batcher's background thread. The thread holds clones of
    /// the store internals it needs to commit and finalize batches.
    ///
    /// `batch_size` is both the channel capacity (so backpressure kicks
    /// in at the same point a single batch could absorb everything in
    /// flight) and the max ops drained into a single `tx.commit()`.
    /// Counts ops (i.e. requests), not jobs — a bulk request counts as
    /// one op regardless of how many jobs it carries.
    ///
    /// `max_jobs` bounds the jobs in one transaction, so a batch can't
    /// grow large enough to hold the write lock for long and stall the
    /// takes and acks queued behind it. It only stops ops being *merged*:
    /// one op is never split, so a request larger than `max_jobs` still
    /// commits whole, in a transaction of its own. 0 means no limit.
    pub(in crate::store) fn start(
        ks: Arc<Keyspaces>,
        dispatch: Arc<Dispatch>,
        budgets: Arc<Budgets>,
        scheduled_index: Arc<ScheduledIndex>,
        event_tx: broadcast::Sender<StoreEvent>,
        batch_size: usize,
        max_jobs: usize,
    ) -> Self {
        // Defend against pathological config — a zero-cap channel would
        // deadlock on send; size 1 effectively disables batching.
        let batch_size = batch_size.max(1);

        let (tx, rx) = std::sync::mpsc::sync_channel::<EnqueueOp>(batch_size);

        std::thread::Builder::new()
            .name("enqueue-batcher".into())
            .spawn(move || {
                // An op taken off the channel that didn't fit in the
                // previous batch. It starts the next one.
                let mut carried = None;

                // Block on the first op (sleeps the thread when idle).
                // When the sender drops, recv() returns Err and we exit,
                // after processing any carried op.
                while let Some(first) = carried.take().or_else(|| rx.recv().ok()) {
                    let (batch, next) = gather(first, &rx, batch_size, max_jobs);
                    carried = next;
                    process_batch(&ks, &dispatch, &budgets, &scheduled_index, &event_tx, batch);
                }
            })
            .expect("failed to spawn enqueue-batcher thread");

        Self { tx }
    }

    /// Push prepared enqueue(s) onto the channel and return a receiver
    /// that resolves when the batch containing this op has been
    /// committed (or failed). Blocks the calling thread if the channel
    /// is full — callers must invoke this from `spawn_blocking`.
    ///
    /// `prepared.len() == 1` for singular enqueue; longer for bulk.
    pub(super) fn submit(
        &self,
        prepared: Vec<PreparedEnqueue>,
    ) -> tokio::sync::oneshot::Receiver<Result<Vec<EnqueueResult>, StoreError>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        // If `send` errors, the channel is disconnected (batcher thread
        // gone). The caller will see the receiver close and surface a
        // StoreError::Internal from the `recv` side.
        let _ = self.tx.send(EnqueueOp {
            prepared,
            reply: reply_tx,
        });
        reply_rx
    }
}

/// Gather a batch starting with `first`, opportunistically draining
/// whatever else is already queued. Non-blocking — if nothing more is
/// waiting, the batch is what we have.
///
/// Stops at `batch_size` ops, or when the next op would take the batch
/// past `max_jobs` jobs (0 for no limit). That op can't go back on the
/// channel, so it is returned to start the next batch. The first op is
/// always taken whatever its size, so an op is never split.
fn gather<T: JobCount>(
    first: T,
    rx: &std::sync::mpsc::Receiver<T>,
    batch_size: usize,
    max_jobs: usize,
) -> (Vec<T>, Option<T>) {
    let mut jobs = first.job_count();
    let mut batch = Vec::with_capacity(batch_size);
    batch.push(first);

    while batch.len() < batch_size {
        let Ok(op) = rx.try_recv() else {
            break;
        };
        if max_jobs > 0 && jobs + op.job_count() > max_jobs {
            return (batch, Some(op));
        }
        jobs += op.job_count();
        batch.push(op);
    }

    (batch, None)
}

/// How many jobs an op adds to a batch.
trait JobCount {
    fn job_count(&self) -> usize;
}

impl JobCount for EnqueueOp {
    fn job_count(&self) -> usize {
        self.prepared.len()
    }
}

/// Apply a coalesced batch to one write tx and fan results back to
/// each waiter's oneshot. Each waiter receives the slice of results
/// corresponding to the jobs they submitted.
fn process_batch(
    ks: &Keyspaces,
    dispatch: &Dispatch,
    budgets: &Budgets,
    scheduled_index: &ScheduledIndex,
    event_tx: &broadcast::Sender<StoreEvent>,
    batch: Vec<EnqueueOp>,
) {
    // Split the coalesced batch into per-op job lists and their reply
    // slots. Ops stay separate all the way through, so one op failing
    // validation does not disturb the others.
    let ops_count = batch.len();
    let mut replies: Vec<tokio::sync::oneshot::Sender<_>> = Vec::with_capacity(ops_count);
    let mut ops: Vec<Vec<PreparedEnqueue>> = Vec::with_capacity(ops_count);

    for op in batch {
        replies.push(op.reply);
        ops.push(op.prepared);
    }
    let jobs_count: usize = ops.iter().map(Vec::len).sum();

    // Every op carries its caller's clock reading; any of them will do
    // for stamping budgets this batch creates.
    let now = ops.iter().flatten().next().map(|p| p.now);

    tracing::debug!(
        ops = ops_count,
        jobs = jobs_count,
        "enqueue auto-batcher: coalesced batch from channel"
    );

    let mut tx = ks.write_tx();
    let (outcomes, created_budgets) = match apply_enqueue_batch(&mut tx, ks, &ops) {
        Ok(r) => r,
        Err(e) => {
            // apply_enqueue_batch failed for the whole batch (e.g. a
            // corruption error reading existing unique-index entries).
            // Drop the tx and report the same error to every waiter.
            drop(tx);
            tracing::error!(
                ops = ops_count,
                jobs = jobs_count,
                error = %e,
                "enqueue auto-batcher: apply_enqueue_batch failed"
            );
            let msg = e.to_string();
            for reply in replies {
                let _ = reply.send(Err(StoreError::Internal(msg.clone())));
            }
            return;
        }
    };

    // Skip the commit when nothing was written — `apply_enqueue` makes
    // zero tx writes for a duplicate, and an op that failed wrote
    // nothing at all. `Folded(_)` writes to the tx too, so it counts as
    // "needs commit".
    let writes: usize = outcomes
        .iter()
        .filter_map(|o| o.as_ref().ok())
        .flatten()
        .filter(|r| !matches!(r, EnqueueResult::Duplicate(_)))
        .count();
    let failed_ops = outcomes.iter().filter(|o| o.is_err()).count();
    let duplicate = jobs_count - writes;

    if writes > 0 {
        // Staged before the commit, while the write lock still excludes
        // every other writer. Doing it afterwards would leave a window
        // in which the jobs are durable but unaccounted for, and a
        // concurrent budget delete would see them as absent.
        if let Some(now) = now {
            stage_budgets(budgets, &created_budgets, created_jobs(&outcomes), now);
        }

        if let Err(e) = ks.commit(tx, ks.enqueue_commit_mode) {
            // Nothing became durable, so nothing may go on referencing
            // it — including the groups this batch would have created.
            unstage_budgets(budgets, &created_budgets, created_jobs(&outcomes));

            tracing::error!(
                ops = ops_count,
                jobs = jobs_count,
                error = %e,
                "enqueue auto-batcher: commit failed"
            );
            let msg = e.to_string();
            for reply in replies {
                let _ = reply.send(Err(StoreError::Internal(msg.clone())));
            }
            return;
        }

        tracing::debug!(
            ops = ops_count,
            jobs = jobs_count,
            writes,
            duplicate,
            failed_ops,
            "enqueue auto-batcher: committed batch"
        );
    } else {
        drop(tx);
        tracing::debug!(
            ops = ops_count,
            jobs = jobs_count,
            duplicate,
            failed_ops,
            "enqueue auto-batcher: skipped commit (nothing written)"
        );
    }

    // Finalize each committed result (index updates, events) and reply
    // to each op with its own outcome. A failed op wrote nothing, so
    // there is nothing to finalize for it — its waiter just gets the
    // error that rejected it.
    for (outcome, reply) in outcomes.into_iter().zip(replies) {
        if let Ok(ref results) = outcome {
            for r in results {
                finalize_enqueue(r, dispatch, scheduled_index, event_tx);
            }
        }
        let _ = reply.send(outcome);
    }
}

/// The jobs a batch actually brought into existence.
///
/// Only `Created` counts. A `Duplicate` returns a job that was already
/// enqueued, and a `Folded` merges a payload into a batched job that
/// already exists — in both cases the job was tracked when it was first
/// created, and counting it again would leave the budget permanently
/// holding a reference to work that has already finished.
///
/// Borrows rather than collects, so the staging and unwinding calls can
/// each walk the same outcomes without either of them owning a copy.
fn created_jobs(outcomes: &[OpOutcome]) -> impl Iterator<Item = &Job> {
    outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().ok())
        .flatten()
        .filter_map(|result| match result {
            EnqueueResult::Created(job) => Some(job),
            EnqueueResult::Duplicate(_) | EnqueueResult::Folded(_) => None,
        })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{Receiver, sync_channel};

    use super::*;

    /// A stand-in op whose value is its job count.
    impl JobCount for usize {
        fn job_count(&self) -> usize {
            *self
        }
    }

    fn queued(ops: &[usize]) -> Receiver<usize> {
        let (tx, rx) = sync_channel(ops.len().max(1));
        for op in ops {
            tx.send(*op).unwrap();
        }
        rx
    }

    #[test]
    fn merges_queued_ops_up_to_the_job_limit() {
        let rx = queued(&[100, 50, 100]);
        let (batch, carried) = gather(50, &rx, 1000, 200);
        assert_eq!(batch, vec![50, 100, 50]);
        assert_eq!(carried, Some(100));
    }

    #[test]
    fn an_op_over_the_limit_is_taken_whole_and_alone() {
        let rx = queued(&[10]);
        let (batch, carried) = gather(1000, &rx, 1000, 200);
        assert_eq!(batch, vec![1000]);
        assert_eq!(carried, Some(10));
    }

    #[test]
    fn a_carried_op_over_the_limit_starts_the_next_batch_alone() {
        let rx = queued(&[1000, 10]);
        let (batch, carried) = gather(10, &rx, 1000, 200);
        assert_eq!(batch, vec![10]);
        assert_eq!(carried, Some(1000));

        let (batch, carried) = gather(carried.unwrap(), &rx, 1000, 200);
        assert_eq!(batch, vec![1000]);
        assert_eq!(carried, Some(10));
    }

    #[test]
    fn zero_means_no_job_limit() {
        let rx = queued(&[1000, 1000]);
        let (batch, carried) = gather(1000, &rx, 1000, 0);
        assert_eq!(batch, vec![1000, 1000, 1000]);
        assert_eq!(carried, None);
    }

    #[test]
    fn the_op_limit_still_applies() {
        let rx = queued(&[1, 1, 1]);
        let (batch, carried) = gather(1, &rx, 2, 200);
        assert_eq!(batch, vec![1, 1]);
        assert_eq!(carried, None);
        assert_eq!(rx.try_recv(), Ok(1));
    }

    #[test]
    fn an_empty_channel_gives_a_batch_of_one() {
        let rx = queued(&[]);
        let (batch, carried) = gather(5, &rx, 1000, 200);
        assert_eq!(batch, vec![5]);
        assert_eq!(carried, None);
    }
}
