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

use super::super::dispatch::Dispatch;
use super::super::results::EnqueueResult;
use super::super::scheduled_index::ScheduledIndex;
use super::super::store::{Keyspaces, StoreEvent};
use super::super::types::StoreError;
use super::apply::apply_enqueue_batch;
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
    pub(in crate::store) fn start(
        ks: Arc<Keyspaces>,
        dispatch: Arc<Dispatch>,
        scheduled_index: Arc<ScheduledIndex>,
        event_tx: broadcast::Sender<StoreEvent>,
        batch_size: usize,
    ) -> Self {
        // Defend against pathological config — a zero-cap channel would
        // deadlock on send; size 1 effectively disables batching.
        let batch_size = batch_size.max(1);

        let (tx, rx) = std::sync::mpsc::sync_channel::<EnqueueOp>(batch_size);

        std::thread::Builder::new()
            .name("enqueue-batcher".into())
            .spawn(move || {
                // Block on the first op (sleeps the thread when idle).
                // When the sender drops, recv() returns Err and we exit.
                while let Ok(first) = rx.recv() {
                    let mut batch = Vec::with_capacity(batch_size);
                    batch.push(first);

                    // Opportunistically drain whatever else is already
                    // queued, up to the batch limit. Non-blocking — if
                    // nothing more is waiting, process what we have.
                    while batch.len() < batch_size {
                        match rx.try_recv() {
                            Ok(op) => batch.push(op),
                            Err(_) => break,
                        }
                    }

                    process_batch(&ks, &dispatch, &scheduled_index, &event_tx, batch);
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

/// Apply a coalesced batch to one write tx and fan results back to
/// each waiter's oneshot. Each waiter receives the slice of results
/// corresponding to the jobs they submitted.
fn process_batch(
    ks: &Keyspaces,
    dispatch: &Dispatch,
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

    tracing::debug!(
        ops = ops_count,
        jobs = jobs_count,
        "enqueue auto-batcher: coalesced batch from channel"
    );

    let mut tx = ks.write_tx();
    let outcomes = match apply_enqueue_batch(&mut tx, ks, &ops) {
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
        if let Err(e) = ks.commit(tx, ks.enqueue_commit_mode) {
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
