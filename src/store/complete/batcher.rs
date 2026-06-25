// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Server-side opportunistic batching for concurrent completions
//! (a.k.a. acks).
//!
//! Worker processes typically dequeue a prefetch batch of jobs and
//! process them concurrently, then ack the completed ones. Each client
//! SDK's `AckProcessor` already batches acks *within* one worker by
//! buffering them into a single bulk-ack HTTP request. This batcher
//! coalesces a second layer: bulk-ack requests *across many concurrent
//! workers* into one fjall transaction.
//!
//! Both layers compose: per-worker batching saves HTTP framing
//! overhead (one request for N jobs); this server-side batcher saves
//! commit-side cost (one `tx.commit()` for N requests' worth of jobs).
//!
//! Both singular `mark_completed` and bulk `mark_completed_bulk` calls
//! flow through this batcher. Each op carries `Vec<String>` ids and a
//! `now` timestamp. The batcher flattens (with per-op + global dedup),
//! pre-reads + CAS-retries inside the batcher thread, commits, then
//! slices per-job results back into per-op `BulkCompleteResult`s for
//! fan-out.
//!
//! Design echoes `EnqueueBatcher`:
//!
//! - Single dedicated OS thread (`complete-batcher`) drains a bounded
//!   `std::sync::mpsc::sync_channel`.
//! - Each request enters the channel with a `oneshot` reply slot.
//! - The batcher blocks on `recv()` for the first op, then
//!   opportunistically drains additional queued ops via `try_recv()`
//!   up to the configured batch size — no time-based windowing, so
//!   single-op traffic gets zero added latency.
//! - Pre-read happens *in the batcher thread*, not in spawn_blocking
//!   like enqueue's prepare. This is a deliberate trade — the CAS
//!   retry loop must re-read on conflict, and doing that inline
//!   inside the batcher is simpler than orchestrating retries across
//!   submitter threads. Per-id reads are fast on hot data.
//! - Backpressure: bounded channel + spawn_blocking-caller pattern.
//! - Shutdown: drop the `CompleteBatcher` → `SyncSender` drops →
//!   `recv()` returns `Err` → worker exits cleanly after draining.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast;

use super::super::ready_index::ReadyIndex;
use super::super::results::BulkCompleteResult;
use super::super::store::{Keyspaces, StoreEvent};
use super::super::types::StoreError;
use super::apply::apply_complete_batch;
use super::pre_read::pre_read_completes;

/// A single completion request in flight: ids to ack, the caller's
/// `now` timestamp, and a oneshot for the per-op `BulkCompleteResult`.
pub(super) struct CompleteOp {
    pub ids: Vec<String>,
    pub now: u64,
    pub reply: tokio::sync::oneshot::Sender<Result<BulkCompleteResult, StoreError>>,
}

/// Auto-batcher for completion ops (singular + bulk). Owns the
/// channel sender; the batcher thread runs detached and exits
/// naturally when the sender drops.
pub(in crate::store) struct CompleteBatcher {
    tx: std::sync::mpsc::SyncSender<CompleteOp>,
}

impl CompleteBatcher {
    /// Spawn the batcher's background thread. The thread holds clones
    /// of the store internals it needs to apply + finalize batches.
    pub(in crate::store) fn start(
        ks: Arc<Keyspaces>,
        ready_index: Arc<ReadyIndex>,
        in_flight_count: Arc<AtomicU64>,
        event_tx: broadcast::Sender<StoreEvent>,
        default_completed_retention_ms: u64,
        batch_size: usize,
    ) -> Self {
        let batch_size = batch_size.max(1);
        let (tx, rx) = std::sync::mpsc::sync_channel::<CompleteOp>(batch_size);

        std::thread::Builder::new()
            .name("complete-batcher".into())
            .spawn(move || {
                while let Ok(first) = rx.recv() {
                    let mut batch = Vec::with_capacity(batch_size);
                    batch.push(first);

                    while batch.len() < batch_size {
                        match rx.try_recv() {
                            Ok(op) => batch.push(op),
                            Err(_) => break,
                        }
                    }

                    process_batch(
                        &ks,
                        &ready_index,
                        &in_flight_count,
                        &event_tx,
                        default_completed_retention_ms,
                        batch,
                    );
                }
            })
            .expect("failed to spawn complete-batcher thread");

        Self { tx }
    }

    /// Push a completion op onto the channel and return a receiver
    /// that resolves when the batch has been committed (or failed).
    /// Blocks the calling thread if the channel is full — callers
    /// must invoke this from `spawn_blocking`.
    pub(super) fn submit(
        &self,
        ids: Vec<String>,
        now: u64,
    ) -> tokio::sync::oneshot::Receiver<Result<BulkCompleteResult, StoreError>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(CompleteOp {
            ids,
            now,
            reply: reply_tx,
        });
        reply_rx
    }
}

/// Apply a coalesced batch in one CAS-retry loop and fan results back
/// to each waiter's oneshot.
fn process_batch(
    ks: &Keyspaces,
    ready_index: &ReadyIndex,
    in_flight_count: &AtomicU64,
    event_tx: &broadcast::Sender<StoreEvent>,
    default_completed_retention_ms: u64,
    batch: Vec<CompleteOp>,
) {
    let ops_count = batch.len();

    // Per-op dedup (mirroring the existing single-op behaviour) then
    // assemble the per-op id lists for fan-out, the globally dedup'd
    // flat id list for pre-read, the replies, and a representative
    // `now` (smallest seen — most conservative purge_at).
    let mut per_op_ids: Vec<Vec<String>> = Vec::with_capacity(ops_count);
    let mut replies: Vec<tokio::sync::oneshot::Sender<_>> = Vec::with_capacity(ops_count);
    let mut flat_ids: Vec<String> = Vec::new();
    let mut global_seen: HashSet<String> = HashSet::new();
    let mut now: u64 = u64::MAX;

    for op in batch {
        now = now.min(op.now);

        let mut op_seen: HashSet<String> = HashSet::with_capacity(op.ids.len());
        let mut op_ids: Vec<String> = Vec::with_capacity(op.ids.len());
        for id in op.ids {
            if !op_seen.insert(id.clone()) {
                continue;
            }
            if global_seen.insert(id.clone()) {
                flat_ids.push(id.clone());
            }
            op_ids.push(id);
        }
        per_op_ids.push(op_ids);
        replies.push(op.reply);
    }

    let jobs_count = flat_ids.len();
    tracing::debug!(
        ops = ops_count,
        jobs = jobs_count,
        "complete auto-batcher: coalesced batch from channel"
    );

    // CAS-retry loop: re-read everything on conflict. Conflicts are
    // rare (most common cause is a concurrent patch_job on a job
    // being completed), so re-reading the full batch is acceptable.
    let (prepared, not_found_set) = loop {
        let (prepared, not_found) =
            match pre_read_completes(&flat_ids, now, ks, default_completed_retention_ms) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        ops = ops_count,
                        jobs = jobs_count,
                        error = %e,
                        "complete auto-batcher: pre_read_completes failed"
                    );
                    let msg = e.to_string();
                    for reply in replies {
                        let _ = reply.send(Err(StoreError::Internal(msg.clone())));
                    }
                    return;
                }
            };

        if prepared.is_empty() {
            let not_found_set: HashSet<String> = not_found.into_iter().collect();
            tracing::debug!(
                ops = ops_count,
                jobs = jobs_count,
                "complete auto-batcher: nothing to commit"
            );
            fan_out(per_op_ids, replies, &HashSet::new(), &not_found_set);
            return;
        }

        let mut tx = ks.write_tx();
        let cas_ok = match apply_complete_batch(&mut tx, ks, &prepared) {
            Ok(b) => b,
            Err(e) => {
                drop(tx);
                tracing::error!(
                    ops = ops_count,
                    jobs = jobs_count,
                    error = %e,
                    "complete auto-batcher: apply_complete_batch failed"
                );
                let msg = e.to_string();
                for reply in replies {
                    let _ = reply.send(Err(StoreError::Internal(msg.clone())));
                }
                return;
            }
        };

        if !cas_ok {
            drop(tx);
            // Re-read and retry. Same loop iteration.
            continue;
        }

        if let Err(e) = ks.commit(tx, ks.default_commit_mode) {
            tracing::error!(
                ops = ops_count,
                jobs = jobs_count,
                error = %e,
                "complete auto-batcher: commit failed"
            );
            let msg = e.to_string();
            for reply in replies {
                let _ = reply.send(Err(StoreError::Internal(msg.clone())));
            }
            return;
        }

        let not_found_set: HashSet<String> = not_found.into_iter().collect();
        break (prepared, not_found_set);
    };

    // Update ready_index once per unique completed id.
    for p in &prepared {
        ready_index.remove(&p.queue, p.priority, &p.id);
    }

    // Broadcast JobCompleted + decrement in_flight_count once per
    // unique completed id (not per occurrence across ops).
    let completed_set: HashSet<String> = prepared.into_iter().map(|p| p.id).collect();
    for id in &completed_set {
        let _ = event_tx.send(StoreEvent::JobCompleted { id: id.clone() });
    }
    let unique_completed = completed_set.len() as u64;
    if unique_completed > 0 {
        let _ = in_flight_count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(unique_completed))
        });
    }

    tracing::debug!(
        ops = ops_count,
        jobs = jobs_count,
        completed = unique_completed,
        not_found = not_found_set.len(),
        "complete auto-batcher: committed batch"
    );

    fan_out(per_op_ids, replies, &completed_set, &not_found_set);
}

/// Build per-op `BulkCompleteResult`s by looking each op's submitted
/// ids up against the global completed/not_found sets and send each
/// op's reply.
fn fan_out(
    per_op_ids: Vec<Vec<String>>,
    replies: Vec<tokio::sync::oneshot::Sender<Result<BulkCompleteResult, StoreError>>>,
    completed_set: &HashSet<String>,
    not_found_set: &HashSet<String>,
) {
    for (op_ids, reply) in per_op_ids.into_iter().zip(replies) {
        let mut completed: Vec<String> = Vec::new();
        let mut not_found: Vec<String> = Vec::new();
        for id in op_ids {
            if completed_set.contains(&id) {
                completed.push(id);
            } else if not_found_set.contains(&id) {
                not_found.push(id);
            }
            // Otherwise: id was submitted but didn't appear in either
            // set. Shouldn't happen given that pre_read_completes
            // partitions every read id into one bucket. Swallow
            // silently if it does — surfacing as a third "unknown"
            // status would break the existing API contract.
        }
        let _ = reply.send(Ok(BulkCompleteResult {
            completed,
            not_found,
        }));
    }
}
