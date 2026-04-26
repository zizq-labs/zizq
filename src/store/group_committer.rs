// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Group commit batching for journal persistence.
//!
//! Batches multiple journal persists into a single fsync to amortise
//! the per-operation latency. Each `tx.commit()` writes to an in-memory
//! BufWriter. Callers then call `persist()` which enqueues a waiter and
//! returns a oneshot receiver. A dedicated OS thread drains waiters,
//! calls `db.persist()` once per batch, and notifies all waiters whose
//! sequence was covered.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fjall::{PersistMode, SingleWriterTxDatabase};

use super::types::CommitMode;

/// Result type sent through the group commit oneshot channel.
/// `Ok(())` means durable; `Err(msg)` means the persist failed.
pub(super) type SyncResult = Result<(), String>;

/// A waiter queued for group commit. The background thread notifies the
/// oneshot when the waiter's batch is durable (or failed).
struct CommitWaiter {
    sequence: u64,
    mode: CommitMode,
    done: tokio::sync::oneshot::Sender<SyncResult>,
}

/// Batches multiple journal persists into a single fsync.
pub(super) struct GroupCommitter {
    tx: std::sync::mpsc::SyncSender<CommitWaiter>,
    sequence: Arc<AtomicU64>,
}

/// Fsync all journal files in the given directory.
///
/// We open a second fd to each `.jnl` file and call `sync_data()` on it
/// (fdatasync). This is equivalent to `sync_all()` (fsync) for durability
/// here because fjall pre-allocates journal files with `set_len()` at
/// creation — writes don't change the file size, so metadata sync is
/// unnecessary. The only skipped work is redundant mtime updates.
///
/// Using a second fd avoids holding fjall's internal journal writer mutex
/// (which `db.persist(SyncAll)` would hold for the entire fsync duration,
/// blocking all concurrent `tx.commit()` calls).
///
/// Safety with respect to journal rotation:
///
/// Rotation is performed by fjall's background worker pool while holding
/// the same journal writer mutex that `db.persist(Buffer)` holds. Since
/// we call `persist(Buffer)` first (which acquires and releases that
/// mutex), rotation cannot happen *during* our persist(Buffer) call.
///
/// If rotation happens *between* our `persist(Buffer)` and our fsync,
/// that's fine: `Writer::rotate()` calls `persist(SyncAll)` on the old
/// journal before switching to the new one, so our data is already
/// durable. Our subsequent fsync on the old file is a harmless no-op.
fn fsync_journal_files(dir: &std::path::Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("jnl") {
            std::fs::File::open(&path)?.sync_data()?;
        }
    }
    Ok(())
}

impl GroupCommitter {
    /// Start the group commit background thread.
    ///
    /// The thread runs until the `SyncSender` is dropped (i.e. when
    /// `GroupCommitter` is dropped as part of `Keyspaces` cleanup).
    /// All in-flight waiters are drained and notified before the
    /// thread performs a final `SyncAll` and exits.
    pub(super) fn start(db: SingleWriterTxDatabase, data_dir: std::path::PathBuf) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<CommitWaiter>(4096);
        let sequence = Arc::new(AtomicU64::new(0));

        {
            let sequence = sequence.clone();
            std::thread::Builder::new()
                .name("group-committer".into())
                .spawn(move || {
                    // Tracks the outcome of the last persist so that
                    // waiters from a failed batch get the error, while
                    // waiters in a new epoch trigger a fresh attempt.
                    let mut last_result: SyncResult = Ok(());
                    let mut was_fsynced = false;

                    // Flush the BufWriter to the OS page cache (fast,
                    // releases fjall's journal writer mutex quickly).
                    let do_flush = {
                        let db = db.clone();
                        move || -> SyncResult {
                            db.persist(PersistMode::Buffer).map_err(|e| {
                                let msg = format!("group commit flush failed: {e}");
                                tracing::error!("{msg}");
                                msg
                            })
                        }
                    };

                    // Fsync journal files via a second fd (slow, but
                    // doesn't hold fjall's journal writer mutex so
                    // concurrent tx.commit() calls can proceed).
                    let do_fsync = || -> SyncResult {
                        fsync_journal_files(&data_dir).map_err(|e| {
                            let msg = format!("group commit fsync failed: {e}");
                            tracing::error!("{msg}");
                            msg
                        })
                    };

                    // Process a single waiter: if its epoch is already
                    // settled, replay the result (upgrading to fsync if
                    // needed). Otherwise advance the epoch and flush,
                    // then fsync only if the waiter requests it.
                    let mut handle = |waiter: CommitWaiter| {
                        if waiter.sequence < sequence.load(Ordering::Relaxed) {
                            // Batched — epoch already settled.
                            if waiter.mode == CommitMode::Fsync && !was_fsynced {
                                last_result = do_fsync();
                                was_fsynced = true;
                            }
                            let _ = waiter.done.send(last_result.clone());
                            return;
                        }

                        // New epoch — always flush.
                        sequence.fetch_add(1, Ordering::Release);
                        last_result = do_flush();
                        was_fsynced = false;

                        if last_result.is_ok() && waiter.mode == CommitMode::Fsync {
                            last_result = do_fsync();
                            was_fsynced = true;
                        }

                        let _ = waiter.done.send(last_result.clone());
                    };

                    // Runs until the SyncSender is dropped, which
                    // closes the channel and causes recv() to return
                    // Err. All queued waiters are drained first.
                    while let Ok(waiter) = rx.recv() {
                        handle(waiter);
                    }

                    // Final sync on shutdown to flush any remaining data.
                    if let Err(e) = db.persist(PersistMode::SyncAll) {
                        tracing::error!("group commit final sync failed: {e:?}");
                    }
                })
                .expect("failed to spawn group committer thread");
        }

        Self { tx, sequence }
    }

    /// Enqueue a persist request and return a receiver that resolves when
    /// this commit's data is durable.
    ///
    /// Called inside `spawn_blocking` — blocks on the bounded channel to
    /// provide natural backpressure.
    pub(super) fn persist(&self, mode: CommitMode) -> tokio::sync::oneshot::Receiver<SyncResult> {
        let sequence = self.sequence.load(Ordering::Acquire);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        // If the channel is full or disconnected, the recv side will get
        // an error which surfaces as a store error.
        let _ = self.tx.send(CommitWaiter {
            sequence,
            mode,
            done: done_tx,
        });
        done_rx
    }
}
