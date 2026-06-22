// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Persistent storage layer to manage job queues.
//!
//! Wraps fjall (database) to provide transactional queue operations across
//! two keyspaces plus two in-memory indexes:
//!
//! - `data`: jobs (`J` tag), payloads (`P` tag), and error records (`E` tag).
//!   Uses bloom filters and a larger memtable. See `RecordKind` for key layouts.
//! - `index`: status (`S` tag), queue (`Q` tag), type (`T` tag), and purge-at
//!   (`A` tag) secondary indexes. No bloom filters, smaller memtable. See
//!   `IndexKind` for key layouts.
//! - In-memory `ReadyIndex`: lock-free crossbeam skip-list priority index of
//!   ready jobs, rebuilt from the status index on startup.
//! - In-memory `ScheduledIndex`: lock-free crossbeam skip-set chronological
//!   index of scheduled jobs, rebuilt from the status index on startup.
//!
//! Each tag byte occupies a disjoint prefix range so that all record/index
//! types coexist without collision. Combined filters (e.g. queue + status)
//! use sorted stream intersection across the tag-prefixed ranges.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::filter::PayloadFilter;

use fjall::config::{FilterPolicy, PinningPolicy};
use fjall::{Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace, Slice};
use tokio::sync::broadcast;
use tokio::task;

use super::cron::CronScheduleIndex;
use super::ready_index::ReadyIndex;
use super::scheduled_index::ScheduledIndex;

use super::complete_batcher::CompleteBatcher;
use super::cron::{CronEntry, CronGroup};
use super::enqueue::{apply_enqueue, finalize_enqueue, prepare_enqueue};
use super::enqueue_batcher::EnqueueBatcher;
use super::group_committer::GroupCommitter;
use super::options::{CronEntryOptions, ReplaceCronGroupOptions};
use super::types::{
    BackoffConfig, CommitMode, EnvConfigError, Job, JobStatus, ScanDirection, StoreError,
    UniqueConstraint, UniqueWhile,
};

/// Events broadcast by the store when job state changes.
///
/// Workers use these events to decide when to check for new work:
///
/// - `JobCreated`: a job became ready. Workers check the queue name, then
///   try to atomically claim the one-shot `token`. Only the winner calls
///   `take_next_job`, avoiding thundering-herd DB contention.
/// - `JobCompleted` / `JobFailed`: workers prune their in-flight set;
///   no DB call needed.
/// - `JobScheduled`: only the scheduler cares; workers ignore it.
///
/// On `Lagged`, workers fall back to a full reconciliation (re-check the
/// DB and reconcile their in-flight set), since they may have missed both
/// creation and completion events.

#[derive(Debug, Clone)]
pub enum StoreEvent {
    /// A job became ready for processing (newly enqueued, requeued, or
    /// promoted from the scheduled index).
    ///
    /// Contains the queue name so workers can cheaply filter irrelevant
    /// events, and a single-use claim token (`Arc<AtomicBool>`) so that
    /// only one worker per event hits the database.
    JobCreated {
        id: String,
        queue: String,
        token: Arc<AtomicBool>,
    },

    /// A job was taken from the ready queue and is now being worked on.
    JobInFlight { id: String },

    /// A job was successfully completed and removed.
    JobCompleted { id: String },

    /// A job failed (either rescheduled for retry or killed).
    ///
    /// Workers use this to prune their in-flight set and free capacity.
    /// The `attempts` count identifies which attempt failed — workers
    /// compare it against the attempt they took so that stale events
    /// from a previous retry cycle are safely ignored.
    ///
    /// For rescheduled jobs a `JobScheduled` event follows immediately
    /// after, waking the scheduler at the new `ready_at`. For killed
    /// jobs this is the only event.
    JobFailed { id: String, attempts: u32 },

    /// A job was enqueued with a future `ready_at` timestamp.
    ///
    /// The scheduler listens for this to wake up early if the new job is
    /// due sooner than whatever it was sleeping until. Workers ignore it.
    JobScheduled { id: String, ready_at: u64 },

    /// A job was explicitly deleted via the API (not the normal lifecycle purge).
    ///
    /// Workers use this to release capacity when an in-flight job is deleted
    /// externally. The take stream treats this like a completion — the job
    /// is gone and the worker should not attempt to ack/nack it.
    JobDeleted { id: String },

    /// A job was modified via PATCH.
    ///
    /// The admin layer uses this to re-diff all tabs (ready, scheduled,
    /// in-flight) since a patch can change queue, priority, status, or
    /// any combination. Workers and the scheduler ignore this — they
    /// receive `JobCreated`/`JobScheduled` separately when a status
    /// transition occurs.
    JobPatched { id: String },

    /// A cron schedule was modified (created, replaced, paused, deleted, etc.).
    ///
    /// The cron scheduler listens for this to recalculate its sleep timer.
    CronScheduleChanged,

    /// Recovery and index rebuilding completed.
    ///
    /// Emitted once when `rebuild_indexes()` finishes. Wakes sleeping
    /// workers (so they attempt a drain) and the scheduler (so it polls
    /// immediately for due jobs).
    IndexRebuilt,
}

/// Provides a handle to the persistent store.
#[derive(Clone)]
pub struct Store {
    /// The storage configuration used to open this store, retained so that
    /// backup snapshots can recreate the database with matching settings.
    pub(super) config: StorageConfig,

    /// Database handle and all disk keyspaces, shared via `Arc` so that
    /// cloning the `Store` (or moving into `spawn_blocking`) is a single
    /// reference-count bump instead of eight.
    pub(super) ks: Arc<Keyspaces>,

    /// In-memory priority index for ready jobs (lock-free).
    ///
    /// Uses `crossbeam-skiplist` SkipMap + `dashmap` DashMap internally —
    /// no external mutex needed.
    pub(super) ready_index: Arc<ReadyIndex>,

    /// In-memory chronological index of scheduled jobs.
    ///
    /// Ordered by `(ready_at, job_id)`. Only jobs in the `Scheduled` state
    /// appear here. A background task scans this index to promote jobs to
    /// `Ready` once their time arrives.
    pub(super) scheduled_index: Arc<ScheduledIndex>,

    /// In-memory index of cron entries ordered by `next_enqueue_at`.
    cron_index: Arc<CronScheduleIndex>,

    /// Default retention period for dead jobs (milliseconds).
    pub(super) default_dead_retention_ms: u64,

    /// Default maximum retries before a failed job is killed.
    pub(super) default_retry_limit: u32,

    /// Default backoff config applied to jobs that don't specify one.
    pub(super) default_backoff: BackoffConfig,

    /// Whether the in-memory indexes have been fully rebuilt.
    ///
    /// Set to `true` in `open()` (fresh/empty indexes are trivially
    /// consistent) and toggled to `false`/`true` around `rebuild_indexes()`.
    /// Guards on `take_next_job`, `list_ready_jobs`, and `scan_ready_ids`
    /// return empty results while `false`.
    pub(super) index_ready: Arc<AtomicBool>,

    /// Current number of in-flight jobs across all connections.
    ///
    /// Incremented when jobs are taken (`take_next_n_jobs`), decremented
    /// when jobs complete, fail, or are requeued. Used by the HTTP layer
    /// to enforce `global_in_flight_limit` without hitting disk.
    pub(super) in_flight_count: Arc<AtomicU64>,

    /// Broadcast channel for store events.
    ///
    /// Subscribers receive notifications when jobs are enqueued, completed,
    /// or otherwise change state. Take handlers use this both to wake up
    /// when new work is available and to prune their in-flight tracking.
    pub(super) event_tx: broadcast::Sender<StoreEvent>,

    /// Server-side auto-batcher for concurrent single-job enqueues.
    /// Owned in an `Arc` so spawn_blocking tasks can cheaply clone the
    /// handle without holding a `&Store` borrow across the .await.
    /// Shut down by dropping (the inner `SyncSender` closes, the worker
    /// drains queued ops and exits).
    pub(super) enqueue_batcher: Arc<EnqueueBatcher>,

    /// Server-side auto-batcher for concurrent completions (acks).
    /// Mirrors `enqueue_batcher` in structure and lifecycle.
    pub(super) complete_batcher: Arc<CompleteBatcher>,
}

/// One-byte tag prefix for records in the `data` keyspace.
///
/// Each record kind occupies a disjoint prefix range so that jobs, payloads,
/// and errors coexist in the same fjall keyspace without collision.
#[repr(u8)]
pub(super) enum RecordKind {
    Cron = b'C',
    Error = b'E',
    Job = b'J',
    Payload = b'P',
}

/// One-byte tag prefix for entries in the `index` keyspace.
///
/// Each index kind occupies a disjoint prefix range so that all secondary
/// indexes coexist in the same fjall keyspace without collision.
#[repr(u8)]
pub(super) enum IndexKind {
    PurgeAt = b'A',
    Queue = b'Q',
    Status = b'S',
    Type = b'T',
    Unique = b'U',
}

/// Groups the fjall database handle and all disk keyspaces into a single
/// cheaply-cloneable unit. Wrapped in `Arc` inside `Store` so that every
/// async method only bumps one reference count instead of three.
pub(super) struct Keyspaces {
    /// Connection to the underlying database.
    pub(super) db: SingleWriterTxDatabase,

    /// Data keyspace — stores jobs (`J` tag), payloads (`P` tag), and
    /// error records (`E` tag). Uses bloom filters and a larger memtable.
    /// See `RecordKind` for the tag layout.
    pub(super) data: SingleWriterTxKeyspace,

    /// Index keyspace — stores status (`S` tag), queue (`Q` tag), type
    /// (`T` tag), and purge-at (`A` tag) secondary indexes. No bloom
    /// filters, smaller memtable. See `IndexKind` for the tag layout.
    pub(super) index: SingleWriterTxKeyspace,

    /// Group committer for batching journal persists.
    ///
    /// Shutdown is driven by dropping this field (which drops the
    /// `SyncSender`, closing the channel). The background thread
    /// drains remaining waiters and performs a final `SyncAll`.
    group_committer: GroupCommitter,

    /// Default commit mode for most operations (dequeue, complete, fail, etc.).
    pub(super) default_commit_mode: CommitMode,

    /// Commit mode for enqueue operations (resolved at construction;
    /// inherits the default when not overridden).
    pub(super) enqueue_commit_mode: CommitMode,
}

impl Keyspaces {
    /// Acquire the single-writer transaction lock.
    pub(super) fn write_tx(&self) -> fjall::SingleWriterWriteTx<'_> {
        self.db.write_tx()
    }

    /// Commit a transaction and block until durable.
    ///
    /// Calls fjall's `tx.commit()` to buffer the write, then sends a sync
    /// request to the group committer and blocks until it completes.
    /// Since this is always called inside `spawn_blocking`, blocking is safe.
    pub(super) fn commit(
        &self,
        tx: fjall::SingleWriterWriteTx<'_>,
        mode: CommitMode,
    ) -> Result<(), StoreError> {
        tx.commit()?;
        let rx = self.group_committer.persist(mode);
        match rx.blocking_recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(StoreError::Db(fjall::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                msg,
            )))),
            Err(_) => Err(StoreError::Db(fjall::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "group committer channel closed",
            )))),
        }
    }
}
// --- K-way merge helpers ---

/// A source of job IDs for k-way merging. Returns `Some(Ok(id))` for the
/// next ID, `Some(Err(e))` on error, or `None` when exhausted.
type MergeSource<'a> = Box<dyn FnMut() -> Option<Result<Vec<u8>, StoreError>> + 'a>;

/// Wrap a fjall range iterator into a `MergeSource`.
///
/// Each entry's key has a `prefix_len`-byte prefix followed by the job ID.
/// This macro exists because forward and reverse iterators are different
/// types in Rust, so a generic function cannot accept both without boxing
/// the iterator itself. The macro monomorphizes over the concrete iterator
/// type while producing the same `MergeSource` closure.
macro_rules! range_source {
    ($iter:expr, $prefix_len:expr) => {{
        let mut iter = $iter;
        let prefix_len = $prefix_len;
        Box::new(move || {
            iter.next().map(|e| {
                let (key, _) = e.into_inner()?;
                Ok(key[prefix_len..].to_vec())
            })
        }) as MergeSource<'_>
    }};
}

/// A lazy, sorted stream of job IDs.
///
/// Wraps a `MergeSource` closure and implements `Iterator` so we get
/// standard combinators like `.take(n)` and `.collect()` for free.
pub(super) struct IdStream<'a>(MergeSource<'a>);

impl<'a> Iterator for IdStream<'a> {
    type Item = Result<Vec<u8>, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.0)()
    }
}

/// Lazily merge multiple sorted `MergeSource`s into a single `IdStream`
/// using a binary heap (k-way merge).
///
/// For ascending order, uses a min-heap (`Reverse`). For descending, a
/// max-heap. Each source must already yield IDs in the matching order.
/// Errors encountered during seeding or iteration are deferred to the
/// next call to `next()`.
fn merge_sources<'a>(mut sources: Vec<MergeSource<'a>>, direction: ScanDirection) -> IdStream<'a> {
    // Deferred error: if seeding the heap hits an error, we store it here
    // and surface it on the first call to `next()`.
    let mut deferred_err: Option<StoreError> = None;

    match direction {
        ScanDirection::Asc => {
            let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(result) = src() {
                    match result {
                        Ok(id) => heap.push(Reverse((id, i))),
                        Err(e) => {
                            deferred_err = Some(e);
                            break;
                        }
                    }
                }
            }

            IdStream(Box::new(move || {
                if let Some(e) = deferred_err.take() {
                    return Some(Err(e));
                }
                let Reverse((id, i)) = heap.pop()?;
                if let Some(result) = sources[i]() {
                    match result {
                        Ok(next) => heap.push(Reverse((next, i))),
                        Err(e) => deferred_err = Some(e),
                    }
                }
                Some(Ok(id))
            }))
        }
        ScanDirection::Desc => {
            let mut heap: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::new();
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(result) = src() {
                    match result {
                        Ok(id) => heap.push((id, i)),
                        Err(e) => {
                            deferred_err = Some(e);
                            break;
                        }
                    }
                }
            }

            IdStream(Box::new(move || {
                if let Some(e) = deferred_err.take() {
                    return Some(Err(e));
                }
                let (id, i) = heap.pop()?;
                if let Some(result) = sources[i]() {
                    match result {
                        Ok(next) => heap.push((next, i)),
                        Err(e) => deferred_err = Some(e),
                    }
                }
                Some(Ok(id))
            }))
        }
    }
}

/// Lazily intersect two sorted `IdStream`s, yielding only IDs present in
/// both.
///
/// Both streams must be sorted in the same `direction`. The intersection
/// preserves that ordering. Internally buffers one item from each stream
/// and advances whichever is "behind" (smaller in asc, larger in desc).
fn intersect_streams<'a>(
    mut a: IdStream<'a>,
    mut b: IdStream<'a>,
    direction: ScanDirection,
) -> IdStream<'a> {
    // Buffered next values from each stream.
    let mut buf_a: Option<Vec<u8>> = None;
    let mut buf_b: Option<Vec<u8>> = None;
    // Whether we've primed the buffers yet.
    let mut primed = false;

    IdStream(Box::new(move || {
        // Prime on first call so construction is cheap.
        if !primed {
            primed = true;
            match a.next() {
                Some(Ok(v)) => buf_a = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
            match b.next() {
                Some(Ok(v)) => buf_b = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }

        loop {
            let (va, vb) = match (buf_a.as_ref(), buf_b.as_ref()) {
                (Some(va), Some(vb)) => (va, vb),
                _ => return None, // One or both streams exhausted.
            };

            match va.cmp(vb) {
                std::cmp::Ordering::Equal => {
                    // Match! Yield this ID and advance both streams.
                    let matched = buf_a.take().unwrap();
                    buf_a = match a.next() {
                        Some(Ok(v)) => Some(v),
                        Some(Err(e)) => return Some(Err(e)),
                        None => None,
                    };
                    buf_b = match b.next() {
                        Some(Ok(v)) => Some(v),
                        Some(Err(e)) => return Some(Err(e)),
                        None => None,
                    };
                    return Some(Ok(matched));
                }
                std::cmp::Ordering::Less => {
                    // a < b: in asc mode a is behind, advance a.
                    //        in desc mode a is ahead, advance b.
                    if direction == ScanDirection::Asc {
                        buf_a = match a.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    } else {
                        buf_b = match b.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    }
                }
                std::cmp::Ordering::Greater => {
                    // a > b: in asc mode b is behind, advance b.
                    //        in desc mode b is ahead, advance a.
                    if direction == ScanDirection::Asc {
                        buf_b = match b.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    } else {
                        buf_a = match a.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    }
                }
            }
        }
    }))
}

// --- Index scan source builders ---
//
// Build MergeSource iterators from the secondary indexes. Used by both
// list_jobs and delete_jobs.

/// Build queue-index MergeSources for the given queues.
fn queue_scan_sources<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    queues: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Vec<MergeSource<'a>> {
    queues
        .iter()
        .map(|queue_name| {
            let prefix_len = queue_name.len() + 3;
            let range_start = Bound::Included(make_queue_key(queue_name, ""));
            let mut range_end_key = Vec::with_capacity(queue_name.len() + 3);
            range_end_key.push(IndexKind::Queue as u8);
            range_end_key.push(0);
            range_end_key.extend_from_slice(queue_name.as_bytes());
            range_end_key.push(1);
            let range_end = Bound::Excluded(range_end_key);

            match (direction, from) {
                (ScanDirection::Asc, Some(cursor)) => range_source!(
                    snapshot.range::<Vec<u8>, _>(
                        ks.index.as_ref(),
                        (
                            Bound::Excluded(make_queue_key(queue_name, cursor)),
                            range_end
                        ),
                    ),
                    prefix_len
                ),
                (ScanDirection::Asc, None) => range_source!(
                    snapshot.range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end),),
                    prefix_len
                ),
                (ScanDirection::Desc, Some(cursor)) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.index.as_ref(),
                            (
                                range_start,
                                Bound::Excluded(make_queue_key(queue_name, cursor))
                            ),
                        )
                        .rev(),
                    prefix_len
                ),
                (ScanDirection::Desc, None) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end))
                        .rev(),
                    prefix_len
                ),
            }
        })
        .collect()
}

/// Build status-index MergeSources for the given statuses.
fn status_scan_sources<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    statuses: &HashSet<JobStatus>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Vec<MergeSource<'a>> {
    statuses
        .iter()
        .map(|status| {
            let prefix = *status as u8;
            let range_start = Bound::Included(vec![IndexKind::Status as u8, 0, prefix, 0]);
            let range_end = Bound::Excluded(vec![IndexKind::Status as u8, 0, prefix + 1, 0]);

            match (direction, from) {
                (ScanDirection::Asc, Some(cursor)) => range_source!(
                    snapshot.range::<Vec<u8>, _>(
                        ks.index.as_ref(),
                        (Bound::Excluded(make_status_key(*status, cursor)), range_end),
                    ),
                    4
                ),
                (ScanDirection::Asc, None) => range_source!(
                    snapshot.range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end),),
                    4
                ),
                (ScanDirection::Desc, Some(cursor)) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.index.as_ref(),
                            (
                                range_start,
                                Bound::Excluded(make_status_key(*status, cursor))
                            ),
                        )
                        .rev(),
                    4
                ),
                (ScanDirection::Desc, None) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end))
                        .rev(),
                    4
                ),
            }
        })
        .collect()
}

/// Build type-index MergeSources for the given types.
fn type_scan_sources<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    types: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Vec<MergeSource<'a>> {
    types
        .iter()
        .map(|type_name| {
            let prefix_len = type_name.len() + 3;
            let range_start = Bound::Included(make_type_key(type_name, ""));
            let mut range_end_key = Vec::with_capacity(type_name.len() + 3);
            range_end_key.push(IndexKind::Type as u8);
            range_end_key.push(0);
            range_end_key.extend_from_slice(type_name.as_bytes());
            range_end_key.push(1);
            let range_end = Bound::Excluded(range_end_key);

            match (direction, from) {
                (ScanDirection::Asc, Some(cursor)) => range_source!(
                    snapshot.range::<Vec<u8>, _>(
                        ks.index.as_ref(),
                        (Bound::Excluded(make_type_key(type_name, cursor)), range_end),
                    ),
                    prefix_len
                ),
                (ScanDirection::Asc, None) => range_source!(
                    snapshot.range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end),),
                    prefix_len
                ),
                (ScanDirection::Desc, Some(cursor)) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.index.as_ref(),
                            (
                                range_start,
                                Bound::Excluded(make_type_key(type_name, cursor))
                            ),
                        )
                        .rev(),
                    prefix_len
                ),
                (ScanDirection::Desc, None) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end))
                        .rev(),
                    prefix_len
                ),
            }
        })
        .collect()
}

/// Build an IdStream from a set of user-provided job IDs, sorted and
/// filtered by the pagination cursor.
fn id_stream(
    ids: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> IdStream<'static> {
    let mut sorted: Vec<Vec<u8>> = ids
        .iter()
        .filter(|id| match (from, direction) {
            (Some(cursor), ScanDirection::Asc) => id.as_str() > cursor.as_str(),
            (Some(cursor), ScanDirection::Desc) => id.as_str() < cursor.as_str(),
            (None, _) => true,
        })
        .map(|id| id.as_bytes().to_vec())
        .collect();

    match direction {
        ScanDirection::Asc => sorted.sort(),
        ScanDirection::Desc => sorted.sort_by(|a, b| b.cmp(a)),
    }

    let mut iter = sorted.into_iter();
    IdStream(Box::new(move || iter.next().map(Ok)))
}

/// A lazy stream of `Job`s, either from an `IdStream` (filtered path) or
/// from a direct range scan of the data keyspace (unfiltered path).
///
/// When `now` is `Some`, jobs past their `purge_at` are skipped (expired).
/// When `needs_payload` is true, the payload is hydrated from the data
/// keyspace. Payload filtering is NOT applied here — use
/// `PayloadFilteredIter` to wrap this stream when a filter is present.
pub(super) enum JobStream<'a, R: Readable> {
    /// Reads jobs by looking up IDs from an index scan.
    ById {
        ids: IdStream<'a>,
        reader: &'a R,
        data_ks: &'a SingleWriterTxKeyspace,
        now: Option<u64>,
        needs_payload: bool,
        source: String,
    },
    /// Reads jobs directly from a range scan of J-tagged keys.
    FullScan {
        /// Boxed to unify the Asc/Desc iterator types.
        entries: Box<dyn Iterator<Item = fjall::Guard> + 'a>,
        data_ks: &'a SingleWriterTxKeyspace,
        reader: &'a R,
        now: Option<u64>,
        needs_payload: bool,
    },
}

impl<'a, R: Readable> JobStream<'a, R> {
    /// Construct a `FullScan` variant for the given direction and cursor.
    pub(super) fn full_scan(
        snapshot: &'a R,
        ks: &'a Keyspaces,
        from: &Option<String>,
        direction: ScanDirection,
        now: Option<u64>,
        needs_payload: bool,
    ) -> Self {
        let job_prefix_start: Vec<u8> = vec![RecordKind::Job as u8, 0];
        let job_prefix_end: Vec<u8> = vec![RecordKind::Job as u8, 1];

        let entries: Box<dyn Iterator<Item = fjall::Guard> + 'a> = match direction {
            ScanDirection::Asc => {
                let start = match from {
                    Some(cursor) => Bound::Excluded(make_job_key(cursor)),
                    None => Bound::Included(job_prefix_start),
                };
                Box::new(snapshot.range::<Vec<u8>, _>(
                    ks.data.as_ref(),
                    (start, Bound::Excluded(job_prefix_end)),
                ))
            }
            ScanDirection::Desc => {
                let end = match from {
                    Some(cursor) => Bound::Excluded(make_job_key(cursor)),
                    None => Bound::Excluded(job_prefix_end),
                };
                Box::new(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.data.as_ref(),
                            (Bound::Included(job_prefix_start), end),
                        )
                        .rev(),
                )
            }
        };

        Self::FullScan {
            entries,
            data_ks: &ks.data,
            reader: snapshot,
            now,
            needs_payload,
        }
    }

    /// Construct a `ById` variant from a pre-built `IdStream`.
    pub(super) fn by_id(
        ids: IdStream<'a>,
        reader: &'a R,
        data_ks: &'a SingleWriterTxKeyspace,
        now: Option<u64>,
        needs_payload: bool,
        source: String,
    ) -> Self {
        Self::ById {
            ids,
            reader,
            data_ks,
            now,
            needs_payload,
            source,
        }
    }
}

impl<'a, R: Readable> Iterator for JobStream<'a, R> {
    type Item = Result<Job, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::ById {
                ids,
                reader,
                data_ks,
                now,
                needs_payload,
                source,
            } => loop {
                let id = match ids.next()? {
                    Ok(id) => id,
                    Err(e) => return Some(Err(e)),
                };

                let id_str = match std::str::from_utf8(&id) {
                    Ok(s) => s,
                    Err(e) => {
                        return Some(Err(StoreError::Corruption(format!(
                            "job ID is not valid UTF-8: {e}"
                        ))));
                    }
                };

                let job_key = make_job_key(id_str);
                let bytes = match reader.get(*data_ks, &job_key) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => {
                        // Job was deleted between index scan and data read.
                        tracing::trace!(
                            id = id_str,
                            source = &**source,
                            "job in index but missing from data keyspace, skipping"
                        );
                        continue;
                    }
                    Err(e) => return Some(Err(e.into())),
                };

                let mut job: Job = match rmp_serde::from_slice(&bytes) {
                    Ok(j) => j,
                    Err(e) => return Some(Err(e.into())),
                };

                if let Some(now) = *now {
                    if job.purge_at.is_some_and(|p| p <= now) {
                        continue;
                    }
                }

                if *needs_payload {
                    let payload_key = make_payload_key(&job.id);
                    match reader.get(*data_ks, &payload_key) {
                        Ok(Some(pb)) => match rmp_serde::from_slice(&pb) {
                            Ok(v) => job.payload = Some(v),
                            Err(e) => return Some(Err(e.into())),
                        },
                        Ok(None) => {}
                        Err(e) => return Some(Err(e.into())),
                    }
                }

                return Some(Ok(job));
            },

            Self::FullScan {
                entries,
                data_ks,
                reader,
                now,
                needs_payload,
            } => loop {
                let entry = entries.next()?;

                let (key, value) = match entry.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => return Some(Err(e.into())),
                };

                let mut job: Job = match rmp_serde::from_slice(&value) {
                    Ok(j) => j,
                    Err(e) => return Some(Err(e.into())),
                };

                if let Some(now) = *now {
                    if job.purge_at.is_some_and(|p| p <= now) {
                        continue;
                    }
                }

                if *needs_payload {
                    // Swap J tag for P tag to look up payload.
                    let mut payload_key = key.to_vec();
                    payload_key[0] = RecordKind::Payload as u8;
                    match reader.get(*data_ks, &payload_key) {
                        Ok(Some(pb)) => match rmp_serde::from_slice(&pb) {
                            Ok(v) => job.payload = Some(v),
                            Err(e) => return Some(Err(e.into())),
                        },
                        Ok(None) => {}
                        Err(e) => return Some(Err(e.into())),
                    }
                }

                return Some(Ok(job));
            },
        }
    }
}

/// Wraps a job iterator and applies a jq payload filter, skipping
/// non-matching jobs. The inner iterator must yield jobs with payloads
/// already hydrated (set `needs_payload: true` on the `JobStream`).
pub(super) struct PayloadFilteredIter<I> {
    pub(super) inner: I,
    pub(super) filter: std::sync::Arc<PayloadFilter>,
}

impl<I: Iterator<Item = Result<Job, StoreError>>> Iterator for PayloadFilteredIter<I> {
    type Item = Result<Job, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let job = match self.inner.next()? {
                Ok(j) => j,
                Err(e) => return Some(Err(e)),
            };

            let payload = job.payload.as_ref().unwrap_or(&serde_json::Value::Null);
            if self.filter.matches(payload) {
                return Some(Ok(job));
            }
        }
    }
}

/// Build an `IdStream` from index filters and/or ID filters.
///
/// Returns `None` when no index filters are active (caller should use a
/// full-scan `JobStream` instead). Returns `Some((stream, source, has_ids))`
/// when at least one filter is active.
pub(super) fn build_id_stream<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    ids: &HashSet<String>,
    statuses: &HashSet<JobStatus>,
    queues: &HashSet<String>,
    types: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Option<(IdStream<'a>, String, bool)> {
    let mut filters: Vec<(&str, IdStream<'a>)> = Vec::new();

    if !queues.is_empty() {
        filters.push((
            "jobs_by_queue",
            merge_sources(
                queue_scan_sources(snapshot, ks, queues, from, direction),
                direction,
            ),
        ));
    }

    if !statuses.is_empty() {
        filters.push((
            "jobs_by_status",
            merge_sources(
                status_scan_sources(snapshot, ks, statuses, from, direction),
                direction,
            ),
        ));
    }

    if !types.is_empty() {
        filters.push((
            "jobs_by_type",
            merge_sources(
                type_scan_sources(snapshot, ks, types, from, direction),
                direction,
            ),
        ));
    }

    let has_id_filter = !ids.is_empty();

    if has_id_filter {
        filters.push(("jobs_by_id", id_stream(ids, from, direction)));
    }

    if filters.is_empty() {
        return None;
    }

    let source_desc = if filters.len() == 1 {
        filters[0].0.to_string()
    } else {
        let names: Vec<&str> = filters.iter().map(|(n, _)| *n).collect();
        format!("({})", names.join(" & "))
    };

    let mut iter = filters.into_iter().map(|(_, s)| s);
    let first = iter.next().unwrap();
    let combined = iter.fold(first, |acc, s| intersect_streams(acc, s, direction));

    Some((combined, source_desc, has_id_filter))
}

/// Tuning parameters for the underlying LSM storage engine.
///
/// Job queue workloads are high-churn: jobs are enqueued, processed,
/// and deleted rapidly, generating many tombstones across all keyspaces.
/// The fjall defaults (64 MiB tables, 512 MiB journal) are tuned for
/// larger, read-heavy workloads. These defaults are sized down for
/// faster tombstone reclamation and tighter disk usage.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Block cache capacity in bytes. Shared across all keyspaces.
    /// Increase when the in-flight set of filter + index + hot data blocks
    /// exceeds the default. Recommended: 20–25 % of available memory.
    pub cache_size: u64,

    /// Target size for SST files in the `data` keyspace (jobs, payloads, errors).
    /// The memtable is sized to match so flushes produce appropriately-sized
    /// L0 tables.
    pub data_table_size: u64,

    /// Target size for SST files in the `index` keyspace (status, queue, type,
    /// purge-at indexes). These store tiny entries so smaller tables are
    /// appropriate. The memtable is sized to match.
    pub index_table_size: u64,

    /// Maximum total size of the WAL (journal) on disk. The journal is
    /// shared across all keyspaces and can only be reclaimed once every
    /// keyspace referenced in it has flushed. The minimum is 64 MiB.
    pub journal_size: u64,

    /// Number of L0 files that triggers compaction into L1. Lower values
    /// mean more frequent compaction (less L0 buildup, faster reads after
    /// recovery) but more write amplification during steady state.
    pub l0_threshold: u8,

    /// Default retention period for completed jobs (milliseconds).
    /// 0 means completed jobs are logically invisible immediately.
    pub default_completed_retention_ms: u64,

    /// Default retention period for dead jobs (milliseconds).
    pub default_dead_retention_ms: u64,

    /// Default maximum retries before a failed job is killed.
    pub default_retry_limit: u32,

    /// Default backoff config applied to jobs that don't specify one.
    pub default_backoff: BackoffConfig,

    /// Default commit mode for most operations.
    pub default_commit_mode: CommitMode,

    /// Optional per-operation commit mode for enqueue. When `None`,
    /// enqueue inherits `default_commit_mode`.
    pub enqueue_commit_mode: Option<CommitMode>,

    /// Maximum number of single-job enqueues coalesced into one write
    /// transaction by the server-side auto-batcher. Also the bounded
    /// channel capacity for in-flight enqueue requests, so it caps both
    /// per-batch size and total in-flight load. Does NOT apply to
    /// explicit `enqueue_bulk` calls, which run their own tx.
    pub enqueue_batch_size: usize,

    /// Maximum number of completion (ack) requests coalesced into one
    /// write transaction. Op-count bounded — a bulk-ack of N jobs
    /// still counts as one op. Same shape as `enqueue_batch_size`.
    pub complete_batch_size: usize,

    /// After a bulk mutation affects at least this many records, force a
    /// full LSM compaction to reclaim tombstones. Without this, leveled
    /// compaction can leave large pockets of garbage in the upper levels
    /// for a long time on quiet databases. Set to 0 to disable.
    pub auto_compact_threshold: u64,
}

/// Default block cache capacity (256 MiB).
pub const DEFAULT_CACHE_SIZE: u64 = 256 * 1024 * 1024;

/// Default target size for data SST files (64 MiB).
pub const DEFAULT_DATA_TABLE_SIZE: u64 = 64 * 1024 * 1024;

/// Default target size for index SST files (8 MiB).
pub const DEFAULT_INDEX_TABLE_SIZE: u64 = 8 * 1024 * 1024;

/// Default maximum journal (WAL) size on disk (64 MiB minimum).
pub const DEFAULT_JOURNAL_SIZE: u64 = 64 * 1024 * 1024;

/// Default L0 compaction threshold.
pub const DEFAULT_L0_THRESHOLD: u8 = 4;

/// Default maximum retries before a failed job is killed.
pub const DEFAULT_RETRY_LIMIT: u32 = 25;

/// Default backoff exponent (power curve steepness).
pub const DEFAULT_BACKOFF_EXPONENT: f32 = 4.0;

/// Default backoff base delay in milliseconds.
pub const DEFAULT_BACKOFF_BASE_MS: u32 = 15_000;

/// Default backoff jitter in milliseconds (max random ms per attempt multiplier).
pub const DEFAULT_BACKOFF_JITTER_MS: u32 = 30_000;

/// Default retention period for completed jobs (milliseconds).
/// 0 means completed jobs are logically invisible immediately.
pub const DEFAULT_COMPLETED_RETENTION_MS: u64 = 0;

/// Default retention period for dead jobs (milliseconds). 7 days.
pub const DEFAULT_DEAD_RETENTION_MS: u64 = 604_800_000;

/// Default threshold for auto-compacting after a bulk mutation. Bulk
/// operations that mutate at least this many records trigger a full
/// compaction once they commit.
pub const DEFAULT_AUTO_COMPACT_THRESHOLD: u64 = 10_000;

/// Default maximum number of concurrent single-job enqueues coalesced
/// into one auto-batched commit. Also the bounded channel capacity.
pub const DEFAULT_ENQUEUE_BATCH_SIZE: usize = 1000;

/// Default maximum number of concurrent completion (ack) requests
/// coalesced into one auto-batched commit. Also the bounded channel
/// capacity.
pub const DEFAULT_COMPLETE_BATCH_SIZE: usize = 1000;

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache_size: DEFAULT_CACHE_SIZE,
            data_table_size: DEFAULT_DATA_TABLE_SIZE,
            index_table_size: DEFAULT_INDEX_TABLE_SIZE,
            journal_size: DEFAULT_JOURNAL_SIZE,
            l0_threshold: DEFAULT_L0_THRESHOLD,
            default_completed_retention_ms: DEFAULT_COMPLETED_RETENTION_MS,
            default_dead_retention_ms: DEFAULT_DEAD_RETENTION_MS,
            default_retry_limit: DEFAULT_RETRY_LIMIT,
            default_backoff: BackoffConfig {
                exponent: DEFAULT_BACKOFF_EXPONENT,
                base_ms: DEFAULT_BACKOFF_BASE_MS,
                jitter_ms: DEFAULT_BACKOFF_JITTER_MS,
            },
            default_commit_mode: CommitMode::default(),
            enqueue_commit_mode: None,
            enqueue_batch_size: DEFAULT_ENQUEUE_BATCH_SIZE,
            complete_batch_size: DEFAULT_COMPLETE_BATCH_SIZE,
            auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
        }
    }
}

impl StorageConfig {
    /// Build a `StorageConfig` from environment variables, falling back to
    /// defaults for any that are unset. Returns an error if a variable is
    /// set but cannot be parsed.
    ///
    /// | Variable                    | Field              |
    /// |-----------------------------|--------------------|
    /// | `ZIZQ_CACHE_SIZE`         | `cache_size`       |
    /// | `ZIZQ_DATA_TABLE_SIZE`    | `data_table_size`  |
    /// | `ZIZQ_INDEX_TABLE_SIZE`   | `index_table_size` |
    /// | `ZIZQ_JOURNAL_SIZE`       | `journal_size`     |
    /// | `ZIZQ_L0_THRESHOLD`       | `l0_threshold`     |
    /// | `ZIZQ_DEFAULT_COMMIT_MODE` | `default_commit_mode` |
    /// | `ZIZQ_ENQUEUE_COMMIT_MODE` | `enqueue_commit_mode` |
    /// | `ZIZQ_ENQUEUE_BATCH_SIZE` | `enqueue_batch_size` |
    /// | `ZIZQ_COMPLETE_BATCH_SIZE` | `complete_batch_size` |
    /// | `ZIZQ_AUTO_COMPACT_THRESHOLD` | `auto_compact_threshold` |
    pub fn from_env() -> Result<Self, EnvConfigError> {
        let defaults = Self::default();
        Ok(Self {
            cache_size: env_parse_bytes("ZIZQ_CACHE_SIZE")?.unwrap_or(defaults.cache_size),
            data_table_size: env_parse_bytes("ZIZQ_DATA_TABLE_SIZE")?
                .unwrap_or(defaults.data_table_size),
            index_table_size: env_parse_bytes("ZIZQ_INDEX_TABLE_SIZE")?
                .unwrap_or(defaults.index_table_size),
            journal_size: env_parse_bytes("ZIZQ_JOURNAL_SIZE")?.unwrap_or(defaults.journal_size),
            l0_threshold: env_parse("ZIZQ_L0_THRESHOLD")?.unwrap_or(defaults.l0_threshold),
            default_completed_retention_ms: defaults.default_completed_retention_ms,
            default_dead_retention_ms: defaults.default_dead_retention_ms,
            default_retry_limit: defaults.default_retry_limit,
            default_backoff: defaults.default_backoff,
            default_commit_mode: match std::env::var("ZIZQ_DEFAULT_COMMIT_MODE").ok().as_deref() {
                Some("fsync") => CommitMode::Fsync,
                Some("buffered") | None => CommitMode::Buffered,
                Some(other) => {
                    return Err(EnvConfigError {
                        name: "ZIZQ_DEFAULT_COMMIT_MODE".into(),
                        value: other.into(),
                    });
                }
            },
            enqueue_commit_mode: match std::env::var("ZIZQ_ENQUEUE_COMMIT_MODE").ok().as_deref() {
                Some("fsync") => Some(CommitMode::Fsync),
                Some("buffered") => Some(CommitMode::Buffered),
                None => None,
                Some(other) => {
                    return Err(EnvConfigError {
                        name: "ZIZQ_ENQUEUE_COMMIT_MODE".into(),
                        value: other.into(),
                    });
                }
            },
            enqueue_batch_size: env_parse("ZIZQ_ENQUEUE_BATCH_SIZE")?
                .unwrap_or(defaults.enqueue_batch_size),
            complete_batch_size: env_parse("ZIZQ_COMPLETE_BATCH_SIZE")?
                .unwrap_or(defaults.complete_batch_size),
            auto_compact_threshold: env_parse("ZIZQ_AUTO_COMPACT_THRESHOLD")?
                .unwrap_or(defaults.auto_compact_threshold),
        })
    }
}

/// Parse an environment variable as a numeric type. Returns `Ok(None)` if
/// unset, `Ok(Some(value))` on success, or `Err` if set but unparseable.
fn env_parse<T: std::str::FromStr>(name: &str) -> Result<Option<T>, EnvConfigError> {
    let val = match std::env::var(name) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    val.parse().map(Some).map_err(|_| EnvConfigError {
        name: name.to_string(),
        value: val,
    })
}

/// Parse an environment variable as a human-readable byte size (e.g. "256MB",
/// "1GiB", or a plain number of bytes). Returns `Ok(None)` if unset.
fn env_parse_bytes(name: &str) -> Result<Option<u64>, EnvConfigError> {
    let val = match std::env::var(name) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    val.parse::<bytesize::ByteSize>()
        .map(|b| Some(b.as_u64()))
        .map_err(|_| EnvConfigError {
            name: name.to_string(),
            value: val,
        })
}

impl Store {
    /// Open or create a store at the given path.
    ///
    /// The path refers to the directory in which fjall stores its keyspace
    /// data. `default_completed_retention_ms` and `default_dead_retention_ms`
    /// control how long completed/dead jobs remain visible before purging.
    pub fn open(
        path: impl AsRef<std::path::Path>,
        config: StorageConfig,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let db = SingleWriterTxDatabase::builder(path)
            .cache_size(config.cache_size)
            .manual_journal_persist(true)
            .max_journaling_size(config.journal_size)
            .open()?;

        let data_compaction = Arc::new(
            fjall::compaction::Leveled::default()
                .with_l0_threshold(config.l0_threshold)
                .with_table_target_size(config.data_table_size),
        );
        let index_compaction = Arc::new(
            fjall::compaction::Leveled::default()
                .with_l0_threshold(config.l0_threshold)
                .with_table_target_size(config.index_table_size),
        );

        // Size the memtable (in-memory write buffer) to match each table
        // target so that flushes produce appropriately-sized L0 tables.
        //
        // Index keyspaces also disable bloom filters since they are only
        // ever range-scanned, never point-read.
        let data_opts = || {
            fjall::KeyspaceCreateOptions::default()
                .compaction_strategy(data_compaction.clone())
                .max_memtable_size(config.data_table_size)
                .filter_block_pinning_policy(PinningPolicy::all(true))
                .index_block_pinning_policy(PinningPolicy::all(true))
        };
        let index_opts = || {
            fjall::KeyspaceCreateOptions::default()
                .compaction_strategy(index_compaction.clone())
                .max_memtable_size(config.index_table_size)
                .filter_policy(FilterPolicy::disabled())
                .index_block_pinning_policy(PinningPolicy::all(true))
        };

        let data = db.keyspace("data", &data_opts)?;
        let index = db.keyspace("index", &index_opts)?;

        let (event_tx, _) = broadcast::channel(1024);

        // Resolve per-operation commit modes: enqueue inherits the
        // default when not explicitly overridden.
        let default_commit_mode = config.default_commit_mode;
        let enqueue_commit_mode = config.enqueue_commit_mode.unwrap_or(default_commit_mode);

        // Start the group committer — a dedicated OS thread that batches
        // journal persists. Shuts down when Keyspaces is dropped (the
        // SyncSender closes, the thread drains and does a final SyncAll).
        let group_committer = GroupCommitter::start(db.clone(), path.to_path_buf());

        let ks = Arc::new(Keyspaces {
            db,
            data,
            index,
            group_committer,
            default_commit_mode,
            enqueue_commit_mode,
        });
        let ready_index = Arc::new(ReadyIndex::new());
        let scheduled_index = Arc::new(ScheduledIndex::new());

        // Start the enqueue auto-batcher — a dedicated OS thread that
        // coalesces concurrent enqueue requests (singular + bulk) into
        // one tx.commit(). Shuts down when this Store (and any
        // spawn_blocking clones of the Arc) drops — the inner
        // SyncSender closes, the worker drains remaining ops and exits.
        let enqueue_batcher = Arc::new(EnqueueBatcher::start(
            ks.clone(),
            ready_index.clone(),
            scheduled_index.clone(),
            event_tx.clone(),
            config.enqueue_batch_size,
        ));

        let in_flight_count = Arc::new(AtomicU64::new(0));

        // Start the complete auto-batcher — same shape as the enqueue
        // batcher, coalesces concurrent completion (ack) requests.
        let complete_batcher = Arc::new(CompleteBatcher::start(
            ks.clone(),
            ready_index.clone(),
            in_flight_count.clone(),
            event_tx.clone(),
            config.default_completed_retention_ms,
            config.complete_batch_size,
        ));

        Ok(Self {
            config: config.clone(),
            ks,
            ready_index,
            scheduled_index,
            cron_index: Arc::new(CronScheduleIndex::new()),
            default_dead_retention_ms: config.default_dead_retention_ms,
            default_retry_limit: config.default_retry_limit,
            default_backoff: config.default_backoff,
            index_ready: Arc::new(AtomicBool::new(true)),
            in_flight_count,
            event_tx,
            enqueue_batcher,
            complete_batcher,
        })
    }

    /// Subscribe to store events.
    pub fn subscribe(&self) -> broadcast::Receiver<StoreEvent> {
        self.event_tx.subscribe()
    }

    /// Total number of ready jobs across all queues.
    pub fn ready_count(&self) -> usize {
        self.ready_index.len()
    }

    /// Total number of in-flight jobs across all connections.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_count.load(Ordering::Relaxed) as usize
    }

    /// Total number of scheduled jobs.
    pub fn scheduled_count(&self) -> usize {
        self.scheduled_index.len()
    }

    /// Force a full LSM compaction of both keyspaces, reclaiming tombstones
    /// that leveled compaction has left in upper levels.
    ///
    /// Leveled compaction triggers on level-size ratios, so after a large
    /// bulk delete (or any operation that produces many tombstones without
    /// matching live writes) the upper levels can sit on garbage for a long
    /// time on a quiet database. A full compaction merges every level down
    /// and drops tombstones whose seqnos are safe to GC.
    ///
    /// Runs on a blocking thread and may take seconds for large keyspaces.
    /// Excludes other (background leveled) compactions while running, but
    /// reads and writes at the LSM level proceed normally — they don't
    /// touch the compaction lock. Concurrent zizq transactions also aren't
    /// serialized against this since we bypass the txn writer lock.
    //
    // `major_compact` is `#[doc(hidden)]` in fjall 3.0.x / 3.1.x. It works
    // and is the intended escape hatch for this use case, but the API is
    // not formally stable yet; recheck on each fjall upgrade.
    pub async fn compact_all(&self) -> Result<(), StoreError> {
        let ks = self.ks.clone();
        task::spawn_blocking(move || -> Result<(), StoreError> {
            ks.data.inner().major_compact()?;
            ks.index.inner().major_compact()?;
            Ok(())
        })
        .await?
    }

    /// Create a consistent backup of the database at the given path.
    ///
    /// Takes a read snapshot of the live database and copies all key/value
    /// pairs from both keyspaces into a new database at `dest`. The new
    /// database is created with the same `StorageConfig` as the live one.
    ///
    /// This runs entirely from a point-in-time snapshot, so it does not
    /// block or lock the live database.
    pub async fn backup_snapshot(
        &self,
        dest: impl AsRef<Path> + Send + 'static,
    ) -> Result<(), StoreError> {
        let ks = self.ks.clone();
        let config = self.config.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();
            let dest = dest.as_ref();

            // Open a new database with matching settings.
            let backup_db = fjall::Database::builder(dest)
                .cache_size(config.cache_size)
                .max_journaling_size(config.journal_size)
                .open()
                .map_err(|e| {
                    StoreError::Internal(format!("failed to open backup database: {e}"))
                })?;

            let data_compaction = Arc::new(
                fjall::compaction::Leveled::default()
                    .with_l0_threshold(config.l0_threshold)
                    .with_table_target_size(config.data_table_size),
            );
            let index_compaction = Arc::new(
                fjall::compaction::Leveled::default()
                    .with_l0_threshold(config.l0_threshold)
                    .with_table_target_size(config.index_table_size),
            );

            let backup_data = backup_db
                .keyspace("data", || {
                    fjall::KeyspaceCreateOptions::default()
                        .compaction_strategy(data_compaction)
                        .max_memtable_size(config.data_table_size)
                        .filter_block_pinning_policy(PinningPolicy::all(true))
                        .index_block_pinning_policy(PinningPolicy::all(true))
                })
                .map_err(|e| {
                    StoreError::Internal(format!("failed to create backup data keyspace: {e}"))
                })?;

            let backup_index = backup_db
                .keyspace("index", || {
                    fjall::KeyspaceCreateOptions::default()
                        .compaction_strategy(index_compaction)
                        .max_memtable_size(config.index_table_size)
                        .filter_policy(FilterPolicy::disabled())
                        .index_block_pinning_policy(PinningPolicy::all(true))
                })
                .map_err(|e| {
                    StoreError::Internal(format!("failed to create backup index keyspace: {e}"))
                })?;

            // Copy all data keyspace entries.
            for entry in snapshot.range::<Vec<u8>, _>(&ks.data, ..) {
                let (key, value) = entry.into_inner()?;
                backup_data
                    .insert(&*key, &*value)
                    .map_err(|e| StoreError::Internal(format!("backup data write failed: {e}")))?;
            }

            // Copy all index keyspace entries.
            for entry in snapshot.range::<Vec<u8>, _>(&ks.index, ..) {
                let (key, value) = entry.into_inner()?;
                backup_index
                    .insert(&*key, &*value)
                    .map_err(|e| StoreError::Internal(format!("backup index write failed: {e}")))?;
            }

            // Flush and close cleanly.
            backup_db
                .persist(fjall::PersistMode::SyncAll)
                .map_err(|e| StoreError::Internal(format!("backup flush failed: {e}")))?;

            drop(backup_db);
            Ok(())
        })
        .await?
    }

    /// Recover state after startup.
    ///
    /// Move orphaned in-flight jobs back to Ready.
    ///
    /// Must complete before accepting API requests to avoid races
    /// with concurrent job completions. The job scan and status
    /// transitions run synchronously; only the durable commit is
    /// awaited.
    pub async fn recover_in_flight(&self) -> Result<usize, StoreError> {
        let store = self.clone();
        task::spawn_blocking(move || store.recover_in_flight_jobs()).await?
    }

    /// Rebuild all in-memory indexes asynchronously.
    ///
    /// Sets `index_ready` to `false` while rebuilding and emits
    /// `IndexRebuilt` when complete. Workers wait on `index_ready`
    /// before taking jobs.
    pub async fn rebuild_indexes(&self) -> Result<(usize, usize, usize), StoreError> {
        self.index_ready.store(false, Ordering::Release);

        let (ready, scheduled, cron) = tokio::try_join!(
            self.rebuild_ready_index(),
            self.rebuild_scheduled_index(),
            self.rebuild_cron_index(),
        )?;

        self.index_ready.store(true, Ordering::Release);
        let _ = self.event_tx.send(StoreEvent::IndexRebuilt);
        Ok((ready, scheduled, cron))
    }

    /// Move orphaned in-flight jobs back to Ready in the LSM indexes.
    ///
    /// Runs synchronously so it completes before any API requests are
    /// accepted. Does not touch the in-memory ready index — that's
    /// handled by `rebuild_ready_index`, which runs immediately after.
    fn recover_in_flight_jobs(&self) -> Result<usize, StoreError> {
        let ks = &self.ks;

        // Scan the status index for all InFlight jobs.
        // InFlight = 2, so the prefix range is [S, 0, 2, 0]..[S, 0, 3, 0].
        let start: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::InFlight as u8, 0];
        let end: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::InFlight as u8 + 1, 0];
        let range = (Bound::Included(start), Bound::Excluded(end));

        // Collect IDs via a read snapshot first — the write tx type
        // does not support range scans.
        let snapshot = ks.db.read_tx();
        let in_flight_ids: Vec<String> = snapshot
            .range::<Vec<u8>, _>(&ks.index, range)
            .map(|entry| {
                let (key, _) = entry.into_inner()?;
                // Key layout: S\0{status_u8}\0{job_id} — skip the 4-byte prefix.
                String::from_utf8(key[4..].to_vec())
                    .map_err(|e| StoreError::Corruption(format!("job ID is not valid UTF-8: {e}")))
            })
            .collect::<Result<_, _>>()?;
        drop(snapshot);

        if in_flight_ids.is_empty() {
            return Ok(0);
        }

        let count = in_flight_ids.len();
        let mut tx = ks.write_tx();

        for id in &in_flight_ids {
            let job_key = make_job_key(id);
            let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                StoreError::Corruption(format!("in-flight job missing from data keyspace: {id:?}"))
            })?;
            let mut job: Job = rmp_serde::from_slice(&job_bytes)?;

            let old_status_key = make_status_key(JobStatus::InFlight, id);
            let new_status_key = make_status_key(JobStatus::Ready, id);
            job.status = JobStatus::Ready.into();
            let updated_bytes = rmp_serde::to_vec_named(&job)?;

            tx.insert(&ks.data, &job_key, &updated_bytes);
            tx.remove(&ks.index, &old_status_key);
            tx.insert(&ks.index, &new_status_key, b"");

            // If unique_while == Queued, restore the unique index when
            // a job recovers from InFlight back to Ready. Only insert if no
            // other job has claimed the key while this one was in-flight.
            if let Some(ref uc) = job.unique {
                if uc.unique_while() == UniqueWhile::Queued {
                    let id_bytes: Slice = id.as_bytes().into();
                    tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                        Some(existing) => Some(existing.clone()),
                        None => Some(id_bytes.clone()),
                    })?;
                }
            }
        }

        ks.commit(tx, ks.default_commit_mode)?;
        Ok(count)
    }

    /// Populate the in-memory ready index from the `jobs_by_status` index.
    ///
    /// Scans for all Ready jobs, reads their metadata to get queue and
    /// priority, and inserts each entry into the skip list. No mutex needed —
    /// each `insert()` is lock-free, and recovery runs before any consumers.
    async fn rebuild_ready_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Ready jobs.
            // Ready = 1, so the prefix range is [S, 0, 1, 0]..[S, 0, 2, 0].
            let start: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::Ready as u8, 0];
            let end: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::Ready as u8 + 1, 0];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let snapshot = ks.db.read_tx();
            let mut count = 0;

            for entry in snapshot.range::<Vec<u8>, _>(&ks.index, range) {
                let (key, _) = entry.into_inner()?;
                // Key layout: S\0{status_u8}\0{job_id} — skip the 4-byte prefix.
                let job_id = String::from_utf8(key[4..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;

                let job_key = make_job_key(&job_id);
                let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "ready job missing from data keyspace: {job_id:?}"
                    ))
                })?;
                let job: Job = rmp_serde::from_slice(&job_bytes)?;

                ready_index.insert(&job.queue, job.priority, job_id);
                count += 1;
            }

            Ok(count)
        })
        .await?
    }

    /// Populate the in-memory scheduled index from the `jobs_by_status` index.
    ///
    /// Scans for all Scheduled jobs, reads their metadata to get `ready_at`,
    /// and inserts each entry into the SkipSet.
    async fn rebuild_scheduled_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Scheduled jobs.
            // Scheduled = 0, so the prefix range is [S, 0, 0, 0]..[S, 0, 1, 0].
            let start: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::Scheduled as u8, 0];
            let end: Vec<u8> = vec![
                IndexKind::Status as u8,
                0,
                JobStatus::Scheduled as u8 + 1,
                0,
            ];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let snapshot = ks.db.read_tx();
            let mut count = 0;

            for entry in snapshot.range::<Vec<u8>, _>(&ks.index, range) {
                let (key, _) = entry.into_inner()?;
                // Key layout: S\0{status_u8}\0{job_id} — skip the 4-byte prefix.
                let job_id = String::from_utf8(key[4..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;

                let job_key = make_job_key(&job_id);
                let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "scheduled job missing from data keyspace: {job_id:?}"
                    ))
                })?;
                let job: Job = rmp_serde::from_slice(&job_bytes)?;

                scheduled_index.insert(job.ready_at, job_id);
                count += 1;
            }

            Ok(count)
        })
        .await?
    }

    /// Rebuild the in-memory cron schedule index from disk.
    ///
    /// Scans all `C` tag entries in the data keyspace, skipping group
    /// metadata records, and inserts each entry's `next_enqueue_at` into
    /// the cron schedule index.
    async fn rebuild_cron_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan all keys with the cron tag prefix: [C .. D)
            let start = vec![RecordKind::Cron as u8];
            let end = vec![RecordKind::Cron as u8 + 1];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let data: &fjall::Keyspace = ks.data.as_ref();
            let mut count = 0;

            for guard in data.range::<Vec<u8>, _>(range) {
                let (key, value) = guard.into_inner()?;

                // Group metadata keys end with \0 (no entry name after it).
                // Entry keys have content after the \0.
                if let Some(pos) = key.iter().position(|&b| b == 0) {
                    if pos + 1 >= key.len() {
                        // Group metadata — skip.
                        continue;
                    }
                }

                let entry: CronEntry = rmp_serde::from_slice(&value)?;
                if let Some(next) = entry.next_enqueue_at {
                    // Extract group name from key: C{group}\0{entry_name}
                    let group_end = key.iter().position(|&b| b == 0).unwrap();
                    let group = String::from_utf8(key[1..group_end].to_vec()).map_err(|e| {
                        StoreError::Corruption(format!("cron group name is not valid UTF-8: {e}"))
                    })?;

                    cron_index.insert(next, group, entry.name.clone());
                    count += 1;
                }
            }

            Ok(count)
        })
        .await?
    }

    // --- Cron scheduling ---

    /// Replace all entries in a cron group.
    ///
    /// Performs a smart merge: entries with unchanged expressions preserve
    /// their scheduling state (`next_enqueue_at`, `last_enqueue_at`).
    /// Entries with changed expressions or new entries get `next_enqueue_at`
    /// computed from `now`. Entries absent from the input are deleted.
    ///
    /// The group metadata record is created if it doesn't exist.
    pub async fn replace_cron_group(
        &self,
        group: &str,
        opts: ReplaceCronGroupOptions,
        now: u64,
    ) -> Result<(CronGroup, Vec<CronEntry>), StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let group_paused = opts.paused;
        let entries = opts.entries;

        task::spawn_blocking(move || -> Result<(CronGroup, Vec<CronEntry>), StoreError> {
            let group_key = make_cron_group_key(&group);
            let prefix = make_cron_group_prefix(&group);

            // Validate all cron expressions before acquiring the write lock.
            let mut computed_next: Vec<Option<u64>> = Vec::with_capacity(entries.len());
            for input in &entries {
                // Validates the expression; None means no future occurrences.
                let next = cron_next_after(&input.expression, now, input.timezone.as_deref())?;
                computed_next.push(next);
            }

            // Acquire the single-writer lock. All reads within this scope
            // are consistent — no other writer can modify data.
            let mut tx = ks.write_tx();

            // Load or create group metadata, applying pause state if requested.
            let existing_group: CronGroup = match ks.data.get(&group_key)? {
                Some(bytes) => {
                    let mut g: CronGroup = rmp_serde::from_slice(&bytes)?;
                    if let Some(paused) = group_paused {
                        if paused != g.paused {
                            match (g.paused, paused) {
                                (false, true) => g.paused_at = Some(now),
                                (true, false) => g.resumed_at = Some(now),
                                _ => {}
                            }
                            g.paused = paused;
                        }
                        tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&g)?);
                    }
                    g
                }
                None => {
                    let g = CronGroup {
                        paused: group_paused.unwrap_or(false),
                        paused_at: if group_paused == Some(true) {
                            Some(now)
                        } else {
                            None
                        },
                        ..CronGroup::default()
                    };
                    tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&g)?);
                    g
                }
            };

            // Load all existing entries for this group.
            let mut range_end = prefix.clone();
            *range_end.last_mut().unwrap() = 1; // \0 -> \1

            let data: &fjall::Keyspace = ks.data.as_ref();
            let existing_entries: std::collections::HashMap<String, CronEntry> = data
                .range::<Vec<u8>, _>((Bound::Included(prefix.clone()), Bound::Excluded(range_end)))
                .skip(1) // skip the group metadata key
                .map(|guard| {
                    let (_, value) = guard.into_inner()?;
                    let entry: CronEntry = rmp_serde::from_slice(&value)?;
                    Ok((entry.name.clone(), entry))
                })
                .collect::<Result<_, StoreError>>()?;

            // Collect input names before consuming entries (needed for
            // deletion check below).
            let input_names: std::collections::HashSet<String> =
                entries.iter().map(|e| e.name.clone()).collect();

            // Build the new entries with smart merge logic.
            let new_entries: Vec<CronEntry> = entries
                .into_iter()
                .zip(computed_next)
                .map(|(input, computed)| {
                    let existing = existing_entries.get(&input.name);
                    merge_cron_entry(input, existing, computed, now)
                })
                .collect();

            // Delete removed entries (present in existing but not in input).
            for (name, _) in &existing_entries {
                if !input_names.contains(name) {
                    tx.remove(&ks.data, &make_cron_entry_key(&group, name));
                }
            }

            // Write new/updated entries.
            for entry in &new_entries {
                let entry_key = make_cron_entry_key(&group, &entry.name);
                let entry_bytes = rmp_serde::to_vec_named(entry)?;
                tx.insert(&ks.data, &entry_key, &entry_bytes);
            }

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----

            // Remove all old entries from the index.
            for (name, old_entry) in &existing_entries {
                if let Some(next) = old_entry.next_enqueue_at {
                    cron_index.remove(next, &group, name);
                }
            }

            // Insert all new entries into the index.
            for entry in &new_entries {
                if let Some(next) = entry.next_enqueue_at {
                    cron_index.insert(next, group.clone(), entry.name.clone());
                }
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok((existing_group, new_entries))
        })
        .await?
    }

    /// List all cron group names.
    ///
    /// Uses a prefix-stepping range scan over `C` tag keys, extracting
    /// distinct group names. Same O(n) approach as `list_queues`.
    pub async fn list_cron_groups(&self) -> Result<Vec<String>, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();

            let end: Vec<u8> = vec![RecordKind::Cron as u8 + 1];
            let mut start: Vec<u8> = vec![RecordKind::Cron as u8];
            let mut groups = Vec::new();

            loop {
                let mut range = snapshot.range::<Vec<u8>, _>(
                    ks.data.as_ref(),
                    (Bound::Included(start.clone()), Bound::Excluded(end.clone())),
                );

                let entry = match range.next() {
                    Some(entry) => entry,
                    None => break,
                };

                let (key, _) = entry.into_inner()?;

                // Key layout: C{group_name}\0... — extract group_name.
                let name_start = 1; // skip C tag
                let name_end = key[name_start..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|p| name_start + p)
                    .unwrap_or(key.len());
                let group_name = std::str::from_utf8(&key[name_start..name_end])
                    .map_err(|e| {
                        StoreError::Corruption(format!("cron group name is not valid UTF-8: {e}"))
                    })?
                    .to_string();
                groups.push(group_name.clone());

                // Advance past all keys for this group: C{group_name}\x01
                start.truncate(1); // keep C tag
                start.extend_from_slice(group_name.as_bytes());
                start.push(1); // one byte past \0 separator
            }

            Ok(groups)
        })
        .await?
    }

    /// Load a cron group and all its entries.
    ///
    /// Returns `None` if the group does not exist.
    pub async fn get_cron_group(
        &self,
        group: &str,
    ) -> Result<Option<(CronGroup, Vec<CronEntry>)>, StoreError> {
        let ks = self.ks.clone();
        let group = group.to_string();

        task::spawn_blocking(
            move || -> Result<Option<(CronGroup, Vec<CronEntry>)>, StoreError> {
                let group_key = make_cron_group_key(&group);
                let prefix = make_cron_group_prefix(&group);

                let snapshot = ks.db.read_tx();

                let group_meta: CronGroup = match snapshot.get(&ks.data, &group_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => return Ok(None),
                };

                let mut range_end = prefix.clone();
                *range_end.last_mut().unwrap() = 1; // \0 → \1

                let entries: Vec<CronEntry> = snapshot
                    .range::<Vec<u8>, _>(
                        ks.data.as_ref(),
                        (Bound::Included(prefix.clone()), Bound::Excluded(range_end)),
                    )
                    .skip(1) // skip group metadata key
                    .map(|guard| {
                        let (_, value) = guard.into_inner()?;
                        let entry: CronEntry = rmp_serde::from_slice(&value)?;
                        Ok(entry)
                    })
                    .collect::<Result<_, StoreError>>()?;

                Ok(Some((group_meta, entries)))
            },
        )
        .await?
    }

    /// Add a single entry to a cron group.
    ///
    /// Creates the group if it does not exist. Returns `Err` with
    /// `StoreError::Conflict` if an entry with the same name already
    /// exists in the group.
    pub async fn add_cron_entry(
        &self,
        group: &str,
        opts: CronEntryOptions,
        now: u64,
    ) -> Result<CronEntry, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<CronEntry, StoreError> {
            // Validate the cron expression before acquiring the write lock.
            let next_enqueue_at = cron_next_after(&opts.expression, now, opts.timezone.as_deref())?;

            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &opts.name);

            let mut tx = ks.write_tx();

            // Load or create group metadata.
            if ks.data.get(&group_key)?.is_none() {
                let group_meta = CronGroup::default();
                tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&group_meta)?);
            }

            // Check for conflict.
            if ks.data.get(&entry_key)?.is_some() {
                return Err(StoreError::Conflict(format!(
                    "cron entry '{}' already exists in group '{}'",
                    opts.name, group
                )));
            }

            let entry = CronEntry {
                name: opts.name,
                expression: opts.expression,
                timezone: opts.timezone,
                paused: opts.paused.unwrap_or(false),
                paused_at: None,
                resumed_at: None,
                job: opts.job,
                next_enqueue_at,
                last_enqueue_at: None,
            };

            tx.insert(&ks.data, &entry_key, &rmp_serde::to_vec_named(&entry)?);

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            if let Some(next) = entry.next_enqueue_at {
                cron_index.insert(next, group.clone(), entry.name.clone());
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(entry)
        })
        .await?
    }

    /// Create or replace a single entry in a cron group.
    ///
    /// Creates the group if it does not exist. If the entry already exists,
    /// applies the same smart merge as `replace_cron_group`: preserves
    /// scheduling state when the expression is unchanged, preserves pause
    /// state when omitted.
    pub async fn put_cron_entry(
        &self,
        group: &str,
        opts: CronEntryOptions,
        now: u64,
    ) -> Result<CronEntry, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<CronEntry, StoreError> {
            let next_enqueue_at = cron_next_after(&opts.expression, now, opts.timezone.as_deref())?;

            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &opts.name);

            let mut tx = ks.write_tx();

            // Load or create group metadata.
            if ks.data.get(&group_key)?.is_none() {
                let group_meta = CronGroup::default();
                tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&group_meta)?);
            }

            // Load existing entry for smart merge.
            let old_entry: Option<CronEntry> = match ks.data.get(&entry_key)? {
                Some(bytes) => Some(rmp_serde::from_slice(&bytes)?),
                None => None,
            };

            let entry = merge_cron_entry(opts, old_entry.as_ref(), next_enqueue_at, now);

            tx.insert(&ks.data, &entry_key, &rmp_serde::to_vec_named(&entry)?);

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            if let Some(old) = &old_entry {
                if let Some(next) = old.next_enqueue_at {
                    cron_index.remove(next, &group, &entry.name);
                }
            }
            if let Some(next) = entry.next_enqueue_at {
                cron_index.insert(next, group.clone(), entry.name.clone());
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(entry)
        })
        .await?
    }

    /// Delete a single entry from a cron group.
    ///
    /// Returns `true` if the entry existed and was deleted, `false` if
    /// the group or entry does not exist.
    /// Update a single cron entry's pause state.
    ///
    /// Returns the updated entry, or `None` if the group or entry does
    /// not exist.
    pub async fn patch_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
        paused: bool,
        now: u64,
    ) -> Result<Option<CronEntry>, StoreError> {
        let ks = self.ks.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &entry_name);

            // Check group exists.
            if ks.data.get(&group_key)?.is_none() {
                return Ok(None);
            }

            let mut tx = ks.write_tx();

            let mut entry: CronEntry = match ks.data.get(&entry_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(None),
            };

            if paused != entry.paused {
                match (entry.paused, paused) {
                    (false, true) => entry.paused_at = Some(now),
                    (true, false) => entry.resumed_at = Some(now),
                    _ => {}
                }
                entry.paused = paused;
                tx.insert(&ks.data, &entry_key, &rmp_serde::to_vec_named(&entry)?);
                ks.commit(tx, ks.default_commit_mode)?;
                let _ = event_tx.send(StoreEvent::CronScheduleChanged);
            }

            Ok(Some(entry))
        })
        .await?
    }

    pub async fn delete_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
    ) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        task::spawn_blocking(move || -> Result<bool, StoreError> {
            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &entry_name);

            // Check group exists.
            if ks.data.get(&group_key)?.is_none() {
                return Ok(false);
            }

            // Load entry for index cleanup.
            let entry: CronEntry = match ks.data.get(&entry_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(false),
            };

            let mut tx = ks.write_tx();
            tx.remove(&ks.data, &entry_key);
            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            if let Some(next) = entry.next_enqueue_at {
                cron_index.remove(next, &group, &entry_name);
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(true)
        })
        .await?
    }

    /// Update group-level metadata (currently just pause state).
    ///
    /// Returns the updated group, or `None` if the group does not exist.
    pub async fn patch_cron_group(
        &self,
        group: &str,
        paused: bool,
        now: u64,
    ) -> Result<Option<CronGroup>, StoreError> {
        let ks = self.ks.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<Option<CronGroup>, StoreError> {
            let group_key = make_cron_group_key(&group);

            let mut tx = ks.write_tx();

            let mut group_meta: CronGroup = match ks.data.get(&group_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(None),
            };

            if paused != group_meta.paused {
                match (group_meta.paused, paused) {
                    (false, true) => group_meta.paused_at = Some(now),
                    (true, false) => group_meta.resumed_at = Some(now),
                    _ => {}
                }
                group_meta.paused = paused;
                tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&group_meta)?);
                ks.commit(tx, ks.default_commit_mode)?;
                let _ = event_tx.send(StoreEvent::CronScheduleChanged);
            }

            Ok(Some(group_meta))
        })
        .await?
    }

    /// Delete every cron group and entry in a single transaction.
    ///
    /// Returns the number of groups deleted. Emits a single
    /// `CronScheduleChanged` event regardless of how many groups were
    /// removed.
    pub async fn delete_cron_groups(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();

        let count = task::spawn_blocking(move || -> Result<usize, StoreError> {
            let data: &fjall::Keyspace = ks.data.as_ref();
            let start: Vec<u8> = vec![RecordKind::Cron as u8];
            let end: Vec<u8> = vec![RecordKind::Cron as u8 + 1];

            let mut tx = ks.write_tx();
            let mut groups_seen: HashSet<Vec<u8>> = HashSet::new();

            for guard in data.range::<Vec<u8>, _>((Bound::Included(start), Bound::Excluded(end))) {
                let (key, _) = guard.into_inner()?;
                let key_bytes = key.as_ref();

                // Key layout: C{group}\0... — the group-meta key is C{group}
                // (no trailing \0). We dedupe by the prefix up to and
                // including the first \0 (or the whole key for the meta).
                let group_end = key_bytes[1..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|p| 1 + p)
                    .unwrap_or(key_bytes.len());
                groups_seen.insert(key_bytes[..group_end].to_vec());

                tx.remove(&ks.data, key_bytes);
            }

            if groups_seen.is_empty() {
                return Ok(0);
            }

            ks.commit(tx, ks.default_commit_mode)?;

            cron_index.clear();

            Ok(groups_seen.len())
        })
        .await??;

        if count > 0 {
            let _ = event_tx.send(StoreEvent::CronScheduleChanged);
        }

        // Reclaim tombstone space when the wipe was large enough.
        let threshold = self.config.auto_compact_threshold;
        if threshold > 0 && count as u64 >= threshold {
            self.compact_all().await?;
        }

        Ok(count)
    }

    /// Delete a cron group and all its entries.
    ///
    /// Returns `true` if the group existed and was deleted, `false` if it
    /// did not exist.
    pub async fn delete_cron_group(&self, group: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<bool, StoreError> {
            let group_key = make_cron_group_key(&group);
            let prefix = make_cron_group_prefix(&group);

            let mut tx = ks.write_tx();

            // Check if the group exists.
            if ks.data.get(&group_key)?.is_none() {
                return Ok(false);
            }

            // Collect all entry keys and their next_enqueue_at for index cleanup.
            let mut range_end = prefix.clone();
            *range_end.last_mut().unwrap() = 1;

            let data: &fjall::Keyspace = ks.data.as_ref();
            let mut entries_to_remove: Vec<(String, Option<u64>)> = Vec::new();

            for guard in data
                .range::<Vec<u8>, _>((Bound::Included(prefix.clone()), Bound::Excluded(range_end)))
            {
                let (key, value) = guard.into_inner()?;
                // Remove everything — group metadata and all entries.
                tx.remove(&ks.data, key.as_ref());

                // Track entry next_enqueue_at for index cleanup (skip group key).
                if key.len() > prefix.len() {
                    let entry: CronEntry = rmp_serde::from_slice(&value)?;
                    entries_to_remove.push((entry.name, entry.next_enqueue_at));
                }
            }

            // Remove the group metadata key itself.
            tx.remove(&ks.data, &group_key);

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            for (entry_name, next) in &entries_to_remove {
                if let Some(next) = next {
                    cron_index.remove(*next, &group, entry_name);
                }
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(true)
        })
        .await?
    }

    /// Load a single cron entry from the store.
    pub async fn get_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
    ) -> Result<Option<CronEntry>, StoreError> {
        let ks = self.ks.clone();
        let key = make_cron_entry_key(group, entry_name);

        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            match ks.data.get(&key)? {
                Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
        .await?
    }

    /// Peek at the earliest due timestamp in the cron schedule index.
    pub fn cron_next_due_at(&self) -> Option<u64> {
        self.cron_index.next_due_at()
    }

    /// Return all cron entries where `next_enqueue_at <= now`.
    ///
    /// Read-only — does not modify the in-memory index. The caller
    /// (cron scheduler) processes each entry via `promote_cron_entry`.
    pub fn next_due_cron_entries(&self, now: u64) -> Vec<(u64, String, String)> {
        self.cron_index.due_entries(now)
    }

    /// Atomically advance a due cron entry's schedule and enqueue its job.
    ///
    /// Pre-reads the entry and group outside the write tx, then uses
    /// `fetch_update` to CAS the entry inside the tx. Retries if the
    /// entry was modified concurrently. Returns early without writing
    /// if the entry is paused, not actually due, or no longer exists.
    pub async fn promote_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
        now: u64,
    ) -> Result<Option<CronEntry>, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            let entry_key = make_cron_entry_key(&group, &entry_name);
            let group_key = make_cron_group_key(&group);

            loop {
                // ---- outside tx: pre-read ----
                let pre_bytes = match ks.data.get(&entry_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(None), // Entry deleted.
                };
                let mut entry: CronEntry = rmp_serde::from_slice(&pre_bytes)?;

                // Not due: either no future occurrence or not yet due.
                match entry.next_enqueue_at {
                    None => return Ok(None),
                    Some(next) if next > now => return Ok(None),
                    _ => {}
                }

                // Check if group or entry is paused.
                let group_paused = match ks.data.get(&group_key)? {
                    Some(bytes) => {
                        let g: CronGroup = rmp_serde::from_slice(&bytes)?;
                        g.paused
                    }
                    None => false,
                };
                let is_paused = group_paused || entry.paused;

                // Compute next occurrence (None if no future occurrences).
                let next = cron_next_after(&entry.expression, now, entry.timezone.as_deref())?;

                // Prepare the job enqueue (unless paused).
                let prepared = if !is_paused {
                    entry.last_enqueue_at = Some(now);
                    Some(prepare_enqueue(entry.job.clone(), now)?)
                } else {
                    None
                };

                // Advance the schedule (time moves forward regardless of pause).
                let old_next = entry.next_enqueue_at;
                entry.next_enqueue_at = next;

                let updated_entry_bytes: Slice = rmp_serde::to_vec_named(&entry)?.into();

                // ---- inside tx: CAS + enqueue ----
                let mut tx = ks.write_tx();

                // CAS the cron entry — retry if it changed since pre-read.
                let prev =
                    tx.fetch_update(&ks.data, &entry_key, |_| Some(updated_entry_bytes.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue; // Entry changed — retry.
                }

                // Enqueue the job if not paused.
                let enqueue_result = if let Some(ref p) = prepared {
                    Some(apply_enqueue(&mut tx, &ks, p)?)
                } else {
                    None
                };

                ks.commit(tx, ks.enqueue_commit_mode)?;

                // ---- outside tx: update in-memory indexes ----

                // Update cron schedule index.
                if let Some(old) = old_next {
                    cron_index.remove(old, &group, &entry_name);
                }
                if let Some(next) = next {
                    cron_index.insert(next, group.clone(), entry_name.clone());
                }

                // Finalize job enqueue (in-memory indexes + events).
                if let Some(ref result) = enqueue_result {
                    finalize_enqueue(result, &ready_index, &scheduled_index, &event_tx);
                }

                return Ok(Some(entry));
            }
        })
        .await?
    }
}

/// Build a job metadata key: `J\0{job_id}`.
pub(super) fn make_job_key(job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(RecordKind::Job as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a payload key: `P\0{job_id}`.
pub(super) fn make_payload_key(job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(RecordKind::Payload as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a status index key: `S\0{status_byte}\0{job_id}`.
pub(super) fn make_status_key(status: JobStatus, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + job_id.len());
    key.push(IndexKind::Status as u8);
    key.push(0);
    key.push(status as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a queue membership index key: `Q\0{queue_name}\0{job_id}`.
pub(super) fn make_queue_key(queue_name: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(queue_name.len() + 3 + job_id.len());
    key.push(IndexKind::Queue as u8);
    key.push(0);
    key.extend_from_slice(queue_name.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a type membership index key: `T\0{job_type}\0{job_id}`.
pub(super) fn make_type_key(job_type: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(job_type.len() + 3 + job_id.len());
    key.push(IndexKind::Type as u8);
    key.push(0);
    key.extend_from_slice(job_type.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a purge-at index key: `A\0{purge_at_be_u64}\0{job_id}`.
///
/// Big-endian encoding gives chronological ordering so the reaper can
/// range-scan from the start to find expired entries.
pub(super) fn make_purge_key(purge_at: u64, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + 3 + job_id.len());
    key.push(IndexKind::PurgeAt as u8);
    key.push(0);
    key.extend_from_slice(&purge_at.to_be_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a unique deduplication index key: `U\0{unique_key}`.
pub(super) fn make_unique_key(unique_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + unique_key.len());
    key.push(IndexKind::Unique as u8);
    key.push(0);
    key.extend_from_slice(unique_key.as_bytes());
    key
}

/// Build a cron group metadata key: `C{group}\0`.
fn make_cron_group_key(group: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + group.len());
    key.push(RecordKind::Cron as u8);
    key.extend_from_slice(group.as_bytes());
    key.push(0);
    key
}

/// Build a cron entry key: `C{group}\0{entry_name}`.
fn make_cron_entry_key(group: &str, entry_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + group.len() + entry_name.len());
    key.push(RecordKind::Cron as u8);
    key.extend_from_slice(group.as_bytes());
    key.push(0);
    key.extend_from_slice(entry_name.as_bytes());
    key
}

/// Build a prefix for scanning all cron entries in a group: `C{group}\0`.
///
/// This is the same as the group metadata key — entries sort immediately
/// after the group record because their names are non-empty.
fn make_cron_group_prefix(group: &str) -> Vec<u8> {
    make_cron_group_key(group)
}

/// Compute the next occurrence of a cron expression after `now_ms`.
///
/// Returns `None` if the expression has no future occurrences.
/// Compute the next occurrence of a cron expression after `now_ms`.
///
/// When `timezone` is `Some`, the expression is evaluated in that IANA
/// timezone (e.g. "Australia/Melbourne"). When `None`, the system's
/// local timezone is used.
///
/// Returns `None` if the expression has no future occurrences.
/// Merge a `CronEntryOptions` with an optional existing `CronEntry`.
///
/// When an existing entry is present and the expression hasn't changed,
/// preserves `next_enqueue_at` and `last_enqueue_at`. When `paused` is
/// omitted in the options, preserves the existing pause state. Tracks
/// `paused_at` / `resumed_at` transitions.
///
/// `computed_next` is the pre-computed next enqueue time from the new
/// expression — used only when the expression has changed or the entry
/// is new.
fn merge_cron_entry(
    opts: CronEntryOptions,
    existing: Option<&CronEntry>,
    computed_next: Option<u64>,
    now: u64,
) -> CronEntry {
    if let Some(old) = existing {
        let (next, last) = if old.expression == opts.expression {
            (old.next_enqueue_at, old.last_enqueue_at)
        } else {
            (computed_next, old.last_enqueue_at)
        };

        let paused = opts.paused.unwrap_or(old.paused);
        let (paused_at, resumed_at) = match (old.paused, paused) {
            (false, true) => (Some(now), old.resumed_at),
            (true, false) => (old.paused_at, Some(now)),
            _ => (old.paused_at, old.resumed_at),
        };

        CronEntry {
            name: opts.name,
            expression: opts.expression,
            timezone: opts.timezone,
            paused,
            paused_at,
            resumed_at,
            job: opts.job,
            next_enqueue_at: next,
            last_enqueue_at: last,
        }
    } else {
        CronEntry {
            name: opts.name,
            expression: opts.expression,
            timezone: opts.timezone,
            paused: opts.paused.unwrap_or(false),
            paused_at: None,
            resumed_at: None,
            job: opts.job,
            next_enqueue_at: computed_next,
            last_enqueue_at: None,
        }
    }
}

fn cron_next_after(
    expression: &str,
    now_ms: u64,
    timezone: Option<&str>,
) -> Result<Option<u64>, StoreError> {
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse(expression)
        .map_err(|e| StoreError::InvalidOperation(format!("invalid cron expression: {e}")))?;

    let now_secs = (now_ms / 1000) as i64;
    let dt = chrono::DateTime::from_timestamp(now_secs, 0)
        .ok_or_else(|| StoreError::InvalidOperation("invalid timestamp".to_string()))?;

    if let Some(tz_name) = timezone {
        let tz: chrono_tz::Tz = tz_name
            .parse()
            .map_err(|_| StoreError::InvalidOperation(format!("invalid timezone: {tz_name:?}")))?;
        let dt_tz = dt.with_timezone(&tz);
        match cron.find_next_occurrence(&dt_tz, false) {
            Ok(next) => Ok(Some(next.timestamp() as u64 * 1000)),
            Err(_) => Ok(None),
        }
    } else {
        let local = dt.with_timezone(&chrono::Local);
        match cron.find_next_occurrence(&local, false) {
            Ok(next) => Ok(Some(next.timestamp() as u64 * 1000)),
            Err(_) => Ok(None),
        }
    }
}

/// Build an error record key: `E\0{job_id}\0{attempt_be_u32}`.
///
/// The null separator lets us prefix-scan all errors for a given job,
/// and the big-endian attempt number gives chronological ordering.
pub(super) fn make_error_key(job_id: &str, attempt: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(job_id.len() + 3 + 4);
    key.push(RecordKind::Error as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&attempt.to_be_bytes());
    key
}

/// Iterate all error record keys for a job via prefix scan.
///
/// The returned iterator yields owned key bytes that the caller can pass
/// to `tx.remove`. The fjall `Iter` is an owned value (no borrow on the
/// keyspace), so it's safe to interleave iteration with mutable tx ops.
pub(super) fn error_keys(ks: &Keyspaces, job_id: &str) -> impl Iterator<Item = Vec<u8>> {
    let mut prefix = Vec::with_capacity(job_id.len() + 3);
    prefix.push(RecordKind::Error as u8);
    prefix.push(0);
    prefix.extend_from_slice(job_id.as_bytes());
    prefix.push(0);

    ks.data
        .inner()
        .prefix(&prefix)
        .filter_map(|guard| guard.key().ok().map(|k| k.to_vec()))
}

/// Pre-computed keys for deleting all data associated with a job.
///
/// Built by `prepare_job_deletion` (no tx required), applied by
/// `apply_job_deletion` (write-only inside an open tx).
pub(super) struct JobDeletion {
    pub(super) id: String,
    status_key: Vec<u8>,
    queue_key: Vec<u8>,
    type_key: Vec<u8>,
    purge_key: Option<Vec<u8>>,
    error_keys: Vec<Vec<u8>>,
    unique_idx_key: Option<Vec<u8>>,
}

/// Collect all keys needed to delete a job. No tx required.
pub(super) fn prepare_job_deletion(job: &Job, status: JobStatus, ks: &Keyspaces) -> JobDeletion {
    let id = &job.id;
    JobDeletion {
        id: id.clone(),
        status_key: make_status_key(status, id),
        queue_key: make_queue_key(&job.queue, id),
        type_key: make_type_key(&job.job_type, id),
        purge_key: job.purge_at.map(|purge_at| make_purge_key(purge_at, id)),
        error_keys: error_keys(ks, id).collect(),
        unique_idx_key: job.unique.as_ref().map(|uc| make_unique_key(&uc.key)),
    }
}

/// Apply pre-computed deletion inside an open tx.
pub(super) fn apply_job_deletion(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    del: &JobDeletion,
    ks: &Keyspaces,
) {
    tx.remove(&ks.data, &make_job_key(&del.id));
    tx.remove_weak(&ks.data, &make_payload_key(&del.id));
    tx.remove(&ks.index, &del.status_key);
    tx.remove_weak(&ks.index, &del.queue_key);
    tx.remove_weak(&ks.index, &del.type_key);

    if let Some(ref purge_key) = del.purge_key {
        tx.remove(&ks.index, purge_key);
    }

    // Only remove the unique index entry if it still belongs to this job.
    // Another job may have already claimed the same key.
    if let Some(ref unique_key) = del.unique_idx_key {
        let job_id = del.id.as_bytes();
        let _ = tx.fetch_update(&ks.index, unique_key, |v| match v {
            Some(v) if v.as_ref() == job_id => None,
            other => other.cloned(),
        });
    }

    for key in &del.error_keys {
        tx.remove_weak(&ks.data, key);
    }
}

/// A job that has been pre-read and prepared for completion. Built by
/// `pre_read_completes` (no tx required), applied by
/// `apply_complete_batch` inside a write transaction.
///
/// Carries the original `pre_bytes` for CAS verification at apply time.
/// If the job's stored bytes changed between pre-read and tx, the apply
/// reports a CAS conflict and the caller must retry the pre-read.
pub(super) struct PreparedComplete {
    pub id: String,
    pub queue: String,
    pub pre_bytes: Slice,
    pub priority: u16,
    /// `None` for zero-retention completions (which delete the job).
    pub updated_bytes: Option<Slice>,
    /// `Some` for zero-retention completions (delete-paths).
    pub deletion: Option<JobDeletion>,
    /// Pre-computed index key updates for non-zero-retention paths.
    pub index_keys: Option<CompletionRetentionKeys>,
    /// Unique constraint snapshot for cleaning up the unique index.
    pub unique: Option<UniqueConstraint>,
}

/// Pre-computed index keys for a non-zero-retention completion.
pub(super) struct CompletionRetentionKeys {
    pub old_status: Vec<u8>,
    pub new_status: Vec<u8>,
    pub purge: Vec<u8>,
}

/// Pre-read the given job ids and build the list of `PreparedComplete`
/// values for those that are still in `InFlight` state. Jobs that are
/// missing or in a different status are pushed into `not_found`.
///
/// Does NOT open a transaction — reads go directly through the
/// keyspace, so the returned `pre_bytes` may be invalidated by
/// concurrent writers. `apply_complete_batch` verifies via CAS and
/// surfaces conflicts to the caller.
pub(super) fn pre_read_completes(
    ids: &[String],
    now: u64,
    ks: &Keyspaces,
    default_completed_retention_ms: u64,
) -> Result<(Vec<PreparedComplete>, Vec<String>), StoreError> {
    let mut prepared: Vec<PreparedComplete> = Vec::with_capacity(ids.len());
    let mut not_found: Vec<String> = Vec::new();

    for id in ids {
        let job_key = make_job_key(id);
        let pre_bytes = match ks.data.get(&job_key)? {
            Some(bytes) => bytes,
            None => {
                not_found.push(id.clone());
                continue;
            }
        };

        let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

        if job.status != JobStatus::InFlight as u8 {
            not_found.push(id.clone());
            continue;
        }

        let retention_ms = job
            .retention
            .as_ref()
            .and_then(|r| r.completed_ms)
            .unwrap_or(default_completed_retention_ms);

        if retention_ms == 0 {
            let del = prepare_job_deletion(&job, JobStatus::InFlight, ks);
            prepared.push(PreparedComplete {
                id: id.clone(),
                queue: job.queue.clone(),
                pre_bytes,
                priority: job.priority,
                updated_bytes: None,
                deletion: Some(del),
                index_keys: None,
                unique: None,
            });
        } else {
            let purge_at = now + retention_ms;
            let old_status_key = make_status_key(JobStatus::InFlight, id);
            let new_status_key = make_status_key(JobStatus::Completed, id);
            let purge_key = make_purge_key(purge_at, id);

            let unique = job.unique.clone();

            let queue = job.queue.clone();
            job.status = JobStatus::Completed.into();
            job.purge_at = Some(purge_at);
            job.completed_at = Some(now);

            let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

            prepared.push(PreparedComplete {
                id: id.clone(),
                queue,
                pre_bytes,
                priority: job.priority,
                updated_bytes: Some(updated_slice),
                deletion: None,
                index_keys: Some(CompletionRetentionKeys {
                    old_status: old_status_key,
                    new_status: new_status_key,
                    purge: purge_key,
                }),
                unique,
            });
        }
    }

    Ok((prepared, not_found))
}

/// Apply a batch of prepared completions to an open write transaction
/// with optimistic-concurrency CAS verification.
///
/// On any CAS mismatch, returns `Ok(false)` immediately without writing
/// further items. The caller must drop the tx and retry the
/// pre-read+apply sequence — the returned tx is left in a stale state
/// (some items may have been written before the conflict was detected).
///
/// On success, every item has been applied to the tx; caller commits.
/// Does NOT commit the transaction.
pub(super) fn apply_complete_batch(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    prepared: &[PreparedComplete],
) -> Result<bool, StoreError> {
    for p in prepared {
        let job_key = make_job_key(&p.id);

        if let Some(ref del) = p.deletion {
            // Zero-retention: delete via CAS.
            let prev = tx.take(&ks.data, &job_key)?;
            if prev.as_deref() != Some(&*p.pre_bytes) {
                return Ok(false);
            }
            apply_job_deletion(tx, del, ks);
        } else if let Some(ref updated) = p.updated_bytes {
            // Non-zero retention: update via CAS.
            let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated.clone()))?;
            if prev.as_deref() != Some(&*p.pre_bytes) {
                return Ok(false);
            }
            let keys = p.index_keys.as_ref().unwrap();
            tx.remove(&ks.index, &keys.old_status);
            tx.insert(&ks.index, &keys.new_status, b"");
            tx.insert(&ks.index, &keys.purge, b"");

            // Remove unique index for Queued or Active scope on
            // completion, but only if it still belongs to this job.
            if let Some(ref uc) = p.unique {
                let scope = uc.unique_while();
                if scope == UniqueWhile::Queued || scope == UniqueWhile::Active {
                    let job_id = p.id.as_bytes();
                    tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                        Some(v) if v.as_ref() == job_id => None,
                        other => other.cloned(),
                    })?;
                }
            }
        }
    }

    Ok(true)
}

/// Compute the backoff delay in milliseconds for a given attempt count.
///
/// Formula: `delay_ms = attempts^exponent + base_ms + rand(0..jitter_ms) * (attempts + 1)`
///
/// The jitter component scales linearly with the attempt count so that
/// later retries spread further apart, reducing collision likelihood when
/// many jobs fail at similar times.
pub(super) fn compute_backoff(attempts: u32, backoff: &BackoffConfig) -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let base_delay = (attempts as f32).powf(backoff.exponent) + backoff.base_ms as f32;

    // Cheap random value using the same approach as rand_id() in http.rs.
    let rand_frac = (RandomState::new().build_hasher().finish() as f64) / (u64::MAX as f64); // 0.0..1.0

    let jitter = rand_frac * backoff.jitter_ms as f64 * (attempts as f64 + 1.0);

    (base_delay as f64 + jitter) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    use crate::store::options::{
        BulkDeleteOptions, CronEntryOptions, EnqueueOptions, ListJobsOptions,
    };
    use crate::store::test_support::test_store;
    use crate::time::now_millis;

    #[tokio::test]
    async fn recover_moves_in_flight_to_ready() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        // Take the job so it becomes InFlight.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, job.id);

        // Nothing left to take.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // Recover should move it back to Ready and rebuild the index.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 1);
        assert_eq!(indexed, 1);

        // The job should be takeable again.
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn recover_returns_zero_when_none_in_flight() {
        let store = test_store();

        // Enqueue a job but don't take it — it stays Ready.
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 0);
        // The already-ready job should be indexed.
        assert_eq!(indexed, 1);
    }

    #[tokio::test]
    async fn recover_preserves_priority() {
        let store = test_store();

        // Enqueue two jobs at different priorities.
        let low = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();

        // Take both so they become InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Recover both.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 2);
        assert_eq!(indexed, 2);

        // They should come back in priority order (high first, then low).
        let first = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        let second = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, high.id);
        assert_eq!(second.id, low.id);
    }

    #[tokio::test]
    async fn recover_ignores_other_statuses() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // A Ready job.
        let ready = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("ready")),
            )
            .await
            .unwrap()
            .into_job();

        // A Scheduled job (far in the future).
        let scheduled = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("scheduled"))
                    .ready_at(now + 600_000),
            )
            .await
            .unwrap()
            .into_job();

        // Recover should find no in-flight jobs, but index the ready one.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 0);
        assert_eq!(indexed, 1);

        // The ready job is still takeable.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, ready.id);

        // The scheduled job is still in the scheduled index.
        let fetched = store
            .get_job(now_millis(), &scheduled.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.status, JobStatus::Scheduled as u8);
    }

    // --- compute_backoff tests ---

    #[test]
    fn compute_backoff_zero_jitter_is_deterministic() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0,
        };

        // attempts=1: 1^2 + 100 = 101
        assert_eq!(compute_backoff(1, &backoff), 101);
        // attempts=2: 2^2 + 100 = 104
        assert_eq!(compute_backoff(2, &backoff), 104);
        // attempts=3: 3^2 + 100 = 109
        assert_eq!(compute_backoff(3, &backoff), 109);
    }

    #[test]
    fn compute_backoff_with_jitter_stays_in_range() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 50,
        };

        // Run many times to exercise randomness.
        for attempts in 1..=10 {
            let delay = compute_backoff(attempts, &backoff);
            let base = (attempts as f64).powf(2.0) + 100.0;
            let max_jitter = 50.0 * (attempts as f64 + 1.0);
            assert!(
                delay >= base as u64,
                "delay {delay} below base {base} for attempt {attempts}"
            );
            assert!(
                delay <= (base + max_jitter) as u64,
                "delay {delay} above max {} for attempt {attempts}",
                base + max_jitter
            );
        }
    }

    #[test]
    fn compute_backoff_zero_attempts() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0,
        };

        // attempts=0: 0^2 + 100 = 100
        assert_eq!(compute_backoff(0, &backoff), 100);
    }

    #[tokio::test]
    async fn fsync_commit_mode_smoke_test() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_commit_mode = CommitMode::Fsync;
        let store = Store::open(dir.path().join("data"), config).unwrap();

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();

        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, job.id);
        assert_eq!(fetched.status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn fsync_enqueue_commit_mode_smoke_test() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_commit_mode = CommitMode::Buffered;
        config.enqueue_commit_mode = Some(CommitMode::Fsync);
        let store = Store::open(dir.path().join("data"), config).unwrap();

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();

        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, job.id);
        assert_eq!(fetched.status, u8::from(JobStatus::Ready));
    }

    // --- ready_count / scheduled_count tests ---

    #[tokio::test]
    async fn ready_count_tracks_enqueue_and_take() {
        let store = test_store();
        let now = now_millis();

        assert_eq!(store.ready_count(), 0);

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(store.ready_count(), 2);

        // Taking a job should decrement the ready count.
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.ready_count(), 1);
    }

    #[tokio::test]
    async fn ready_count_excludes_scheduled() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(store.ready_count(), 1);
    }

    #[tokio::test]
    async fn scheduled_count_tracks_enqueue_and_promote() {
        let store = test_store();
        let now = now_millis();

        assert_eq!(store.scheduled_count(), 0);

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 120_000),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(store.scheduled_count(), 2);

        // Promoting a scheduled job should decrement the scheduled count.
        store.promote_scheduled(&job).await.unwrap();
        assert_eq!(store.scheduled_count(), 1);
    }

    #[tokio::test]
    async fn scheduled_count_excludes_ready() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(store.scheduled_count(), 1);
    }

    // --- compact_all + auto-compact ---

    #[tokio::test]
    async fn compact_all_succeeds_on_empty_store() {
        let store = test_store();
        store.compact_all().await.unwrap();
    }

    #[tokio::test]
    async fn compact_all_succeeds_after_writes_and_deletes() {
        let store = test_store();
        let now = now_millis();

        for _ in 0..50 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }
        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 50);

        store.compact_all().await.unwrap();
    }

    #[tokio::test]
    async fn backup_snapshot_copies_all_data() {
        let store = test_store();
        let now = now_millis();

        // Enqueue some jobs across different queues.
        for i in 0..5 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", &format!("q{i}"), serde_json::json!({"i": i})),
                )
                .await
                .unwrap();
        }

        // Create the backup.
        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join("data");
        store.backup_snapshot(backup_path.clone()).await.unwrap();

        // Open the backup as a new store and verify the data.
        let restored = Store::open(&backup_path, Default::default()).unwrap();
        let opts = ListJobsOptions::new().limit(100).now(now);
        let page = restored.list_jobs(opts).await.unwrap();
        assert_eq!(page.jobs.len(), 5);

        let queues = restored.list_queues().await.unwrap();
        assert_eq!(queues, vec!["q0", "q1", "q2", "q3", "q4"]);
    }

    #[tokio::test]
    async fn backup_snapshot_is_point_in_time() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("before")),
            )
            .await
            .unwrap();

        // Take the backup.
        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join("data");
        store.backup_snapshot(backup_path.clone()).await.unwrap();

        // Enqueue more after the backup.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("after")),
            )
            .await
            .unwrap();

        // The backup should only contain the job from before.
        let restored = Store::open(&backup_path, Default::default()).unwrap();
        let opts = ListJobsOptions::new().limit(100).now(now);
        let page = restored.list_jobs(opts).await.unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!("before")));
    }

    // --- Cron scheduling ---

    /// Fixed timestamp for cron tests: 2023-11-14 22:13:20 UTC.
    /// Chosen to be mid-minute so cron calculations are deterministic.
    const CRON_NOW: u64 = 1_700_000_000_000;

    fn cron_entry_opts(
        name: &str,
        expression: &str,
        queue: &str,
        job_type: &str,
    ) -> CronEntryOptions {
        CronEntryOptions {
            name: name.to_string(),
            expression: expression.to_string(),
            timezone: None,
            paused: None,
            job: EnqueueOptions::new(job_type, queue, serde_json::json!({})),
        }
    }

    #[tokio::test]
    async fn replace_cron_group_creates_new_group() {
        let store = test_store();
        let now = CRON_NOW;

        let (group, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("every-minute", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert!(!group.paused);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "every-minute");
        assert_eq!(entries[0].expression, "* * * * *");
        assert!(entries[0].next_enqueue_at.is_some());
        assert!(entries[0].next_enqueue_at.unwrap() > now);
        assert!(entries[0].last_enqueue_at.is_none());
        assert!(!entries[0].paused);
    }

    #[tokio::test]
    async fn replace_cron_group_preserves_state_when_expression_unchanged() {
        let store = test_store();
        let now = CRON_NOW;

        let (_, entries1) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "*/5 * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let original_next = entries1[0].next_enqueue_at;

        // Replace with the same expression — next_enqueue_at should be preserved.
        let (_, entries2) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "*/5 * * * *", "q", "test")],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert_eq!(entries2[0].next_enqueue_at, original_next);
    }

    #[tokio::test]
    async fn replace_cron_group_recomputes_next_when_expression_changes() {
        let store = test_store();
        let now = CRON_NOW;

        let (_, entries1) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let original_next = entries1[0].next_enqueue_at;

        // Replace with a very different expression — next_enqueue_at should change.
        let (_, entries2) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "0 0 * * *", "q", "test")],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert_ne!(entries2[0].next_enqueue_at, original_next);
    }

    #[tokio::test]
    async fn replace_cron_group_removes_absent_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "* * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        // Replace with only e1 — e2 should be removed.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "e1");

        // Verify e2 is gone from disk.
        let e2 = store.get_cron_entry("default", "e2").await.unwrap();
        assert!(e2.is_none());
    }

    #[tokio::test]
    async fn replace_cron_group_empty_entries_removes_entries_but_keeps_group() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Replace with empty entries — entries removed, group persists.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![],
                },
                now,
            )
            .await
            .unwrap();

        assert!(entries.is_empty());

        // Entry should be gone.
        let e1 = store.get_cron_entry("default", "e1").await.unwrap();
        assert!(e1.is_none());

        // Group should still exist.
        let group = store.get_cron_group("default").await.unwrap();
        assert!(group.is_some());
    }

    #[tokio::test]
    async fn replace_cron_group_invalid_expression_returns_error() {
        let store = test_store();
        let now = CRON_NOW;

        let result = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("bad", "not a cron expr", "q", "test")],
                },
                now,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replace_cron_group_preserves_pause_when_omitted() {
        let store = test_store();
        let now = CRON_NOW;

        // Create an entry and manually pause it.
        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![CronEntryOptions {
                        paused: Some(true),
                        ..cron_entry_opts("e1", "* * * * *", "q", "test")
                    }],
                },
                now,
            )
            .await
            .unwrap();

        // Replace with paused omitted — should preserve paused state.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert!(entries[0].paused);
    }

    #[tokio::test]
    async fn replace_cron_group_explicit_pause_overrides() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Explicitly pause.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![CronEntryOptions {
                        paused: Some(true),
                        ..cron_entry_opts("e1", "* * * * *", "q", "test")
                    }],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert!(entries[0].paused);
        assert!(entries[0].paused_at.is_some());
    }

    #[tokio::test]
    async fn get_cron_entry_returns_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let entry = store.get_cron_entry("default", "e1").await.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name, "e1");
    }

    #[tokio::test]
    async fn get_cron_entry_returns_none_for_missing() {
        let store = test_store();

        let entry = store.get_cron_entry("default", "missing").await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn promote_cron_entry_enqueues_job_and_advances() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Get the entry's next_enqueue_at and call promote at that time.
        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        let result = store
            .promote_cron_entry("default", "e1", due_at)
            .await
            .unwrap();

        let updated = result.unwrap();
        assert!(updated.next_enqueue_at.is_some());
        assert!(updated.next_enqueue_at.unwrap() > due_at);
        assert_eq!(updated.last_enqueue_at, Some(due_at));

        // Verify a job was enqueued.
        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["q".to_string()].into()))
            .await
            .unwrap();
        assert_eq!(jobs.jobs.len(), 1);
        assert_eq!(jobs.jobs[0].job_type, "test");
    }

    #[tokio::test]
    async fn promote_cron_entry_skips_when_not_due() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Call promote before the entry is due.
        let result = store
            .promote_cron_entry("default", "e1", now)
            .await
            .unwrap();

        assert!(result.is_none());

        // No job should be enqueued.
        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["q".to_string()].into()))
            .await
            .unwrap();
        assert!(jobs.jobs.is_empty());
    }

    #[tokio::test]
    async fn promote_cron_entry_skips_paused_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![CronEntryOptions {
                        paused: Some(true),
                        ..cron_entry_opts("e1", "* * * * *", "q", "test")
                    }],
                },
                now,
            )
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        let result = store
            .promote_cron_entry("default", "e1", due_at)
            .await
            .unwrap();

        // Entry should be returned (schedule advanced) but no job enqueued.
        let updated = result.unwrap();
        assert!(updated.next_enqueue_at.unwrap() > due_at);
        assert!(updated.last_enqueue_at.is_none()); // Not set when paused.

        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["q".to_string()].into()))
            .await
            .unwrap();
        assert!(jobs.jobs.is_empty());
    }

    #[tokio::test]
    async fn promote_cron_entry_skips_deleted_entry() {
        let store = test_store();

        let result = store
            .promote_cron_entry("default", "missing", now_millis())
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn rebuild_cron_index_populates_from_disk() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "*/5 * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        // Verify the index has entries.
        assert!(store.cron_next_due_at().is_some());

        // Simulate a restart by rebuilding.
        let (_, _, cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(cron, 2);

        // Index should still work.
        assert!(store.cron_next_due_at().is_some());
    }

    #[tokio::test]
    async fn cron_next_due_at_reflects_schedule() {
        let store = test_store();
        let now = CRON_NOW;

        assert!(store.cron_next_due_at().is_none());

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let next = store.cron_next_due_at();
        assert!(next.is_some());
        assert!(next.unwrap() > now);
    }

    #[tokio::test]
    async fn next_due_cron_entries_returns_due_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        // Before due time — nothing.
        let due = store.next_due_cron_entries(now);
        assert!(due.is_empty());

        // At due time.
        let due = store.next_due_cron_entries(due_at);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, "default");
        assert_eq!(due[0].2, "e1");
    }

    #[tokio::test]
    async fn replace_cron_group_respects_timezone() {
        let store = test_store();
        let now = CRON_NOW; // 2023-11-14 22:13:20 UTC

        // Create two entries with the same expression but different timezones.
        // "0 9 * * *" = 9:00 AM daily.
        // In UTC, next 9:00 AM is 2023-11-15 09:00 UTC.
        // In Australia/Melbourne (UTC+11 in Nov), next 9:00 AM local is
        // 2023-11-14 22:00 UTC (already passed) → 2023-11-15 22:00 UTC.
        let utc_entry = CronEntryOptions {
            timezone: Some("UTC".to_string()),
            ..cron_entry_opts("utc-9am", "0 9 * * *", "q", "test")
        };
        let melb_entry = CronEntryOptions {
            timezone: Some("Australia/Melbourne".to_string()),
            ..cron_entry_opts("melb-9am", "0 9 * * *", "q", "test")
        };

        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![utc_entry, melb_entry],
                },
                now,
            )
            .await
            .unwrap();

        let utc_next = entries
            .iter()
            .find(|e| e.name == "utc-9am")
            .unwrap()
            .next_enqueue_at
            .unwrap();
        let melb_next = entries
            .iter()
            .find(|e| e.name == "melb-9am")
            .unwrap()
            .next_enqueue_at
            .unwrap();

        // They should be different — Melbourne 9 AM is a different UTC time than UTC 9 AM.
        assert_ne!(utc_next, melb_next);

        // UTC 9 AM should be earlier (it's ~11 hours before Melbourne 9 AM in UTC).
        assert!(utc_next < melb_next);
    }

    #[tokio::test]
    async fn replace_cron_group_invalid_timezone_returns_error() {
        let store = test_store();
        let now = CRON_NOW;

        let entry = CronEntryOptions {
            timezone: Some("Not/A/Timezone".to_string()),
            ..cron_entry_opts("bad-tz", "* * * * *", "q", "test")
        };

        let result = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![entry],
                },
                now,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replace_cron_group_accepts_6_field_expression() {
        let store = test_store();
        let now = CRON_NOW;

        // 6-field: second minute hour dom month dow
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("every-30s", "*/30 * * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        // With seconds, next occurrence should be within 30 seconds.
        let next = entries[0].next_enqueue_at.unwrap();
        assert!(next > now);
        assert!(next <= now + 30_000);
    }

    #[tokio::test]
    async fn list_cron_groups_returns_names() {
        let store = test_store();
        let now = CRON_NOW;

        assert!(store.list_cron_groups().await.unwrap().is_empty());

        for name in &["alpha", "beta", "gamma"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        let mut groups = store.list_cron_groups().await.unwrap();
        groups.sort();
        assert_eq!(groups, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn list_cron_groups_handles_common_prefixes() {
        let store = test_store();
        let now = CRON_NOW;

        // Groups with shared prefixes that could confuse prefix scanning.
        for name in &["billing", "billing-events", "billing-payments"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        let mut groups = store.list_cron_groups().await.unwrap();
        groups.sort();
        assert_eq!(
            groups,
            vec!["billing", "billing-events", "billing-payments"]
        );
    }

    #[tokio::test]
    async fn delete_cron_group_removes_group_and_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "* * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        assert!(store.delete_cron_group("default").await.unwrap());

        // Group should be gone.
        assert!(store.get_cron_group("default").await.unwrap().is_none());

        // Entries should be gone.
        assert!(
            store
                .get_cron_entry("default", "e1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_cron_entry("default", "e2")
                .await
                .unwrap()
                .is_none()
        );

        // Should not appear in listing.
        assert!(store.list_cron_groups().await.unwrap().is_empty());

        // Index should be empty.
        assert!(store.cron_next_due_at().is_none());
    }

    #[tokio::test]
    async fn delete_cron_group_returns_false_for_missing() {
        let store = test_store();
        assert!(!store.delete_cron_group("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn delete_cron_groups_returns_zero_for_empty_store() {
        let store = test_store();
        let count = store.delete_cron_groups().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_cron_groups_removes_all_groups_and_entries() {
        let store = test_store();
        let now = CRON_NOW;

        for name in &["g1", "g2", "g3"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        entries: vec![
                            cron_entry_opts("e1", "* * * * *", "q", "test"),
                            cron_entry_opts("e2", "*/5 * * * *", "q", "test"),
                        ],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        let count = store.delete_cron_groups().await.unwrap();
        assert_eq!(count, 3);

        assert!(store.list_cron_groups().await.unwrap().is_empty());
        assert!(store.cron_next_due_at().is_none());
        assert!(store.get_cron_entry("g1", "e1").await.unwrap().is_none());
        assert!(store.get_cron_group("g2").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_cron_group_does_not_affect_other_groups() {
        let store = test_store();
        let now = CRON_NOW;

        for name in &["keep", "delete"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        store.delete_cron_group("delete").await.unwrap();

        let groups = store.list_cron_groups().await.unwrap();
        assert_eq!(groups, vec!["keep"]);

        // The kept group should still have its entry.
        assert!(store.get_cron_entry("keep", "e1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn get_cron_group_returns_group_and_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "*/5 * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        let (group, entries) = store.get_cron_group("default").await.unwrap().unwrap();
        assert!(!group.paused);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "e1");
        assert_eq!(entries[1].name, "e2");
    }

    #[tokio::test]
    async fn get_cron_group_returns_none_for_missing() {
        let store = test_store();
        assert!(store.get_cron_group("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn patch_cron_group_pauses_and_unpauses() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Pause.
        let group = store
            .patch_cron_group("default", true, now + 1000)
            .await
            .unwrap()
            .unwrap();
        assert!(group.paused);
        assert_eq!(group.paused_at, Some(now + 1000));
        assert!(group.resumed_at.is_none());

        // Unpause.
        let group = store
            .patch_cron_group("default", false, now + 2000)
            .await
            .unwrap()
            .unwrap();
        assert!(!group.paused);
        assert_eq!(group.paused_at, Some(now + 1000));
        assert_eq!(group.resumed_at, Some(now + 2000));
    }

    #[tokio::test]
    async fn patch_cron_group_returns_none_for_missing() {
        let store = test_store();
        assert!(
            store
                .patch_cron_group("missing", true, CRON_NOW)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_cron_entry_removes_single_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "* * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        assert!(store.delete_cron_entry("default", "e1").await.unwrap());

        // e1 is gone, e2 remains.
        assert!(
            store
                .get_cron_entry("default", "e1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_cron_entry("default", "e2")
                .await
                .unwrap()
                .is_some()
        );

        // Group still exists.
        assert!(store.get_cron_group("default").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_cron_entry_returns_false_for_missing_group() {
        let store = test_store();
        assert!(!store.delete_cron_entry("nonexistent", "e1").await.unwrap());
    }

    #[tokio::test]
    async fn delete_cron_entry_returns_false_for_missing_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert!(!store.delete_cron_entry("default", "missing").await.unwrap());
    }

    #[tokio::test]
    async fn delete_cron_entry_leaves_empty_group() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert!(store.delete_cron_entry("default", "e1").await.unwrap());

        // Group persists with no entries.
        let (group, entries) = store.get_cron_group("default").await.unwrap().unwrap();
        assert!(!group.paused);
        assert!(entries.is_empty());
    }
}
