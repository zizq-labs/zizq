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

use super::cron::{CronEntry, CronGroup};
use super::group_committer::GroupCommitter;
use super::options::{
    BulkDeleteOptions, BulkPatchOptions, CronEntryOptions, EnqueueOptions, FailureOptions,
    ListErrorsOptions, ListJobsOptions, PatchJobOptions,
};
use super::results::{BulkCompleteResult, EnqueueResult, ListErrorsPage, ListJobsPage};
use super::types::{
    BackoffConfig, CommitMode, EnvConfigError, ErrorRecord, Job, JobStatus, ScanDirection,
    StoreError, UniqueConstraint, UniqueWhile,
};

impl PatchJobOptions {
    /// Apply all patches to a mutable job, including status transitions
    /// arising from `ready_at` changes relative to `now`.
    ///
    /// Returns a `PatchDiff` capturing the before/after values needed for
    /// index updates, keyspace maintenance, and event emission.
    pub(crate) fn apply(&self, job: &mut Job, now: u64) -> PatchDiff {
        let old_status = JobStatus::try_from(job.status).unwrap();
        let old_queue = job.queue.clone();
        let old_priority = job.priority;
        let old_ready_at = job.ready_at;

        if let Some(ref queue) = self.queue {
            job.queue = queue.clone();
        }
        if let Some(priority) = self.priority {
            job.priority = priority;
        }
        if let Some(ref retry_limit) = self.retry_limit {
            job.retry_limit = *retry_limit;
        }
        if let Some(ref backoff) = self.backoff {
            job.backoff = backoff.clone();
        }
        match &self.retention {
            Some(None) => job.retention = None,
            Some(Some(patch)) => {
                job.retention = patch.clone().apply(job.retention.take());
            }
            None => {}
        }

        // Apply ready_at and any resulting status transition.
        match self.ready_at {
            Some(None) => {
                // "Make it ready now" — only meaningful for Scheduled jobs.
                // For Ready/InFlight, leave ready_at unchanged.
                if old_status == JobStatus::Scheduled {
                    job.ready_at = now;
                    job.status = JobStatus::Ready as u8;
                }
            }
            Some(Some(ts)) => {
                job.ready_at = ts;
                if ts > now && old_status == JobStatus::Ready {
                    job.status = JobStatus::Scheduled as u8;
                } else if ts <= now && old_status == JobStatus::Scheduled {
                    job.status = JobStatus::Ready as u8;
                }
            }
            None => {}
        }

        PatchDiff {
            id: job.id.clone(),
            old_status,
            new_status: JobStatus::try_from(job.status).unwrap(),
            old_queue,
            new_queue: job.queue.clone(),
            old_priority,
            new_priority: job.priority,
            old_ready_at,
            new_ready_at: job.ready_at,
        }
    }
}

/// Lightweight diff capturing the before/after values of a patched job.
///
/// Used by both single-job and bulk patch to drive keyspace updates,
/// in-memory index updates, and event emission without retaining full
/// `Job` copies.
pub(crate) struct PatchDiff {
    id: String,
    old_status: JobStatus,
    new_status: JobStatus,
    old_queue: String,
    new_queue: String,
    old_priority: u16,
    new_priority: u16,
    old_ready_at: u64,
    new_ready_at: u64,
}

impl PatchDiff {
    /// Update on-disk index keyspaces for status and queue changes.
    ///
    /// Must be called before `commit()`.
    fn update_keyspaces(&self, tx: &mut fjall::SingleWriterWriteTx<'_>, ks: &Keyspaces) {
        if self.old_status != self.new_status {
            tx.remove(&ks.index, &make_status_key(self.old_status, &self.id));
            tx.insert(&ks.index, &make_status_key(self.new_status, &self.id), b"");
        }
        if self.old_queue != self.new_queue {
            tx.remove(&ks.index, &make_queue_key(&self.old_queue, &self.id));
            tx.insert(&ks.index, &make_queue_key(&self.new_queue, &self.id), b"");
        }
    }

    /// Update in-memory ready and scheduled indexes.
    ///
    /// Must be called after a successful `commit()`.
    fn update_indexes(&self, ready_index: &ReadyIndex, scheduled_index: &ScheduledIndex) {
        match (self.old_status, self.new_status) {
            (JobStatus::Ready, JobStatus::Ready) => {
                if self.old_queue != self.new_queue || self.old_priority != self.new_priority {
                    ready_index.remove(&self.old_queue, self.old_priority, &self.id);
                    ready_index.insert(&self.new_queue, self.new_priority, self.id.clone());
                }
            }
            (JobStatus::Ready, JobStatus::Scheduled) => {
                ready_index.remove(&self.old_queue, self.old_priority, &self.id);
                scheduled_index.insert(self.new_ready_at, self.id.clone());
            }
            (JobStatus::Scheduled, JobStatus::Ready) => {
                scheduled_index.remove(self.old_ready_at, &self.id);
                ready_index.insert(&self.new_queue, self.new_priority, self.id.clone());
            }
            (JobStatus::Scheduled, JobStatus::Scheduled) => {
                if self.old_ready_at != self.new_ready_at {
                    scheduled_index.remove(self.old_ready_at, &self.id);
                    scheduled_index.insert(self.new_ready_at, self.id.clone());
                }
            }
            _ => {} // InFlight — no index changes.
        }
    }

    /// Emit store events for status transitions and the patch itself.
    ///
    /// Must be called after sync.
    fn emit_events(&self, event_tx: &broadcast::Sender<StoreEvent>) {
        match (self.old_status, self.new_status) {
            (JobStatus::Ready, JobStatus::Scheduled) => {
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: self.id.clone(),
                    ready_at: self.new_ready_at,
                });
            }
            (JobStatus::Scheduled, JobStatus::Ready) => {
                let _ = event_tx.send(StoreEvent::JobCreated {
                    id: self.id.clone(),
                    queue: self.new_queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            (JobStatus::Scheduled, JobStatus::Scheduled)
                if self.old_ready_at != self.new_ready_at =>
            {
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: self.id.clone(),
                    ready_at: self.new_ready_at,
                });
            }
            _ => {}
        }

        let _ = event_tx.send(StoreEvent::JobPatched {
            id: self.id.clone(),
        });
    }
}

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
    config: StorageConfig,

    /// Database handle and all disk keyspaces, shared via `Arc` so that
    /// cloning the `Store` (or moving into `spawn_blocking`) is a single
    /// reference-count bump instead of eight.
    ks: Arc<Keyspaces>,

    /// In-memory priority index for ready jobs (lock-free).
    ///
    /// Uses `crossbeam-skiplist` SkipMap + `dashmap` DashMap internally —
    /// no external mutex needed.
    ready_index: Arc<ReadyIndex>,

    /// In-memory chronological index of scheduled jobs.
    ///
    /// Ordered by `(ready_at, job_id)`. Only jobs in the `Scheduled` state
    /// appear here. A background task scans this index to promote jobs to
    /// `Ready` once their time arrives.
    scheduled_index: Arc<ScheduledIndex>,

    /// In-memory index of cron entries ordered by `next_enqueue_at`.
    cron_index: Arc<CronScheduleIndex>,

    /// Default retention period for completed jobs (milliseconds).
    default_completed_retention_ms: u64,

    /// Default retention period for dead jobs (milliseconds).
    default_dead_retention_ms: u64,

    /// Default maximum retries before a failed job is killed.
    default_retry_limit: u32,

    /// Default backoff config applied to jobs that don't specify one.
    default_backoff: BackoffConfig,

    /// Whether the in-memory indexes have been fully rebuilt.
    ///
    /// Set to `true` in `open()` (fresh/empty indexes are trivially
    /// consistent) and toggled to `false`/`true` around `rebuild_indexes()`.
    /// Guards on `take_next_job`, `list_ready_jobs`, and `scan_ready_ids`
    /// return empty results while `false`.
    index_ready: Arc<AtomicBool>,

    /// Current number of in-flight jobs across all connections.
    ///
    /// Incremented when jobs are taken (`take_next_n_jobs`), decremented
    /// when jobs complete, fail, or are requeued. Used by the HTTP layer
    /// to enforce `global_in_flight_limit` without hitting disk.
    in_flight_count: Arc<AtomicU64>,

    /// Broadcast channel for store events.
    ///
    /// Subscribers receive notifications when jobs are enqueued, completed,
    /// or otherwise change state. Take handlers use this both to wake up
    /// when new work is available and to prune their in-flight tracking.
    event_tx: broadcast::Sender<StoreEvent>,
}

/// One-byte tag prefix for records in the `data` keyspace.
///
/// Each record kind occupies a disjoint prefix range so that jobs, payloads,
/// and errors coexist in the same fjall keyspace without collision.
#[repr(u8)]
enum RecordKind {
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
enum IndexKind {
    PurgeAt = b'A',
    Queue = b'Q',
    Status = b'S',
    Type = b'T',
    Unique = b'U',
}

/// Groups the fjall database handle and all disk keyspaces into a single
/// cheaply-cloneable unit. Wrapped in `Arc` inside `Store` so that every
/// async method only bumps one reference count instead of three.
struct Keyspaces {
    /// Connection to the underlying database.
    db: SingleWriterTxDatabase,

    /// Data keyspace — stores jobs (`J` tag), payloads (`P` tag), and
    /// error records (`E` tag). Uses bloom filters and a larger memtable.
    /// See `RecordKind` for the tag layout.
    data: SingleWriterTxKeyspace,

    /// Index keyspace — stores status (`S` tag), queue (`Q` tag), type
    /// (`T` tag), and purge-at (`A` tag) secondary indexes. No bloom
    /// filters, smaller memtable. See `IndexKind` for the tag layout.
    index: SingleWriterTxKeyspace,

    /// Group committer for batching journal persists.
    ///
    /// Shutdown is driven by dropping this field (which drops the
    /// `SyncSender`, closing the channel). The background thread
    /// drains remaining waiters and performs a final `SyncAll`.
    group_committer: GroupCommitter,

    /// Default commit mode for most operations (dequeue, complete, fail, etc.).
    default_commit_mode: CommitMode,

    /// Commit mode for enqueue operations (resolved at construction;
    /// inherits the default when not overridden).
    enqueue_commit_mode: CommitMode,
}

impl Keyspaces {
    /// Acquire the single-writer transaction lock.
    fn write_tx(&self) -> fjall::SingleWriterWriteTx<'_> {
        self.db.write_tx()
    }

    /// Commit a transaction and block until durable.
    ///
    /// Calls fjall's `tx.commit()` to buffer the write, then sends a sync
    /// request to the group committer and blocks until it completes.
    /// Since this is always called inside `spawn_blocking`, blocking is safe.
    fn commit(
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
struct IdStream<'a>(MergeSource<'a>);

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
enum JobStream<'a, R: Readable> {
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
    fn full_scan(
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
    fn by_id(
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
struct PayloadFilteredIter<I> {
    inner: I,
    filter: std::sync::Arc<PayloadFilter>,
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
fn build_id_stream<'a>(
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

        Ok(Self {
            config: config.clone(),
            ks: Arc::new(Keyspaces {
                db,
                data,
                index,
                group_committer,
                default_commit_mode,
                enqueue_commit_mode,
            }),
            ready_index: Arc::new(ReadyIndex::new()),
            scheduled_index: Arc::new(ScheduledIndex::new()),
            cron_index: Arc::new(CronScheduleIndex::new()),
            default_completed_retention_ms: config.default_completed_retention_ms,
            default_dead_retention_ms: config.default_dead_retention_ms,
            default_retry_limit: config.default_retry_limit,
            default_backoff: config.default_backoff,
            index_ready: Arc::new(AtomicBool::new(true)),
            in_flight_count: Arc::new(AtomicU64::new(0)),
            event_tx,
        })
    }

    /// Enqueue a new job.
    ///
    /// Generates a unique job ID and inserts the job into the `jobs`
    /// keyspace plus the appropriate indexes. If `ready_at` is in the
    /// future the job enters the `Scheduled` state and is indexed in
    /// the in-memory scheduled index; otherwise it goes straight to `Ready`
    /// and into the priority indexes.
    ///
    /// Subscribers are notified when a job enters the ready state.
    pub async fn enqueue(
        &self,
        now: u64,
        opts: EnqueueOptions,
    ) -> Result<EnqueueResult, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();

        task::spawn_blocking(move || -> Result<_, StoreError> {
            let prepared = prepare_enqueue(opts, now)?;

            let mut tx = ks.write_tx();
            let result = apply_enqueue(&mut tx, &ks, &prepared)?;

            if result.is_duplicate() {
                drop(tx);
            } else {
                ks.commit(tx, ks.enqueue_commit_mode)?;
            }

            finalize_enqueue(&result, &ready_index, &scheduled_index, &event_tx);

            Ok(result)
        })
        .await?
    }

    /// Enqueue multiple jobs in a single transaction.
    ///
    /// All jobs are serialized and inserted under one write transaction with
    /// a single commit. In-memory indexes are updated after commit succeeds.
    /// Events are broadcast after the sync completes.
    pub async fn enqueue_bulk(
        &self,
        now: u64,
        batch: Vec<EnqueueOptions>,
    ) -> Result<Vec<EnqueueResult>, StoreError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();

        task::spawn_blocking(move || -> Result<_, StoreError> {
            use std::collections::HashMap;

            let prepared: Vec<PreparedEnqueue> = batch
                .into_iter()
                .map(|opts| prepare_enqueue(opts, now))
                .collect::<Result<_, StoreError>>()?;

            let mut tx = ks.write_tx();

            // Maps unique_key -> index in `results` for intra-batch conflicts.
            let mut batch_unique_keys: HashMap<String, usize> = HashMap::new();
            let mut results: Vec<EnqueueResult> = Vec::with_capacity(prepared.len());

            for p in &prepared {
                // Intra-batch unique conflict check (no DB read needed).
                if let Some(ref uc) = p.job.unique {
                    if let Some(&existing_idx) = batch_unique_keys.get(&uc.key) {
                        results.push(EnqueueResult::Duplicate(
                            results[existing_idx].job().clone(),
                        ));
                        continue;
                    }
                }

                let result = apply_enqueue(&mut tx, &ks, p)?;

                // Track unique keys for intra-batch dedup.
                if let EnqueueResult::Created(ref job) = result {
                    if let Some(ref uc) = job.unique {
                        batch_unique_keys.insert(uc.key.clone(), results.len());
                    }
                }

                results.push(result);
            }

            ks.commit(tx, ks.enqueue_commit_mode)?;

            for r in &results {
                finalize_enqueue(r, &ready_index, &scheduled_index, &event_tx);
            }

            Ok(results)
        })
        .await?
    }

    /// Take the next job from the priority index.
    ///
    /// Atomically claims the highest-priority (lowest number), oldest job
    /// from the lock-free ready index and marks it as in-flight. Returns `None`
    /// if no ready jobs match.
    ///
    /// When `queues` is non-empty, only jobs in the specified queues are
    /// considered. When empty, all ready jobs are candidates.
    ///
    /// The `claim()` call removes the entry from the skip-list index before
    /// we even start the fjall transaction — this prevents duplicates without
    /// needing a mutex around peek+remove. If the fjall commit fails, we
    /// re-insert into the index to restore consistency.
    ///
    /// Subscribers are not notified.
    pub async fn take_next_job(
        &self,
        now: u64,
        queues: &HashSet<String>,
    ) -> Result<Option<Job>, StoreError> {
        Ok(self
            .take_next_n_jobs(now, queues, 1)
            .await?
            .into_iter()
            .next())
    }

    /// Take up to `n` ready jobs from the queue in a single batch.
    ///
    /// Claims up to `n` candidates from the ready index, validates them,
    /// writes all status transitions in one transaction, and hydrates
    /// payloads. Returns a `Vec<Job>` that may be shorter than `n` if
    /// the queue had fewer ready jobs.
    pub async fn take_next_n_jobs(
        &self,
        now: u64,
        queues: &HashSet<String>,
        n: usize,
    ) -> Result<Vec<Job>, StoreError> {
        if n == 0 || !self.index_ready.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let event_tx = self.event_tx.clone();
        let queues = queues.clone();

        let spawn_start = std::time::Instant::now();
        let jobs = task::spawn_blocking(move || -> Result<_, StoreError> {
            let blocking_start = std::time::Instant::now();
            let spawn_wait = blocking_start.duration_since(spawn_start);

            // PreRead is declared outside the loop since struct definitions
            // aren't allowed inside loop bodies in all editions.
            struct PreRead {
                job: Job,
                pre_bytes: Slice,
                job_key: Vec<u8>,
                old_status_key: Vec<u8>,
                new_status_key: Vec<u8>,
                updated_slice: Slice,
                job_id: String,
            }

            // Retry loop: on CAS mismatch we re-insert into the ready
            // index and re-claim, same pattern as take_next_job.
            loop {
                // 1. Claim phase — collect up to `n` candidates from the ready index.
                let mut claimed: Vec<(u16, String, String)> = Vec::with_capacity(n);
                while claimed.len() < n {
                    match ready_index.claim(&queues) {
                        Some(entry) => claimed.push(entry),
                        None => break,
                    }
                }

                if claimed.is_empty() {
                    tracing::trace!(
                        spawn_wait_ms = spawn_wait.as_secs_f64() * 1000.0,
                        "take_next_n_jobs: empty"
                    );
                    return Ok(Vec::new());
                }

                // 2. Pre-read phase — read each claimed job from the data keyspace.
                //    Filter out any that are deleted or no longer Ready.
                let mut valid: Vec<PreRead> = Vec::with_capacity(claimed.len());

                for (_priority, job_id, _queue) in &claimed {
                    let job_key = make_job_key(job_id);
                    let pre_bytes = match ks.data.get(&job_key)? {
                        Some(bytes) => bytes,
                        None => continue, // Deleted.
                    };

                    let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;
                    if job.status != JobStatus::Ready as u8 {
                        continue; // No longer ready.
                    }

                    let old_status_key = make_status_key(JobStatus::Ready, &job.id);
                    let new_status_key = make_status_key(JobStatus::InFlight, &job.id);

                    job.status = JobStatus::InFlight.into();
                    job.dequeued_at = Some(now);

                    let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                    valid.push(PreRead {
                        job,
                        pre_bytes,
                        job_key,
                        old_status_key,
                        new_status_key,
                        updated_slice,
                        job_id: job_id.clone(),
                    });
                }

                if valid.is_empty() {
                    tracing::trace!(
                        spawn_wait_ms = spawn_wait.as_secs_f64() * 1000.0,
                        claimed = claimed.len(),
                        "take_next_n_jobs: all claimed candidates invalid"
                    );
                    return Ok(Vec::new());
                }

                // 3. Write phase — one transaction for all valid pre-reads.
                //    If any CAS fails, abort the entire transaction since
                //    fetch_update already wrote into the tx buffer.
                let mut tx = ks.write_tx();
                let mut cas_conflict_detected = false;

                for pre in &valid {
                    let prev = tx.fetch_update(&ks.data, &pre.job_key, |_| {
                        Some(pre.updated_slice.clone())
                    })?;

                    if prev.as_deref() != Some(&*pre.pre_bytes) {
                        cas_conflict_detected = true;
                        break;
                    }

                    tx.remove(&ks.index, &pre.old_status_key);
                    tx.insert(&ks.index, &pre.new_status_key, b"");

                    // If unique_while == Queued, remove the unique index when
                    // the job transitions out of the queue (Ready -> InFlight),
                    // but only if it still belongs to this job.
                    if let Some(ref uc) = pre.job.unique {
                        if uc.unique_while() == UniqueWhile::Queued {
                            let job_id = pre.job_id.as_bytes();
                            tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                                Some(v) if v.as_ref() == job_id => None,
                                other => other.cloned(),
                            })?;
                        }
                    }
                }

                if cas_conflict_detected {
                    // CAS mismatch — abort the whole batch, re-insert
                    // valid entries into the ready index, and retry.
                    tracing::trace!(
                        valid = valid.len(),
                        "take_next_n_jobs: CAS conflict, retrying"
                    );
                    drop(tx);
                    for pre in &valid {
                        ready_index.insert(&pre.job.queue, pre.job.priority, pre.job_id.clone());
                    }
                    continue; // Back to the top of the retry loop
                }

                let write_tx_elapsed = blocking_start.elapsed();

                if let Err(e) = ks.commit(tx, ks.default_commit_mode) {
                    // 4. Rollback — re-insert valid entries into the
                    //    ready index and notify workers.
                    let _tx = ks.write_tx();
                    for pre in &valid {
                        ready_index.insert(&pre.job.queue, pre.job.priority, pre.job_id.clone());

                        let _ = event_tx.send(StoreEvent::JobCreated {
                            id: pre.job_id.clone(),
                            queue: pre.job.queue.clone(),
                            token: Arc::new(AtomicBool::new(false)),
                        });
                    }
                    return Err(e);
                }

                // 5. Hydrate phase — read payloads for all committed jobs.
                let hydrate_start = std::time::Instant::now();
                for pre in &mut valid {
                    let payload_key = make_payload_key(&pre.job_id);
                    if let Some(payload_bytes) = ks.data.get(&payload_key)? {
                        pre.job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
                    }
                }
                let hydrate_elapsed = hydrate_start.elapsed();

                let count = valid.len();
                let jobs: Vec<Job> = valid.into_iter().map(|pre| pre.job).collect();

                tracing::trace!(
                    spawn_wait_ms = spawn_wait.as_secs_f64() * 1000.0,
                    write_tx_ms = write_tx_elapsed.as_secs_f64() * 1000.0,
                    hydrate_ms = hydrate_elapsed.as_secs_f64() * 1000.0,
                    total_ms = blocking_start.elapsed().as_secs_f64() * 1000.0,
                    count,
                    "take_next_n_jobs"
                );

                return Ok(jobs);
            }
        })
        .await??;

        // Update the in-flight counter and broadcast events.
        if !jobs.is_empty() {
            self.in_flight_count
                .fetch_add(jobs.len() as u64, Ordering::Relaxed);
        }
        for job in &jobs {
            let _ = self
                .event_tx
                .send(StoreEvent::JobInFlight { id: job.id.clone() });
        }

        Ok(jobs)
    }

    /// Mark a job as successfully completed.
    ///
    /// Transitions the job to Completed status and sets `purge_at` so the
    /// reaper will hard-delete it after the retention period. Returns `true`
    /// if the job was found in the in-flight state, `false` if it was not.
    ///
    /// Subscribers are notified on success.
    pub async fn mark_completed(&self, now: u64, id: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let default_completed_retention_ms = self.default_completed_retention_ms;
        let id = id.to_string();
        let id_for_event = id.clone();

        let completed = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Retry loop: pre-read and pre-compute changes outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(false),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok(false);
                }

                let retention_ms = job
                    .retention
                    .as_ref()
                    .and_then(|r| r.completed_ms)
                    .unwrap_or(default_completed_retention_ms);

                if retention_ms == 0 {
                    // Zero retention: prepare deletion keys outside tx.
                    let del = prepare_job_deletion(&job, JobStatus::InFlight, &ks);

                    // ---- inside tx ----
                    let mut tx = ks.write_tx();
                    let prev = tx.take(&ks.data, &job_key)?;
                    if prev.as_deref() != Some(&*pre_bytes) {
                        drop(tx);
                        continue;
                    }
                    apply_job_deletion(&mut tx, &del, &ks);
                    ks.commit(tx, ks.default_commit_mode)?;

                    ready_index.remove(&job.queue, job.priority, &id);

                    return Ok(true);
                }

                // Non-zero retention: defer deletion to the reaper.
                let purge_at = now + retention_ms;

                let old_status_key = make_status_key(JobStatus::InFlight, &id);
                let new_status_key = make_status_key(JobStatus::Completed, &id);
                let purge_key = make_purge_key(purge_at, &id);

                job.status = JobStatus::Completed.into();
                job.purge_at = Some(purge_at);
                job.completed_at = Some(now);

                let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                // ---- inside tx ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");
                tx.insert(&ks.index, &purge_key, b"");

                // Remove unique index for Queued or Active scope on completion,
                // but only if it still belongs to this job.
                if let Some(ref uc) = job.unique {
                    let scope = uc.unique_while();
                    if scope == UniqueWhile::Queued || scope == UniqueWhile::Active {
                        let job_id = id.as_bytes();
                        tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                            Some(v) if v.as_ref() == job_id => None,
                            other => other.cloned(),
                        })?;
                    }
                }

                ks.commit(tx, ks.default_commit_mode)?;

                ready_index.remove(&job.queue, job.priority, &id);

                return Ok(true);
            }
        })
        .await??;

        if completed {
            let _ = self
                .in_flight_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
            let _ = self
                .event_tx
                .send(StoreEvent::JobCompleted { id: id_for_event });
        }

        Ok(completed)
    }

    /// Mark multiple jobs as successfully completed in a single transaction.
    ///
    /// Jobs not found in the in-flight set are collected in `not_found` but
    /// do not prevent valid jobs from being committed. Returns a
    /// `BulkCompleteResult` with both lists.
    pub async fn mark_completed_bulk(
        &self,
        now: u64,
        ids: &[String],
    ) -> Result<BulkCompleteResult, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let default_completed_retention_ms = self.default_completed_retention_ms;
        let mut seen = HashSet::with_capacity(ids.len());
        let ids: Vec<String> = ids.iter().filter(|id| seen.insert(*id)).cloned().collect();
        let event_tx = self.event_tx.clone();

        let result =
            task::spawn_blocking(move || -> Result<_, StoreError> {
                /// A job that has been pre-read and prepared for completion.
                struct Prepared {
                    id: String,
                    queue: String,
                    pre_bytes: Slice,
                    priority: u16,
                    /// `None` for zero-retention (will be deleted).
                    updated_bytes: Option<Slice>,
                    /// `Some` for zero-retention (will be deleted).
                    deletion: Option<JobDeletion>,
                    /// Index key updates for non-zero retention jobs.
                    index_keys: Option<RetentionKeys>,
                    /// Unique constraint for this job (if any).
                    unique: Option<UniqueConstraint>,
                }

                /// Pre-computed index keys for a non-zero-retention completion.
                struct RetentionKeys {
                    old_status: Vec<u8>,
                    new_status: Vec<u8>,
                    purge: Vec<u8>,
                }

                let mut not_found: Vec<String> = Vec::new();

                loop {
                    // ---- outside tx: pre-read and prepare ----
                    let mut prepared: Vec<Prepared> = Vec::new();

                    for id in &ids {
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
                            let del = prepare_job_deletion(&job, JobStatus::InFlight, &ks);
                            prepared.push(Prepared {
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

                            prepared.push(Prepared {
                                id: id.clone(),
                                queue,
                                pre_bytes,
                                priority: job.priority,
                                updated_bytes: Some(updated_slice),
                                deletion: None,
                                index_keys: Some(RetentionKeys {
                                    old_status: old_status_key,
                                    new_status: new_status_key,
                                    purge: purge_key,
                                }),
                                unique,
                            });
                        }
                    }

                    if prepared.is_empty() {
                        return Ok(BulkCompleteResult {
                            completed: Vec::new(),
                            not_found,
                        });
                    }

                    // ---- inside tx ----
                    let mut tx = ks.write_tx();
                    let mut cas_conflict_detected = false;

                    for p in &prepared {
                        let job_key = make_job_key(&p.id);

                        if let Some(ref del) = p.deletion {
                            // Zero-retention: delete via CAS.
                            let prev = tx.take(&ks.data, &job_key)?;
                            if prev.as_deref() != Some(&*p.pre_bytes) {
                                cas_conflict_detected = true;
                                break;
                            }
                            apply_job_deletion(&mut tx, del, &ks);
                        } else if let Some(ref updated) = p.updated_bytes {
                            // Non-zero retention: update via CAS.
                            let prev =
                                tx.fetch_update(&ks.data, &job_key, |_| Some(updated.clone()))?;
                            if prev.as_deref() != Some(&*p.pre_bytes) {
                                cas_conflict_detected = true;
                                break;
                            }
                            let keys = p.index_keys.as_ref().unwrap();
                            tx.remove(&ks.index, &keys.old_status);
                            tx.insert(&ks.index, &keys.new_status, b"");
                            tx.insert(&ks.index, &keys.purge, b"");

                            // Remove unique index for Queued or Active scope,
                            // but only if it still belongs to this job.
                            if let Some(ref uc) = p.unique {
                                let scope = uc.unique_while();
                                if scope == UniqueWhile::Queued || scope == UniqueWhile::Active {
                                    let job_id = p.id.as_bytes();
                                    tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| {
                                        match v {
                                            Some(v) if v.as_ref() == job_id => None,
                                            other => other.cloned(),
                                        }
                                    })?;
                                }
                            }
                        }
                    }

                    if cas_conflict_detected {
                        drop(tx);
                        not_found.clear();
                        continue; // Retry from pre-read.
                    }

                    ks.commit(tx, ks.default_commit_mode)?;

                    // Update ready_index for all completed jobs.
                    for p in &prepared {
                        ready_index.remove(&p.queue, p.priority, &p.id);
                    }

                    let completed: Vec<String> = prepared.into_iter().map(|p| p.id).collect();

                    return Ok(BulkCompleteResult {
                        completed,
                        not_found,
                    });
                }
            })
            .await?;

        let bulk_result = result?;

        if !bulk_result.completed.is_empty() {
            let _ = self
                .in_flight_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(bulk_result.completed.len() as u64))
                });
        }

        // Broadcast JobCompleted for each completed job.
        for id in &bulk_result.completed {
            let _ = event_tx.send(StoreEvent::JobCompleted { id: id.clone() });
        }

        Ok(bulk_result)
    }

    /// Record a job failure and either schedule a retry or kill the job
    /// depending on the backoff policy.
    ///
    /// Returns `Some(job)` with metadata-only fields if the job was found in
    /// the InFlight state. The returned job's status will be either `Scheduled`
    /// in the retry scenario or `Dead` if the retry limit has been reached.
    /// Returns `None` if the job wasn't found or wasn't in the InFlight state.
    ///
    /// Dead jobs are transitioned to Dead status with a `purge_at` timestamp
    /// so the reaper can hard-delete them after the retention period.
    pub async fn record_failure(
        &self,
        now: u64,
        id: &str,
        opts: FailureOptions,
    ) -> Result<Option<Job>, StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();
        let ready_index = self.ready_index.clone();
        let default_dead_retention_ms = self.default_dead_retention_ms;
        let default_retry_limit = self.default_retry_limit;
        let default_backoff = self.default_backoff.clone();

        let id = id.to_string();
        let id_for_event = id.clone();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Extract fields from opts before the retry loop so they can
            // be cloned on each iteration.
            let kill = opts.kill;
            let retry_at = opts.retry_at;
            let error_message = opts.message;
            let error_type_opt = opts.error_type;
            let error_backtrace = opts.backtrace;

            // Retry loop: pre-read and pre-compute updates outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            let job = loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok(None);
                }

                job.attempts += 1;
                job.failed_at = Some(now);

                let error_key = make_error_key(&id, job.attempts);

                let error_record = ErrorRecord {
                    message: error_message.clone(),
                    error_type: error_type_opt.clone(),
                    backtrace: error_backtrace.clone(),
                    dequeued_at: job.dequeued_at.unwrap_or(0),
                    failed_at: now,
                    attempt: job.attempts,
                };

                let error_bytes = rmp_serde::to_vec_named(&error_record)?;

                let retry_limit = job.retry_limit.unwrap_or(default_retry_limit);

                if kill {
                    job.status = JobStatus::Dead.into();
                } else if let Some(ra) = retry_at {
                    job.status = JobStatus::Scheduled.into();
                    job.ready_at = ra;
                } else if job.attempts > retry_limit {
                    job.status = JobStatus::Dead.into();
                } else {
                    let backoff = job.backoff.as_ref().unwrap_or(&default_backoff);
                    let delay_ms = compute_backoff(job.attempts, backoff);
                    job.status = JobStatus::Scheduled.into();
                    job.ready_at = now + delay_ms;
                };

                let old_status_key = make_status_key(JobStatus::InFlight, &id);

                if job.status == JobStatus::Scheduled as u8 {
                    let new_status_key = make_status_key(JobStatus::Scheduled, &id);
                    let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                    // ---- inside tx (writes only, plus one compare) ----
                    let mut tx = ks.write_tx();

                    let prev =
                        tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                    if prev.as_deref() != Some(&*pre_bytes) {
                        drop(tx);
                        continue;
                    }

                    tx.insert(&ks.data, &error_key, &error_bytes);
                    tx.remove(&ks.index, &old_status_key);
                    tx.insert(&ks.index, &new_status_key, b"");

                    // If unique_while == Queued, restore the unique index now
                    // that the job is re-entering the queue. Only insert if no
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

                    ks.commit(tx, ks.default_commit_mode)?;

                    scheduled_index.insert(job.ready_at, id.clone());

                    break job;
                }

                // Dead: check retention to decide sync delete vs deferred.
                let retention_ms = job
                    .retention
                    .as_ref()
                    .and_then(|r| r.dead_ms)
                    .unwrap_or(default_dead_retention_ms);

                if retention_ms == 0 {
                    // Zero retention: prepare deletion keys outside tx.
                    let del = prepare_job_deletion(&job, JobStatus::InFlight, &ks);

                    // ---- inside tx ----
                    let mut tx = ks.write_tx();
                    let prev = tx.take(&ks.data, &job_key)?;
                    if prev.as_deref() != Some(&*pre_bytes) {
                        drop(tx);
                        continue;
                    }
                    apply_job_deletion(&mut tx, &del, &ks);
                    job.status = JobStatus::Dead.into();
                    ks.commit(tx, ks.default_commit_mode)?;

                    ready_index.remove(&job.queue, job.priority, &id);

                    break job;
                }

                // Non-zero retention: defer deletion to the reaper.
                let purge_at = now + retention_ms;
                let new_status_key = make_status_key(JobStatus::Dead, &id);
                let purge_key = make_purge_key(purge_at, &id);

                job.purge_at = Some(purge_at);
                let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                // ---- inside tx ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.insert(&ks.data, &error_key, &error_bytes);
                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");
                tx.insert(&ks.index, &purge_key, b"");

                // Remove unique index for Queued or Active scope on death,
                // but only if it still belongs to this job.
                if let Some(ref uc) = job.unique {
                    let scope = uc.unique_while();
                    if scope == UniqueWhile::Queued || scope == UniqueWhile::Active {
                        let job_id = id.as_bytes();
                        tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                            Some(v) if v.as_ref() == job_id => None,
                            other => other.cloned(),
                        })?;
                    }
                }

                ks.commit(tx, ks.default_commit_mode)?;

                ready_index.remove(&job.queue, job.priority, &id);

                break job;
            };

            Ok(Some(job))
        })
        .await??;

        // Emit events outside spawn_blocking due to &self.event_tx.
        if let Some(ref job) = result {
            let _ = self
                .in_flight_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });

            // Always emit JobFailed first so workers prune their
            // in-flight set before any downstream events arrive.
            //
            // The attempts count must match what the worker stored when
            // it took the job — that's the pre-increment value, since
            // record_failure bumps attempts by 1 inside the closure.
            let _ = self.event_tx.send(StoreEvent::JobFailed {
                id: id_for_event,
                attempts: job.attempts - 1,
            });

            if job.status == JobStatus::Scheduled as u8 {
                // Rescheduled for retry — tell the scheduler to wake up
                // at the new ready_at.
                let _ = self.event_tx.send(StoreEvent::JobScheduled {
                    id: job.id.clone(),
                    ready_at: job.ready_at,
                });
            }
        }

        Ok(result)
    }

    /// Look up a job by ID.
    ///
    /// Returns `None` if the job does not exist, or if its `purge_at`
    /// timestamp has passed (logically invisible before the reaper
    /// physically deletes it).
    pub async fn get_job(&self, now: u64, id: &str) -> Result<Option<Job>, StoreError> {
        let ks = self.ks.clone();
        let id = id.to_string();

        task::spawn_blocking(move || {
            let job_key = make_job_key(&id);
            let Some(bytes) = ks.data.get(&job_key)? else {
                return Ok(None);
            };
            let mut job: Job = rmp_serde::from_slice(&bytes)?;

            // Filter out expired jobs (logically invisible).
            if job.purge_at.is_some_and(|p| p <= now) {
                return Ok(None);
            }

            // Hydrate the payload from the data keyspace.
            let payload_key = make_payload_key(&id);
            if let Some(payload_bytes) = ks.data.get(&payload_key)? {
                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
            }

            Ok(Some(job))
        })
        .await?
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

    /// Atomically move a in-flight job back to the priority queue.
    ///
    /// Looks up the job from the `jobs` keyspace to reconstruct its
    /// prioritised queue keys.
    ///
    /// Returns `true` if the job was actually requeued, `false` if it was
    /// already acked (no longer in the in-flight state).
    ///
    /// Subscribers are notified.
    pub async fn requeue(&self, id: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let id = id.to_string();
        let event_id = id.clone();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Retry loop: pre-read and pre-compute updates outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok(None);
                }

                let old_status_key = make_status_key(JobStatus::InFlight, &id);
                let new_status_key = make_status_key(JobStatus::Ready, &id);

                let queue = job.queue.clone();
                let priority = job.priority;
                job.status = JobStatus::Ready.into();

                let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                // ---- inside tx (writes only, plus one compare) ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");

                ks.commit(tx, ks.default_commit_mode)?;

                // Insert into the in-memory ready index after commit succeeds.
                ready_index.insert(&queue, priority, id.clone());

                return Ok(Some(queue));
            }
        })
        .await??;

        if let Some(queue) = result {
            let _ = self
                .in_flight_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
            let _ = self.event_tx.send(StoreEvent::JobCreated {
                id: event_id,
                queue,
                token: Arc::new(AtomicBool::new(false)),
            });
            return Ok(true);
        }

        Ok(false)
    }

    /// List jobs with cursor-based pagination.
    ///
    /// Returns a page of jobs along with options for fetching the next and
    /// previous pages. Internally over-selects by one to determine whether
    /// more results exist in the scan direction.
    ///
    /// # Examples
    ///
    /// Fetch the first page with default options:
    ///
    /// ```rust
    /// let page = store.list_jobs(ListJobsOptions::new()).await?;
    /// for job in &page.jobs {
    ///     println!("{}: {}", job.id, job.queue);
    /// }
    /// ```
    ///
    /// Paginate through all jobs:
    ///
    /// ```rust
    /// let mut opts = ListJobsOptions::new().limit(100);
    /// loop {
    ///     let page = store.list_jobs(opts).await?;
    ///     for job in &page.jobs {
    ///         process(job);
    ///     }
    ///     match page.next {
    ///         Some(next) => opts = next,
    ///         None => break,
    ///     }
    /// }
    /// ```
    ///
    /// Fetch the most recent 10 jobs:
    ///
    /// ```rust
    /// let page = store
    ///     .list_jobs(ListJobsOptions::new().direction(ScanDirection::Desc).limit(10))
    ///     .await?;
    /// ```
    pub async fn list_jobs(&self, opts: ListJobsOptions) -> Result<ListJobsPage, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();
            let fetch = opts.limit.saturating_add(1);

            let id_stream_opt = build_id_stream(
                &snapshot,
                &ks,
                &opts.ids,
                &opts.statuses,
                &opts.queues,
                &opts.types,
                &opts.from,
                opts.direction,
            );

            let job_stream = match id_stream_opt {
                Some((ids, source, _has_id_filter)) => {
                    JobStream::by_id(ids, &snapshot, &ks.data, opts.now, true, source)
                }
                None => {
                    JobStream::full_scan(&snapshot, &ks, &opts.from, opts.direction, opts.now, true)
                }
            };

            let mut rows: Vec<Job> = if let Some(filter) = opts.filter.clone() {
                PayloadFilteredIter {
                    inner: job_stream,
                    filter,
                }
                .take(fetch)
                .collect::<Result<Vec<_>, _>>()?
            } else {
                job_stream.take(fetch).collect::<Result<Vec<_>, _>>()?
            };

            // If we got more rows than the requested limit, we know there's a
            // next page, and we can just discard that extra row now.
            let has_more = rows.len() > opts.limit;
            if has_more {
                rows.truncate(opts.limit);
            }

            // Build next/prev options, preserving all filters.
            let make_opts = |cursor: &str, direction: ScanDirection| {
                ListJobsOptions::new()
                    .from(cursor.to_string())
                    .direction(direction)
                    .limit(opts.limit)
                    .ids(opts.ids.clone())
                    .statuses(opts.statuses.clone())
                    .queues(opts.queues.clone())
                    .types(opts.types.clone())
                    .now(opts.now)
                    .filter(opts.filter.clone())
            };

            // Next page: exists if over-select found an extra row.
            let next = if has_more {
                let last_id = &rows.last().unwrap().id;
                Some(make_opts(last_id, opts.direction))
            } else {
                None
            };

            // Previous page: exists if a cursor was provided (we came from
            // somewhere).
            let prev = if opts.from.is_some() && !rows.is_empty() {
                let first_id = &rows.first().unwrap().id;
                Some(make_opts(first_id, opts.direction.reverse()))
            } else {
                None
            };

            Ok(ListJobsPage {
                jobs: rows,
                next,
                prev,
            })
        })
        .await?
    }

    /// Count jobs matching the given filters.
    ///
    /// Uses the same index scan and filter machinery as `list_jobs` but
    /// only counts matching IDs instead of hydrating full job records.
    /// Payload hydration is skipped unless a payload filter is specified.
    pub async fn count_jobs(&self, opts: ListJobsOptions) -> Result<usize, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            let snapshot = ks.db.read_tx();

            let id_stream_opt = build_id_stream(
                &snapshot,
                &ks,
                &opts.ids,
                &opts.statuses,
                &opts.queues,
                &opts.types,
                &None,              // no cursor for count
                ScanDirection::Asc, // direction doesn't matter for count
            );

            let needs_payload = opts.filter.is_some();

            let mut job_stream = match id_stream_opt {
                Some((ids, source, _has_id_filter)) => {
                    JobStream::by_id(ids, &snapshot, &ks.data, opts.now, needs_payload, source)
                }
                None => JobStream::full_scan(
                    &snapshot,
                    &ks,
                    &None,
                    ScanDirection::Asc,
                    opts.now,
                    needs_payload,
                ),
            };

            let count = if let Some(filter) = opts.filter.clone() {
                PayloadFilteredIter {
                    inner: job_stream,
                    filter,
                }
                .try_fold(0, |acc, r| r.map(|_| acc + 1))?
            } else {
                job_stream.try_fold(0, |acc, r| r.map(|_| acc + 1))?
            };

            Ok(count)
        })
        .await?
    }

    /// Return all distinct queue names present in the store.
    ///
    /// Uses a seek-skip algorithm on the on-disk queue index to enumerate
    /// queue names in O(distinct queues), not O(total jobs). Each step is
    /// a single LSM tree seek via `range()`.
    pub async fn list_queues(&self) -> Result<Vec<String>, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();

            let end: Vec<u8> = vec![IndexKind::Queue as u8 + 1, 0];
            let mut start: Vec<u8> = vec![IndexKind::Queue as u8, 0];
            let mut queues = Vec::new();

            loop {
                let mut range = snapshot.range::<Vec<u8>, _>(
                    ks.index.as_ref(),
                    (Bound::Included(start.clone()), Bound::Excluded(end.clone())),
                );

                let entry = match range.next() {
                    Some(entry) => entry,
                    None => break,
                };

                let (key, _) = entry.into_inner()?;

                // Key layout: Q\0{queue_name}\0{job_id} — extract queue_name.
                let name_start = 2; // skip Q\0
                let name_end = key[name_start..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|p| name_start + p)
                    .unwrap_or(key.len());
                let queue_name = std::str::from_utf8(&key[name_start..name_end])
                    .map_err(|e| {
                        StoreError::Corruption(format!("queue name is not valid UTF-8: {e}"))
                    })?
                    .to_string();
                queues.push(queue_name.clone());

                // Advance past all entries for this queue: Q\0{queue_name}\x01
                start.truncate(2);
                start.extend_from_slice(queue_name.as_bytes());
                start.push(1); // one byte past \0 separator
            }

            Ok(queues)
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

    /// List ready jobs in priority order from the in-memory index.
    ///
    /// Iterates the `ReadyIndex` SkipMap and hydrates job metadata from
    /// the `jobs` keyspace. Skips jobs whose metadata is missing (claimed
    /// between index scan and disk read — next snapshot corrects). Does
    /// NOT hydrate payloads.
    pub async fn list_ready_jobs(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let ready_index = self.ready_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let mut jobs = Vec::with_capacity(limit);
            for (_priority, job_id) in ready_index.iter().skip(offset) {
                if jobs.len() >= limit {
                    break;
                }
                let job_key = make_job_key(&job_id);
                if let Some(bytes) = ks.data.get(&job_key)? {
                    let job: Job = rmp_serde::from_slice(&bytes)?;
                    jobs.push(job);
                }
            }
            Ok(jobs)
        })
        .await?
    }

    /// Scan the in-memory ReadyIndex and return up to `limit` priority keys.
    ///
    /// Returns `Vec<(priority, job_id)>` in priority order — zero disk I/O.
    /// Used by the admin event handler to diff capped ready windows.
    pub async fn scan_ready_ids(&self, offset: usize, limit: usize) -> Vec<(u16, String)> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Vec::new();
        }

        let ready_index = self.ready_index.clone();

        task::spawn_blocking(move || ready_index.iter().skip(offset).take(limit).collect())
            .await
            .unwrap_or_default()
    }

    /// List scheduled jobs in chronological order from the in-memory index.
    ///
    /// Iterates the `ScheduledIndex` SkipSet and hydrates job metadata from
    /// the `jobs` keyspace. Skips jobs whose metadata is missing. Does NOT
    /// hydrate payloads.
    pub async fn list_scheduled_jobs(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let scheduled_index = self.scheduled_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let mut jobs = Vec::with_capacity(limit);
            for (_ready_at, job_id) in scheduled_index.iter().skip(offset) {
                if jobs.len() >= limit {
                    break;
                }
                let job_key = make_job_key(&job_id);
                if let Some(bytes) = ks.data.get(&job_key)? {
                    let job: Job = rmp_serde::from_slice(&bytes)?;
                    jobs.push(job);
                }
            }
            Ok(jobs)
        })
        .await?
    }

    /// Scan the in-memory ScheduledIndex and return up to `limit` keys.
    ///
    /// Returns `Vec<(ready_at, job_id)>` in chronological order — zero disk I/O.
    /// Used by the admin event handler to diff capped scheduled windows.
    pub async fn scan_scheduled_ids(&self, offset: usize, limit: usize) -> Vec<(u64, String)> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Vec::new();
        }

        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || scheduled_index.iter().skip(offset).take(limit).collect())
            .await
            .unwrap_or_default()
    }

    /// Get a single error record by job ID and attempt number.
    ///
    /// Returns `None` if the job or attempt doesn't exist.
    pub async fn get_error(
        &self,
        job_id: &str,
        attempt: u32,
    ) -> Result<Option<ErrorRecord>, StoreError> {
        let ks = self.ks.clone();
        let job_id = job_id.to_string();

        task::spawn_blocking(move || {
            let key = make_error_key(&job_id, attempt);
            match ks.data.get(&key)? {
                Some(bytes) => {
                    let mut record: ErrorRecord = rmp_serde::from_slice(&bytes)?;
                    record.attempt = attempt;
                    Ok(Some(record))
                }
                None => Ok(None),
            }
        })
        .await
        .unwrap()
    }

    /// List error records for a job with cursor-based pagination.
    ///
    /// Returns a page of `ErrorRecord`s plus `next` / `prev` cursors for
    /// follow-up requests. The cursor is the attempt number (u32), which is
    /// the natural sort key within a job's error records.
    pub async fn list_errors(&self, opts: ListErrorsOptions) -> Result<ListErrorsPage, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            // We take 1 more record than requested so that we know if there's
            // a next page or not.
            let fetch = opts.limit.saturating_add(1);

            // Build the prefix for this job: `E\0{job_id}\0`
            let mut prefix = Vec::with_capacity(opts.job_id.len() + 3);
            prefix.push(RecordKind::Error as u8);
            prefix.push(0);
            prefix.extend_from_slice(opts.job_id.as_bytes());
            prefix.push(0);

            // End sentinel: `E\0{job_id}\x01` — one byte past the null separator,
            // so the range covers all `E\0{job_id}\0{...}` keys.
            let mut end_sentinel = Vec::with_capacity(opts.job_id.len() + 3);
            end_sentinel.push(RecordKind::Error as u8);
            end_sentinel.push(0);
            end_sentinel.extend_from_slice(opts.job_id.as_bytes());
            end_sentinel.push(1);

            let start = match opts.from {
                Some(cursor) => Bound::Excluded(make_error_key(&opts.job_id, cursor)),
                None => Bound::Included(prefix),
            };
            let end = Bound::Excluded(end_sentinel);

            let mut rows = Vec::with_capacity(fetch.min(1024));

            match opts.direction {
                ScanDirection::Asc => {
                    for entry in ks
                        .data
                        .inner()
                        .range::<Vec<u8>, _>((start, end))
                        .take(fetch)
                    {
                        let (key, value) = entry.into_inner()?;
                        let mut record: ErrorRecord = rmp_serde::from_slice(&value)?;
                        // Extract the attempt number from the last 4 bytes of the key.
                        let attempt_bytes: [u8; 4] =
                            key[key.len() - 4..].try_into().map_err(|_| {
                                StoreError::Corruption(
                                    "error key must end with 4-byte attempt".into(),
                                )
                            })?;
                        record.attempt = u32::from_be_bytes(attempt_bytes);
                        rows.push(record);
                    }
                }
                ScanDirection::Desc => {
                    for entry in ks
                        .data
                        .inner()
                        .range::<Vec<u8>, _>((start, end))
                        .rev()
                        .take(fetch)
                    {
                        let (key, value) = entry.into_inner()?;
                        let mut record: ErrorRecord = rmp_serde::from_slice(&value)?;
                        let attempt_bytes: [u8; 4] =
                            key[key.len() - 4..].try_into().map_err(|_| {
                                StoreError::Corruption(
                                    "error key must end with 4-byte attempt".into(),
                                )
                            })?;
                        record.attempt = u32::from_be_bytes(attempt_bytes);
                        rows.push(record);
                    }
                }
            }

            // If we got more rows than the requested limit, there's a next page.
            let has_more = rows.len() > opts.limit;
            if has_more {
                rows.truncate(opts.limit);
            }

            // Build next/prev options, preserving direction and limit.
            let make_opts = |cursor: u32, direction: ScanDirection| {
                ListErrorsOptions::new(&opts.job_id)
                    .from(cursor)
                    .direction(direction)
                    .limit(opts.limit)
            };

            // Next page: exists if over-select found an extra row.
            let next = if has_more {
                let last_attempt = rows.last().unwrap().attempt;
                Some(make_opts(last_attempt, opts.direction))
            } else {
                None
            };

            // Previous page: exists if a cursor was provided (we came from
            // somewhere).
            let prev = if opts.from.is_some() && !rows.is_empty() {
                let first_attempt = rows.first().unwrap().attempt;
                Some(make_opts(first_attempt, opts.direction.reverse()))
            } else {
                None
            };

            Ok(ListErrorsPage {
                errors: rows,
                next,
                prev,
            })
        })
        .await?
    }

    /// Return up to `limit` due scheduled jobs, plus the next wake-up time.
    ///
    /// Scans the in-memory `ScheduledIndex` for entries whose
    /// `ready_at <= now` and loads their full job metadata from the `jobs`
    /// keyspace.
    ///
    /// Returns `(due_jobs, next_ready_at)` where `next_ready_at` is the
    /// `ready_at` of the first future job (if any). The scheduler uses
    /// this to know how long to sleep before the next scan.
    pub async fn next_scheduled(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<(Vec<Job>, Option<u64>), StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || -> Result<(Vec<Job>, Option<u64>), StoreError> {
            let (due_entries, next_ready_at) = scheduled_index.next_due(now, limit);

            let mut result = Vec::with_capacity(due_entries.len());
            for (ready_at, job_id) in &due_entries {
                let job_key = make_job_key(job_id);
                let job_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => {
                        // Job was deleted between next_due() and this read.
                        // Clean up the stale scheduled index entry.
                        tracing::trace!(
                            job_id,
                            "scheduled job missing from data keyspace, skipping"
                        );
                        scheduled_index.remove(*ready_at, job_id);
                        continue;
                    }
                };
                result.push(rmp_serde::from_slice(&job_bytes)?);
            }

            Ok((result, next_ready_at))
        })
        .await?
    }

    /// Promote a single scheduled job to the Ready state.
    ///
    /// Atomically removes the job from the scheduled index, swaps its
    /// status from Scheduled to Ready, and inserts it into the priority
    /// indexes so it becomes takeable.
    ///
    /// Subscribers are notified with `StoreEvent::JobCreated`.
    pub async fn promote_scheduled(&self, job: &Job) -> Result<(), StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();

        let id = job.id.clone();
        let event_id = job.id.clone();
        let event_queue = job.queue.clone();
        let queue = job.queue.clone();
        let priority = job.priority;
        let ready_at = job.ready_at;

        task::spawn_blocking(move || -> Result<_, StoreError> {
            // Remove from the in-memory scheduled index *before* commit to
            // prevent data races (same as take_next_job).
            scheduled_index.remove(ready_at, &id);

            // Retry loop: pre-read and pre-compute updates outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(()), // Already gone.
                };

                let current: Job = rmp_serde::from_slice(&pre_bytes)?;

                if current.status != JobStatus::Scheduled as u8 {
                    return Ok(()); // No longer scheduled.
                }

                let old_status_key = make_status_key(JobStatus::Scheduled, &id);
                let new_status_key = make_status_key(JobStatus::Ready, &id);

                let mut updated = current;
                updated.status = JobStatus::Ready.into();

                let updated_slice: Slice = rmp_serde::to_vec_named(&updated)?.into();

                // ---- inside tx (writes only, plus one compare) ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");

                let _sync = match ks.commit(tx, ks.default_commit_mode) {
                    Ok(sync) => sync,
                    Err(e) => {
                        // Commit failed — re-insert into the scheduled index
                        // if the job is still Scheduled.
                        let _tx = ks.write_tx();
                        if let Ok(Some(bytes)) = ks.data.get(&job_key) {
                            if let Ok(current) = rmp_serde::from_slice::<Job>(&bytes) {
                                if current.status == JobStatus::Scheduled as u8 {
                                    scheduled_index.insert(ready_at, id);
                                }
                            }
                        }
                        return Err(e);
                    }
                };

                // Insert into the in-memory ready index after commit succeeds.
                ready_index.insert(&queue, priority, id);

                return Ok(());
            }
        })
        .await??;

        let _ = self.event_tx.send(StoreEvent::JobCreated {
            id: event_id,
            queue: event_queue,
            token: Arc::new(AtomicBool::new(false)),
        });

        Ok(())
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

    /// Fetch a batch of jobs whose purge_at has passed.
    ///
    /// Returns `(batch_of_job_ids, has_more)`. The reaper calls this
    /// repeatedly to drain expired jobs.
    pub async fn next_expired(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<(Vec<String>, bool), StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || -> Result<(Vec<String>, bool), StoreError> {
            // Scan from the start of the purge-at index up to `now` (inclusive).
            // Key layout: A\0{purge_at_be_u64}\0{job_id}
            let start_key: Vec<u8> = vec![IndexKind::PurgeAt as u8, 0];
            let end_key = make_purge_key(now + 1, "");
            let range = (Bound::Included(start_key), Bound::Excluded(end_key));

            let mut ids = Vec::new();
            for entry in ks.index.inner().range::<Vec<u8>, _>(range) {
                let (key, _) = entry.into_inner()?;
                // Extract job_id: skip 2 bytes (tag) + 8 bytes (purge_at) + 1 byte (null separator).
                let job_id = String::from_utf8(key[11..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;
                ids.push(job_id);
                if ids.len() > limit {
                    break;
                }
            }

            let has_more = ids.len() > limit;
            if has_more {
                ids.truncate(limit);
            }
            Ok((ids, has_more))
        })
        .await?
    }

    /// Hard-delete a job and all associated data.
    ///
    /// Called by the reaper for each expired job. Returns `true` if the
    /// job was found and deleted, `false` if already purged.
    pub async fn purge_job(&self, id: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let in_flight_count = self.in_flight_count.clone();
        let id = id.to_string();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // ---- outside tx ----
            let job_key = make_job_key(&id);
            let job_bytes = match ks.data.get(&job_key)? {
                Some(bytes) => bytes,
                None => return Ok(false), // Already purged.
            };

            let job: Job = rmp_serde::from_slice(&job_bytes)?;
            let status = JobStatus::try_from(job.status).map_err(|v| {
                StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
            })?;

            let del = prepare_job_deletion(&job, status, &ks);

            // ---- inside tx (writes only) ----
            let mut tx = ks.write_tx();
            apply_job_deletion(&mut tx, &del, &ks);
            ks.commit(tx, ks.default_commit_mode)?;

            // Remove from in-memory indexes based on the job's status.
            match status {
                JobStatus::Ready => {
                    ready_index.remove(&job.queue, job.priority, &id);
                }
                JobStatus::Scheduled => {
                    scheduled_index.remove(job.ready_at, &id);
                }
                JobStatus::InFlight => {
                    in_flight_count
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(1))
                        })
                        .ok();
                }
                _ => {}
            }

            Ok(true)
        })
        .await??;

        Ok(result)
    }

    /// Delete a job by ID via the API.
    ///
    /// Delegates to `purge_job` for the actual deletion, then emits a
    /// `JobDeleted` event so that workers can release capacity for
    /// in-flight jobs that were deleted externally.
    pub async fn delete_job(&self, id: &str) -> Result<bool, StoreError> {
        let deleted = self.purge_job(id).await?;
        if deleted {
            let _ = self
                .event_tx
                .send(StoreEvent::JobDeleted { id: id.to_string() });
        }
        Ok(deleted)
    }

    /// Bulk-delete jobs matching the given filters.
    ///
    /// Acquires the write lock, scans matching jobs under a consistent
    /// snapshot, deletes each one, and commits in a single transaction.
    /// Returns the count of deleted jobs.
    ///
    /// Emits `StoreEvent::JobDeleted` for each deleted job.
    pub async fn delete_jobs(&self, opts: BulkDeleteOptions) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let in_flight_count = self.in_flight_count.clone();
        let event_tx = self.event_tx.clone();

        let deleted_ids = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Acquire the write lock first, then open a snapshot.
            // The snapshot sees the latest committed state, and since we
            // hold the writer lock, nothing can commit while we scan + delete.
            let mut tx = ks.write_tx();
            let snapshot = ks.db.read_tx();

            let has_payload_filter = opts.filter.is_some();

            // Build the job stream: filtered by indexes, or full scan.
            let jobs: Box<dyn Iterator<Item = Result<Job, StoreError>> + '_> =
                if let Some((id_stream, source, _)) = build_id_stream(
                    &snapshot,
                    &ks,
                    &opts.ids,
                    &opts.statuses,
                    &opts.queues,
                    &opts.types,
                    &None,
                    ScanDirection::Asc,
                ) {
                    let stream = JobStream::by_id(
                        id_stream,
                        &snapshot,
                        &ks.data,
                        None,               // no expiry filtering for deletion
                        has_payload_filter, // only hydrate if filter needs it
                        source,
                    );
                    Box::new(stream) as Box<dyn Iterator<Item = Result<Job, StoreError>> + '_>
                } else {
                    let stream = JobStream::full_scan(
                        &snapshot,
                        &ks,
                        &None,
                        ScanDirection::Asc,
                        None,
                        has_payload_filter,
                    );
                    Box::new(stream) as Box<dyn Iterator<Item = Result<Job, StoreError>> + '_>
                };

            let jobs: Box<dyn Iterator<Item = Result<Job, StoreError>> + '_> =
                if let Some(ref filter) = opts.filter {
                    Box::new(PayloadFilteredIter {
                        inner: jobs,
                        filter: Arc::clone(filter),
                    })
                } else {
                    jobs
                };

            // Track deleted jobs for post-commit cleanup.
            struct Deleted {
                id: String,
                queue: String,
                status: JobStatus,
                priority: u16,
                ready_at: u64,
            }

            let mut deleted: Vec<Deleted> = Vec::new();

            for result in jobs {
                let job = result?;
                let status = JobStatus::try_from(job.status).map_err(|v| {
                    StoreError::Corruption(format!(
                        "job {} has unrecognized status byte: {v}",
                        job.id
                    ))
                })?;

                let del = prepare_job_deletion(&job, status, &ks);
                apply_job_deletion(&mut tx, &del, &ks);

                deleted.push(Deleted {
                    id: job.id,
                    queue: job.queue,
                    status,
                    priority: job.priority,
                    ready_at: job.ready_at,
                });
            }

            if deleted.is_empty() {
                return Ok(Vec::new());
            }

            ks.commit(tx, ks.default_commit_mode)?;

            // Post-commit: clean up in-memory indexes.
            let mut in_flight_removed: u64 = 0;
            for d in &deleted {
                match d.status {
                    JobStatus::Ready => {
                        ready_index.remove(&d.queue, d.priority, &d.id);
                    }
                    JobStatus::Scheduled => {
                        scheduled_index.remove(d.ready_at, &d.id);
                    }
                    JobStatus::InFlight => {
                        in_flight_removed += 1;
                    }
                    _ => {}
                }
            }

            if in_flight_removed > 0 {
                in_flight_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(in_flight_removed))
                    })
                    .ok();
            }

            let deleted_ids: Vec<String> = deleted.into_iter().map(|d| d.id).collect();
            Ok(deleted_ids)
        })
        .await??;

        for id in &deleted_ids {
            let _ = event_tx.send(StoreEvent::JobDeleted { id: id.clone() });
        }

        Ok(deleted_ids.len())
    }

    /// Patch a single job's mutable fields.
    ///
    /// Returns `Ok(Some(job))` with the updated job on success,
    /// `Ok(None)` if the job does not exist, or `Err` on failure.
    ///
    /// Returns `Err(StoreError::InvalidOperation)` if the job is in a
    /// terminal state (Completed or Dead).
    pub async fn patch_job(
        &self,
        now: u64,
        id: &str,
        patch: PatchJobOptions,
    ) -> Result<Option<Job>, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();
        let id = id.to_string();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            let job_key = make_job_key(&id);

            let mut tx = ks.write_tx();

            // Read, patch, and write in a single fetch_update call.
            // Errors are captured in patch_err since the closure can only
            // return Option<Slice>.
            let mut patched_job: Option<Job> = None;
            let mut diff: Option<PatchDiff> = None;
            let mut patch_err: Option<StoreError> = None;

            let prev = tx.fetch_update(&ks.data, &job_key, |existing| {
                let bytes = existing?;

                let mut job: Job = match rmp_serde::from_slice(bytes) {
                    Ok(j) => j,
                    Err(e) => {
                        patch_err = Some(e.into());
                        return Some(bytes.clone());
                    }
                };

                let status = match JobStatus::try_from(job.status) {
                    Ok(s) => s,
                    Err(v) => {
                        patch_err = Some(StoreError::Corruption(format!(
                            "job {} has unrecognized status byte: {v}",
                            job.id
                        )));
                        return Some(bytes.clone());
                    }
                };

                // Reject patches on terminal jobs.
                if matches!(status, JobStatus::Completed | JobStatus::Dead) {
                    patch_err = Some(StoreError::InvalidOperation(format!(
                        "cannot patch job {} in {} status",
                        job.id,
                        match status {
                            JobStatus::Completed => "completed",
                            JobStatus::Dead => "dead",
                            _ => unreachable!(),
                        }
                    )));
                    return Some(bytes.clone());
                }

                diff = Some(patch.apply(&mut job, now));

                match rmp_serde::to_vec_named(&job) {
                    Ok(updated) => {
                        patched_job = Some(job);
                        Some(updated.into())
                    }
                    Err(e) => {
                        patch_err = Some(e.into());
                        Some(bytes.clone())
                    }
                }
            })?;

            // Propagate any error from inside the closure.
            if let Some(e) = patch_err {
                return Err(e);
            }

            // If prev was None, the job doesn't exist.
            if prev.is_none() {
                return Ok(None);
            }

            let job = match patched_job {
                Some(j) => j,
                None => return Ok(None),
            };
            let diff = diff.unwrap();

            diff.update_keyspaces(&mut tx, &ks);

            ks.commit(tx, ks.default_commit_mode)?;

            diff.update_indexes(&ready_index, &scheduled_index);
            diff.emit_events(&event_tx);

            Ok(Some(job))
        })
        .await??;

        Ok(result)
    }

    /// Patch all jobs matching the given filters in a single transaction.
    ///
    /// Scans for matching jobs, applies the patch to each, commits once,
    /// then updates in-memory indexes and emits events. Terminal jobs
    /// (Completed/Dead) are silently skipped.
    ///
    /// Returns the count of actually patched jobs.
    pub async fn patch_jobs(&self, opts: BulkPatchOptions) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();

        let diffs = task::spawn_blocking(move || -> Result<_, StoreError> {
            let mut tx = ks.write_tx();
            let snapshot = ks.db.read_tx();

            let has_payload_filter = opts.filter.is_some();
            let now = crate::time::now_millis();

            let jobs: Box<dyn Iterator<Item = Result<Job, StoreError>> + '_> =
                if let Some((id_stream, source, _)) = build_id_stream(
                    &snapshot,
                    &ks,
                    &opts.ids,
                    &opts.statuses,
                    &opts.queues,
                    &opts.types,
                    &None,
                    ScanDirection::Asc,
                ) {
                    Box::new(JobStream::by_id(
                        id_stream,
                        &snapshot,
                        &ks.data,
                        None,
                        has_payload_filter,
                        source,
                    ))
                } else {
                    Box::new(JobStream::full_scan(
                        &snapshot,
                        &ks,
                        &None,
                        ScanDirection::Asc,
                        None,
                        has_payload_filter,
                    ))
                };

            let jobs: Box<dyn Iterator<Item = Result<Job, StoreError>> + '_> =
                if let Some(ref filter) = opts.filter {
                    Box::new(PayloadFilteredIter {
                        inner: jobs,
                        filter: Arc::clone(filter),
                    })
                } else {
                    jobs
                };

            let mut diffs: Vec<PatchDiff> = Vec::new();

            for result in jobs {
                let mut job = result?;
                let status = match JobStatus::try_from(job.status) {
                    Ok(s) => s,
                    Err(_) => continue, // Skip corrupt jobs.
                };

                // Skip terminal jobs.
                if matches!(status, JobStatus::Completed | JobStatus::Dead) {
                    continue;
                }

                let diff = opts.patch.apply(&mut job, now);
                diff.update_keyspaces(&mut tx, &ks);

                let updated: Slice = rmp_serde::to_vec_named(&job)?.into();
                let job_key = make_job_key(&job.id);
                tx.fetch_update(&ks.data, &job_key, |_| Some(updated.clone()))?;

                diffs.push(diff);
            }

            if diffs.is_empty() {
                return Ok(Vec::new());
            }

            ks.commit(tx, ks.default_commit_mode)?;

            for diff in &diffs {
                diff.update_indexes(&ready_index, &scheduled_index);
            }

            Ok(diffs)
        })
        .await??;

        for diff in &diffs {
            diff.emit_events(&event_tx);
        }

        Ok(diffs.len())
    }

    /// Hard-delete a batch of jobs in a single transaction.
    ///
    /// Opens one write transaction, deletes all found jobs, and commits once.
    /// Returns the count of actually deleted jobs. Missing jobs are silently
    /// skipped.
    pub async fn purge_batch(&self, ids: &[String]) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let ids = ids.to_vec();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // ---- outside tx: prepare all deletions ----
            let mut deletions: Vec<(JobDeletion, String, u16)> = Vec::new();

            for id in &ids {
                let job_key = make_job_key(id);
                let job_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => continue, // Already purged.
                };

                let job: Job = rmp_serde::from_slice(&job_bytes)?;
                let status = JobStatus::try_from(job.status).map_err(|v| {
                    StoreError::Corruption(format!(
                        "job {} has unrecognized status byte: {v}",
                        job.id
                    ))
                })?;

                let priority = job.priority;
                let queue = job.queue.clone();
                deletions.push((prepare_job_deletion(&job, status, &ks), queue, priority));
            }

            // ---- inside tx (writes only) ----
            let count = deletions.len();
            let mut tx = ks.write_tx();
            for (del, _, _) in &deletions {
                apply_job_deletion(&mut tx, del, &ks);
            }
            ks.commit(tx, ks.default_commit_mode)?;

            // Defensive: remove from ready_index in case of inconsistency.
            for (del, queue, priority) in deletions {
                ready_index.remove(&queue, priority, &del.id);
            }

            Ok(count)
        })
        .await??;

        Ok(result)
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
        entries: Vec<CronEntryOptions>,
        now: u64,
    ) -> Result<(CronGroup, Vec<CronEntry>), StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<(CronGroup, Vec<CronEntry>), StoreError> {
            let group_key = make_cron_group_key(&group);
            let prefix = make_cron_group_prefix(&group);

            // Validate all cron expressions before acquiring the write lock.
            let mut computed_next: Vec<Option<u64>> = Vec::with_capacity(entries.len());
            for input in &entries {
                // Validates the expression; None means no future occurrences.
                let next = cron_next_after(&input.expression, now)?;
                computed_next.push(next);
            }

            // Acquire the single-writer lock. All reads within this scope
            // are consistent — no other writer can modify data.
            let mut tx = ks.write_tx();

            // Load or create group metadata.
            let existing_group: CronGroup = match ks.data.get(&group_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => {
                    let group = CronGroup::default();
                    tx.insert(&ks.data, &group_key, &rmp_serde::to_vec(&group)?);
                    group
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
            let mut new_entries = Vec::with_capacity(input_names.len());

            for (input, computed) in entries.into_iter().zip(computed_next) {
                let entry = if let Some(old) = existing_entries.get(&input.name) {
                    let (next, last) = if old.expression == input.expression {
                        // Cron expr is the same — don't reset state.
                        (old.next_enqueue_at, old.last_enqueue_at)
                    } else {
                        // Cron expr changed — recalculate.
                        (computed, old.last_enqueue_at)
                    };

                    // Preserve original paused state unless overridden.
                    let paused = input.paused.unwrap_or(old.paused);
                    let (paused_at, resumed_at) = match (old.paused, paused) {
                        // Unpaused -> Paused: set paused_at
                        (false, true) => (Some(now), old.resumed_at),
                        // Paused -> Unpaused: set resumed_at
                        (true, false) => (old.paused_at, Some(now)),
                        // No change
                        _ => (old.paused_at, old.resumed_at),
                    };

                    CronEntry {
                        name: input.name,
                        expression: input.expression,
                        paused,
                        paused_at,
                        resumed_at,
                        job: input.job,
                        next_enqueue_at: next,
                        last_enqueue_at: last,
                    }
                } else {
                    CronEntry {
                        name: input.name,
                        expression: input.expression,
                        paused: input.paused.unwrap_or(false),
                        paused_at: None,
                        resumed_at: None,
                        job: input.job,
                        next_enqueue_at: computed,
                        last_enqueue_at: None,
                    }
                };

                new_entries.push(entry);
            }

            // Delete removed entries (present in existing but not in input).
            for (name, _) in &existing_entries {
                if !input_names.contains(name) {
                    tx.remove(&ks.data, &make_cron_entry_key(&group, name));
                }
            }

            // Write new/updated entries.
            for entry in &new_entries {
                let entry_key = make_cron_entry_key(&group, &entry.name);
                let entry_bytes = rmp_serde::to_vec(entry)?;
                tx.insert(&ks.data, &entry_key, &entry_bytes);
            }

            // If the group is now empty, clean up the group metadata key.
            if new_entries.is_empty() {
                tx.remove(&ks.data, &group_key);
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
                let next = cron_next_after(&entry.expression, now)?;

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

                let updated_entry_bytes: Slice = rmp_serde::to_vec(&entry)?.into();

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
fn make_job_key(job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(RecordKind::Job as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a payload key: `P\0{job_id}`.
fn make_payload_key(job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(RecordKind::Payload as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a status index key: `S\0{status_byte}\0{job_id}`.
fn make_status_key(status: JobStatus, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + job_id.len());
    key.push(IndexKind::Status as u8);
    key.push(0);
    key.push(status as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a queue membership index key: `Q\0{queue_name}\0{job_id}`.
fn make_queue_key(queue_name: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(queue_name.len() + 3 + job_id.len());
    key.push(IndexKind::Queue as u8);
    key.push(0);
    key.extend_from_slice(queue_name.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a type membership index key: `T\0{job_type}\0{job_id}`.
fn make_type_key(job_type: &str, job_id: &str) -> Vec<u8> {
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
fn make_purge_key(purge_at: u64, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + 3 + job_id.len());
    key.push(IndexKind::PurgeAt as u8);
    key.push(0);
    key.extend_from_slice(&purge_at.to_be_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a unique deduplication index key: `U\0{unique_key}`.
fn make_unique_key(unique_key: &str) -> Vec<u8> {
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
fn cron_next_after(expression: &str, now_ms: u64) -> Result<Option<u64>, StoreError> {
    let cron = croner::Cron::new(expression)
        .parse()
        .map_err(|e| StoreError::InvalidOperation(format!("invalid cron expression: {e}")))?;

    let now_secs = (now_ms / 1000) as i64;
    let dt = chrono::DateTime::from_timestamp(now_secs, 0)
        .ok_or_else(|| StoreError::InvalidOperation("invalid timestamp".to_string()))?;

    match cron.find_next_occurrence(&dt, false) {
        Ok(next) => Ok(Some(next.timestamp() as u64 * 1000)),
        Err(_) => Ok(None),
    }
}

/// Build an error record key: `E\0{job_id}\0{attempt_be_u32}`.
///
/// The null separator lets us prefix-scan all errors for a given job,
/// and the big-endian attempt number gives chronological ordering.
fn make_error_key(job_id: &str, attempt: u32) -> Vec<u8> {
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
fn error_keys(ks: &Keyspaces, job_id: &str) -> impl Iterator<Item = Vec<u8>> {
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
struct JobDeletion {
    id: String,
    status_key: Vec<u8>,
    queue_key: Vec<u8>,
    type_key: Vec<u8>,
    purge_key: Option<Vec<u8>>,
    error_keys: Vec<Vec<u8>>,
    unique_idx_key: Option<Vec<u8>>,
}

/// Collect all keys needed to delete a job. No tx required.
fn prepare_job_deletion(job: &Job, status: JobStatus, ks: &Keyspaces) -> JobDeletion {
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
fn apply_job_deletion(tx: &mut fjall::SingleWriterWriteTx<'_>, del: &JobDeletion, ks: &Keyspaces) {
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

/// Pre-computed data for inserting a job into the store.
///
/// Built by `prepare_enqueue` from `EnqueueOptions`, consumed by
/// `apply_enqueue` inside a write transaction. This separates the pure
/// computation (serialization, key building) from the transactional writes,
/// allowing callers to compose enqueue with other operations in the same tx.
struct PreparedEnqueue {
    job: Job,
    meta_bytes: Vec<u8>,
    payload_bytes: Vec<u8>,
    job_key: Vec<u8>,
    payload_key: Vec<u8>,
    queue_key: Vec<u8>,
    status_key: Vec<u8>,
    type_key: Vec<u8>,
    unique_idx_key: Option<Vec<u8>>,
}

/// Build a `PreparedEnqueue` from `EnqueueOptions` and the current time.
///
/// Pure computation — no IO, no transaction needed.
fn prepare_enqueue(opts: EnqueueOptions, now: u64) -> Result<PreparedEnqueue, StoreError> {
    let unique_while_scope = match (opts.unique_key.as_ref(), opts.unique_while) {
        (Some(_), Some(scope)) => Some(scope),
        (Some(_), None) => Some(UniqueWhile::Queued),
        _ => None,
    };

    let id = scru128::new_string();
    let ready_at = opts.ready_at.unwrap_or(now);
    let scheduled = ready_at > now;
    let status = if scheduled {
        JobStatus::Scheduled
    } else {
        JobStatus::Ready
    };

    let payload_bytes = rmp_serde::to_vec_named(&opts.payload)?;

    let job = Job {
        id: id.clone(),
        job_type: opts.job_type,
        queue: opts.queue,
        priority: opts.priority,
        payload: Some(opts.payload),
        status: status.into(),
        ready_at,
        attempts: 0,
        retry_limit: opts.retry_limit,
        backoff: opts.backoff,
        dequeued_at: None,
        failed_at: None,
        retention: opts.retention,
        purge_at: None,
        completed_at: None,
        unique: opts.unique_key.map(|k| UniqueConstraint {
            key: k,
            scope: unique_while_scope.unwrap_or(UniqueWhile::Queued) as u8,
        }),
    };

    let mut meta = job.clone();
    meta.payload = None;
    let meta_bytes = rmp_serde::to_vec_named(&meta)?;

    Ok(PreparedEnqueue {
        job_key: make_job_key(&id),
        payload_key: make_payload_key(&id),
        queue_key: make_queue_key(&job.queue, &id),
        status_key: make_status_key(status, &id),
        type_key: make_type_key(&job.job_type, &id),
        unique_idx_key: job.unique.as_ref().map(|uc| make_unique_key(&uc.key)),
        job,
        meta_bytes,
        payload_bytes,
    })
}

/// Insert a prepared enqueue into an open write transaction.
///
/// Checks for unique constraint conflicts first. Returns `Duplicate` if a
/// conflict is found (nothing is written), `Created` otherwise.
///
/// Does NOT commit the transaction — the caller is responsible for
/// committing (possibly after adding other writes to the same tx).
fn apply_enqueue(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    p: &PreparedEnqueue,
) -> Result<EnqueueResult, StoreError> {
    // Check for a unique conflict.
    if let Some(ref uc) = p.job.unique {
        let scope = uc.unique_while();
        let idx_key = p.unique_idx_key.as_ref().unwrap();

        if let Some(existing_id_bytes) = ks.index.get(idx_key)? {
            let existing_id = std::str::from_utf8(&existing_id_bytes).map_err(|e| {
                StoreError::Corruption(format!("unique index value is not valid UTF-8: {e}"))
            })?;

            if let Some(existing_meta_bytes) = ks.data.get(&make_job_key(existing_id))? {
                let existing_meta: Job = rmp_serde::from_slice(&existing_meta_bytes)?;
                if let Ok(existing_status) = JobStatus::try_from(existing_meta.status) {
                    if scope.conflicts_with(existing_status) {
                        return Ok(EnqueueResult::Duplicate(existing_meta));
                    }
                }
            }
        }
    }

    tx.insert(&ks.data, &p.job_key, &p.meta_bytes);
    tx.insert(&ks.data, &p.payload_key, &p.payload_bytes);
    tx.insert(&ks.index, &p.queue_key, b"");
    tx.insert(&ks.index, &p.status_key, b"");
    tx.insert(&ks.index, &p.type_key, b"");

    if let Some(ref idx_key) = p.unique_idx_key {
        tx.insert(&ks.index, idx_key, p.job.id.as_bytes());
    }

    Ok(EnqueueResult::Created(p.job.clone()))
}

/// Update in-memory indexes for a successfully enqueued job.
///
/// Call after the transaction has been committed. Updates the ready or
/// scheduled index depending on the job's status.
fn finalize_enqueue(
    result: &EnqueueResult,
    ready_index: &ReadyIndex,
    scheduled_index: &ScheduledIndex,
    event_tx: &broadcast::Sender<StoreEvent>,
) {
    if let EnqueueResult::Created(job) = result {
        match JobStatus::try_from(job.status) {
            Ok(JobStatus::Ready) => {
                ready_index.insert(&job.queue, job.priority, job.id.clone());
                let _ = event_tx.send(StoreEvent::JobCreated {
                    id: job.id.clone(),
                    queue: job.queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            Ok(JobStatus::Scheduled) => {
                scheduled_index.insert(job.ready_at, job.id.clone());
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: job.id.clone(),
                    ready_at: job.ready_at,
                });
            }
            _ => {
                tracing::warn!(
                    job_id = %job.id,
                    status = job.status,
                    "enqueue produced unexpected status"
                );
            }
        }
    }
}

/// Compute the backoff delay in milliseconds for a given attempt count.
///
/// Formula: `delay_ms = attempts^exponent + base_ms + rand(0..jitter_ms) * (attempts + 1)`
///
/// The jitter component scales linearly with the attempt count so that
/// later retries spread further apart, reducing collision likelihood when
/// many jobs fail at similar times.
fn compute_backoff(attempts: u32, backoff: &BackoffConfig) -> u64 {
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
    use std::sync::Arc;
    use std::time::SystemTime;

    use crate::store::options::RetentionConfigPatch;
    use crate::store::types::RetentionConfig;
    use crate::time::now_millis;

    fn test_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        store
    }

    fn test_store_with_retention(completed_ms: u64, dead_ms: u64) -> Store {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_completed_retention_ms = completed_ms;
        config.default_dead_retention_ms = dead_ms;
        let store = Store::open(dir.path().join("data"), config).unwrap();
        std::mem::forget(dir);
        store
    }

    #[tokio::test]
    async fn enqueue_returns_job_with_id() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"task": "test"})),
            )
            .await
            .unwrap()
            .into_job();

        assert!(!job.id.is_empty());
        assert_eq!(job.queue, "default");
        assert_eq!(job.priority, 0);
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, Some(serde_json::json!({"task": "test"})));
    }

    #[tokio::test]
    async fn enqueue_generates_unique_ids() {
        let store = test_store();
        let job1 = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let job2 = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        assert_ne!(job1.id, job2.id);
    }

    #[tokio::test]
    async fn enqueue_ids_are_fifo_ordered() {
        let store = test_store();
        let first = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let second = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        let third = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        // scru128 IDs sort lexicographically in generation order.
        assert!(first.id < second.id);
        assert!(second.id < third.id);
    }

    #[tokio::test]
    async fn take_next_job_returns_none_when_empty() {
        let store = test_store();
        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(job.is_none());
    }

    #[tokio::test]
    async fn take_next_job_returns_highest_priority() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        store
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
                EnqueueOptions::new("test", "default", serde_json::json!("mid")).priority(5),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.priority, 1);
        assert_eq!(job.status, u8::from(JobStatus::InFlight));
        assert_eq!(job.payload, Some(serde_json::json!("high")));
    }

    #[tokio::test]
    async fn take_next_job_fifo_within_same_priority() {
        let store = test_store();
        let first = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, first.id);
    }

    #[tokio::test]
    async fn take_next_job_removes_from_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

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

        assert_ne!(first.id, second.id);
        assert_eq!(first.payload, Some(serde_json::json!("a")));
        assert_eq!(second.payload, Some(serde_json::json!("b")));

        // Queue should now be empty.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn mark_completed_removes_job() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(store.mark_completed(now_millis(), &job.id).await.unwrap());

        // Job should no longer be dequeue-able even if re-enqueue were attempted
        // via crash recovery — it's gone from the jobs keyspace entirely.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn mark_completed_returns_false_for_unknown_id() {
        let store = test_store();
        assert!(
            !store
                .mark_completed(now_millis(), "nonexistent")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn mark_completed_returns_false_on_double_ack() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert!(store.mark_completed(now_millis(), &job.id).await.unwrap());
        assert!(!store.mark_completed(now_millis(), &job.id).await.unwrap());
    }

    #[tokio::test]
    async fn take_next_job_never_returns_duplicates_under_contention() {
        let store = Arc::new(test_store());
        let num_jobs = 50;

        for i in 0..num_jobs {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        // Spawn many concurrent workers all racing to take jobs.
        let mut handles = Vec::new();
        for _ in 0..20 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(job) = store
                    .take_next_job(now_millis(), &HashSet::new())
                    .await
                    .unwrap()
                {
                    taken.push(job.id);
                }
                taken
            }));
        }

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.await.unwrap());
        }

        // Every job should have been taken exactly once.
        assert_eq!(all_ids.len(), num_jobs);
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), num_jobs);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn take_next_job_filtered_never_returns_duplicates_under_contention() {
        let store = Arc::new(test_store());
        let num_jobs = 50;
        let queue = "contention";

        for i in 0..num_jobs {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", queue, serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let queues: HashSet<String> = [queue.to_string()].into_iter().collect();

        // Spawn many concurrent workers all racing to take jobs
        // using the queue-filtered path.
        let mut handles = Vec::new();
        for _ in 0..20 {
            let store = store.clone();
            let queues = queues.clone();
            handles.push(tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(job) = store.take_next_job(now_millis(), &queues).await.unwrap() {
                    taken.push(job.id.clone());
                    // Also verify the ack works (job is in InFlight status).
                    assert!(
                        store.mark_completed(now_millis(), &job.id).await.unwrap(),
                        "mark_completed returned false for job {}",
                        job.id,
                    );
                }
                taken
            }));
        }

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.await.unwrap());
        }

        // Every job should have been taken and acked exactly once.
        assert_eq!(all_ids.len(), num_jobs);
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), num_jobs);
    }

    // --- take_next_job queue filter tests ---

    #[tokio::test]
    async fn take_next_job_filters_by_single_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["reports".into()]);
        let job = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, b.id);
        assert_eq!(job.queue, "reports");

        // No more reports jobs.
        assert!(
            store
                .take_next_job(now_millis(), &queues)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_filters_by_multiple_queues() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into(), "webhooks".into()]);
        let first = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, a.id);

        let second = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.id, c.id);

        assert!(
            store
                .take_next_job(now_millis(), &queues)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_respects_priority() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into()]);
        let job = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, high.id);
        assert_eq!(job.priority, 1);
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_returns_none_for_empty_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["reports".into()]);
        assert!(
            store
                .take_next_job(now_millis(), &queues)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_picks_best_across_queues() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into(), "webhooks".into()]);
        let job = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, high.id);
        assert_eq!(job.queue, "webhooks");
        assert_eq!(job.priority, 1);
    }

    #[tokio::test]
    async fn take_next_job_sets_dequeued_at() {
        let store = test_store();
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let dequeued_at = job.dequeued_at.expect("dequeued_at should be set");
        assert!(
            dequeued_at >= before && dequeued_at <= after,
            "dequeued_at ({dequeued_at}) should be between {before} and {after}"
        );
    }

    #[tokio::test]
    async fn take_next_job_dequeued_at_persists() {
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
        let stored = store
            .get_job(now_millis(), &taken.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.dequeued_at, taken.dequeued_at);
    }

    // --- take_next_n_jobs tests ---

    #[tokio::test]
    async fn take_next_n_jobs_returns_up_to_n() {
        let store = test_store();
        for i in 0..5 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 3)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn take_next_n_jobs_returns_fewer_when_not_enough() {
        let store = test_store();
        for i in 0..2 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 5)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn take_next_n_jobs_empty_store() {
        let store = test_store();
        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 5)
            .await
            .unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn take_next_n_jobs_respects_priority() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        store
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
                EnqueueOptions::new("test", "default", serde_json::json!("mid")).priority(5),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 3)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].priority, 1);
        assert_eq!(jobs[1].priority, 5);
        assert_eq!(jobs[2].priority, 10);
    }

    #[tokio::test]
    async fn take_next_n_jobs_fifo_within_priority() {
        let store = test_store();
        let first = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let second = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 2)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, first.id);
        assert_eq!(jobs[1].id, second.id);
    }

    #[tokio::test]
    async fn take_next_n_jobs_filters_by_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "alpha", serde_json::json!("a1")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "beta", serde_json::json!("b1")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "alpha", serde_json::json!("a2")),
            )
            .await
            .unwrap()
            .into_job();

        let queues: HashSet<String> = ["alpha".to_string()].into_iter().collect();
        let jobs = store
            .take_next_n_jobs(now_millis(), &queues, 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|j| j.queue == "alpha"));
    }

    #[tokio::test]
    async fn take_next_n_jobs_marks_in_flight() {
        let store = test_store();
        for i in 0..3 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 3)
            .await
            .unwrap();
        for job in &jobs {
            assert_eq!(job.status, u8::from(JobStatus::InFlight));
            assert!(job.dequeued_at.is_some());
        }
    }

    #[tokio::test]
    async fn take_next_n_jobs_hydrates_payload() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"key": "value"})),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 1)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].payload, Some(serde_json::json!({"key": "value"})));
    }

    #[tokio::test]
    async fn take_next_n_jobs_broadcasts_events() {
        let store = test_store();
        let mut rx = store.subscribe();

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        // Drain the enqueue events.
        while let Ok(StoreEvent::JobCreated { .. }) = rx.try_recv() {}

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 2)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);

        // We should receive exactly 2 JobInFlight events.
        let mut in_flight_ids = Vec::new();
        for _ in 0..2 {
            match rx.try_recv() {
                Ok(StoreEvent::JobInFlight { id }) => in_flight_ids.push(id),
                other => panic!("expected JobInFlight, got {:?}", other),
            }
        }
        assert_eq!(in_flight_ids.len(), 2);
        assert!(in_flight_ids.contains(&jobs[0].id));
        assert!(in_flight_ids.contains(&jobs[1].id));
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
    async fn enqueue_broadcasts_job_created() {
        let store = test_store();
        let mut rx = store.subscribe();

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "default"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mark_completed_broadcasts_job_completed() {
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

        let mut rx = store.subscribe();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCompleted { id } => assert_eq!(id, job.id),
            other => panic!("expected JobCompleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn requeue_broadcasts_job_created() {
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
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "default"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_job_returns_enqueued_job() {
        let store = test_store();
        let enqueued = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("hello")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .get_job(now_millis(), &enqueued.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, enqueued.id);
        assert_eq!(job.queue, "default");
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, Some(serde_json::json!("hello")));
    }

    #[tokio::test]
    async fn get_job_returns_none_for_unknown_id() {
        let store = test_store();
        assert!(
            store
                .get_job(now_millis(), "nonexistent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_job_returns_none_after_completion() {
        let store = test_store();
        let enqueued = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .mark_completed(now_millis(), &enqueued.id)
            .await
            .unwrap();

        assert!(
            store
                .get_job(now_millis(), &enqueued.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_asc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().direction(ScanDirection::Desc))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, c.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_respects_limit() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
    }

    #[tokio::test]
    async fn list_jobs_cursor_is_exclusive_asc() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().from(&a.id))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_cursor_is_exclusive_desc() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .from(&c.id)
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_empty_store() {
        let store = test_store();
        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert!(page.jobs.is_empty());
        assert!(page.next.is_none());
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_jobs_cursor_past_end() {
        let store = test_store();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().from(&c.id))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_includes_in_flight_jobs() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::InFlight));
        assert_eq!(page.jobs[1].status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn list_jobs_excludes_completed_jobs() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().now(now_millis()))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_next_present_when_more_results() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);

        let next = page.next.unwrap();
        assert_eq!(next.from.as_deref(), Some(b.id.as_str()));
        assert_eq!(next.direction, ScanDirection::Asc);
        assert_eq!(next.limit, 2);
    }

    #[tokio::test]
    async fn list_jobs_next_absent_on_last_page() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_prev_absent_on_first_page() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_jobs_prev_present_when_cursor_provided() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().from(&a.id).limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);

        let prev = page.prev.unwrap();
        assert_eq!(prev.from.as_deref(), Some(b.id.as_str()));
        assert_eq!(prev.direction, ScanDirection::Desc);
        assert_eq!(prev.limit, 2);
    }

    #[tokio::test]
    async fn list_jobs_next_page_returns_remaining() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // First page.
        let page1 = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page1.jobs.len(), 2);

        // Follow next to get second page.
        let page2 = store.list_jobs(page1.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    // --- list_jobs status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_ready_status() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        // Take the first job so it becomes InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Ready])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn list_jobs_filters_by_in_flight_status() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::InFlight));
    }

    #[tokio::test]
    async fn list_jobs_status_filter_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_status_filter_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        // Follow next page — should preserve the status filter.
        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_status_filter_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_status_filter_reflects_requeue() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Now in-flight.
        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        // Requeue — should move back to ready.
        store.requeue(&a.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Ready])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_status_filter_reflects_completion() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    // --- list_jobs queue filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_single_queue() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().queues(HashSet::from(["emails".into()])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_queues() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new().queues(HashSet::from(["emails".into(), "webhooks".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_merges_in_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();
        let d = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("d")),
            )
            .await
            .unwrap()
            .into_job();

        // Ascending: interleaved by job ID (time order).
        let page = store
            .list_jobs(
                ListJobsOptions::new().queues(HashSet::from(["emails".into(), "webhooks".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 4);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
        assert_eq!(page.jobs[3].id, d.id);

        // Descending: reverse order.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into(), "webhooks".into()]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 4);
        assert_eq!(page.jobs[0].id, d.id);
        assert_eq!(page.jobs[1].id, c.id);
        assert_eq!(page.jobs[2].id, b.id);
        assert_eq!(page.jobs[3].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().queues(HashSet::from(["reports".into()])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into(), "reports".into()]);

        let page = store
            .list_jobs(ListJobsOptions::new().queues(queues).limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        // Follow next page — should preserve the queue filter.
        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_reflects_completion() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .now(now_millis()),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    // --- list_jobs multi-status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_statuses() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a and b so they become InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Filter by Ready + InFlight — should get all 3.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_multi_status_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    // --- list_jobs combined queue + status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_queue_and_status() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let _c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a so it becomes InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Only ready jobs in the emails queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, _c.id);

        // InFlight jobs in emails queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);

        // Ready jobs in reports queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["reports".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_multiple_queues_and_statuses() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();
        let d = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("d")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a and b so they become InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready + InFlight in emails + webhooks (excludes reports).
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into(), "webhooks".into()]))
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, d.id);

        // Verify reports/c is excluded.
        assert!(page.jobs.iter().all(|j| j.id != c.id));
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        // Follow next — should preserve both queue and status filters.
        let next = page.next.unwrap();
        assert!(!next.queues.is_empty());
        assert!(!next.statuses.is_empty());

        let page2 = store.list_jobs(next).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_reflects_requeue() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        // InFlight in emails.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        // Requeue — should move back to ready.
        store.requeue(&a.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert!(page.jobs.is_empty());

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_reflects_completion() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);

        // InFlight should be empty after completion.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_multi_status_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    // --- list_jobs type filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_single_type() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().types(HashSet::from(["send_email".into()])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_types() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_sms", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into(), "send_sms".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_type_filter_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().types(HashSet::from(["no_such_type".into()])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_type_filter_reflects_completion() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .now(now_millis()),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_type_filter_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_type_filter_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    // --- list_jobs combined type + queue + status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_queue() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "low", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .queues(HashSet::from(["high".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_status() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a so it becomes in-flight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // InFlight send_email jobs only.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_queue_and_status() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "low", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("d")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a so it becomes in-flight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready send_email in high queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .queues(HashSet::from(["high".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        // b is the only ready send_email in the high queue (a is in-flight).
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!("b")));
    }

    #[tokio::test]
    async fn enqueue_with_future_ready_at_creates_scheduled_job() {
        let store = test_store();
        let future_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000; // 1 minute from now

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future_ms),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(job.status, u8::from(JobStatus::Scheduled));
        assert_eq!(job.ready_at, future_ms);

        // Scheduled jobs should not be dequeue-able.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_none());
    }

    #[tokio::test]
    async fn enqueue_with_past_ready_at_creates_ready_job() {
        let store = test_store();
        let past_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 1000; // 1 second ago

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(past_ms),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.ready_at, past_ms);

        // Should be dequeue-able immediately.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn enqueue_without_ready_at_defaults_to_now() {
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert!(job.ready_at >= before);
        assert!(job.ready_at <= after);
    }

    // --- next_scheduled / promote_scheduled tests ---

    #[tokio::test]
    async fn promote_scheduled_moves_due_job_to_ready() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Enqueue a job scheduled in the future so it enters Scheduled state.
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.status, u8::from(JobStatus::Scheduled));

        // Pretend time has passed: ask for jobs due at ready_at.
        let (batch, _) = store.next_scheduled(now + 60_000, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        store.promote_scheduled(&batch[0]).await.unwrap();

        // It should now be takeable.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn next_scheduled_skips_future_jobs() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future_ms = now + 3_600_000; // 1 hour from now

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future_ms),
            )
            .await
            .unwrap()
            .into_job();

        // Asking with current time returns no due jobs, but reports the
        // next ready_at so the caller knows when to wake up.
        let (batch, next) = store.next_scheduled(now, 10).await.unwrap();
        assert!(batch.is_empty());
        assert_eq!(next, Some(future_ms));

        // Still not takeable.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // But asking with future time returns it.
        let (batch, _) = store.next_scheduled(future_ms, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, job.id);
    }

    #[tokio::test]
    async fn next_scheduled_respects_limit() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future = now + 60_000;

        for i in 0..5u64 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "q", serde_json::json!(i)).ready_at(future + i),
                )
                .await
                .unwrap()
                .into_job();
        }

        // All 5 are due at future+10, ask for only 2.
        let (batch, next) = store.next_scheduled(future + 10, 2).await.unwrap();
        assert_eq!(batch.len(), 2);
        // next_ready_at is None because we hit the limit, not because there
        // are no more scheduled jobs.
        assert_eq!(next, None);

        // Ask for all of them.
        let (batch, _) = store.next_scheduled(future + 10, 10).await.unwrap();
        assert_eq!(batch.len(), 5);
    }

    #[tokio::test]
    async fn promote_scheduled_fires_job_created_event() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future = now + 60_000;

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        let mut rx = store.subscribe();
        let (batch, _) = store.next_scheduled(future, 10).await.unwrap();
        store.promote_scheduled(&batch[0]).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "q"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn next_scheduled_returns_earliest_first() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let base = now + 60_000;

        // Enqueue in reverse order to confirm sorting is by ready_at.
        let later = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("later")).ready_at(base + 5000),
            )
            .await
            .unwrap()
            .into_job();
        let earlier = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("earlier"))
                    .ready_at(base + 1000),
            )
            .await
            .unwrap()
            .into_job();

        let (batch, _) = store.next_scheduled(base + 5000, 10).await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].id, earlier.id);
        assert_eq!(batch[1].id, later.id);
    }

    #[tokio::test]
    async fn next_scheduled_returns_empty_when_no_scheduled_jobs() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Enqueue a ready job (no ready_at).
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let (batch, next) = store.next_scheduled(now, 10).await.unwrap();
        assert!(batch.is_empty());
        // No scheduled jobs at all, so no next_ready_at either.
        assert_eq!(next, None);
    }

    #[tokio::test]
    async fn enqueue_scheduled_fires_job_scheduled_event() {
        let store = test_store();
        let mut rx = store.subscribe();
        let future = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        assert!(matches!(
            rx.recv().await.unwrap(),
            StoreEvent::JobScheduled { ready_at, .. } if ready_at == future
        ));
    }

    #[tokio::test]
    async fn promote_scheduled_is_idempotent() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future = now + 60_000;

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        let (batch, _) = store.next_scheduled(future, 10).await.unwrap();
        store.promote_scheduled(&batch[0]).await.unwrap();

        // Promoting again should be a no-op (job is already Ready).
        store.promote_scheduled(&job).await.unwrap();

        // Should still be takeable exactly once.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn next_scheduled_returns_next_ready_at_for_mixed_batch() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // One due job and one future job.
        let due = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("due")).ready_at(now + 100),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("future"))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        // Ask as of now + 200: only the first job is due.
        let (batch, next) = store.next_scheduled(now + 200, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, due.id);
        assert_eq!(next, Some(now + 60_000));
    }

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

    // --- record_failure tests ---

    /// Build a FailureOptions with sensible defaults for testing.
    fn test_failure_opts() -> FailureOptions {
        FailureOptions {
            message: "something broke".into(),
            error_type: None,
            backtrace: None,
            retry_at: None,
            kill: false,
        }
    }

    /// Create a test store with a specific retry limit and zero-jitter backoff.
    fn test_store_with_retry_limit(retry_limit: u32) -> Store {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_retry_limit = retry_limit;
        config.default_backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0, // deterministic
        };
        let store = Store::open(dir.path().join("data"), config).unwrap();
        std::mem::forget(dir);
        store
    }

    /// Enqueue a job and take it so it's in the InFlight state.
    async fn enqueue_and_take(store: &Store) -> Job {
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("payload")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn record_failure_retries_under_limit() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.attempts, 1);
        assert!(result.failed_at.is_some());
        // ready_at should be in the future (backoff from the store's default config).
        assert!(result.ready_at > 0);
    }

    #[tokio::test]
    async fn record_failure_kills_when_retries_exhausted() {
        // retry_limit=0 means the first failure kills.
        let store = test_store_with_retry_limit(0);
        let job = enqueue_and_take(&store).await;

        let opts = test_failure_opts();

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);
        assert_eq!(result.attempts, 1);

        // Dead job is still visible during its retention period.
        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn record_failure_kills_when_kill_flag_set() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.kill = true;

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);

        // Dead job is still visible during its retention period.
        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn record_failure_kill_overrides_remaining_retries() {
        // retry_limit=100, but kill=true should still kill.
        let store = test_store_with_retry_limit(100);
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.kill = true;

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn record_failure_retry_at_overrides_backoff() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.retry_at = Some(999_999);

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, 999_999);
    }

    #[tokio::test]
    async fn record_failure_retry_at_overrides_exhausted_limit() {
        // retry_limit=0, but retry_at should force a retry anyway.
        let store = test_store_with_retry_limit(0);
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.retry_at = Some(123_456);

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, 123_456);
    }

    #[tokio::test]
    async fn record_failure_returns_none_for_unknown_id() {
        let store = test_store();
        let result = store
            .record_failure(now_millis(), "nonexistent", test_failure_opts())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn record_failure_returns_none_for_ready_job() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        // Don't take it — job is still Ready.
        let job = store
            .list_jobs(ListJobsOptions::new())
            .await
            .unwrap()
            .jobs
            .pop()
            .unwrap();

        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn record_failure_increments_attempts_across_retries() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // First failure: attempts goes from 0 -> 1.
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        // The job is now Scheduled. Promote it and take it again.
        let scheduled = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Second failure: attempts goes from 1 -> 2.
        let result = store
            .record_failure(now_millis(), &retaken.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.attempts, 2);
    }

    #[tokio::test]
    async fn record_failure_writes_error_record() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.message = "connection timeout".into();
        opts.error_type = Some("TimeoutError".into());
        opts.backtrace = Some("at line 42".into());

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Read the error record from the data keyspace.
        let key = make_error_key(&job.id, 1);
        let raw = store.ks.data.inner().get(&key).unwrap().unwrap();
        let err: ErrorRecord = rmp_serde::from_slice(&raw).unwrap();

        assert_eq!(err.message, "connection timeout");
        assert_eq!(err.error_type.as_deref(), Some("TimeoutError"));
        assert_eq!(err.backtrace.as_deref(), Some("at line 42"));
        assert!(err.failed_at > 0);
    }

    #[tokio::test]
    async fn record_failure_dead_retains_error_records_for_reaper() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Kill immediately — error records are now retained until the
        // reaper purges the job.
        let mut opts = test_failure_opts();
        opts.kill = true;

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Error records should still exist (cleanup deferred to reaper).
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(!keys.is_empty());

        // purge_job should clean them up.
        store.purge_job(&job.id).await.unwrap();
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn record_failure_dead_retains_payload_for_reaper() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Verify payload exists before kill.
        let payload_key = make_payload_key(&job.id);
        assert!(store.ks.data.inner().get(&payload_key).unwrap().is_some());

        let mut opts = test_failure_opts();
        opts.kill = true;

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Payload should still exist (cleanup deferred to reaper).
        assert!(store.ks.data.inner().get(&payload_key).unwrap().is_some());

        // purge_job should clean it up.
        store.purge_job(&job.id).await.unwrap();
        assert!(store.ks.data.inner().get(&payload_key).unwrap().is_none());
    }

    #[tokio::test]
    async fn record_failure_retry_broadcasts_job_failed_then_scheduled() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut rx = store.subscribe();
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();

        // First event: JobFailed so workers prune their in-flight set.
        match rx.recv().await.unwrap() {
            StoreEvent::JobFailed { id, attempts } => {
                assert_eq!(id, job.id);
                // attempts=0: the pre-increment value that matches what
                // the worker stored when it took the job.
                assert_eq!(attempts, 0);
            }
            other => panic!("expected JobFailed, got {other:?}"),
        }

        // Second event: JobScheduled so the scheduler wakes up.
        match rx.recv().await.unwrap() {
            StoreEvent::JobScheduled { ready_at, .. } => assert!(ready_at > 0),
            other => panic!("expected JobScheduled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_failure_dead_broadcasts_job_failed() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut rx = store.subscribe();
        let mut opts = test_failure_opts();
        opts.kill = true;
        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap();

        // Even for killed jobs, the event is JobFailed — workers don't
        // need to distinguish retry from kill; they just prune in-flight.
        match rx.recv().await.unwrap() {
            StoreEvent::JobFailed { id, attempts } => {
                assert_eq!(id, job.id);
                assert_eq!(attempts, 0);
            }
            other => panic!("expected JobFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_failure_does_not_hydrate_payload() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        // Payload should be None (not hydrated), not the stored value.
        assert_eq!(result.payload, None);
    }

    #[tokio::test]
    async fn record_failure_uses_job_retry_limit_over_default() {
        let store = test_store();

        // Enqueue with per-job retry_limit=1.
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")).retry_limit(1),
            )
            .await
            .unwrap()
            .into_job();
        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // First failure (attempts 0->1): should retry (1 <= 1).
        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);

        // Promote and take again.
        let scheduled = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Second failure (attempts 1->2): should die (2 > 1).
        let result = store
            .record_failure(now_millis(), &retaken.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn mark_completed_retains_error_records_for_reaper() {
        // Use non-zero retention so mark_completed defers to the reaper
        // instead of synchronously deleting.
        let store = test_store_with_retention(60_000, 60_000);
        let job = enqueue_and_take(&store).await;

        // Fail once (retry), then promote, take, and complete.
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();

        let scheduled = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store
            .mark_completed(now_millis(), &retaken.id)
            .await
            .unwrap();

        // Error records are now retained until the reaper purges the job.
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(!keys.is_empty());

        // purge_job should clean them up.
        store.purge_job(&job.id).await.unwrap();
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(keys.is_empty());
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

    // --- list_errors tests ---

    /// Helper: fail a in-flight job, promote+retake it, and return the retaken job.
    ///
    /// This cycles one failure through the store so that `list_errors` has
    /// something to read. The `error_msg` lets callers tag each failure for
    /// assertion clarity.
    async fn fail_and_retake(store: &Store, job_id: &str, error_msg: &str) -> Job {
        let mut opts = test_failure_opts();
        opts.message = error_msg.into();
        store
            .record_failure(now_millis(), job_id, opts)
            .await
            .unwrap()
            .unwrap();

        // Promote from Scheduled -> Ready and take again.
        let scheduled = store.get_job(now_millis(), job_id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn list_errors_returns_empty_for_job_without_errors() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("payload")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id))
            .await
            .unwrap();

        assert!(page.errors.is_empty());
        assert!(page.next.is_none());
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_errors_returns_errors_in_asc_order() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Fail twice to produce two error records (attempts 1, 2).
        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let mut opts = test_failure_opts();
        opts.message = "error two".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 2);
        assert_eq!(page.errors[0].message, "error one");
        assert_eq!(page.errors[1].message, "error two");
    }

    #[tokio::test]
    async fn list_errors_returns_errors_in_desc_order() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let mut opts = test_failure_opts();
        opts.message = "error two".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id).direction(ScanDirection::Desc))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 2);
        // Desc: newest first.
        assert_eq!(page.errors[0].message, "error two");
        assert_eq!(page.errors[1].message, "error one");
    }

    #[tokio::test]
    async fn list_errors_paginates_with_limit() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Fail three times.
        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let retaken = fail_and_retake(&store, &retaken.id, "error two").await;
        let mut opts = test_failure_opts();
        opts.message = "error three".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        // Page 1: limit=2, should see errors 1 and 2 with a next cursor.
        let page = store
            .list_errors(ListErrorsOptions::new(&job.id).limit(2))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 2);
        assert_eq!(page.errors[0].message, "error one");
        assert_eq!(page.errors[1].message, "error two");
        assert!(page.next.is_some());

        // Page 2: follow the next cursor.
        let page2 = store.list_errors(page.next.unwrap()).await.unwrap();

        assert_eq!(page2.errors.len(), 1);
        assert_eq!(page2.errors[0].message, "error three");
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_errors_cursor_is_exclusive() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let mut opts = test_failure_opts();
        opts.message = "error two".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        // Start after attempt 1 — should only see attempt 2.
        let page = store
            .list_errors(ListErrorsOptions::new(&job.id).from(1))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 1);
        assert_eq!(page.errors[0].attempt, 2);
        assert_eq!(page.errors[0].message, "error two");
    }

    #[tokio::test]
    async fn list_errors_includes_attempt_number() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let retaken = fail_and_retake(&store, &job.id, "first").await;
        let mut opts = test_failure_opts();
        opts.message = "second".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id))
            .await
            .unwrap();

        // Attempts are 1-based: first failure = 1, second = 2.
        assert_eq!(page.errors[0].attempt, 1);
        assert_eq!(page.errors[1].attempt, 2);
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

    #[tokio::test]
    async fn list_ready_jobs_returns_priority_order() {
        let store = test_store();
        let now = now_millis();

        // Enqueue jobs at different priorities.
        let low = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(0),
            )
            .await
            .unwrap()
            .into_job();
        let mid = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(5),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store.list_ready_jobs(0, 10).await.unwrap();
        let ids: Vec<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
        assert_eq!(ids, vec![high.id, mid.id, low.id]);
    }

    #[tokio::test]
    async fn list_ready_jobs_respects_limit() {
        let store = test_store();
        let now = now_millis();

        for _ in 0..5 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store.list_ready_jobs(0, 3).await.unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn list_ready_jobs_empty_store() {
        let store = test_store();
        let jobs = store.list_ready_jobs(0, 10).await.unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn list_ready_jobs_excludes_in_flight() {
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
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        // Take one job — it moves to InFlight.
        store.take_next_job(now, &HashSet::new()).await.unwrap();

        let jobs = store.list_ready_jobs(0, 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
    }

    // --- list_scheduled_jobs tests ---

    #[tokio::test]
    async fn list_scheduled_jobs_returns_chronological_order() {
        let store = test_store();
        let now = now_millis();

        // Enqueue jobs at different ready_at times.
        let late = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 30_000),
            )
            .await
            .unwrap()
            .into_job();
        let early = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 10_000),
            )
            .await
            .unwrap()
            .into_job();
        let mid = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 20_000),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        let ids: Vec<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
        assert_eq!(ids, vec![early.id, mid.id, late.id]);
    }

    #[tokio::test]
    async fn list_scheduled_jobs_respects_limit() {
        let store = test_store();
        let now = now_millis();

        for i in 0..5 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .ready_at(now + (i + 1) * 10_000),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store.list_scheduled_jobs(0, 3).await.unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn list_scheduled_jobs_empty_store() {
        let store = test_store();
        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn list_scheduled_jobs_excludes_ready() {
        let store = test_store();
        let now = now_millis();

        // One ready job.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        // One scheduled job.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
    }

    // --- count_jobs tests ---

    #[tokio::test]
    async fn count_jobs_empty_store() {
        let store = test_store();
        let count = store.count_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn count_jobs_returns_total() {
        let store = test_store();
        let now = now_millis();
        for i in 0..7 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(i)))
                .await
                .unwrap();
        }
        let count = store
            .count_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(count, 7);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_status() {
        let store = test_store();
        let now = now_millis();

        // Enqueue 3 ready jobs.
        for i in 0..3 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(i)))
                .await
                .unwrap();
        }

        // Take one (moves to in_flight).
        store
            .take_next_job(now, &std::collections::HashSet::new())
            .await
            .unwrap();

        let ready_count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .statuses([JobStatus::Ready].into()),
            )
            .await
            .unwrap();
        assert_eq!(ready_count, 2);

        let in_flight_count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .statuses([JobStatus::InFlight].into()),
            )
            .await
            .unwrap();
        assert_eq!(in_flight_count, 1);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_queue() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "emails", serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "emails", serde_json::json!(2)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "reports", serde_json::json!(3)),
            )
            .await
            .unwrap();

        let count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .queues(["emails".to_string()].into()),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_type() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "q", serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "q", serde_json::json!(2)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("generate_report", "q", serde_json::json!(3)),
            )
            .await
            .unwrap();

        let count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .types(["send_email".to_string()].into()),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn count_jobs_with_payload_filter() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"score": 10})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"score": 50})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"score": 90})),
            )
            .await
            .unwrap();

        let filter = crate::filter::PayloadFilter::compile(".score > 20").unwrap();
        let count = store
            .count_jobs(ListJobsOptions::new().now(now).filter(Arc::new(filter)))
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn count_jobs_combined_filters() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "emails", serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "emails", serde_json::json!(2)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "reports", serde_json::json!(3)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("generate_report", "emails", serde_json::json!(4)),
            )
            .await
            .unwrap();

        let count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .queues(["emails".to_string()].into())
                    .types(["send_email".to_string()].into()),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
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

    // --- enqueue_bulk tests ---

    #[tokio::test]
    async fn enqueue_bulk_returns_all_jobs() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("a", "q1", serde_json::json!(1)),
                    EnqueueOptions::new("b", "q2", serde_json::json!(2)),
                    EnqueueOptions::new("c", "q3", serde_json::json!(3)),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].job_type, "a");
        assert_eq!(jobs[1].job_type, "b");
        assert_eq!(jobs[2].job_type, "c");
    }

    #[tokio::test]
    async fn enqueue_bulk_ids_are_monotonic() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert!(jobs[0].id < jobs[1].id);
        assert!(jobs[1].id < jobs[2].id);
    }

    #[tokio::test]
    async fn enqueue_bulk_empty_is_noop() {
        let store = test_store();
        let jobs = store
            .enqueue_bulk(now_millis(), vec![])
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect::<Vec<_>>();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn enqueue_bulk_jobs_are_retrievable() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "q1", serde_json::json!("a")),
                    EnqueueOptions::new("test", "q2", serde_json::json!("b")),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        for job in &jobs {
            let fetched = store.get_job(now, &job.id).await.unwrap().unwrap();
            assert_eq!(fetched.id, job.id);
            assert_eq!(fetched.queue, job.queue);
        }
    }

    #[tokio::test]
    async fn enqueue_bulk_jobs_are_takeable() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                ],
            )
            .await
            .unwrap();

        let taken1 = store.take_next_job(now, &HashSet::new()).await.unwrap();
        let taken2 = store.take_next_job(now, &HashSet::new()).await.unwrap();
        let taken3 = store.take_next_job(now, &HashSet::new()).await.unwrap();

        assert!(taken1.is_some());
        assert!(taken2.is_some());
        assert!(taken3.is_none());
    }

    #[tokio::test]
    async fn enqueue_bulk_mixed_ready_and_scheduled() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .ready_at(future),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert_eq!(jobs[0].status, u8::from(JobStatus::Ready));
        assert_eq!(jobs[1].status, u8::from(JobStatus::Scheduled));
        assert_eq!(jobs[1].ready_at, future);

        // Only the ready job should be takeable.
        let taken = store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert!(taken.is_some());
        let none = store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn enqueue_bulk_respects_priority() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(100),
                    EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
                ],
            )
            .await
            .unwrap();

        // Higher priority (lower number) should be taken first.
        let taken = store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.payload, Some(serde_json::json!("high")));
    }

    #[tokio::test]
    async fn enqueue_bulk_broadcasts_events() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future = now + 60_000;

        store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "q1", serde_json::json!(null)),
                    EnqueueOptions::new("test", "q2", serde_json::json!(null)).ready_at(future),
                ],
            )
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCreated { queue, .. } => assert_eq!(queue, "q1"),
            other => panic!("expected JobCreated, got {other:?}"),
        }
        match rx.recv().await.unwrap() {
            StoreEvent::JobScheduled { .. } => {}
            other => panic!("expected JobScheduled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn enqueue_bulk_preserves_options() {
        let store = test_store();
        let now = now_millis();

        let jobs: Vec<Job> = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .priority(42)
                        .retry_limit(5)
                        .backoff(BackoffConfig {
                            exponent: 3.0,
                            base_ms: 2000,
                            jitter_ms: 500,
                        })
                        .retention(RetentionConfig {
                            completed_ms: Some(3600_000),
                            dead_ms: Some(7200_000),
                        }),
                ],
            )
            .await
            .unwrap()
            .into_iter()
            .map(EnqueueResult::into_job)
            .collect();

        assert_eq!(jobs[0].priority, 42);
        assert_eq!(jobs[0].retry_limit, Some(5));
        let backoff = jobs[0].backoff.as_ref().unwrap();
        assert_eq!(backoff.exponent, 3.0);
        assert_eq!(backoff.base_ms, 2000);
        assert_eq!(backoff.jitter_ms, 500);
        let retention = jobs[0].retention.as_ref().unwrap();
        assert_eq!(retention.completed_ms, Some(3600_000));
        assert_eq!(retention.dead_ms, Some(7200_000));
    }

    // --- Unique jobs ---

    #[tokio::test]
    async fn unique_enqueue_returns_duplicate_on_conflict() {
        let store = test_store();
        let now = now_millis();

        let opts = || EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("key1");

        let r1 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r1.is_duplicate());

        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, r1.job().id);
    }

    #[tokio::test]
    async fn unique_different_keys_do_not_conflict() {
        let store = test_store();
        let now = now_millis();

        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("a"),
            )
            .await
            .unwrap();
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("b"),
            )
            .await
            .unwrap();

        assert!(!r1.is_duplicate());
        assert!(!r2.is_duplicate());
        assert_ne!(r1.job().id, r2.job().id);
    }

    #[tokio::test]
    async fn unique_queued_scope_allows_after_take() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Queued)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r1.is_duplicate());

        // Take the job (Ready -> InFlight) — should release the lock.
        let taken = store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, r1.job().id);

        // Now a duplicate should be allowed.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r2.is_duplicate());
        assert_ne!(r2.job().id, r1.job().id);
    }

    #[tokio::test]
    async fn unique_active_scope_blocks_while_in_flight() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Active)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r1.is_duplicate());

        // Take (Ready -> InFlight).
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Should still conflict — active scope includes InFlight.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, r1.job().id);
    }

    #[tokio::test]
    async fn unique_active_scope_allows_after_complete() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Active)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Should be allowed now — job is completed, not active.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_exists_scope_blocks_while_completed() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Exists)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Should still conflict — exists scope includes Completed.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_exists_scope_allows_after_purge() {
        let store = test_store_with_retention(0, 0);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Exists)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        // Zero retention — completion immediately deletes the job.
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Job is purged — should be allowed now.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(!r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_stale_index_for_reaped_job_does_not_block() {
        let store = test_store_with_retention(1, 1);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Exists),
            )
            .await
            .unwrap();

        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &r1.job().id).await.unwrap();

        // Purge the job directly.
        store.purge_job(&r1.job().id).await.unwrap();

        // Unique index entry is stale (points to reaped job) — should not block.
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Exists),
            )
            .await
            .unwrap();
        assert!(!r2.is_duplicate());
    }

    #[tokio::test]
    async fn unique_bulk_dedup_within_batch() {
        let store = test_store();
        let now = now_millis();

        let opts = || EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("same");

        let results = store
            .enqueue_bulk(now, vec![opts(), opts(), opts()])
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(!results[0].is_duplicate());
        assert!(results[1].is_duplicate());
        assert!(results[2].is_duplicate());
        assert_eq!(results[1].job().id, results[0].job().id);
        assert_eq!(results[2].job().id, results[0].job().id);
    }

    #[tokio::test]
    async fn unique_queued_scope_restores_on_failure_retry() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let opts = || {
            EnqueueOptions::new("task", "q", serde_json::json!(null))
                .unique_key("k")
                .unique_while(UniqueWhile::Queued)
        };

        let r1 = store.enqueue(now, opts()).await.unwrap();
        let job_id = r1.into_job().id;

        // Take, then fail with retry (Scheduled).
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store
            .record_failure(
                now,
                &job_id,
                FailureOptions {
                    message: "oops".into(),
                    error_type: None,
                    backtrace: None,
                    retry_at: Some(now + 60_000),
                    kill: false,
                },
            )
            .await
            .unwrap();

        // Job is back in Scheduled — unique lock should be restored.
        let r2 = store.enqueue(now, opts()).await.unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, job_id);
    }

    #[tokio::test]
    async fn unique_concurrent_enqueues_produce_one_job() {
        let store = Arc::new(test_store());
        let now = now_millis();

        let mut handles = Vec::new();
        for _ in 0..20 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.enqueue(
                    now,
                    EnqueueOptions::new("task", "q", serde_json::json!(null)).unique_key("race"),
                )
                .await
                .unwrap()
            }));
        }

        let results: Vec<EnqueueResult> = futures_util::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let created: Vec<&EnqueueResult> = results.iter().filter(|r| !r.is_duplicate()).collect();
        assert_eq!(created.len(), 1, "exactly one job should be created");

        // All duplicates should reference the same job.
        let expected_id = &created[0].job().id;
        for r in &results {
            assert_eq!(&r.job().id, expected_id);
        }
    }

    #[tokio::test]
    async fn unique_no_key_has_no_overhead() {
        let store = test_store();
        let now = now_millis();

        // Enqueue two identical jobs without unique_key — both should succeed.
        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        assert!(!r1.is_duplicate());
        assert!(!r2.is_duplicate());
        assert_ne!(r1.job().id, r2.job().id);
    }

    #[tokio::test]
    async fn unique_recover_in_flight_restores_queued_lock() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let r1 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Queued),
            )
            .await
            .unwrap();

        // Take the job (removes unique lock for queued scope).
        store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Simulate crash recovery — in-flight jobs move back to ready.
        let recovered = store.recover_in_flight().await.unwrap();
        assert_eq!(recovered, 1);

        // Unique lock should be restored — duplicate blocked.
        let r2 = store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null))
                    .unique_key("k")
                    .unique_while(UniqueWhile::Queued),
            )
            .await
            .unwrap();
        assert!(r2.is_duplicate());
        assert_eq!(r2.job().id, r1.job().id);
    }

    // --- delete_job ---

    #[tokio::test]
    async fn delete_job_removes_ready_job() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        assert!(store.delete_job(&job.id).await.unwrap());
        assert!(
            store
                .get_job(now_millis(), &job.id)
                .await
                .unwrap()
                .is_none()
        );

        // Should not be takeable.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(taken.is_none());
    }

    #[tokio::test]
    async fn delete_job_removes_scheduled_job() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let future = now_millis() + 60_000;
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        assert!(store.delete_job(&job.id).await.unwrap());
        assert!(
            store
                .get_job(now_millis(), &job.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_job_removes_in_flight_job() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = enqueue_and_take(&store).await;

        assert!(store.delete_job(&job.id).await.unwrap());
        assert!(
            store
                .get_job(now_millis(), &job.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_job_returns_false_for_missing_job() {
        let store = test_store();
        assert!(!store.delete_job("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn delete_job_emits_job_deleted_event() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let mut rx = store.subscribe();
        store.delete_job(&job.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobDeleted { id } => assert_eq!(id, job.id),
            other => panic!("expected JobDeleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_job_does_not_emit_event_for_missing_job() {
        let store = test_store();
        let mut rx = store.subscribe();

        store.delete_job("nonexistent").await.unwrap();

        // No event should be emitted. Use try_recv to check.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn purge_job_does_not_emit_job_deleted_event() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let mut rx = store.subscribe();
        store.purge_job(&job.id).await.unwrap();

        // purge_job should NOT emit events.
        assert!(rx.try_recv().is_err());
    }

    // --- list_jobs with ID filter ---

    #[tokio::test]
    async fn list_jobs_id_filter_returns_matching_jobs() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(now, EnqueueOptions::new("b", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let _j3 = store
            .enqueue(now, EnqueueOptions::new("c", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [j1.id.clone(), j2.id.clone()].into();
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 2);
        let returned_ids: HashSet<String> = page.jobs.iter().map(|j| j.id.clone()).collect();
        assert!(returned_ids.contains(&j1.id));
        assert!(returned_ids.contains(&j2.id));
    }

    #[tokio::test]
    async fn list_jobs_id_filter_skips_nonexistent_ids() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [j1.id.clone(), "0000000000000000000000000".into()].into();
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j1.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_combined_with_status() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();
        let j1 = store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(now, EnqueueOptions::new("b", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take j1 so it becomes InFlight.
        store.take_next_job(now, &HashSet::new()).await.unwrap();

        // Filter for both IDs but only Ready status — should only return j2.
        let ids: HashSet<String> = [j1.id.clone(), j2.id.clone()].into();
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .ids(ids)
                    .statuses([JobStatus::Ready].into())
                    .now(now),
            )
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j2.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_combined_with_payload_filter() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(
                now,
                EnqueueOptions::new("a", "q", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(
                now,
                EnqueueOptions::new("b", "q", serde_json::json!({"x": 2})),
            )
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [j1.id.clone(), j2.id.clone()].into();
        let filter = Arc::new(PayloadFilter::compile(".x == 1").unwrap());
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).filter(filter).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j1.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_skips_nonexistent_with_payload_filter() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(
                now,
                EnqueueOptions::new("a", "q", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [
            j1.id.clone(),
            "0000000000000000000000000".into(), // nonexistent but valid format
        ]
        .into();
        let filter = Arc::new(PayloadFilter::compile(".").unwrap());
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).filter(filter).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j1.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_empty_when_none_match() {
        let store = test_store();
        let ids: HashSet<String> = ["0000000000000000000000000".into()].into();
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).now(now_millis()))
            .await
            .unwrap();

        assert!(page.jobs.is_empty());
    }

    // --- delete_jobs (bulk) ---

    #[tokio::test]
    async fn delete_jobs_by_status() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        // Create 3 jobs: take and complete one, leave two ready.
        for _ in 0..3 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }
        let taken = store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now, &taken.id).await.unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.statuses = [JobStatus::Ready].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        // The completed job should still exist.
        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].status, JobStatus::Completed as u8);
    }

    #[tokio::test]
    async fn delete_jobs_by_queue() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "keep", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "delete_me", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "delete_me", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.queues = ["delete_me".into()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].queue, "keep");
    }

    #[tokio::test]
    async fn delete_jobs_by_ids() {
        let store = test_store();
        let now = now_millis();

        let j1 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j3 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let mut opts = BulkDeleteOptions::new();
        opts.ids = [j1.id.clone(), j3.id.clone()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j2.id);
    }

    #[tokio::test]
    async fn delete_jobs_by_payload_filter() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"x": 2})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"x": 3})),
            )
            .await
            .unwrap();

        let mut opts = BulkDeleteOptions::new();
        opts.filter = Some(Arc::new(PayloadFilter::compile(".x > 1").unwrap()));
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!({"x": 1})));
    }

    #[tokio::test]
    async fn delete_jobs_with_no_filters_deletes_all() {
        let store = test_store();
        let now = now_millis();

        for _ in 0..5 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }

        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 5);

        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn delete_jobs_combined_filters() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        // Two jobs in queue "a", one taken (in-flight), one ready.
        store
            .enqueue(now, EnqueueOptions::new("t", "a", serde_json::json!(null)))
            .await
            .unwrap();
        store
            .enqueue(now, EnqueueOptions::new("t", "a", serde_json::json!(null)))
            .await
            .unwrap();
        // One job in queue "b", ready.
        store
            .enqueue(now, EnqueueOptions::new("t", "b", serde_json::json!(null)))
            .await
            .unwrap();

        // Take one from "a".
        store
            .take_next_job(now, &["a".into()].into())
            .await
            .unwrap();

        // Delete ready jobs in queue "a" only.
        let mut opts = BulkDeleteOptions::new();
        opts.queues = ["a".into()].into();
        opts.statuses = [JobStatus::Ready].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 1);

        // In-flight "a" and ready "b" should remain.
        let page = store
            .list_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
    }

    #[tokio::test]
    async fn delete_jobs_emits_events() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap();
        store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap();

        let mut rx = store.subscribe();
        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 2);

        // Should receive 2 JobDeleted events.
        for _ in 0..2 {
            match rx.recv().await.unwrap() {
                StoreEvent::JobDeleted { .. } => {}
                other => panic!("expected JobDeleted, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn delete_jobs_returns_zero_for_no_matches() {
        let store = test_store();
        let mut opts = BulkDeleteOptions::new();
        opts.queues = ["nonexistent".into()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_jobs_skips_nonexistent_ids() {
        let store = test_store();
        let now = now_millis();

        let j1 = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let mut opts = BulkDeleteOptions::new();
        opts.ids = [j1.id, "0000000000000000000000000".into()].into();
        let count = store.delete_jobs(opts).await.unwrap();
        assert_eq!(count, 1);
    }

    // --- patch_job ---

    #[tokio::test]
    async fn patch_job_updates_retry_limit() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            retry_limit: Some(Some(10)),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.retry_limit, Some(10));
    }

    #[tokio::test]
    async fn patch_job_clears_retry_limit_with_null() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retry_limit(5),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.retry_limit, Some(5));

        let patch = PatchJobOptions {
            retry_limit: Some(None), // clear
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.retry_limit, None);
    }

    #[tokio::test]
    async fn patch_job_updates_backoff() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let new_backoff = BackoffConfig {
            exponent: 3.0,
            base_ms: 5000,
            jitter_ms: 1000,
        };

        let patch = PatchJobOptions {
            backoff: Some(Some(new_backoff.clone())),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let b = result.backoff.unwrap();
        assert_eq!(b.exponent, 3.0);
        assert_eq!(b.base_ms, 5000);
        assert_eq!(b.jitter_ms, 1000);
    }

    #[tokio::test]
    async fn patch_job_updates_retention() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: Some(Some(60_000)),
                dead_ms: Some(Some(120_000)),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, Some(60_000));
        assert_eq!(r.dead_ms, Some(120_000));
    }

    #[tokio::test]
    async fn patch_job_merges_retention_fields() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retention(RetentionConfig {
                    completed_ms: Some(60_000),
                    dead_ms: Some(120_000),
                }),
            )
            .await
            .unwrap()
            .into_job();

        // Patch only dead_ms — completed_ms should be preserved.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: None,
                dead_ms: Some(Some(300_000)),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, Some(60_000));
        assert_eq!(r.dead_ms, Some(300_000));
    }

    #[tokio::test]
    async fn patch_job_retention_merge_into_none() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Job has no retention — patching dead_ms should create retention with only dead_ms.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: None,
                dead_ms: Some(Some(300_000)),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, None);
        assert_eq!(r.dead_ms, Some(300_000));
    }

    #[tokio::test]
    async fn patch_job_clearing_both_retention_fields_clears_retention() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retention(RetentionConfig {
                    completed_ms: Some(60_000),
                    dead_ms: Some(120_000),
                }),
            )
            .await
            .unwrap()
            .into_job();

        // Clear both sub-fields — retention itself should become None.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: Some(None),
                dead_ms: Some(None),
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert!(result.retention.is_none());
    }

    #[tokio::test]
    async fn patch_job_clearing_one_retention_field_preserves_other() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retention(RetentionConfig {
                    completed_ms: Some(60_000),
                    dead_ms: Some(120_000),
                }),
            )
            .await
            .unwrap()
            .into_job();

        // Clear completed_ms, leave dead_ms untouched.
        let patch = PatchJobOptions {
            retention: Some(Some(RetentionConfigPatch {
                completed_ms: Some(None),
                dead_ms: None,
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        let r = result.retention.unwrap();
        assert_eq!(r.completed_ms, None);
        assert_eq!(r.dead_ms, Some(120_000));
    }

    #[tokio::test]
    async fn patch_job_leaves_unmentioned_fields_unchanged() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retry_limit(5),
            )
            .await
            .unwrap()
            .into_job();

        // Patch only backoff — retry_limit should stay.
        let patch = PatchJobOptions {
            backoff: Some(Some(BackoffConfig {
                exponent: 2.0,
                base_ms: 1000,
                jitter_ms: 500,
            })),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.retry_limit, Some(5));
        assert!(result.backoff.is_some());
    }

    #[tokio::test]
    async fn patch_job_rejects_completed_job() {
        let store = test_store_with_retention(60_000, 60_000);
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        store.mark_completed(now, &job.id).await.unwrap();

        let patch = PatchJobOptions {
            retry_limit: Some(Some(10)),
            ..Default::default()
        };

        let err = store.patch_job(now, &job.id, patch).await.unwrap_err();
        assert!(matches!(err, StoreError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn patch_job_returns_none_for_missing_job() {
        let store = test_store();
        let patch = PatchJobOptions::default();
        let result = store
            .patch_job(now_millis(), "nonexistent", patch)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn patch_job_updates_queue() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.queue, "q2");

        // Verify persisted.
        let fetched = store.get_job(now, &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.queue, "q2");
    }

    #[tokio::test]
    async fn patch_job_updates_priority() {
        let store = test_store();
        let now = now_millis();
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let patch = PatchJobOptions {
            priority: Some(100),
            ..Default::default()
        };

        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.priority, 100);
    }

    #[tokio::test]
    async fn patch_job_queue_updates_ready_index() {
        let store = test_store();
        let now = now_millis();

        // Enqueue a job on q1.
        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Move it to q2.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        // Taking from q1 should yield nothing.
        let queues: HashSet<String> = ["q1".into()].into();
        let taken = store.take_next_n_jobs(now, &queues, 1).await.unwrap();
        assert!(taken.is_empty());

        // Taking from q2 should yield the job.
        let queues: HashSet<String> = ["q2".into()].into();
        let taken = store.take_next_n_jobs(now, &queues, 1).await.unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].id, job.id);
    }

    #[tokio::test]
    async fn patch_job_priority_updates_ready_index() {
        let store = test_store();
        let now = now_millis();

        // Enqueue two jobs at different priorities.
        // job_a at priority 100, job_b at priority 200.
        let job_a = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).priority(100),
            )
            .await
            .unwrap()
            .into_job();
        let job_b = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).priority(200),
            )
            .await
            .unwrap()
            .into_job();

        // Before patch: job_a (100) should be taken first.
        // Patch job_b to priority 50, making it higher priority.
        let patch = PatchJobOptions {
            priority: Some(50),
            ..Default::default()
        };
        store.patch_job(now, &job_b.id, patch).await.unwrap();

        // Taking should yield job_b first (priority 50 < 100).
        let taken = store
            .take_next_n_jobs(now, &HashSet::<String>::new(), 2)
            .await
            .unwrap();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].id, job_b.id);
        assert_eq!(taken[1].id, job_a.id);
    }

    #[tokio::test]
    async fn patch_job_queue_on_scheduled_job() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        // Enqueue a scheduled job.
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q1", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        // Move it to q2 while still scheduled.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.queue, "q2");

        // Verify persisted — when it becomes ready it should be on q2.
        let fetched = store.get_job(now, &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.queue, "q2");
    }

    #[tokio::test]
    async fn patch_job_queue_on_inflight_job() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take the job to put it in-flight.
        store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();

        // Move it to q2 while in-flight.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.queue, "q2");
    }

    #[tokio::test]
    async fn patch_job_ready_at_future_moves_ready_to_scheduled() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.status, JobStatus::Ready as u8);

        // Set ready_at to the future → should become Scheduled.
        let patch = PatchJobOptions {
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, future);

        // Should no longer be takeable.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_none());
    }

    #[tokio::test]
    async fn patch_job_clear_ready_at_moves_scheduled_to_ready() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(job.status, JobStatus::Scheduled as u8);

        // Clear ready_at → should become Ready.
        let patch = PatchJobOptions {
            ready_at: Some(None),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Ready as u8);

        // Should now be takeable.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn patch_job_past_ready_at_moves_scheduled_to_ready() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        // Set ready_at to the past → should become Ready.
        let patch = PatchJobOptions {
            ready_at: Some(Some(now - 1000)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Ready as u8);
    }

    #[tokio::test]
    async fn patch_job_updates_scheduled_index_time() {
        let store = test_store();
        let now = now_millis();
        let future1 = now + 60_000;
        let future2 = now + 120_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future1),
            )
            .await
            .unwrap()
            .into_job();

        // Move scheduled time further out.
        let patch = PatchJobOptions {
            ready_at: Some(Some(future2)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, future2);

        // Verify the scheduled count is still 1 (old entry removed, new inserted).
        assert_eq!(store.scheduled_count(), 1);
    }

    #[tokio::test]
    async fn patch_job_ready_at_on_inflight_is_data_only() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take to put in-flight.
        store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();

        // Set ready_at while in-flight — should update data but not change status.
        let patch = PatchJobOptions {
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::InFlight as u8);
        assert_eq!(result.ready_at, future);
    }

    #[tokio::test]
    async fn patch_job_ready_at_and_queue_together() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q1", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Move to q2 and schedule for the future in one patch.
        let patch = PatchJobOptions {
            queue: Some("q2".into()),
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        let result = store.patch_job(now, &job.id, patch).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.queue, "q2");
        assert_eq!(result.ready_at, future);

        // Not takeable now.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_none());

        // Make it ready again via clearing ready_at.
        let patch = PatchJobOptions {
            ready_at: Some(None),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        // Should be on q2.
        let queues: HashSet<String> = ["q2".into()].into();
        let taken = store.take_next_job(now, &queues).await.unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
    }

    #[tokio::test]
    async fn patch_job_emits_scheduled_when_ready_becomes_scheduled() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        let patch = PatchJobOptions {
            ready_at: Some(Some(future)),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobScheduled { id, ready_at }) => {
                assert_eq!(id, job.id);
                assert_eq!(ready_at, future);
            }
            other => panic!("expected JobScheduled, got {:?}", other),
        }
        assert!(matches!(rx.try_recv(), Ok(StoreEvent::JobPatched { .. })));
    }

    #[tokio::test]
    async fn patch_job_emits_created_when_scheduled_becomes_ready() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future = now + 60_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future),
            )
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        let patch = PatchJobOptions {
            ready_at: Some(None),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobCreated { id, queue, .. }) => {
                assert_eq!(id, job.id);
                assert_eq!(queue, "q");
            }
            other => panic!("expected JobCreated, got {:?}", other),
        }
        assert!(matches!(rx.try_recv(), Ok(StoreEvent::JobPatched { .. })));
    }

    #[tokio::test]
    async fn patch_job_emits_scheduled_when_scheduled_time_changes() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();
        let future1 = now + 60_000;
        let future2 = now + 120_000;

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!(null)).ready_at(future1),
            )
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        let patch = PatchJobOptions {
            ready_at: Some(Some(future2)),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobScheduled { id, ready_at }) => {
                assert_eq!(id, job.id);
                assert_eq!(ready_at, future2);
            }
            other => panic!("expected JobScheduled, got {:?}", other),
        }
        assert!(matches!(rx.try_recv(), Ok(StoreEvent::JobPatched { .. })));
    }

    #[tokio::test]
    async fn patch_job_no_status_event_when_status_unchanged() {
        let store = test_store();
        let mut rx = store.subscribe();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Drain enqueue events.
        while rx.try_recv().is_ok() {}

        // Patch only priority — no status change, only JobPatched.
        let patch = PatchJobOptions {
            priority: Some(100),
            ..Default::default()
        };
        store.patch_job(now, &job.id, patch).await.unwrap();

        match rx.try_recv() {
            Ok(StoreEvent::JobPatched { id }) => assert_eq!(id, job.id),
            other => panic!("expected JobPatched, got {:?}", other),
        }
        assert!(rx.try_recv().is_err(), "expected no further events");
    }

    // --- bulk patch_jobs tests ---

    #[tokio::test]
    async fn patch_jobs_updates_matching_jobs() {
        let store = test_store();
        let now = now_millis();

        for i in 0..3 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("t", "q1", serde_json::json!({"i": i})),
                )
                .await
                .unwrap();
        }

        let opts = BulkPatchOptions {
            ids: HashSet::new(),
            statuses: HashSet::new(),
            queues: ["q1".into()].into(),
            types: HashSet::new(),
            filter: None,
            patch: PatchJobOptions {
                queue: Some("q2".into()),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 3);

        // Verify all jobs are on q2 now.
        let page = store
            .list_jobs(ListJobsOptions::new().queues(["q2".into()].into()))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
    }

    #[tokio::test]
    async fn patch_jobs_skips_terminal_jobs() {
        let store = test_store();
        let now = now_millis();

        let job = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take and complete the job.
        store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        store.mark_completed(now, &job.id).await.unwrap();

        let opts = BulkPatchOptions {
            ids: [job.id.clone()].into(),
            statuses: HashSet::new(),
            queues: HashSet::new(),
            types: HashSet::new(),
            filter: None,
            patch: PatchJobOptions {
                priority: Some(99),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn patch_jobs_status_transitions() {
        let store = test_store();
        let now = now_millis();
        let future = now + 60_000;

        // Enqueue 2 ready jobs.
        for _ in 0..2 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }

        // Patch them all to scheduled.
        let opts = BulkPatchOptions {
            ids: HashSet::new(),
            statuses: HashSet::new(),
            queues: HashSet::new(),
            types: HashSet::new(),
            filter: None,
            patch: PatchJobOptions {
                ready_at: Some(Some(future)),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 2);

        // Nothing should be takeable.
        let taken = store
            .take_next_job(now, &HashSet::<String>::new())
            .await
            .unwrap();
        assert!(taken.is_none());

        assert_eq!(store.scheduled_count(), 2);
    }

    #[tokio::test]
    async fn patch_jobs_with_id_filter() {
        let store = test_store();
        let now = now_millis();

        let job_a = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let _job_b = store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Only patch job_a.
        let opts = BulkPatchOptions {
            ids: [job_a.id.clone()].into(),
            statuses: HashSet::new(),
            queues: HashSet::new(),
            types: HashSet::new(),
            filter: None,
            patch: PatchJobOptions {
                priority: Some(99),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 1);

        let fetched = store.get_job(now, &job_a.id).await.unwrap().unwrap();
        assert_eq!(fetched.priority, 99);
    }

    #[tokio::test]
    async fn patch_jobs_returns_zero_for_no_matches() {
        let store = test_store();

        let opts = BulkPatchOptions {
            ids: HashSet::new(),
            statuses: HashSet::new(),
            queues: ["nonexistent".into()].into(),
            types: HashSet::new(),
            filter: None,
            patch: PatchJobOptions {
                priority: Some(99),
                ..Default::default()
            },
        };

        let count = store.patch_jobs(opts).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn list_queues_returns_distinct_names() {
        let store = test_store();
        let now = now_millis();

        // Enqueue jobs on 3 different queues.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "alpha", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "beta", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "gamma", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let queues = store.list_queues().await.unwrap();
        assert_eq!(queues, vec!["alpha", "beta", "gamma"]);

        // Enqueue more jobs on existing queues — count should not change.
        store
            .enqueue(
                now,
                EnqueueOptions::new("other", "alpha", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("other", "beta", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let queues = store.list_queues().await.unwrap();
        assert_eq!(queues, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn list_queues_empty_store() {
        let store = test_store();
        let queues = store.list_queues().await.unwrap();
        assert!(queues.is_empty());
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

    /// Regression test: enqueue to queue X, delete all, enqueue to queue X
    /// again, then take with a queue filter. The filtered take must deliver
    /// the new jobs. Previously, `delete_jobs` only removed entries from the
    /// global ready index but not the per-queue index, leaving orphaned
    /// entries that caused the filtered take to spin in an infinite loop.
    #[tokio::test]
    async fn take_filtered_works_after_delete_and_reenqueue() {
        let store = test_store();
        let now = now_millis();
        let queues: HashSet<String> = ["myqueue".to_string()].into();

        // 1. Enqueue a job to the target queue.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("first")),
            )
            .await
            .unwrap();

        // 2. Delete all jobs (unscoped).
        let deleted = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(deleted, 1);

        // 3. Enqueue a new job to the same queue.
        let enqueued = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("second")),
            )
            .await
            .unwrap()
            .into_job();

        // 4. Filtered take must return the new job.
        let taken = store.take_next_job(now, &queues).await.unwrap();
        assert!(taken.is_some(), "filtered take should return the new job");
        assert_eq!(taken.unwrap().id, enqueued.id);
    }

    /// Same as above but with a queue-scoped delete instead of unscoped.
    #[tokio::test]
    async fn take_filtered_works_after_scoped_delete_and_reenqueue() {
        let store = test_store();
        let now = now_millis();
        let queues: HashSet<String> = ["myqueue".to_string()].into();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("first")),
            )
            .await
            .unwrap();

        let mut delete_opts = BulkDeleteOptions::new();
        delete_opts.queues = ["myqueue".to_string()].into();
        let deleted = store.delete_jobs(delete_opts).await.unwrap();
        assert_eq!(deleted, 1);

        let enqueued = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("second")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store.take_next_job(now, &queues).await.unwrap();
        assert!(taken.is_some(), "filtered take should return the new job");
        assert_eq!(taken.unwrap().id, enqueued.id);
    }
}
