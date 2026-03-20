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
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crossbeam_skiplist::{SkipMap, SkipSet};
use dashmap::DashMap;

use fjall::config::{FilterPolicy, PinningPolicy};
use fjall::{PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace, Slice};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::task;

/// Error type returned by store operations.
#[derive(Debug)]
pub enum StoreError {
    /// The underlying database (fjall) returned an error.
    Db(fjall::Error),

    /// A job could not be serialized.
    Serialize(rmp_serde::encode::Error),

    /// A job could not be deserialized.
    Deserialize(rmp_serde::decode::Error),

    /// A blocking task was cancelled or panicked.
    TaskJoin(task::JoinError),

    /// Internal data inconsistency (e.g. queue references a missing job).
    Corruption(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Db(e) => write!(f, "database error: {e}"),
            StoreError::Serialize(e) => write!(f, "serialization error: {e}"),
            StoreError::Deserialize(e) => write!(f, "deserialization error: {e}"),
            StoreError::TaskJoin(e) => write!(f, "blocking task failed: {e}"),
            StoreError::Corruption(msg) => write!(f, "data corruption: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<fjall::Error> for StoreError {
    fn from(e: fjall::Error) -> Self {
        StoreError::Db(e)
    }
}

impl From<rmp_serde::encode::Error> for StoreError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        StoreError::Serialize(e)
    }
}

impl From<rmp_serde::decode::Error> for StoreError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        StoreError::Deserialize(e)
    }
}

impl From<task::JoinError> for StoreError {
    fn from(e: task::JoinError) -> Self {
        StoreError::TaskJoin(e)
    }
}

/// Lifecycle status of a job.
///
/// Stored as a `u8` in the job record. The enum provides type-safe access
/// in Rust code while keeping storage compact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum JobStatus {
    /// The job is in the store, but is scheduled to be queued at a later time.
    Scheduled = 0,

    /// The job is in the priority queue ready to be worked.
    Ready = 1,

    /// The job has been delivered to a client and is in-flight.
    InFlight = 2,

    /// The job was successfully completed by a worker.
    Completed = 3,

    /// The job failed too many times and exhausted its retry policy.
    Dead = 4,
}

impl From<JobStatus> for u8 {
    fn from(s: JobStatus) -> Self {
        s as u8
    }
}

impl TryFrom<u8> for JobStatus {
    type Error = u8;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Scheduled),
            1 => Ok(Self::Ready),
            2 => Ok(Self::InFlight),
            3 => Ok(Self::Completed),
            4 => Ok(Self::Dead),
            other => Err(other),
        }
    }
}

/// A job stored in the queue keyspace.
///
/// Jobs are identified using scru128 because it is time-sequenced and high
/// entropy.
///
/// Serialized field names are reduced to single letters to save space while
/// permitting schema evolution. It's marginal relative to the payload sizes
/// but it's an easy reduction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique job identifier (scru128).
    #[serde(rename = "I")]
    pub id: String,

    /// Job type, e.g. "send_email" or "generate_report".
    #[serde(rename = "T")]
    pub job_type: String,

    /// Queue this job belongs to.
    #[serde(rename = "Q")]
    pub queue: String,

    /// Priority (lower number = higher priority).
    #[serde(rename = "N")]
    pub priority: u16,

    /// Arbitrary payload provided by the client.
    ///
    /// Stored in a separate `payloads` keyspace so that status-change
    /// operations (take, requeue, promote, recover) only re-write the small
    /// metadata record. `None` when the payload has not been hydrated (i.e.
    /// when reading metadata only from the `jobs` keyspace).
    #[serde(rename = "P")]
    #[serde(default)]
    pub payload: Option<serde_json::Value>,

    /// Current lifecycle status, stored as a u8 which converts to `JobStatus`.
    #[serde(rename = "S")]
    pub status: u8,

    /// When the job becomes eligible to run (milliseconds since Unix epoch).
    #[serde(rename = "r")]
    #[serde(default)]
    pub ready_at: u64,

    /// Number of times this job has failed (incremented on each failure).
    #[serde(rename = "a")]
    #[serde(default)]
    pub attempts: u32,

    /// Per-job override for maximum retries on failure before the job is killed.
    /// When `None`, the server default applies at failure time.
    /// `retry_limit = Some(0)` means the first failure kills (1 total run).
    /// `retry_limit = Some(3)` means kills after 4th failure (1 original + 3 retries).
    #[serde(rename = "l")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_limit: Option<u32>,

    /// Per-job backoff configuration override. When `None`, server defaults
    /// are used at backoff computation time. All three parameters form a
    /// single curve — if you override one, you override all three.
    #[serde(rename = "b")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<BackoffConfig>,

    /// When the job was last dequeued (milliseconds since Unix epoch).
    /// Overwritten on each dequeue.
    #[serde(rename = "d")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dequeued_at: Option<u64>,

    /// When the job last failed (milliseconds since Unix epoch).
    /// Overwritten on each failure.
    #[serde(rename = "f")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u64>,

    /// Per-job retention configuration override. When `None`, server
    /// defaults are used at completion/death time.
    #[serde(rename = "e")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionConfig>,

    /// When the reaper should hard-delete this job (milliseconds since
    /// Unix epoch). Set when a job transitions to Completed or Dead.
    #[serde(rename = "p")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purge_at: Option<u64>,

    /// When the job was completed (milliseconds since Unix epoch).
    #[serde(rename = "c")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
}

/// Retention configuration for completed and dead jobs.
///
/// Controls how long completed and dead jobs remain visible in the API
/// before the reaper purges them. When `None` on a job, server defaults
/// apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Retention period for completed jobs (milliseconds). When `None`, server default applies.
    #[serde(rename = "c")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_ms: Option<u64>,

    /// Retention period for dead jobs (milliseconds). When `None`, server default applies.
    #[serde(rename = "d")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_ms: Option<u64>,
}

/// Backoff curve parameters for retry delay calculation.
///
/// The three parameters define a single curve:
///   `delay_ms = attempts^exponent + base_ms + rand(0..jitter_ms) * (attempts + 1)`
///
/// These are grouped into a struct because overriding one in isolation
/// doesn't make sense — the parameters work together  to shape the curve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    /// Power curve steepness (attempts ** exponent).
    #[serde(rename = "x")]
    pub exponent: f32,

    /// Minimum delay in milliseconds.
    #[serde(rename = "b")]
    pub base_ms: u32,

    /// Max random milliseconds per attempt multiplier (0..jitter_ms).
    #[serde(rename = "j")]
    pub jitter_ms: u32,
}

/// An error record stored in the `errors` keyspace.
///
/// One record is written per failure, append-only.
/// Key: `{job_id}\0{attempt_be_u32}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRecord {
    /// Error message from the worker.
    #[serde(rename = "m")]
    pub message: String,

    /// Error class, e.g. "TimeoutError".
    #[serde(rename = "t")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,

    /// Stack trace / backtrace.
    #[serde(rename = "b")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<String>,

    /// Copied from the job's `dequeued_at`. Preserves dequeue -> fail latency.
    #[serde(rename = "d")]
    pub dequeued_at: u64,

    /// Copied from the job's `failed_at` timestamp.
    #[serde(rename = "f")]
    pub failed_at: u64,

    /// Which attempt this error corresponds to (1-based).
    ///
    /// Always overwritten from the key when reading via `list_errors`, so the
    /// stored value is for forward-compat only. Old records without this field
    /// deserialize as 0 thanks to `#[serde(default)]`.
    #[serde(rename = "a")]
    #[serde(default)]
    pub attempt: u32,
}

/// Direction for scanning the jobs keyspace when listing jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    /// Oldest first (ascending by job ID).
    Asc,

    /// Newest first (descending by job ID).
    Desc,
}

impl ScanDirection {
    /// Return the opposite direction.
    pub fn reverse(self) -> Self {
        match self {
            Self::Asc => Self::Desc,
            Self::Desc => Self::Asc,
        }
    }
}

/// Options for listing jobs with cursor-based pagination in `Store::list_jobs`.
#[derive(Debug, Clone)]
pub struct ListJobsOptions {
    /// Cursor: start after this job ID (exclusive). `None` means start from
    /// the beginning (asc) or end (desc).
    pub from: Option<String>,

    /// Scan direction.
    pub direction: ScanDirection,

    /// Maximum number of jobs to return.
    pub limit: usize,

    /// Optional status filter. When non-empty, only jobs with one of these
    /// statuses are returned, using the status index for efficient lookup.
    pub statuses: HashSet<JobStatus>,

    /// Optional queue filter. When non-empty, only jobs belonging to one of
    /// these queues are returned, using the queue index.
    pub queues: HashSet<String>,

    /// Optional type filter. When non-empty, only jobs with one of these
    /// types are returned, using the type index.
    pub types: HashSet<String>,

    /// Current time for filtering out expired jobs (purge_at <= now).
    /// When `Some`, jobs past their purge_at are skipped.
    pub now: Option<u64>,
}

impl ListJobsOptions {
    /// Create default options (ascending, no cursor, no filters, limit 50).
    pub fn new() -> Self {
        Self {
            from: None,
            direction: ScanDirection::Asc,
            limit: 50,
            statuses: HashSet::new(),
            queues: HashSet::new(),
            types: HashSet::new(),
            now: None,
        }
    }

    /// Set the cursor from which to paginate and return `self`.
    pub fn from(mut self, cursor: impl Into<String>) -> Self {
        self.from = Some(cursor.into());
        self
    }

    /// Set the direction in which to paginate and return `self`.
    pub fn direction(mut self, direction: ScanDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Set the maximum number of records to return and return `self`.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Filter by job status and return `self`.
    ///
    /// An empty set means "all statuses".
    pub fn statuses(mut self, statuses: HashSet<JobStatus>) -> Self {
        self.statuses = statuses;
        self
    }

    /// Filter by queue membership and return `self`.
    ///
    /// An empty set means "all queues".
    pub fn queues(mut self, queues: HashSet<String>) -> Self {
        self.queues = queues;
        self
    }

    /// Filter by job type and return `self`.
    ///
    /// An empty set means "all types".
    pub fn types(mut self, types: HashSet<String>) -> Self {
        self.types = types;
        self
    }

    /// Set the current time for filtering expired jobs and return `self`.
    pub fn now(mut self, now: u64) -> Self {
        self.now = Some(now);
        self
    }
}

/// A page of jobs returned by `Store::list_jobs`.
#[derive(Debug)]
pub struct ListJobsPage {
    /// The jobs on this page.
    pub jobs: Vec<Job>,

    /// Options to fetch the next page, or `None` if this is the last page.
    pub next: Option<ListJobsOptions>,

    /// Options to fetch the previous page, or `None` if this is the first page.
    pub prev: Option<ListJobsOptions>,
}

/// Options for listing error records with cursor-based pagination.
#[derive(Debug, Clone)]
pub struct ListErrorsOptions {
    /// Job ID whose errors to list.
    pub job_id: String,

    /// Cursor: start after this attempt number (exclusive). `None` means start
    /// from the beginning (asc) or end (desc).
    pub from: Option<u32>,

    /// Scan direction.
    pub direction: ScanDirection,

    /// Maximum number of error records to return.
    pub limit: usize,
}

impl ListErrorsOptions {
    /// Create default options for the given job (ascending, no cursor, limit 50).
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            from: None,
            direction: ScanDirection::Asc,
            limit: 50,
        }
    }

    /// Set the cursor from which to paginate and return `self`.
    pub fn from(mut self, cursor: u32) -> Self {
        self.from = Some(cursor);
        self
    }

    /// Set the direction in which to paginate and return `self`.
    pub fn direction(mut self, direction: ScanDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Set the maximum number of records to return and return `self`.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// A page of error records returned by `Store::list_errors`.
#[derive(Debug)]
pub struct ListErrorsPage {
    /// The error records on this page.
    pub errors: Vec<ErrorRecord>,

    /// Options to fetch the next page, or `None` if this is the last page.
    pub next: Option<ListErrorsOptions>,

    /// Options to fetch the previous page, or `None` if this is the first page.
    pub prev: Option<ListErrorsOptions>,
}

/// Options for enqueuing a new job.
pub struct EnqueueOptions {
    /// Job type, e.g. "send_email" or "generate_report".
    pub job_type: String,

    /// Queue this job belongs to.
    pub queue: String,

    /// Arbitrary payload provided by the client.
    pub payload: serde_json::Value,

    /// Priority (lower number = higher priority). Defaults to 0.
    pub priority: u16,

    /// When the job becomes eligible to run (milliseconds since Unix epoch).
    /// `None` means immediately.
    pub ready_at: Option<u64>,

    /// Per-job override for maximum retries. When `None`, the server
    /// default applies at failure time.
    pub retry_limit: Option<u32>,

    /// Per-job backoff configuration override. When `None`, server defaults
    /// apply at failure time.
    pub backoff: Option<BackoffConfig>,

    /// Per-job retention configuration override. When `None`, server
    /// defaults are used at completion/death time.
    pub retention: Option<RetentionConfig>,
}

impl EnqueueOptions {
    /// Create options with required fields; priority defaults to 0, ready_at
    /// to now, all retry/backoff fields to None (server defaults apply).
    pub fn new(
        job_type: impl Into<String>,
        queue: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            job_type: job_type.into(),
            queue: queue.into(),
            payload,
            priority: 0,
            ready_at: None,
            retry_limit: None,
            backoff: None,
            retention: None,
        }
    }

    /// Set the job priority and return `self`.
    pub fn priority(mut self, priority: u16) -> Self {
        self.priority = priority;
        self
    }

    /// Set the ready_at timestamp and return `self`.
    pub fn ready_at(mut self, ready_at: u64) -> Self {
        self.ready_at = Some(ready_at);
        self
    }

    /// Set the retry limit and return `self`.
    pub fn retry_limit(mut self, retry_limit: u32) -> Self {
        self.retry_limit = Some(retry_limit);
        self
    }

    /// Set the backoff configuration and return `self`.
    pub fn backoff(mut self, backoff: BackoffConfig) -> Self {
        self.backoff = Some(backoff);
        self
    }

    /// Set the retention configuration and return `self`.
    pub fn retention(mut self, retention: RetentionConfig) -> Self {
        self.retention = Some(retention);
        self
    }
}

/// Options for recording a job failure.
pub struct FailureOptions {
    /// Error message from the worker.
    pub message: String,

    /// Error class, e.g. "TimeoutError".
    pub error_type: Option<String>,

    /// Stack trace / backtrace.
    pub backtrace: Option<String>,

    /// Force retry at a specific time, even if max retries exceeded.
    pub retry_at: Option<u64>,

    /// Kill the job immediately, regardless of retry limit.
    ///
    /// A false value does not mean "prevent default killing". If the job
    /// should be retried, pass a future dated retry_at value.
    pub kill: bool,
}

/// Result of a bulk completion operation.
pub struct BulkCompleteResult {
    /// IDs that were successfully marked as completed.
    pub completed: Vec<String>,
    /// IDs that were not found in the in-flight set.
    pub not_found: Vec<String>,
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

    /// Recovery and index rebuilding completed.
    ///
    /// Emitted once when `rebuild_indexes()` finishes. Wakes sleeping
    /// workers (so they attempt a drain) and the scheduler (so it polls
    /// immediately for due jobs).
    IndexRebuilt,
}

/// In-memory priority index for ready jobs (lock-free).
///
/// The ready index is inherently ephemeral — jobs live in it for milliseconds
/// under normal throughput — which is worst-case for LSM trees (high tombstone
/// churn causes scan latency spikes tied to compaction cycles). Skip lists give
/// O(log n) lookups without a global mutex, so enqueue (insert) and take
/// (remove) can operate on the index concurrently. The authoritative source of
/// data remains in the database; this index is rebuilt on startup via
/// `Store::rebuild_ready_index` and then kept in sync at runtime.
///
/// Keys are `(priority, job_id)`. `Ord` on `(u16, String)` gives priority ASC,
/// then job_id ASC — exactly the ordering needed (FIFO within same priority,
/// since scru128 IDs are lexicographically time-ordered).
struct ReadyIndex {
    /// Global prioritised job index.
    ///
    /// All claims to take a job use this index as the authoritative source of
    /// truth. If removal from this set fails (CAS operation under the hood)
    /// then a new candidate must be selected.
    ///
    /// Key: `(priority, job_id)`, Value: `queue_name`.
    /// The value stores the queue name so that after `remove()` we know which
    /// per-queue set to clean up.
    global: SkipMap<(u16, String), String>,

    /// Per-queue indexes — for queue-filtered candidate selection.
    ///
    /// `DashMap` provides concurrent access to the `queue -> SkipSet` mapping
    /// without a global lock.
    ///
    /// Entries are only removed from this set if they were successfully
    /// claimed in the global set. No entries should exist in the global set
    /// that do not exist in the per-queue set (insert order is per-queue first
    /// then global).
    by_queue: DashMap<String, SkipSet<(u16, String)>>,
}

impl ReadyIndex {
    fn new() -> Self {
        Self {
            global: SkipMap::new(),
            by_queue: DashMap::new(),
        }
    }

    /// Insert a job into both the per-queue and global indexes.
    ///
    /// Inserts into `by_queue` first, then `global`. This ordering
    /// guarantees that any entry visible in `global` is already in
    /// `by_queue`, so the unfiltered `remove()` path's by_queue cleanup always
    /// finds the entry. The filtered path may briefly peek a by_queue entry
    /// whose global counterpart isn't inserted yet — `global.get()` returns
    /// `None` and it retries harmlessly.
    fn insert(&self, queue: &str, priority: u16, job_id: String) {
        let entry = (priority, job_id);
        self.by_queue
            .entry(queue.to_string())
            .or_insert_with(SkipSet::new)
            .insert(entry.clone());
        self.global.insert(entry, queue.to_string());
    }

    /// Atomically claim the highest-priority (lowest number), oldest ready job.
    ///
    /// Returns `(priority, job_id, queue)` if a job was claimed, or `None` if
    /// the index is empty (or empty for the requested queues).
    ///
    /// Both paths use the same CAS pattern: find a candidate entry, call
    /// `Entry::remove()` (only one thread wins the CAS), and retry on failure.
    ///
    /// - Unfiltered (queues is empty): peeks `global.front()` directly.
    /// - Queue-filtered: peeks each target queue's SkipSet front, picks
    ///   the minimum, then looks up the global entry via `get()`.
    ///
    /// Important: `SkipMap::remove(&key)` is *not* safe as a claim
    /// mechanism — it returns `Some(entry)` to ALL concurrent callers that
    /// find the node, regardless of who actually marked it. Only
    /// `Entry::remove()` (which returns `bool` from the CAS) is safe.
    fn claim(&self, queues: &HashSet<String>) -> Option<(u16, String, String)> {
        loop {
            // Find the best candidate. Unfiltered peeks global directly;
            // filtered peeks per-queue sets and resolves via global.get().
            // front() and get() both skip marked (already-claimed) nodes.
            let candidate = if queues.is_empty() {
                // If we're just taking a job from any queue, it's just
                // whatever is at the front of the global queue.
                let entry = self.global.front()?; // ?: or return None
                let key = entry.key().clone();
                let queue = entry.value().clone();
                Some((entry, key, queue))
            } else {
                // If we're taking a job from a specific set of queues, we need
                // to find the first job on each of those queues and then
                // select the highest priority/earliest job ID.
                let (priority, job_id, queue) = queues
                    .iter()
                    .filter_map(|q| {
                        let set = self.by_queue.get(q)?;
                        let front = set.front()?;
                        let (priority, job_id) = front.value().clone();
                        Some((priority, job_id, q.clone()))
                    })
                    .min()?; // ?: or return None

                // Look up the global entry for the CAS. Returns None if:
                // (a) not yet in global (insert race — by_queue is populated
                //     first, global second), or
                // (b) already claimed and marked by another thread.
                // In either case, don't touch by_queue — the insert will
                // complete shortly (a), or the winner will clean up (b).
                let key = (priority, job_id);
                self.global.get(&key).map(|entry| (entry, key, queue))
            };

            // Claim via Entry::remove() — CAS, only one thread wins.
            if let Some((entry, (priority, job_id), queue)) = candidate {
                if entry.remove() {
                    // Won the claim — clean up the per-queue set.
                    if let Some(set) = self.by_queue.get(&queue) {
                        set.remove(&(priority, job_id.clone()));
                    }
                    return Some((priority, job_id, queue));
                }
            } // else loop again
        }
    }
}

/// In-memory chronological index of scheduled jobs.
///
/// Keeps a `SkipSet<(u64, String)>` ordered by `(ready_at, job_id)`.
/// The tuple's derived `Ord` gives chronological ordering (ready_at ASC),
/// then FIFO within the same timestamp (scru128 IDs are lexicographically
/// time-ordered).
///
/// Lock-free — uses `crossbeam-skiplist` internally. The scheduler is the
/// sole consumer of `next_due`/`remove`, so there's no concurrent claim
/// contention; producers (`enqueue`, `record_failure`) only call `insert`.
struct ScheduledIndex {
    entries: SkipSet<(u64, String)>,
}

impl ScheduledIndex {
    fn new() -> Self {
        Self {
            entries: SkipSet::new(),
        }
    }

    fn insert(&self, ready_at: u64, job_id: String) {
        self.entries.insert((ready_at, job_id));
    }

    fn remove(&self, ready_at: u64, job_id: &str) {
        self.entries.remove(&(ready_at, job_id.to_string()));
    }

    /// Collect entries where `ready_at <= now`, up to `limit`.
    ///
    /// Returns `(due_entries, next_ready_at)`:
    /// - `due_entries`: Vec of `(ready_at, job_id)` tuples that are due now.
    /// - `next_ready_at`: The `ready_at` of the first future entry, if any.
    ///   `None` if the batch limit was hit (caller should loop immediately)
    ///   or if there are no more scheduled jobs at all.
    fn next_due(&self, now: u64, limit: usize) -> (Vec<(u64, String)>, Option<u64>) {
        let mut due = Vec::new();
        for entry in self.entries.iter() {
            let (ready_at, job_id) = entry.value();
            if *ready_at > now {
                // This entry (and everything after) is in the future.
                return (due, Some(*ready_at));
            }
            if due.len() >= limit {
                // Hit the batch cap but there are more due entries.
                // Return without a next_ready_at so the caller loops immediately.
                return (due, None);
            }
            due.push((*ready_at, job_id.clone()));
        }
        // Exhausted the index — no more scheduled jobs at all.
        (due, None)
    }
}

/// Provides a handle to the persistent store.
#[derive(Clone)]
pub struct Store {
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

/// Result type sent through the group commit oneshot channel.
/// `Ok(())` means durable; `Err(msg)` means the persist failed.
type SyncResult = Result<(), String>;

/// A waiter queued for group commit. The background thread notifies the
/// oneshot when the waiter's batch is durable (or failed).
struct CommitWaiter {
    sequence: u64,
    mode: CommitMode,
    done: tokio::sync::oneshot::Sender<SyncResult>,
}

/// Batches multiple journal persists into a single fsync.
///
/// Each `tx.commit()` writes to an in-memory BufWriter. Callers then call
/// `persist()` which enqueues a waiter and returns a oneshot receiver. A
/// dedicated OS thread drains waiters, calls `db.persist()` once per batch,
/// and notifies all waiters whose sequence was covered.
struct GroupCommitter {
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
    fn start(db: SingleWriterTxDatabase, data_dir: std::path::PathBuf) -> Self {
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
    fn persist(&self, mode: CommitMode) -> tokio::sync::oneshot::Receiver<SyncResult> {
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

/// One-byte tag prefix for records in the `data` keyspace.
///
/// Each record kind occupies a disjoint prefix range so that jobs, payloads,
/// and errors coexist in the same fjall keyspace without collision.
#[repr(u8)]
enum RecordKind {
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

    /// Commit a transaction and enqueue a group persist.
    ///
    /// Returns the oneshot receiver that resolves with `Ok(())` when
    /// durable, or `Err(msg)` if the persist failed.
    fn commit(
        &self,
        tx: fjall::SingleWriterWriteTx<'_>,
        mode: CommitMode,
    ) -> Result<tokio::sync::oneshot::Receiver<SyncResult>, StoreError> {
        tx.commit()?;
        Ok(self.group_committer.persist(mode))
    }
}

/// Await an optional group commit sync, returning the value on success.
///
/// All `spawn_blocking` closures return `(T, Option<Receiver>)` — `Some`
/// when a commit happened, `None` on early-return paths. This helper
/// unwraps the `Result`, awaits the sync if present, and returns `T`.
async fn await_sync<T>(
    result: Result<(T, Option<tokio::sync::oneshot::Receiver<SyncResult>>), StoreError>,
) -> Result<T, StoreError> {
    let (value, rx) = result?;
    if let Some(rx) = rx {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                return Err(StoreError::Db(fjall::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    msg,
                ))));
            }
            Err(_) => {
                return Err(StoreError::Db(fjall::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "group committer channel closed",
                ))));
            }
        }
    }
    Ok(value)
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

/// Look up full job records from an iterator of raw IDs, returning an error
/// if any ID is missing from the `data` keyspace (data corruption). Hydrates
/// each job's payload from the same keyspace using the `P` tag prefix.
///
/// Reads are performed through the provided `Readable` (typically a snapshot)
/// to ensure consistency with the index scan that produced the IDs.
///
/// When `now` is `Some`, jobs with `purge_at <= now` are skipped (logically
/// invisible before the reaper physically deletes them). The iterator
/// continues scanning past expired jobs until `limit` entries are collected
/// or the stream is exhausted.
fn load_jobs_by_ids(
    reader: &impl Readable,
    data_ks: &SingleWriterTxKeyspace,
    ids: impl Iterator<Item = Result<Vec<u8>, StoreError>>,
    source: &str,
    now: Option<u64>,
    limit: usize,
) -> Result<Vec<Job>, StoreError> {
    let mut rows = Vec::new();
    for id in ids {
        let id = id?;
        let job_key = make_job_key(
            std::str::from_utf8(&id)
                .map_err(|e| StoreError::Corruption(format!("job ID is not valid UTF-8: {e}")))?,
        );
        let bytes = reader.get(data_ks, &job_key)?.ok_or_else(|| {
            StoreError::Corruption(format!(
                "job in {source} but missing from data keyspace: {:?}",
                String::from_utf8_lossy(&id),
            ))
        })?;
        let mut job: Job = rmp_serde::from_slice(&bytes)?;

        // Skip expired jobs (logically invisible).
        if let Some(now) = now {
            if job.purge_at.is_some_and(|p| p <= now) {
                continue;
            }
        }

        // Hydrate the payload from the data keyspace.
        let payload_key = make_payload_key(&job.id);
        if let Some(payload_bytes) = reader.get(data_ks, &payload_key)? {
            job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
        }

        rows.push(job);
        if rows.len() >= limit {
            break;
        }
    }
    Ok(rows)
}

/// Controls whether the group committer does a full fsync after each batch.
///
/// - `Buffered`: only flushes the BufWriter to the OS page cache. Survives
///   process crashes (data is in the kernel page cache) but NOT OS/power
///   failure. Very fast — limited only by lock contention.
/// - `Fsync`: flushes to page cache then fsyncs journal files via a second
///   fd. Survives OS/power failure. Throughput is bounded by disk fsync
///   latency (~3-5 ms on macOS NVMe, <1 ms on Linux).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitMode {
    #[default]
    Buffered,
    Fsync,
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
            cache_size: env_parse("ZIZQ_CACHE_SIZE")?.unwrap_or(defaults.cache_size),
            data_table_size: env_parse("ZIZQ_DATA_TABLE_SIZE")?.unwrap_or(defaults.data_table_size),
            index_table_size: env_parse("ZIZQ_INDEX_TABLE_SIZE")?
                .unwrap_or(defaults.index_table_size),
            journal_size: env_parse("ZIZQ_JOURNAL_SIZE")?.unwrap_or(defaults.journal_size),
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

/// Error returned when an environment variable is set but cannot be parsed.
#[derive(Debug)]
pub struct EnvConfigError {
    /// The environment variable name.
    pub name: String,
    /// The raw value that failed to parse.
    pub value: String,
}

impl fmt::Display for EnvConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid value for {}: {:?}", self.name, self.value,)
    }
}

impl std::error::Error for EnvConfigError {}

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
    pub async fn enqueue(&self, now: u64, opts: EnqueueOptions) -> Result<Job, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            let id = scru128::new_string();

            // Read the `ready_at` timestamp, defaulting to now if not present.
            let ready_at = opts.ready_at.unwrap_or(now);

            // We need to check this in a couple of spots: if the job is ready
            // now, it goes into the Ready status, otherwise it goes into the
            // Scheduled status.
            let scheduled = ready_at > now;

            let status = if scheduled {
                JobStatus::Scheduled
            } else {
                JobStatus::Ready
            };

            // Serialize the payload separately — it goes into its own keyspace
            // so that status-change operations never re-write it.
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
            };

            // Serialize metadata without the payload for the jobs keyspace.
            let mut meta = job.clone();
            meta.payload = None;
            let meta_bytes = rmp_serde::to_vec_named(&meta)?;

            let job_key = make_job_key(&id);
            let payload_key = make_payload_key(&id);
            let queue_key = make_queue_key(&job.queue, &id);
            let status_key = make_status_key(status, &id);
            let type_key = make_type_key(&job.job_type, &id);

            let mut tx = ks.write_tx();
            tx.insert(&ks.data, &job_key, &meta_bytes);
            tx.insert(&ks.data, &payload_key, &payload_bytes);
            tx.insert(&ks.index, &queue_key, b"");
            tx.insert(&ks.index, &status_key, b"");
            tx.insert(&ks.index, &type_key, b"");

            let sync = ks.commit(tx, ks.enqueue_commit_mode)?;

            // Insert into the in-memory indexes after commit succeeds.
            // Ready jobs go into the ready index; scheduled jobs go into the
            // scheduled index (promoted later by the scheduler).
            if scheduled {
                scheduled_index.insert(ready_at, id.clone());
            } else {
                ready_index.insert(&job.queue, job.priority, job.id.clone());
            }

            Ok((job, Some(sync)))
        })
        .await?;

        let job = await_sync(result).await?;

        match JobStatus::try_from(job.status) {
            Ok(JobStatus::Ready) => {
                let _ = self.event_tx.send(StoreEvent::JobCreated {
                    id: job.id.clone(),
                    queue: job.queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            Ok(JobStatus::Scheduled) => {
                let _ = self.event_tx.send(StoreEvent::JobScheduled {
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

        Ok(job)
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
    ) -> Result<Vec<Job>, StoreError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Pre-build all jobs and serialize before acquiring the write lock.
            struct PreparedJob {
                job: Job,
                meta_bytes: Vec<u8>,
                payload_bytes: Vec<u8>,
                job_key: Vec<u8>,
                payload_key: Vec<u8>,
                queue_key: Vec<u8>,
                status_key: Vec<u8>,
                type_key: Vec<u8>,
                scheduled: bool,
            }

            let prepared: Vec<PreparedJob> = batch
                .into_iter()
                .map(|opts| {
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
                    };

                    let mut meta = job.clone();
                    meta.payload = None;
                    let meta_bytes = rmp_serde::to_vec_named(&meta)?;

                    let job_key = make_job_key(&id);
                    let payload_key = make_payload_key(&id);
                    let queue_key = make_queue_key(&job.queue, &id);
                    let status_key = make_status_key(status, &id);
                    let type_key = make_type_key(&job.job_type, &id);

                    Ok(PreparedJob {
                        job,
                        meta_bytes,
                        payload_bytes,
                        job_key,
                        payload_key,
                        queue_key,
                        status_key,
                        type_key,
                        scheduled,
                    })
                })
                .collect::<Result<_, StoreError>>()?;

            // Single write transaction for all jobs.
            let mut tx = ks.write_tx();
            for p in &prepared {
                tx.insert(&ks.data, &p.job_key, &p.meta_bytes);
                tx.insert(&ks.data, &p.payload_key, &p.payload_bytes);
                tx.insert(&ks.index, &p.queue_key, b"");
                tx.insert(&ks.index, &p.status_key, b"");
                tx.insert(&ks.index, &p.type_key, b"");
            }

            let sync = ks.commit(tx, ks.enqueue_commit_mode)?;

            // Update in-memory indexes after commit succeeds.
            for p in &prepared {
                if p.scheduled {
                    scheduled_index.insert(p.job.ready_at, p.job.id.clone());
                } else {
                    ready_index.insert(&p.job.queue, p.job.priority, p.job.id.clone());
                }
            }

            let jobs: Vec<Job> = prepared.into_iter().map(|p| p.job).collect();
            Ok((jobs, Some(sync)))
        })
        .await?;

        let jobs = await_sync(result).await?;

        // Broadcast events after sync completes.
        for job in &jobs {
            match JobStatus::try_from(job.status) {
                Ok(JobStatus::Ready) => {
                    let _ = self.event_tx.send(StoreEvent::JobCreated {
                        id: job.id.clone(),
                        queue: job.queue.clone(),
                        token: Arc::new(AtomicBool::new(false)),
                    });
                }
                Ok(JobStatus::Scheduled) => {
                    let _ = self.event_tx.send(StoreEvent::JobScheduled {
                        id: job.id.clone(),
                        ready_at: job.ready_at,
                    });
                }
                _ => {
                    tracing::warn!(
                        job_id = %job.id,
                        status = job.status,
                        "enqueue_bulk produced unexpected status"
                    );
                }
            }
        }

        Ok(jobs)
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
        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
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
                    return Ok((Vec::new(), None));
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
                    return Ok((Vec::new(), None));
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

                let sync = match ks.commit(tx, ks.default_commit_mode) {
                    Ok(sync) => sync,
                    Err(e) => {
                        // 4. Rollback — re-insert valid entries into the
                        //    ready index and notify workers.
                        let _tx = ks.write_tx();
                        for pre in &valid {
                            ready_index.insert(
                                &pre.job.queue,
                                pre.job.priority,
                                pre.job_id.clone(),
                            );

                            let _ = event_tx.send(StoreEvent::JobCreated {
                                id: pre.job_id.clone(),
                                queue: pre.job.queue.clone(),
                                token: Arc::new(AtomicBool::new(false)),
                            });
                        }
                        return Err(e);
                    }
                };

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

                return Ok((jobs, Some(sync)));
            }
        })
        .await?;

        let jobs = await_sync(result).await?;

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

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Retry loop: pre-read and pre-compute changes outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok((false, None)),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok((false, None));
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
                    let sync = ks.commit(tx, ks.default_commit_mode)?;

                    ready_index.global.remove(&(job.priority, id));

                    return Ok((true, Some(sync)));
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

                let sync = ks.commit(tx, ks.default_commit_mode)?;

                ready_index.global.remove(&(job.priority, id));

                return Ok((true, Some(sync)));
            }
        })
        .await?;

        let completed = await_sync(result).await?;

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

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            /// A job that has been pre-read and prepared for completion.
            struct Prepared {
                id: String,
                pre_bytes: Slice,
                priority: u16,
                /// `None` for zero-retention (will be deleted).
                updated_bytes: Option<Slice>,
                /// `Some` for zero-retention (will be deleted).
                deletion: Option<JobDeletion>,
                /// Index key updates for non-zero retention jobs.
                index_keys: Option<RetentionKeys>,
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
                            pre_bytes,
                            priority: job.priority,
                            updated_bytes: None,
                            deletion: Some(del),
                            index_keys: None,
                        });
                    } else {
                        let purge_at = now + retention_ms;
                        let old_status_key = make_status_key(JobStatus::InFlight, id);
                        let new_status_key = make_status_key(JobStatus::Completed, id);
                        let purge_key = make_purge_key(purge_at, id);

                        job.status = JobStatus::Completed.into();
                        job.purge_at = Some(purge_at);
                        job.completed_at = Some(now);

                        let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                        prepared.push(Prepared {
                            id: id.clone(),
                            pre_bytes,
                            priority: job.priority,
                            updated_bytes: Some(updated_slice),
                            deletion: None,
                            index_keys: Some(RetentionKeys {
                                old_status: old_status_key,
                                new_status: new_status_key,
                                purge: purge_key,
                            }),
                        });
                    }
                }

                if prepared.is_empty() {
                    return Ok((
                        BulkCompleteResult {
                            completed: Vec::new(),
                            not_found,
                        },
                        None,
                    ));
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
                    }
                }

                if cas_conflict_detected {
                    drop(tx);
                    not_found.clear();
                    continue; // Retry from pre-read.
                }

                let sync = ks.commit(tx, ks.default_commit_mode)?;

                // Update ready_index for all completed jobs.
                for p in &prepared {
                    ready_index.global.remove(&(p.priority, p.id.clone()));
                }

                let completed: Vec<String> = prepared.into_iter().map(|p| p.id).collect();

                return Ok((
                    BulkCompleteResult {
                        completed,
                        not_found,
                    },
                    Some(sync),
                ));
            }
        })
        .await?;

        let bulk_result = await_sync(result).await?;

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
            let (job, sync) = loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok((None, None)),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok((None, None));
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
                    let sync = ks.commit(tx, ks.default_commit_mode)?;

                    scheduled_index.insert(job.ready_at, id.clone());

                    break (job, sync);
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
                    let sync = ks.commit(tx, ks.default_commit_mode)?;

                    ready_index.global.remove(&(job.priority, id));

                    break (job, sync);
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

                let sync = ks.commit(tx, ks.default_commit_mode)?;

                ready_index.global.remove(&(job.priority, id));

                break (job, sync);
            };

            Ok((Some(job), Some(sync)))
        })
        .await?;

        let result = await_sync(result).await?;

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
        self.ready_index.global.len()
    }

    /// Total number of in-flight jobs across all connections.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_count.load(Ordering::Relaxed) as usize
    }

    /// Total number of scheduled jobs.
    pub fn scheduled_count(&self) -> usize {
        self.scheduled_index.entries.len()
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
                    None => return Ok((None, None)),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok((None, None));
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

                let sync = ks.commit(tx, ks.default_commit_mode)?;

                // Insert into the in-memory ready index after commit succeeds.
                ready_index.insert(&queue, priority, id.clone());

                return Ok((Some(queue), Some(sync)));
            }
        })
        .await?;

        if let Some(queue) = await_sync(result).await? {
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
            // We take 1 more job than requested so that we know if there's a
            // next page or not.
            let fetch = opts.limit.saturating_add(1);
            let mut rows = Vec::with_capacity(fetch.min(1024));

            // Helper: build queue-index MergeSources for the given queues.
            let queue_sources = |queues: &HashSet<String>,
                                 from: &Option<String>,
                                 direction: ScanDirection|
             -> Vec<MergeSource<'_>> {
                queues
                    .iter()
                    .map(|queue_name| {
                        let prefix_len = queue_name.len() + 3;
                        let start = match from {
                            Some(cursor) => Bound::Excluded(make_queue_key(queue_name, cursor)),
                            None => Bound::Included(make_queue_key(queue_name, "")),
                        };
                        let mut end_key = Vec::with_capacity(queue_name.len() + 3);
                        end_key.push(IndexKind::Queue as u8);
                        end_key.push(0);
                        end_key.extend_from_slice(queue_name.as_bytes());
                        end_key.push(1);
                        let end = Bound::Excluded(end_key);

                        match direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&ks.index, (start, end)),
                                prefix_len
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&ks.index, (start, end)).rev(),
                                prefix_len
                            ),
                        }
                    })
                    .collect()
            };

            // Helper: build status-index MergeSources for the given statuses.
            let status_sources = |statuses: &HashSet<JobStatus>,
                                  from: &Option<String>,
                                  direction: ScanDirection|
             -> Vec<MergeSource<'_>> {
                statuses
                    .iter()
                    .map(|status| {
                        let prefix = *status as u8;
                        let start = match from {
                            Some(cursor) => Bound::Excluded(make_status_key(*status, cursor)),
                            None => Bound::Included(vec![IndexKind::Status as u8, 0, prefix, 0]),
                        };
                        let end = Bound::Excluded(vec![IndexKind::Status as u8, 0, prefix + 1, 0]);

                        match direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&ks.index, (start, end)),
                                4
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&ks.index, (start, end)).rev(),
                                4
                            ),
                        }
                    })
                    .collect()
            };

            // Helper: build type-index MergeSources for the given types.
            // Same key layout as queue: `{type}\0{job_id}`.
            let type_sources = |types: &HashSet<String>,
                                from: &Option<String>,
                                direction: ScanDirection|
             -> Vec<MergeSource<'_>> {
                types
                    .iter()
                    .map(|type_name| {
                        let prefix_len = type_name.len() + 3;
                        let start = match from {
                            Some(cursor) => Bound::Excluded(make_type_key(type_name, cursor)),
                            None => Bound::Included(make_type_key(type_name, "")),
                        };
                        let mut end_key = Vec::with_capacity(type_name.len() + 3);
                        end_key.push(IndexKind::Type as u8);
                        end_key.push(0);
                        end_key.extend_from_slice(type_name.as_bytes());
                        end_key.push(1);
                        let end = Bound::Excluded(end_key);

                        match direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&ks.index, (start, end)),
                                prefix_len
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&ks.index, (start, end)).rev(),
                                prefix_len
                            ),
                        }
                    })
                    .collect()
            };

            // Collect an IdStream per active filter dimension. When
            // multiple filters are active we intersect them; when only one
            // is active we use it directly.
            let mut filters: Vec<(&str, IdStream<'_>)> = Vec::new();

            if !opts.queues.is_empty() {
                filters.push((
                    "jobs_by_queue",
                    merge_sources(
                        queue_sources(&opts.queues, &opts.from, opts.direction),
                        opts.direction,
                    ),
                ));
            }
            if !opts.statuses.is_empty() {
                filters.push((
                    "jobs_by_status",
                    merge_sources(
                        status_sources(&opts.statuses, &opts.from, opts.direction),
                        opts.direction,
                    ),
                ));
            }
            if !opts.types.is_empty() {
                filters.push((
                    "jobs_by_type",
                    merge_sources(
                        type_sources(&opts.types, &opts.from, opts.direction),
                        opts.direction,
                    ),
                ));
            }

            if !filters.is_empty() {
                // Build a human-readable source description for error
                // messages, e.g. "jobs_by_queue" or
                // "(jobs_by_queue & jobs_by_status)".
                let source_desc = if filters.len() == 1 {
                    filters[0].0.to_string()
                } else {
                    let names: Vec<&str> = filters.iter().map(|(n, _)| *n).collect();
                    format!("({})", names.join(" & "))
                };

                let direction = opts.direction;
                let mut iter = filters.into_iter().map(|(_, s)| s);
                let first = iter.next().unwrap();
                let combined = iter.fold(first, |acc, s| intersect_streams(acc, s, direction));

                rows =
                    load_jobs_by_ids(&snapshot, &ks.data, combined, &source_desc, opts.now, fetch)?;
            } else {
                // No filter — scan the data keyspace for J-tagged keys.
                // Range: [J, 0]..[J, 1] covers all job metadata entries.
                let job_prefix_start: Vec<u8> = vec![RecordKind::Job as u8, 0];
                let job_prefix_end: Vec<u8> = vec![RecordKind::Job as u8, 1];
                match opts.direction {
                    ScanDirection::Asc => {
                        let range = match &opts.from {
                            Some(cursor) => snapshot.range::<Vec<u8>, _>(
                                &ks.data,
                                (
                                    Bound::Excluded(make_job_key(cursor)),
                                    Bound::Excluded(job_prefix_end),
                                ),
                            ),
                            None => snapshot.range::<Vec<u8>, _>(
                                &ks.data,
                                (
                                    Bound::Included(job_prefix_start),
                                    Bound::Excluded(job_prefix_end),
                                ),
                            ),
                        };

                        for entry in range {
                            let (key, value) = entry.into_inner()?;
                            let mut job: Job = rmp_serde::from_slice(&value)?;
                            if let Some(now) = opts.now {
                                if job.purge_at.is_some_and(|p| p <= now) {
                                    continue;
                                }
                            }
                            // Swap J tag for P tag to look up payload.
                            let mut payload_key = key.to_vec();
                            payload_key[0] = RecordKind::Payload as u8;
                            if let Some(payload_bytes) = ks.data.get(&payload_key)? {
                                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
                            }
                            rows.push(job);
                            if rows.len() >= fetch {
                                break;
                            }
                        }
                    }
                    ScanDirection::Desc => {
                        let range = match &opts.from {
                            Some(cursor) => snapshot.range::<Vec<u8>, _>(
                                &ks.data,
                                (
                                    Bound::Included(job_prefix_start),
                                    Bound::Excluded(make_job_key(cursor)),
                                ),
                            ),
                            None => snapshot.range::<Vec<u8>, _>(
                                &ks.data,
                                (
                                    Bound::Included(job_prefix_start),
                                    Bound::Excluded(job_prefix_end),
                                ),
                            ),
                        };

                        for entry in range.rev() {
                            let (key, value) = entry.into_inner()?;
                            let mut job: Job = rmp_serde::from_slice(&value)?;
                            if let Some(now) = opts.now {
                                if job.purge_at.is_some_and(|p| p <= now) {
                                    continue;
                                }
                            }
                            // Swap J tag for P tag to look up payload.
                            let mut payload_key = key.to_vec();
                            payload_key[0] = RecordKind::Payload as u8;
                            if let Some(payload_bytes) = ks.data.get(&payload_key)? {
                                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
                            }
                            rows.push(job);
                            if rows.len() >= fetch {
                                break;
                            }
                        }
                    }
                }
            }

            // If we got more rows than the requested limit, we know there's a
            // next page, and we can just discard that extra row now.
            let has_more = rows.len() > opts.limit;
            if has_more {
                rows.truncate(opts.limit);
            }

            // Build next/prev options, preserving all filters.
            let make_opts = |cursor: &str, direction: ScanDirection| {
                let mut o = ListJobsOptions::new()
                    .from(cursor.to_string())
                    .direction(direction)
                    .limit(opts.limit)
                    .statuses(opts.statuses.clone())
                    .queues(opts.queues.clone())
                    .types(opts.types.clone());
                if let Some(now) = opts.now {
                    o = o.now(now);
                }
                o
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
            for entry in ready_index.global.iter().skip(offset) {
                if jobs.len() >= limit {
                    break;
                }
                let ((_priority, job_id), _queue) = (entry.key(), entry.value());
                let job_key = make_job_key(job_id);
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

        task::spawn_blocking(move || {
            let mut ids = Vec::with_capacity(limit);
            for entry in ready_index.global.iter().skip(offset) {
                if ids.len() >= limit {
                    break;
                }
                let (priority, job_id) = entry.key();
                ids.push((*priority, job_id.clone()));
            }
            ids
        })
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
            for entry in scheduled_index.entries.iter().skip(offset) {
                if jobs.len() >= limit {
                    break;
                }
                let (_ready_at, job_id) = entry.value();
                let job_key = make_job_key(job_id);
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

        task::spawn_blocking(move || {
            let mut ids = Vec::with_capacity(limit);
            for entry in scheduled_index.entries.iter().skip(offset) {
                if ids.len() >= limit {
                    break;
                }
                let (ready_at, job_id) = entry.value();
                ids.push((*ready_at, job_id.clone()));
            }
            ids
        })
        .await
        .unwrap_or_default()
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
            for (_ready_at, job_id) in &due_entries {
                let job_key = make_job_key(job_id);
                let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "job in scheduled index but missing from data keyspace: {job_id:?}",
                    ))
                })?;
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

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
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
                    None => return Ok(((), None)), // Already gone.
                };

                let current: Job = rmp_serde::from_slice(&pre_bytes)?;

                if current.status != JobStatus::Scheduled as u8 {
                    return Ok(((), None)); // No longer scheduled.
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

                let sync = match ks.commit(tx, ks.default_commit_mode) {
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

                return Ok(((), Some(sync)));
            }
        })
        .await?;

        await_sync(result).await?;

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
        let (count, rx) = self.recover_in_flight_jobs()?;
        if let Some(rx) = rx {
            await_sync(Ok(((), Some(rx)))).await?;
        }
        Ok(count)
    }

    /// Rebuild all in-memory indexes asynchronously.
    ///
    /// Sets `index_ready` to `false` while rebuilding and emits
    /// `IndexRebuilt` when complete. Workers wait on `index_ready`
    /// before taking jobs.
    pub async fn rebuild_indexes(&self) -> Result<(usize, usize), StoreError> {
        self.index_ready.store(false, Ordering::Release);

        let (ready, scheduled) =
            tokio::try_join!(self.rebuild_ready_index(), self.rebuild_scheduled_index(),)?;

        self.index_ready.store(true, Ordering::Release);
        let _ = self.event_tx.send(StoreEvent::IndexRebuilt);
        Ok((ready, scheduled))
    }

    /// Move orphaned in-flight jobs back to Ready in the LSM indexes.
    ///
    /// Runs synchronously so it completes before any API requests are
    /// accepted. Does not touch the in-memory ready index — that's
    /// handled by `rebuild_ready_index`, which runs immediately after.
    fn recover_in_flight_jobs(
        &self,
    ) -> Result<(usize, Option<tokio::sync::oneshot::Receiver<SyncResult>>), StoreError> {
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
            return Ok((0, None));
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
        }

        let sync = ks.commit(tx, ks.default_commit_mode)?;
        Ok((count, Some(sync)))
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
        let id = id.to_string();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // ---- outside tx ----
            let job_key = make_job_key(&id);
            let job_bytes = match ks.data.get(&job_key)? {
                Some(bytes) => bytes,
                None => return Ok((false, None)), // Already purged.
            };

            let job: Job = rmp_serde::from_slice(&job_bytes)?;
            let status = JobStatus::try_from(job.status).map_err(|v| {
                StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
            })?;

            let del = prepare_job_deletion(&job, status, &ks);

            // ---- inside tx (writes only) ----
            let mut tx = ks.write_tx();
            apply_job_deletion(&mut tx, &del, &ks);
            let sync = ks.commit(tx, ks.default_commit_mode)?;

            // Defensive: remove from ready_index in case of inconsistency.
            ready_index.global.remove(&(job.priority, id));

            Ok((true, Some(sync)))
        })
        .await?;

        Ok(await_sync(result).await?)
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
            let mut deletions: Vec<(JobDeletion, u16)> = Vec::new();

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
                deletions.push((prepare_job_deletion(&job, status, &ks), priority));
            }

            // ---- inside tx (writes only) ----
            let count = deletions.len();
            let mut tx = ks.write_tx();
            for (del, _) in &deletions {
                apply_job_deletion(&mut tx, del, &ks);
            }
            let sync = ks.commit(tx, ks.default_commit_mode)?;

            // Defensive: remove from ready_index in case of inconsistency.
            for (del, priority) in deletions {
                ready_index.global.remove(&(priority, del.id));
            }

            Ok((count, Some(sync)))
        })
        .await?;

        Ok(await_sync(result).await?)
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
    }
}

/// Apply pre-computed deletion inside an open tx. No reads.
fn apply_job_deletion(tx: &mut fjall::SingleWriterWriteTx<'_>, del: &JobDeletion, ks: &Keyspaces) {
    tx.remove(&ks.data, &make_job_key(&del.id));
    tx.remove_weak(&ks.data, &make_payload_key(&del.id));
    tx.remove(&ks.index, &del.status_key);
    tx.remove_weak(&ks.index, &del.queue_key);
    tx.remove_weak(&ks.index, &del.type_key);

    if let Some(ref purge_key) = del.purge_key {
        tx.remove(&ks.index, purge_key);
    }

    for key in &del.error_keys {
        tx.remove_weak(&ks.data, key);
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
            .unwrap();

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
            .unwrap();
        let job2 = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap();

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
            .unwrap();
        let second = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap();
        let third = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("mid")).priority(5),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
                .unwrap();
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
                .unwrap();
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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
                .unwrap();
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
                .unwrap();
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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("mid")).priority(5),
            )
            .await
            .unwrap();

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
            .unwrap();
        let second = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "beta", serde_json::json!("b1")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "alpha", serde_json::json!("a2")),
            )
            .await
            .unwrap();

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
                .unwrap();
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
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap();

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
            .unwrap();
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
            .unwrap();

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
            .unwrap();
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
            .unwrap();
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
            .unwrap();

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
            .unwrap();
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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();
        let d = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("d")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let _c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("c")),
            )
            .await
            .unwrap();
        let d = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("d")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_sms", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "low", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("b")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "low", serde_json::json!("c")),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("d")),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();
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
            .unwrap();

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
                .unwrap();
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
            .unwrap();

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
            .unwrap();
        let earlier = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("earlier"))
                    .ready_at(base + 1000),
            )
            .await
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("future"))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap();

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
            .unwrap();

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
        let (indexed, _scheduled) = store.rebuild_indexes().await.unwrap();
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
            .unwrap();

        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled) = store.rebuild_indexes().await.unwrap();
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
            .unwrap();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap();

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
        let (indexed, _scheduled) = store.rebuild_indexes().await.unwrap();
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
            .unwrap();

        // A Scheduled job (far in the future).
        let scheduled = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("scheduled"))
                    .ready_at(now + 600_000),
            )
            .await
            .unwrap();

        // Recover should find no in-flight jobs, but index the ready one.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled) = store.rebuild_indexes().await.unwrap();
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
            .unwrap();
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
            .unwrap();

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
            .unwrap();
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
            .unwrap();

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
            .unwrap();

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
            .unwrap();

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
            .unwrap();
        let high = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(0),
            )
            .await
            .unwrap();
        let mid = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(5),
            )
            .await
            .unwrap();

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
                .unwrap();
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
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap();

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
            .unwrap();
        let early = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 10_000),
            )
            .await
            .unwrap();
        let mid = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 20_000),
            )
            .await
            .unwrap();

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
                .unwrap();
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
            .unwrap();

        // One scheduled job.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap();

        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
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
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap();
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
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap();

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
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 120_000),
            )
            .await
            .unwrap();
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
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap();

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
            .unwrap();
        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap();

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
            .unwrap();
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
            .unwrap();
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

        let jobs = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("a", "q1", serde_json::json!(1)),
                    EnqueueOptions::new("b", "q2", serde_json::json!(2)),
                    EnqueueOptions::new("c", "q3", serde_json::json!(3)),
                ],
            )
            .await
            .unwrap();

        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].job_type, "a");
        assert_eq!(jobs[1].job_type, "b");
        assert_eq!(jobs[2].job_type, "c");
    }

    #[tokio::test]
    async fn enqueue_bulk_ids_are_monotonic() {
        let store = test_store();
        let now = now_millis();

        let jobs = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                ],
            )
            .await
            .unwrap();

        assert!(jobs[0].id < jobs[1].id);
        assert!(jobs[1].id < jobs[2].id);
    }

    #[tokio::test]
    async fn enqueue_bulk_empty_is_noop() {
        let store = test_store();
        let jobs = store.enqueue_bulk(now_millis(), vec![]).await.unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn enqueue_bulk_jobs_are_retrievable() {
        let store = test_store();
        let now = now_millis();

        let jobs = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "q1", serde_json::json!("a")),
                    EnqueueOptions::new("test", "q2", serde_json::json!("b")),
                ],
            )
            .await
            .unwrap();

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

        let jobs = store
            .enqueue_bulk(
                now,
                vec![
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .ready_at(future),
                ],
            )
            .await
            .unwrap();

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

        let jobs = store
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
            .unwrap();

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
}
