// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Persistent storage layer to manage job queues.
//!
//! Wraps fjall (database) to provide transactional queue operations across
//! six keyspaces plus two in-memory indexes:
//!
//! - `jobs`: source of truth, keyed by job ID, stores job metadata (no payload).
//! - `payloads`: immutable job payloads, keyed by job ID.
//! - `jobs_by_status`: status index, keyed by `{status_u8}\0{job_id}`.
//! - `jobs_by_queue`: queue membership index, keyed by `{queue_name}\0{job_id}`.
//! - `jobs_by_type`: type membership index, keyed by `{job_type}\0{job_id}`.
//! - `errors`: error records per failure, keyed by `{job_id}\0{attempt_be_u32}`.
//! - In-memory `ReadyIndex`: lock-free crossbeam skip-list priority index of
//!   ready jobs, rebuilt from the `jobs` keyspace on startup.
//! - In-memory `ScheduledIndex`: BTreeSet-based chronological index of
//!   scheduled jobs, rebuilt from the `jobs` keyspace on startup.
//!
//! Combined filters (e.g. queue + status) use sorted stream intersection
//! across the individual indexes rather than a compound index.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashSet};
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use crossbeam_skiplist::{SkipMap, SkipSet};
use dashmap::DashMap;

use fjall::config::FilterPolicy;
use fjall::{Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
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

    /// The job is currently being processed by a worker.
    Working = 2,

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
            2 => Ok(Self::Working),
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
    #[serde(rename = "i")]
    pub id: String,

    /// Job type, e.g. "send_email" or "generate_report".
    #[serde(rename = "t")]
    pub job_type: String,

    /// Queue this job belongs to.
    #[serde(rename = "q")]
    pub queue: String,

    /// Priority (lower number = higher priority).
    #[serde(rename = "n")]
    pub priority: u16,

    /// Arbitrary payload provided by the client.
    ///
    /// Stored in a separate `payloads` keyspace so that status-change
    /// operations (take, requeue, promote, recover) only re-write the small
    /// metadata record. `None` when the payload has not been hydrated (i.e.
    /// when reading metadata only from the `jobs` keyspace).
    #[serde(rename = "p")]
    #[serde(default)]
    pub payload: Option<serde_json::Value>,

    /// Current lifecycle status, stored as a u8 which converts to `JobStatus`.
    #[serde(rename = "s")]
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
    pub base_ms: f32,

    /// Max random milliseconds per attempt multiplier (0..jitter_ms).
    #[serde(rename = "j")]
    pub jitter_ms: f32,
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
}

/// Options for recording a job failure.
pub struct FailureOptions {
    /// Error message from the worker.
    pub error: String,

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

    /// Server default retry limit (used when job has no override).
    pub default_retry_limit: u32,

    /// Server default backoff config (used when job has no override).
    pub default_backoff: BackoffConfig,
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
        queue: String,
        token: Arc<AtomicBool>,
    },

    /// A job was successfully completed and removed.
    JobCompleted(String),

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
    JobScheduled { ready_at: u64 },
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
/// Keeps a `BTreeSet<(u64, String)>` ordered by `(ready_at, job_id)`.
/// The tuple's derived `Ord` gives chronological ordering (ready_at ASC),
/// then FIFO within the same timestamp (scru128 IDs are lexicographically
/// time-ordered).
///
/// Protected by `std::sync::Mutex` (not tokio) — critical sections are
/// nanoseconds (BTreeSet insert/remove).
struct ScheduledIndex {
    entries: BTreeSet<(u64, String)>,
}

impl ScheduledIndex {
    fn new() -> Self {
        Self {
            entries: BTreeSet::new(),
        }
    }

    fn insert(&mut self, ready_at: u64, job_id: String) {
        self.entries.insert((ready_at, job_id));
    }

    fn remove(&mut self, ready_at: u64, job_id: &str) {
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
        for (ready_at, job_id) in &self.entries {
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
    /// Connection to the underlying database.
    db: SingleWriterTxDatabase,

    /// Reference to the jobs keyspace.
    ///
    /// Jobs are keyed by their scru128 identifier which provides basic FIFO
    /// semantics. This is the authoritative source of job metadata, but it
    /// is not priority ordered. Payloads are stored separately.
    jobs: SingleWriterTxKeyspace,

    /// Immutable job payloads, keyed by job ID.
    ///
    /// Separated from the `jobs` keyspace so that status-change operations
    /// (take, requeue, promote, recover) only re-write the small metadata
    /// record — less WAL data, less compaction, less time holding the
    /// single-writer lock.
    payloads: SingleWriterTxKeyspace,

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
    scheduled_index: Arc<Mutex<ScheduledIndex>>,

    /// Reference to the status index keyspace.
    ///
    /// Keyed by `{status_u8}\0{job_id}` with empty values. Enables efficient
    /// range scans for jobs in a particular status without scanning the entire
    /// `jobs` keyspace.
    jobs_by_status: SingleWriterTxKeyspace,

    /// Queue membership index.
    ///
    /// Keyed by `{queue_name}\0{job_id}` with empty values. Enables
    /// efficient lookup of all jobs belonging to one or more queues.
    ///
    /// Currently write-once/remove-once (written at enqueue, removed at
    /// completion/death), so removals use `remove_weak` to avoid
    /// tombstone buildup. If we add queue-move support in the future,
    /// the same key could be re-inserted and this must revert to
    /// regular `remove`.
    jobs_by_queue: SingleWriterTxKeyspace,

    /// Type membership index.
    ///
    /// Keyed by `{job_type}\0{job_id}` with empty values. Immutable —
    /// only written at enqueue and removed at completion. No updates
    /// on status transitions.
    jobs_by_type: SingleWriterTxKeyspace,

    /// Error records, keyed by `{job_id}\0{attempt_be_u32}`.
    ///
    /// One record per failure (append-only). Enables per-job error
    /// history via prefix scan. Cleaned up when the job is completed
    /// or killed (dead).
    errors: SingleWriterTxKeyspace,

    /// Broadcast channel for store events.
    ///
    /// Subscribers receive notifications when jobs are enqueued, completed,
    /// or otherwise change state. Take handlers use this both to wake up
    /// when new work is available and to prune their in-flight tracking.
    event_tx: broadcast::Sender<StoreEvent>,
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
/// if any ID is missing from the `jobs` keyspace (data corruption). Hydrates
/// each job's payload from the separate `payloads` keyspace.
fn load_jobs_by_ids(
    jobs_ks: &SingleWriterTxKeyspace,
    payloads_ks: &SingleWriterTxKeyspace,
    ids: impl Iterator<Item = Result<Vec<u8>, StoreError>>,
    source: &str,
) -> Result<Vec<Job>, StoreError> {
    let mut rows = Vec::new();
    for id in ids {
        let id = id?;
        let bytes = jobs_ks.get(&id)?.ok_or_else(|| {
            StoreError::Corruption(format!(
                "job in {source} but missing from jobs keyspace: {:?}",
                String::from_utf8_lossy(&id),
            ))
        })?;
        let mut job: Job = rmp_serde::from_slice(&bytes)?;

        // Hydrate the payload from the payloads keyspace.
        if let Some(payload_bytes) = payloads_ks.get(&id)? {
            job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
        }

        rows.push(job);
    }
    Ok(rows)
}

/// Tuning parameters for the underlying LSM storage engine.
///
/// Job queue workloads are high-churn: jobs are enqueued, processed,
/// and deleted rapidly, generating many tombstones across all keyspaces.
/// The fjall defaults (64 MiB tables, 512 MiB journal) are tuned for
/// larger, read-heavy workloads. These defaults are sized down for
/// faster tombstone reclamation and tighter disk usage.
#[derive(Debug, Clone, Copy)]
pub struct StorageConfig {
    /// Target size for SST files in data keyspaces (jobs, payloads, errors).
    /// The memtable is sized to match so flushes produce appropriately-sized
    /// L0 tables.
    pub data_table_size: u64,

    /// Target size for SST files in index keyspaces (status, queue, type,
    /// priority indexes). These store tiny entries so smaller tables are
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
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_table_size: 64 * 1024 * 1024, // 64 MiB
            index_table_size: 8 * 1024 * 1024, // 8 MiB
            journal_size: 64 * 1024 * 1024,    // 64 MiB (minimum)
            l0_threshold: 4,
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
    /// | `ZANXIO_DATA_TABLE_SIZE`    | `data_table_size`  |
    /// | `ZANXIO_INDEX_TABLE_SIZE`   | `index_table_size` |
    /// | `ZANXIO_JOURNAL_SIZE`       | `journal_size`     |
    /// | `ZANXIO_L0_THRESHOLD`       | `l0_threshold`     |
    pub fn from_env() -> Result<Self, EnvConfigError> {
        let defaults = Self::default();
        Ok(Self {
            data_table_size: env_parse("ZANXIO_DATA_TABLE_SIZE")?
                .unwrap_or(defaults.data_table_size),
            index_table_size: env_parse("ZANXIO_INDEX_TABLE_SIZE")?
                .unwrap_or(defaults.index_table_size),
            journal_size: env_parse("ZANXIO_JOURNAL_SIZE")?.unwrap_or(defaults.journal_size),
            l0_threshold: env_parse("ZANXIO_L0_THRESHOLD")?.unwrap_or(defaults.l0_threshold),
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
    /// data.
    pub fn open(
        path: impl AsRef<std::path::Path>,
        config: StorageConfig,
    ) -> Result<Self, StoreError> {
        let db = SingleWriterTxDatabase::builder(path)
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
        };
        let index_opts = || {
            fjall::KeyspaceCreateOptions::default()
                .compaction_strategy(index_compaction.clone())
                .max_memtable_size(config.index_table_size)
                .filter_policy(FilterPolicy::disabled())
        };

        let jobs = db.keyspace("jobs", &data_opts)?;
        let payloads = db.keyspace("payloads", &data_opts)?;
        let jobs_by_status = db.keyspace("jobs_by_status", &index_opts)?;
        let jobs_by_queue = db.keyspace("jobs_by_queue", &index_opts)?;
        let jobs_by_type = db.keyspace("jobs_by_type", &index_opts)?;
        let errors = db.keyspace("errors", &data_opts)?;

        let (event_tx, _) = broadcast::channel(1024);

        Ok(Self {
            db,
            jobs,
            payloads,
            ready_index: Arc::new(ReadyIndex::new()),
            scheduled_index: Arc::new(Mutex::new(ScheduledIndex::new())),
            jobs_by_status,
            jobs_by_queue,
            jobs_by_type,
            errors,
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
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue = self.jobs_by_queue.clone();
        let jobs_by_type = self.jobs_by_type.clone();

        let job = task::spawn_blocking(move || -> Result<Job, StoreError> {
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
            };

            // Serialize metadata without the payload for the jobs keyspace.
            let mut meta = job.clone();
            meta.payload = None;
            let meta_bytes = rmp_serde::to_vec_named(&meta)?;

            let queue_key = make_queue_key(&job.queue, &id);
            let status_key = make_status_key(status, &id);
            let type_key = make_type_key(&job.job_type, &id);

            let mut tx = db.write_tx();
            tx.insert(&jobs, &id, &meta_bytes);
            tx.insert(&payloads, &id, &payload_bytes);
            tx.insert(&jobs_by_queue, &queue_key, b"");
            tx.insert(&jobs_by_status, &status_key, b"");
            tx.insert(&jobs_by_type, &type_key, b"");

            tx.commit()?;

            // Insert into the in-memory indexes after commit succeeds.
            // Ready jobs go into the ready index; scheduled jobs go into the
            // scheduled index (promoted later by the scheduler).
            if scheduled {
                scheduled_index.lock().unwrap().insert(ready_at, id.clone());
            } else {
                ready_index.insert(&job.queue, job.priority, job.id.clone());
            }

            Ok(job)
        })
        .await??;

        match JobStatus::try_from(job.status) {
            Ok(JobStatus::Ready) => {
                let _ = self.event_tx.send(StoreEvent::JobCreated {
                    queue: job.queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            Ok(JobStatus::Scheduled) => {
                let _ = self.event_tx.send(StoreEvent::JobScheduled {
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

    /// Take the next job from the priority index.
    ///
    /// Atomically claims the highest-priority (lowest number), oldest job
    /// from the lock-free ready index and marks it as working. Returns `None`
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
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let ready_index = self.ready_index.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let queues = queues.clone();

        let spawn_start = std::time::Instant::now();
        task::spawn_blocking(move || {
            let blocking_start = std::time::Instant::now();
            let spawn_wait = blocking_start.duration_since(spawn_start);

            // Claim the best candidate from the lock-free ready index.
            // This atomically removes the entry — two concurrent callers
            // each get a different job (no mutex needed).
            let scan_start = std::time::Instant::now();
            let (_priority, job_id, queue) = match ready_index.claim(&queues) {
                Some(entry) => entry,
                None => {
                    tracing::trace!(
                        spawn_wait_us = spawn_wait.as_micros(),
                        "take_next_job: empty"
                    );
                    return Ok(None);
                }
            };
            let scan_elapsed = scan_start.elapsed();

            // Acquire the fjall write lock *after* claiming from the index.
            // The index claim already prevents duplicates, so we just need
            // the write lock for the LSM transaction.
            let mut tx = db.write_tx();
            let write_tx_elapsed = blocking_start.elapsed();

            // Look up job metadata from the jobs keyspace.
            let meta_bytes = jobs.get(&job_id)?.ok_or_else(|| {
                StoreError::Corruption(format!(
                    "job in ready index but missing from jobs keyspace: {job_id:?}",
                ))
            })?;

            let mut job: Job = rmp_serde::from_slice(&meta_bytes)?;
            let old_status_key = make_status_key(JobStatus::Ready, &job.id);
            job.status = JobStatus::Working.into();
            job.dequeued_at = Some(now);
            let new_status_key = make_status_key(JobStatus::Working, &job.id);

            // Only write metadata back — payload is immutable and stays in
            // its own keyspace.
            let meta_bytes = rmp_serde::to_vec_named(&job)?;

            // Update job record and status indexes.
            let commit_start = std::time::Instant::now();
            tx.insert(&jobs, &job_id, &meta_bytes);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");

            if let Err(e) = tx.commit() {
                // Commit failed — the LSM transaction rolled back. Acquire
                // a new write transaction to serialize against concurrent
                // writers, check whether the job is still Ready, and
                // re-insert into the index if so.
                let _tx = db.write_tx();
                if let Ok(Some(bytes)) = jobs.get(&job_id) {
                    if let Ok(current) = rmp_serde::from_slice::<Job>(&bytes) {
                        if current.status == JobStatus::Ready as u8 {
                            ready_index.insert(&current.queue, current.priority, current.id);
                        }
                    }
                }
                return Err(e.into());
            }
            let commit_elapsed = commit_start.elapsed();

            // Hydrate the payload from the payloads keyspace for the caller.
            let hydrate_start = std::time::Instant::now();
            if let Some(payload_bytes) = payloads.get(&job_id)? {
                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
            }
            let hydrate_elapsed = hydrate_start.elapsed();

            tracing::trace!(
                spawn_wait_us = spawn_wait.as_micros(),
                write_tx_us = write_tx_elapsed.as_micros(),
                scan_us = scan_elapsed.as_micros(),
                commit_us = commit_elapsed.as_micros(),
                hydrate_us = hydrate_elapsed.as_micros(),
                total_us = blocking_start.elapsed().as_micros(),
                job_id = %job.id,
                queue = %queue,
                "take_next_job"
            );

            Ok(Some(job))
        })
        .await?
    }

    /// Mark a job as successfully completed.
    ///
    /// Removes the job from the `jobs` keyspace and removes associated index
    /// entries. Returns `true` if the job was found in the working state,
    /// `false` if it was not (e.g. already acked or never existed).
    ///
    /// Subscribers are notified on success.
    pub async fn mark_completed(&self, id: &str) -> Result<bool, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue = self.jobs_by_queue.clone();
        let jobs_by_type = self.jobs_by_type.clone();
        let errors = self.errors.clone();
        let id = id.to_string();
        let id_for_event = id.clone();

        let completed = task::spawn_blocking(move || {
            // Acquire the write lock before checking, so no other writer
            // can remove the job between our check and the remove.
            let mut tx = db.write_tx();

            let job_bytes = match jobs.get(&id)? {
                Some(bytes) => bytes,
                None => return Ok(false), // No such job. Already completed?
            };

            let job: Job = rmp_serde::from_slice(&job_bytes)?;

            // Can't mark a job as completed if it's not currently working.
            if job.status != JobStatus::Working as u8 {
                return Ok(false);
            }

            // Clean up all indexes.
            let status_key = make_status_key(JobStatus::Working, &id);
            let queue_key = make_queue_key(&job.queue, &id);
            let type_key = make_type_key(&job.job_type, &id);

            tx.remove(&jobs, &id);
            tx.remove_weak(&payloads, &id);
            tx.remove(&jobs_by_status, &status_key);
            tx.remove_weak(&jobs_by_queue, &queue_key);
            tx.remove_weak(&jobs_by_type, &type_key);

            // Clean up any error records from previous failures.
            for key in error_keys(&errors, &id) {
                tx.remove_weak(&errors, &key);
            }

            tx.commit()?;

            Ok(true)
        })
        .await?;

        if let Ok(true) = &completed {
            // Best-effort: if no subscribers, the send error is harmless.
            let _ = self.event_tx.send(StoreEvent::JobCompleted(id_for_event));
        }

        completed
    }

    /// Record a job failure and either schedule a retry or kill the job
    /// depending on the backoff policy.
    ///
    /// Returns `Some(job)` with metadata-only fields if the job was found in
    /// the Working state. The returned job's status will be either `Scheduled`
    /// in the retry scenario or `Dead` if the retry limit has been reached.
    /// Returns `None` if the job wasn't found or wasn't in the Working state.
    ///
    /// For dead jobs, the job record and all associated data (payload,
    /// indexes, error records) are deleted from the store.
    pub async fn record_failure(
        &self,
        now: u64,
        id: &str,
        opts: FailureOptions,
    ) -> Result<Option<Job>, StoreError> {
        let db = self.db.clone();

        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue = self.jobs_by_queue.clone();
        let jobs_by_type = self.jobs_by_type.clone();
        let scheduled_index = self.scheduled_index.clone();
        let errors = self.errors.clone();

        let id = id.to_string();
        let id_for_event = id.clone();

        let result: Result<Option<Job>, StoreError> = task::spawn_blocking(move || {
            let mut tx = db.write_tx();

            // Load the job to check its status and update it.
            let job_bytes = match jobs.get(&id)? {
                Some(bytes) => bytes,
                None => return Ok(None),
            };

            let mut job: Job = rmp_serde::from_slice(&job_bytes)?;

            // Can only fail a job that is currently being worked on.
            if job.status != JobStatus::Working as u8 {
                return Ok(None);
            }

            // Record the failure time and increment the count.
            job.attempts += 1;
            job.failed_at = Some(now);

            // Write the error record (append-only).
            let error_key = make_error_key(&id, job.attempts);
            let error_record = ErrorRecord {
                message: opts.error,
                error_type: opts.error_type,
                backtrace: opts.backtrace,
                dequeued_at: job.dequeued_at.unwrap_or(0),
                failed_at: now,
                attempt: job.attempts,
            };
            let error_bytes = rmp_serde::to_vec_named(&error_record)?;
            tx.insert(&errors, &error_key, &error_bytes);

            // Client options may override the default backoff policy here so
            // we always check those first and then apply the appropriate
            // backoff policy.

            let retry_limit = job.retry_limit.unwrap_or(opts.default_retry_limit);

            if opts.kill {
                // If the client requested to force kill this job, mark it dead.
                job.status = JobStatus::Dead.into();
            } else if let Some(retry_at) = opts.retry_at {
                // If the client requested to reschedule this job, mark it
                // scheduled and set the new ready_at.
                job.status = JobStatus::Scheduled.into();
                job.ready_at = retry_at;
            } else if job.attempts > retry_limit {
                // Client didn't specify what to do and retries have exhausted.
                job.status = JobStatus::Dead.into();
            } else {
                // Client didn't specify what to do and we are able to retry.

                // Calculate backoff delay.
                let backoff = job.backoff.as_ref().unwrap_or(&opts.default_backoff);
                let delay_ms = compute_backoff(job.attempts, backoff);
                job.status = JobStatus::Scheduled.into();
                job.ready_at = now + delay_ms;
            };

            let old_status_key = make_status_key(JobStatus::Working, &id);

            if job.status == JobStatus::Scheduled as u8 {
                // Move job back into the scheduled index.
                let new_status_key = make_status_key(JobStatus::Scheduled, &id);

                let meta_bytes = rmp_serde::to_vec_named(&job)?;
                tx.insert(&jobs, &id, &meta_bytes);
                tx.remove(&jobs_by_status, &old_status_key);
                tx.insert(&jobs_by_status, &new_status_key, b"");
                tx.commit()?;

                // Insert into the in-memory scheduled index after commit succeeds.
                scheduled_index
                    .lock()
                    .unwrap()
                    .insert(job.ready_at, id.clone());
            } else {
                // Delete the job and all associated data.
                let queue_key = make_queue_key(&job.queue, &id);
                let type_key = make_type_key(&job.job_type, &id);

                tx.remove(&jobs, &id);
                tx.remove_weak(&payloads, &id);
                tx.remove(&jobs_by_status, &old_status_key);
                tx.remove_weak(&jobs_by_queue, &queue_key);
                tx.remove_weak(&jobs_by_type, &type_key);

                // Clean up all previously committed error records.
                for key in error_keys(&errors, &id) {
                    tx.remove_weak(&errors, &key);
                }

                // Also remove the error record we just inserted above.
                // The prefix scan reads committed state, so it won't
                // see the in-flight insert — we must remove it explicitly.
                tx.remove_weak(&errors, &error_key);

                tx.commit()?;
            }

            Ok(Some(job))
        })
        .await?;

        // Emit events outside spawn_blocking due to &self.event_tx.
        if let Ok(Some(ref job)) = result {
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
                    ready_at: job.ready_at,
                });
            }
        }

        result
    }

    /// Look up a job by ID.
    ///
    /// Returns `None` if the job does not exist (or has been completed).
    pub async fn get_job(&self, id: &str) -> Result<Option<Job>, StoreError> {
        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let id = id.to_string();

        task::spawn_blocking(move || {
            let Some(bytes) = jobs.get(&id)? else {
                return Ok(None);
            };
            let mut job: Job = rmp_serde::from_slice(&bytes)?;

            // Hydrate the payload from the payloads keyspace.
            if let Some(payload_bytes) = payloads.get(&id)? {
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

    /// Atomically move a working job back to the priority queue.
    ///
    /// Looks up the job from the `jobs` keyspace to reconstruct its
    /// prioritised queue keys.
    ///
    /// Returns `true` if the job was actually requeued, `false` if it was
    /// already acked (no longer in the working state).
    ///
    /// Subscribers are notified.
    pub async fn requeue(&self, id: &str) -> Result<bool, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let ready_index = self.ready_index.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let id = id.to_string();

        // Returns Some((queue_name, priority)) if the job was requeued, None otherwise.
        let requeued = task::spawn_blocking(
            move || -> Result<Option<(String, u16, String)>, StoreError> {
                let mut tx = db.write_tx();

                let old_status_key = make_status_key(JobStatus::Working, &id);
                if jobs_by_status.get(&old_status_key)?.is_none() {
                    return Ok(None);
                }

                // The job is still working, so it must still be in the jobs
                // keyspace (only mark_completed removes from both).
                let job_bytes = jobs.get(&id)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "working job missing from jobs keyspace: {id:?}",
                    ))
                })?;
                let mut job: Job = rmp_serde::from_slice(&job_bytes)?;
                let queue = job.queue.clone();
                let priority = job.priority;
                job.status = JobStatus::Ready.into();
                let new_status_key = make_status_key(JobStatus::Ready, &id);
                let job_bytes = rmp_serde::to_vec_named(&job)?;

                tx.insert(&jobs, &id, &job_bytes);
                tx.remove(&jobs_by_status, &old_status_key);
                tx.insert(&jobs_by_status, &new_status_key, b"");
                tx.commit()?;

                // Insert into the in-memory ready index after commit succeeds.
                ready_index.insert(&queue, priority, id.clone());

                Ok(Some((queue, priority, id)))
            },
        )
        .await??;

        if let Some((queue, _, _)) = requeued {
            let _ = self.event_tx.send(StoreEvent::JobCreated {
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
        let db = self.db.clone();
        let jobs_ks = self.jobs.clone();
        let payloads_ks = self.payloads.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue = self.jobs_by_queue.clone();
        let jobs_by_type = self.jobs_by_type.clone();

        task::spawn_blocking(move || {
            let snapshot = db.read_tx();
            // We take 1 more job than requested so that we know if there's a
            // next page or not.
            let fetch = opts.limit + 1;
            let mut rows = Vec::with_capacity(fetch);

            // Helper: build queue-index MergeSources for the given queues.
            let queue_sources = |queues: &HashSet<String>,
                                 from: &Option<String>,
                                 direction: ScanDirection|
             -> Vec<MergeSource<'_>> {
                queues
                    .iter()
                    .map(|queue_name| {
                        let prefix_len = queue_name.len() + 1;
                        let start = match from {
                            Some(cursor) => Bound::Excluded(make_queue_key(queue_name, cursor)),
                            None => Bound::Included(make_queue_key(queue_name, "")),
                        };
                        let mut end_key = Vec::with_capacity(queue_name.len() + 1);
                        end_key.extend_from_slice(queue_name.as_bytes());
                        end_key.push(1);
                        let end = Bound::Excluded(end_key);

                        match direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&jobs_by_queue, (start, end)),
                                prefix_len
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_queue, (start, end))
                                    .rev(),
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
                            None => Bound::Included(vec![prefix, 0]),
                        };
                        let end = Bound::Excluded(vec![prefix + 1, 0]);

                        match direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&jobs_by_status, (start, end)),
                                2
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_status, (start, end))
                                    .rev(),
                                2
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
                        let prefix_len = type_name.len() + 1;
                        let start = match from {
                            Some(cursor) => Bound::Excluded(make_type_key(type_name, cursor)),
                            None => Bound::Included(make_type_key(type_name, "")),
                        };
                        let mut end_key = Vec::with_capacity(type_name.len() + 1);
                        end_key.extend_from_slice(type_name.as_bytes());
                        end_key.push(1);
                        let end = Bound::Excluded(end_key);

                        match direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&jobs_by_type, (start, end)),
                                prefix_len
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_type, (start, end))
                                    .rev(),
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
                    load_jobs_by_ids(&jobs_ks, &payloads_ks, combined.take(fetch), &source_desc)?;
            } else {
                // No filter — scan the jobs keyspace directly.
                match opts.direction {
                    ScanDirection::Asc => {
                        let range = match &opts.from {
                            Some(cursor) => snapshot.range::<&str, _>(
                                &jobs_ks,
                                (Bound::Excluded(cursor.as_str()), Bound::Unbounded),
                            ),
                            None => snapshot.range::<&str, _>(&jobs_ks, ..),
                        };

                        for entry in range.take(fetch) {
                            let (key, value) = entry.into_inner()?;
                            let mut job: Job = rmp_serde::from_slice(&value)?;
                            if let Some(payload_bytes) = payloads_ks.get(&*key)? {
                                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
                            }
                            rows.push(job);
                        }
                    }
                    ScanDirection::Desc => {
                        let range = match &opts.from {
                            Some(cursor) => snapshot.range::<&str, _>(
                                &jobs_ks,
                                (Bound::Unbounded, Bound::Excluded(cursor.as_str())),
                            ),
                            None => snapshot.range::<&str, _>(&jobs_ks, ..),
                        };

                        for entry in range.rev().take(fetch) {
                            let (key, value) = entry.into_inner()?;
                            let mut job: Job = rmp_serde::from_slice(&value)?;
                            if let Some(payload_bytes) = payloads_ks.get(&*key)? {
                                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
                            }
                            rows.push(job);
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
                ListJobsOptions::new()
                    .from(cursor.to_string())
                    .direction(direction)
                    .limit(opts.limit)
                    .statuses(opts.statuses.clone())
                    .queues(opts.queues.clone())
                    .types(opts.types.clone())
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

    /// List error records for a job with cursor-based pagination.
    ///
    /// Returns a page of `ErrorRecord`s plus `next` / `prev` cursors for
    /// follow-up requests. The cursor is the attempt number (u32), which is
    /// the natural sort key within a job's error records.
    pub async fn list_errors(&self, opts: ListErrorsOptions) -> Result<ListErrorsPage, StoreError> {
        let errors_ks = self.errors.clone();

        task::spawn_blocking(move || {
            // We take 1 more record than requested so that we know if there's
            // a next page or not.
            let fetch = opts.limit + 1;

            // Build the prefix for this job: `{job_id}\0`
            let mut prefix = Vec::with_capacity(opts.job_id.len() + 1);
            prefix.extend_from_slice(opts.job_id.as_bytes());
            prefix.push(0);

            // End sentinel: `{job_id}\x01` — one byte past the null separator,
            // so the range covers all `{job_id}\0{...}` keys.
            let mut end_sentinel = Vec::with_capacity(opts.job_id.len() + 1);
            end_sentinel.extend_from_slice(opts.job_id.as_bytes());
            end_sentinel.push(1);

            let start = match opts.from {
                Some(cursor) => Bound::Excluded(make_error_key(&opts.job_id, cursor)),
                None => Bound::Included(prefix),
            };
            let end = Bound::Excluded(end_sentinel);

            let mut rows = Vec::with_capacity(fetch);

            match opts.direction {
                ScanDirection::Asc => {
                    for entry in errors_ks
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
                    for entry in errors_ks
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
        let jobs = self.jobs.clone();
        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || -> Result<(Vec<Job>, Option<u64>), StoreError> {
            // Lock the BTreeSet, collect due entries, unlock. The mutex is
            // held only for the in-memory scan — fjall reads happen outside.
            let (due_entries, next_ready_at) = scheduled_index.lock().unwrap().next_due(now, limit);

            let mut result = Vec::with_capacity(due_entries.len());
            for (_ready_at, job_id) in &due_entries {
                let job_bytes = jobs.get(job_id)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "job in scheduled index but missing from jobs keyspace: {job_id:?}",
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
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let jobs_by_status = self.jobs_by_status.clone();

        let id = job.id.clone();
        let event_queue = job.queue.clone();
        let queue = job.queue.clone();
        let priority = job.priority;
        let ready_at = job.ready_at;

        task::spawn_blocking(move || -> Result<(), StoreError> {
            // Remove from the in-memory scheduled index *before* commit to
            // prevent data races (same as take_next_job).
            scheduled_index.lock().unwrap().remove(ready_at, &id);

            let mut tx = db.write_tx();

            // Re-read the job inside the write lock to confirm it's still
            // scheduled (a concurrent promote or deletion could have removed
            // it).
            let job_bytes = match jobs.get(&id)? {
                Some(bytes) => bytes,
                None => return Ok(()), // Already gone.
            };
            let current: Job = rmp_serde::from_slice(&job_bytes)?;
            if current.status != JobStatus::Scheduled as u8 {
                return Ok(()); // No longer scheduled.
            }

            // Swap status indexes: Scheduled -> Ready.
            let old_status_key = make_status_key(JobStatus::Scheduled, &id);
            let new_status_key = make_status_key(JobStatus::Ready, &id);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");

            // Update the job record itself.
            let mut updated = current;
            updated.status = JobStatus::Ready.into();
            let updated_bytes = rmp_serde::to_vec_named(&updated)?;
            tx.insert(&jobs, &id, &updated_bytes);

            if let Err(e) = tx.commit() {
                // Commit failed — the LSM transaction rolled back. Acquire
                // a new write transaction to serialize against concurrent
                // writers, check whether the job is still Scheduled, and
                // re-insert into the index if so.
                let _tx = db.write_tx();
                if let Ok(Some(bytes)) = jobs.get(&id) {
                    if let Ok(current) = rmp_serde::from_slice::<Job>(&bytes) {
                        if current.status == JobStatus::Scheduled as u8 {
                            scheduled_index.lock().unwrap().insert(ready_at, id);
                        }
                    }
                }
                return Err(e.into());
            }

            // Insert into the in-memory ready index after commit succeeds.
            ready_index.insert(&queue, priority, id);

            Ok(())
        })
        .await??;

        let _ = self.event_tx.send(StoreEvent::JobCreated {
            queue: event_queue,
            token: Arc::new(AtomicBool::new(false)),
        });

        Ok(())
    }

    /// Recover state after startup.
    ///
    /// Three-phase recovery that must run before accepting requests:
    ///
    /// 1. Working -> Ready: If the server crashed while jobs were being
    ///    processed, those jobs are stuck in the `Working` state with no
    ///    client handling them. This phase scans the status index and
    ///    atomically moves them back to `Ready`.
    ///
    /// 2. Rebuild ready index: The in-memory skip list is empty after
    ///    opening the database. This phase scans the status index for all
    ///    `Ready` jobs (including any just recovered above) and populates
    ///    the index so `take_next_job` can find them.
    ///
    /// 3. Rebuild scheduled index: The in-memory BTreeSet is empty after
    ///    opening the database. This phase scans the status index for all
    ///    `Scheduled` jobs and populates the chronological index so the
    ///    scheduler can find them.
    ///
    /// This method must be called before any subscribers exist (no events are
    /// fired for recovered jobs).
    ///
    /// Returns `(recovered, ready_indexed, scheduled_indexed)` — the number
    /// of orphaned working jobs moved back to Ready, the total number of
    /// Ready jobs in the index, and the total number of Scheduled jobs in
    /// the index.
    pub async fn recover(&self) -> Result<(usize, usize, usize), StoreError> {
        let recovered = self.recover_working_jobs().await?;
        let ready = self.rebuild_ready_index().await?;
        let scheduled = self.rebuild_scheduled_index().await?;
        Ok((recovered, ready, scheduled))
    }

    /// Move orphaned working jobs back to Ready in the LSM indexes.
    ///
    /// Does not touch the in-memory ready index — that's handled by
    /// `rebuild_ready_index`, which runs immediately after.
    async fn recover_working_jobs(&self) -> Result<usize, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let jobs_by_status = self.jobs_by_status.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Working jobs.
            // Working = 2, so the prefix range is [2, 0]..[3, 0].
            let start: Vec<u8> = vec![JobStatus::Working as u8, 0];
            let end: Vec<u8> = vec![JobStatus::Working as u8 + 1, 0];
            let range = (Bound::Included(start), Bound::Excluded(end));

            // Collect IDs first — we can't hold an iterator across the write tx.
            let snapshot = db.read_tx();
            let working_ids: Vec<String> = snapshot
                .range::<Vec<u8>, _>(&jobs_by_status, range)
                .map(|entry| {
                    let (key, _) = entry.into_inner()?;
                    // Key layout: {status_u8}\0{job_id} — skip the 2-byte prefix.
                    String::from_utf8(key[2..].to_vec()).map_err(|e| {
                        StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                    })
                })
                .collect::<Result<_, _>>()?;
            drop(snapshot);

            if working_ids.is_empty() {
                return Ok(0);
            }

            let count = working_ids.len();
            let mut tx = db.write_tx();

            for id in &working_ids {
                let job_bytes = jobs.get(id)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "working job missing from jobs keyspace: {id:?}"
                    ))
                })?;
                let mut job: Job = rmp_serde::from_slice(&job_bytes)?;

                // Swap status indexes: Working -> Ready.
                let old_status_key = make_status_key(JobStatus::Working, id);
                let new_status_key = make_status_key(JobStatus::Ready, id);
                tx.remove(&jobs_by_status, &old_status_key);
                tx.insert(&jobs_by_status, &new_status_key, b"");

                // Update the job record itself.
                job.status = JobStatus::Ready.into();
                let updated_bytes = rmp_serde::to_vec_named(&job)?;
                tx.insert(&jobs, id, &updated_bytes);
            }

            tx.commit()?;
            Ok(count)
        })
        .await?
    }

    /// Populate the in-memory ready index from the `jobs_by_status` index.
    ///
    /// Scans for all Ready jobs, reads their metadata to get queue and
    /// priority, and inserts each entry into the skip list. No mutex needed —
    /// each `insert()` is lock-free, and recovery runs before any consumers.
    async fn rebuild_ready_index(&self) -> Result<usize, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let ready_index = self.ready_index.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Ready jobs.
            // Ready = 1, so the prefix range is [1, 0]..[2, 0].
            let start: Vec<u8> = vec![JobStatus::Ready as u8, 0];
            let end: Vec<u8> = vec![JobStatus::Ready as u8 + 1, 0];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let snapshot = db.read_tx();
            let mut count = 0;

            for entry in snapshot.range::<Vec<u8>, _>(&jobs_by_status, range) {
                let (key, _) = entry.into_inner()?;
                // Key layout: {status_u8}\0{job_id} — skip the 2-byte prefix.
                let job_id = String::from_utf8(key[2..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;

                let job_bytes = jobs.get(&job_id)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "ready job missing from jobs keyspace: {job_id:?}"
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
    /// and inserts each entry into the BTreeSet.
    async fn rebuild_scheduled_index(&self) -> Result<usize, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Scheduled jobs.
            // Scheduled = 0, so the prefix range is [0, 0]..[1, 0].
            let start: Vec<u8> = vec![JobStatus::Scheduled as u8, 0];
            let end: Vec<u8> = vec![JobStatus::Scheduled as u8 + 1, 0];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let snapshot = db.read_tx();
            let mut idx = scheduled_index.lock().unwrap();
            let mut count = 0;

            for entry in snapshot.range::<Vec<u8>, _>(&jobs_by_status, range) {
                let (key, _) = entry.into_inner()?;
                // Key layout: {status_u8}\0{job_id} — skip the 2-byte prefix.
                let job_id = String::from_utf8(key[2..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;

                let job_bytes = jobs.get(&job_id)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "scheduled job missing from jobs keyspace: {job_id:?}"
                    ))
                })?;
                let job: Job = rmp_serde::from_slice(&job_bytes)?;

                idx.insert(job.ready_at, job_id);
                count += 1;
            }

            Ok(count)
        })
        .await?
    }
}

/// Build a status index key: `{status_byte}\0{job_id}`.
fn make_status_key(status: JobStatus, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(status as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a queue membership index key: `{queue_name}\0{job_id}`.
fn make_queue_key(queue_name: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(queue_name.len() + 1 + job_id.len());
    key.extend_from_slice(queue_name.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a type membership index key: `{job_type}\0{job_id}`.
fn make_type_key(job_type: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(job_type.len() + 1 + job_id.len());
    key.extend_from_slice(job_type.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build an error record key: `{job_id}\0{attempt_be_u32}`.
///
/// The null separator lets us prefix-scan all errors for a given job,
/// and the big-endian attempt number gives chronological ordering.
fn make_error_key(job_id: &str, attempt: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(job_id.len() + 1 + 4);
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
fn error_keys(errors: &SingleWriterTxKeyspace, job_id: &str) -> impl Iterator<Item = Vec<u8>> {
    let mut prefix = Vec::with_capacity(job_id.len() + 1);
    prefix.extend_from_slice(job_id.as_bytes());
    prefix.push(0);

    errors
        .inner()
        .prefix(&prefix)
        .filter_map(|guard| guard.key().ok().map(|k| k.to_vec()))
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

    let base_delay = (attempts as f32).powf(backoff.exponent) + backoff.base_ms;

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
        assert_eq!(job.status, u8::from(JobStatus::Working));
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
        assert!(store.mark_completed(&job.id).await.unwrap());

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
        assert!(!store.mark_completed("nonexistent").await.unwrap());
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
        assert!(store.mark_completed(&job.id).await.unwrap());
        assert!(!store.mark_completed(&job.id).await.unwrap());
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
                    // Also verify the ack works (job is in Working status).
                    assert!(
                        store.mark_completed(&job.id).await.unwrap(),
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
        let stored = store.get_job(&taken.id).await.unwrap().unwrap();
        assert_eq!(stored.dequeued_at, taken.dequeued_at);
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
        assert_eq!(retaken.status, u8::from(JobStatus::Working));
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
        assert!(store.mark_completed(&taken.id).await.unwrap());

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
        store.mark_completed(&taken.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCompleted(id) => assert_eq!(id, job.id),
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

        let job = store.get_job(&enqueued.id).await.unwrap().unwrap();
        assert_eq!(job.id, enqueued.id);
        assert_eq!(job.queue, "default");
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, Some(serde_json::json!("hello")));
    }

    #[tokio::test]
    async fn get_job_returns_none_for_unknown_id() {
        let store = test_store();
        assert!(store.get_job("nonexistent").await.unwrap().is_none());
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
        store.mark_completed(&enqueued.id).await.unwrap();

        assert!(store.get_job(&enqueued.id).await.unwrap().is_none());
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
    async fn list_jobs_includes_working_jobs() {
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
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Working));
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
        store.mark_completed(&taken.id).await.unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
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

        // Take the first job so it becomes Working.
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
    async fn list_jobs_filters_by_working_status() {
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
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Working])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Working));
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
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Working])))
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

        // Now working.
        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Working])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        // Requeue — should move back to ready.
        store.requeue(&a.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Working])))
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
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Working])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        store.mark_completed(&taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Working])))
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
        store.mark_completed(&taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().queues(HashSet::from(["emails".into()])))
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

        // Take a and b so they become Working.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Filter by Ready + Working — should get all 3.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::Working])),
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
            .unwrap(); // a -> working

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::Working]))
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

        // Take a so it becomes Working.
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

        // Working jobs in emails queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Working])),
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

        // Take a and b so they become Working.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready + Working in emails + webhooks (excludes reports).
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into(), "webhooks".into()]))
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::Working])),
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
            .unwrap(); // a -> working

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::Working]))
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
                    .statuses(HashSet::from([JobStatus::Working])),
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
            .unwrap(); // a -> working

        // Working in emails.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Working])),
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
                    .statuses(HashSet::from([JobStatus::Working])),
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
        store.mark_completed(&taken.id).await.unwrap();

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

        // Working should be empty after completion.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Working])),
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
            .unwrap(); // a -> working

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::Working]))
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
        store.mark_completed(&taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().types(HashSet::from(["send_email".into()])))
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

        // Take a so it becomes working.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Working send_email jobs only.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .statuses(HashSet::from([JobStatus::Working])),
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

        // Take a so it becomes working.
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
        // b is the only ready send_email in the high queue (a is working).
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
            StoreEvent::JobScheduled { ready_at } if ready_at == future
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
    async fn recover_moves_working_to_ready() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap();

        // Take the job so it becomes Working.
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
        let (recovered, indexed, _scheduled) = store.recover().await.unwrap();
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
    async fn recover_returns_zero_when_none_working() {
        let store = test_store();

        // Enqueue a job but don't take it — it stays Ready.
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap();

        let (recovered, indexed, _scheduled) = store.recover().await.unwrap();
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

        // Take both so they become Working.
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
        let (recovered, indexed, _scheduled) = store.recover().await.unwrap();
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

        // Recover should find no working jobs, but index the ready one.
        let (recovered, indexed, _scheduled) = store.recover().await.unwrap();
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
        let fetched = store.get_job(&scheduled.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Scheduled as u8);
    }

    // --- record_failure tests ---

    /// Build a FailureOptions with sensible defaults for testing.
    /// retry_limit=3, zero-jitter backoff so delays are deterministic.
    fn test_failure_opts() -> FailureOptions {
        FailureOptions {
            error: "something broke".into(),
            error_type: None,
            backtrace: None,
            retry_at: None,
            kill: false,
            default_retry_limit: 3,
            default_backoff: BackoffConfig {
                exponent: 2.0,
                base_ms: 100.0,
                jitter_ms: 0.0, // deterministic
            },
        }
    }

    /// Enqueue a job and take it so it's in the Working state.
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
        // ready_at should be in the future (base_ms=100, exponent=2, 1^2+100=101).
        assert!(result.ready_at > 0);
    }

    #[tokio::test]
    async fn record_failure_kills_when_retries_exhausted() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // retry_limit=0 means the first failure kills.
        let mut opts = test_failure_opts();
        opts.default_retry_limit = 0;

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);
        assert_eq!(result.attempts, 1);

        // Job should be gone from the store.
        assert!(store.get_job(&job.id).await.unwrap().is_none());
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
        assert!(store.get_job(&job.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn record_failure_kill_overrides_remaining_retries() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // retry_limit=100, but kill=true should still kill.
        let mut opts = test_failure_opts();
        opts.default_retry_limit = 100;
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
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // retry_limit=0, but retry_at should force a retry anyway.
        let mut opts = test_failure_opts();
        opts.default_retry_limit = 0;
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
        let scheduled = store.get_job(&job.id).await.unwrap().unwrap();
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
        opts.error = "connection timeout".into();
        opts.error_type = Some("TimeoutError".into());
        opts.backtrace = Some("at line 42".into());

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Read the error record from the errors keyspace.
        let key = make_error_key(&job.id, 1);
        let raw = store.errors.inner().get(&key).unwrap().unwrap();
        let err: ErrorRecord = rmp_serde::from_slice(&raw).unwrap();

        assert_eq!(err.message, "connection timeout");
        assert_eq!(err.error_type.as_deref(), Some("TimeoutError"));
        assert_eq!(err.backtrace.as_deref(), Some("at line 42"));
        assert!(err.failed_at > 0);
    }

    #[tokio::test]
    async fn record_failure_dead_cleans_up_error_records() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Kill immediately — the error record written during this call
        // should also be cleaned up since dead jobs are fully removed.
        let mut opts = test_failure_opts();
        opts.kill = true;

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // No error records should remain.
        let keys: Vec<_> = error_keys(&store.errors, &job.id).collect();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn record_failure_dead_removes_payload() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Verify payload exists before kill.
        assert!(store.payloads.inner().get(&job.id).unwrap().is_some());

        let mut opts = test_failure_opts();
        opts.kill = true;

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Payload should be gone.
        assert!(store.payloads.inner().get(&job.id).unwrap().is_none());
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
            StoreEvent::JobScheduled { ready_at } => assert!(ready_at > 0),
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
        let scheduled = store.get_job(&job.id).await.unwrap().unwrap();
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
    async fn mark_completed_cleans_up_error_records() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Fail once (retry), then promote, take, and complete.
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();

        let scheduled = store.get_job(&job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(&retaken.id).await.unwrap();

        // Error records from the earlier failure should be gone.
        let keys: Vec<_> = error_keys(&store.errors, &job.id).collect();
        assert!(keys.is_empty());
    }

    // --- compute_backoff tests ---

    #[test]
    fn compute_backoff_zero_jitter_is_deterministic() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100.0,
            jitter_ms: 0.0,
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
            base_ms: 100.0,
            jitter_ms: 50.0,
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
            base_ms: 100.0,
            jitter_ms: 0.0,
        };

        // attempts=0: 0^2 + 100 = 100
        assert_eq!(compute_backoff(0, &backoff), 100);
    }

    // --- list_errors tests ---

    /// Helper: fail a working job, promote+retake it, and return the retaken job.
    ///
    /// This cycles one failure through the store so that `list_errors` has
    /// something to read. The `error_msg` lets callers tag each failure for
    /// assertion clarity.
    async fn fail_and_retake(store: &Store, job_id: &str, error_msg: &str) -> Job {
        let mut opts = test_failure_opts();
        opts.error = error_msg.into();
        store
            .record_failure(now_millis(), job_id, opts)
            .await
            .unwrap()
            .unwrap();

        // Promote from Scheduled -> Ready and take again.
        let scheduled = store.get_job(job_id).await.unwrap().unwrap();
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
        opts.error = "error two".into();
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
        opts.error = "error two".into();
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
        opts.error = "error three".into();
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
        opts.error = "error two".into();
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
        opts.error = "second".into();
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
}
