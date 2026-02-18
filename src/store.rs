// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Persistent storage layer to manage job queues.
//!
//! Wraps fjall (database) to provide transactional queue operations across
//! eight keyspaces:
//!
//! - `jobs`: source of truth, keyed by job ID, stores job metadata (no payload).
//! - `payloads`: immutable job payloads, keyed by job ID.
//! - `ready_jobs_by_priority`: partial priority index of ready jobs, keyed by `{priority_b36:4}\0{job_id}`.
//! - `ready_jobs_by_queue_and_priority`: per-queue partial priority index of ready jobs, keyed by `{queue_name}\0{priority_b36:4}\0{job_id}`.
//! - `scheduled_jobs_by_ready_at`: partial index of scheduled jobs, keyed by `{ready_at_be_u64}\0{job_id}`.
//! - `jobs_by_status`: status index, keyed by `{status_u8}\0{job_id}`.
//! - `jobs_by_queue`: queue membership index, keyed by `{queue_name}\0{job_id}`.
//! - `jobs_by_queue_and_status`: compound index, keyed by `{queue_name}\0{status_u8}\0{job_id}`.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

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
    /// metadata record. Defaults to `Null` when deserializing from the
    /// `jobs` keyspace, which no longer contains the payload.
    #[serde(rename = "p")]
    #[serde(default)]
    pub payload: serde_json::Value,

    /// Current lifecycle status, stored as a u8 which converts to `JobStatus`.
    #[serde(rename = "s")]
    pub status: u8,

    /// When the job becomes eligible to run (milliseconds since Unix epoch).
    #[serde(rename = "r")]
    #[serde(default)]
    pub ready_at: u64,
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

/// Options for enqueuing a new job.
pub struct EnqueueOptions {
    /// Queue this job belongs to.
    pub queue: String,

    /// Arbitrary payload provided by the client.
    pub payload: serde_json::Value,

    /// Priority (lower number = higher priority). Defaults to 0.
    pub priority: u16,

    /// When the job becomes eligible to run (milliseconds since Unix epoch).
    /// `None` means immediately.
    pub ready_at: Option<u64>,
}

impl EnqueueOptions {
    /// Create options with required fields; priority defaults to 0, ready_at
    /// to now.
    pub fn new(queue: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            queue: queue.into(),
            payload,
            priority: 0,
            ready_at: None,
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
}

/// Events broadcast by the store when job state changes.
///
/// Workers use these events to decide when to check for new work:
///
/// - `JobCreated`: a job became ready. Workers check the queue name, then
///   try to atomically claim the one-shot `token`. Only the winner calls
///   `take_next_job`, avoiding thundering-herd DB contention.
/// - `JobCompleted`: workers prune their in-flight set; no DB call needed.
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

    /// A job was enqueued with a future `ready_at` timestamp.
    ///
    /// The scheduler listens for this to wake up early if the new job is
    /// due sooner than whatever it was sleeping until. Workers ignore it.
    JobScheduled { ready_at: u64 },
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

    /// Partial index of ready jobs, ordered by priority then job ID.
    ///
    /// Only jobs in the `Ready` state appear here. Taking a job removes it
    /// from this index; requeueing adds it back.
    ready_jobs_by_priority: SingleWriterTxKeyspace,

    /// Per-queue partial index of ready jobs, ordered by priority then job ID.
    ///
    /// Keyed by `{queue_name}\0{priority_b36:4}\0{job_id}` with job ID as
    /// value. Same lifecycle as `ready_jobs_by_priority` but partitioned by
    /// queue, enabling queue-filtered take via k-way merge.
    ready_jobs_by_queue_and_priority: SingleWriterTxKeyspace,

    /// Partial index of scheduled jobs, ordered by ready_at then job ID.
    ///
    /// Keyed by `{ready_at_be_u64}\0{job_id}` with empty values. Only jobs
    /// in the `Scheduled` state appear here. A background task scans this
    /// index to promote jobs to `Ready` once their time arrives.
    scheduled_jobs_by_ready_at: SingleWriterTxKeyspace,

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
    jobs_by_queue: SingleWriterTxKeyspace,

    /// Compound queue + status index.
    ///
    /// Keyed by `{queue_name}\0{status_u8}\0{job_id}` with empty values.
    /// Enables efficient combined filtering by queue and status.
    jobs_by_queue_and_status: SingleWriterTxKeyspace,

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

/// Merge multiple sorted sources of job IDs using a binary heap, collecting
/// up to `limit` IDs in the requested order.
///
/// For ascending, uses a min-heap (via `Reverse`). For descending, uses a
/// max-heap. Each source must already yield IDs in the matching order.
fn kway_merge(
    mut sources: Vec<MergeSource<'_>>,
    direction: ScanDirection,
    limit: usize,
) -> Result<Vec<Vec<u8>>, StoreError> {
    let mut result = Vec::with_capacity(limit);

    match direction {
        ScanDirection::Asc => {
            let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(id) = src() {
                    heap.push(Reverse((id?, i)));
                }
            }
            while let Some(Reverse((id, i))) = heap.pop() {
                result.push(id);
                if result.len() >= limit {
                    break;
                }
                if let Some(next) = sources[i]() {
                    heap.push(Reverse((next?, i)));
                }
            }
        }
        ScanDirection::Desc => {
            let mut heap: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::new();
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(id) = src() {
                    heap.push((id?, i));
                }
            }
            while let Some((id, i)) = heap.pop() {
                result.push(id);
                if result.len() >= limit {
                    break;
                }
                if let Some(next) = sources[i]() {
                    heap.push((next?, i));
                }
            }
        }
    }

    Ok(result)
}

/// Look up full job records by ID, returning an error if any ID is missing
/// from the `jobs` keyspace (data corruption). Hydrates each job's payload
/// from the separate `payloads` keyspace.
fn load_jobs_by_ids(
    jobs_ks: &SingleWriterTxKeyspace,
    payloads_ks: &SingleWriterTxKeyspace,
    ids: &[Vec<u8>],
    index_name: &str,
    rows: &mut Vec<Job>,
) -> Result<(), StoreError> {
    for id in ids {
        let bytes = jobs_ks.get(id)?.ok_or_else(|| {
            StoreError::Corruption(format!(
                "job in {index_name} but missing from jobs keyspace: {:?}",
                String::from_utf8_lossy(id),
            ))
        })?;
        let mut job: Job = rmp_serde::from_slice(&bytes)?;

        // Hydrate the payload from the payloads keyspace.
        if let Some(payload_bytes) = payloads_ks.get(id)? {
            job.payload = rmp_serde::from_slice(&payload_bytes)?;
        }

        rows.push(job);
    }
    Ok(())
}

impl Store {
    /// Open or create a store at the given path.
    ///
    /// The path refers to the directory in which fjall stores its keyspace
    /// data.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let db = SingleWriterTxDatabase::builder(path).open()?;

        // Index keyspaces are only ever range-scanned, never point-read,
        // so bloom filters would waste memory and CPU during flushes.
        // Point-read keyspaces (jobs, payloads) keep the default bloom
        // filters for efficient ID lookups.
        let index_opts: fn() -> fjall::KeyspaceCreateOptions =
            || fjall::KeyspaceCreateOptions::default().filter_policy(FilterPolicy::disabled());

        let jobs = db.keyspace("jobs", fjall::KeyspaceCreateOptions::default)?;
        let payloads = db.keyspace("payloads", fjall::KeyspaceCreateOptions::default)?;
        let ready_jobs_by_priority = db.keyspace("ready_jobs_by_priority", index_opts)?;
        let ready_jobs_by_queue_and_priority =
            db.keyspace("ready_jobs_by_queue_and_priority", index_opts)?;
        let scheduled_jobs_by_ready_at = db.keyspace("scheduled_jobs_by_ready_at", index_opts)?;
        let jobs_by_status = db.keyspace("jobs_by_status", index_opts)?;
        let jobs_by_queue = db.keyspace("jobs_by_queue", index_opts)?;
        let jobs_by_queue_and_status = db.keyspace("jobs_by_queue_and_status", index_opts)?;

        let (event_tx, _) = broadcast::channel(1024);

        Ok(Self {
            db,
            jobs,
            payloads,
            ready_jobs_by_priority,
            ready_jobs_by_queue_and_priority,
            scheduled_jobs_by_ready_at,
            jobs_by_status,
            jobs_by_queue,
            jobs_by_queue_and_status,
            event_tx,
        })
    }

    /// Enqueue a new job.
    ///
    /// Generates a unique job ID and inserts the job into the `jobs`
    /// keyspace plus the appropriate indexes. If `ready_at` is in the
    /// future the job enters the `Scheduled` state and is indexed in
    /// `scheduled_jobs_by_ready_at`; otherwise it goes straight to `Ready`
    /// and into the priority indexes.
    ///
    /// Subscribers are notified when a job enters the ready state.
    pub async fn enqueue(&self, opts: EnqueueOptions) -> Result<Job, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let ready_jobs_by_queue_and_priority = self.ready_jobs_by_queue_and_priority.clone();
        let scheduled_jobs_by_ready_at = self.scheduled_jobs_by_ready_at.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue = self.jobs_by_queue.clone();
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();

        let job = task::spawn_blocking(move || -> Result<Job, StoreError> {
            let id = scru128::new_string();

            // Get the current time which is both a default `ready_at` and a
            // point of comparison to know if the job should be scheduled or
            // should be queued immediately.
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

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
                queue: opts.queue,
                priority: opts.priority,
                payload: opts.payload,
                status: status.into(),
                ready_at,
            };

            // Serialize metadata without the payload for the jobs keyspace.
            let mut meta = job.clone();
            meta.payload = serde_json::Value::Null;
            let meta_bytes = rmp_serde::to_vec_named(&meta)?;

            let queue_key = make_queue_key(&job.queue, &id);
            let status_key = make_status_key(status, &id);
            let queue_status_key = make_queue_status_key(&job.queue, status, &id);

            let mut tx = db.write_tx();
            tx.insert(&jobs, &id, &meta_bytes);
            tx.insert(&payloads, &id, &payload_bytes);
            tx.insert(&jobs_by_queue, &queue_key, b"");
            tx.insert(&jobs_by_status, &status_key, b"");
            tx.insert(&jobs_by_queue_and_status, &queue_status_key, b"");

            // The `ready_jobs_by_priority*` and `scheduled_jobs_by_ready_at`
            // indexes are partial. We have to insert into either one
            // (exclusive) depending on whether or not the job is ready to run
            // now.
            if scheduled {
                let scheduled_key = make_scheduled_key(ready_at, &id);
                tx.insert(&scheduled_jobs_by_ready_at, &scheduled_key, b"");
            } else {
                let priority_key = make_priority_key(job.priority, &id);
                let queue_priority_key = make_queue_priority_key(&job.queue, job.priority, &id);
                tx.insert(&ready_jobs_by_priority, &priority_key, b"");
                tx.insert(&ready_jobs_by_queue_and_priority, &queue_priority_key, b"");
            }

            tx.commit()?;
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
    /// Atomically removes the highest-priority (lowest number), oldest job
    /// and marks it as working. Returns `None` if no ready jobs match.
    ///
    /// When `queues` is non-empty, only jobs in the specified queues are
    /// considered, using a k-way merge across per-queue priority ranges.
    /// When empty, all ready jobs are candidates.
    ///
    /// Subscribers are not notified.
    pub async fn take_next_job(&self, queues: &HashSet<String>) -> Result<Option<Job>, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let payloads = self.payloads.clone();
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let ready_jobs_by_queue_and_priority = self.ready_jobs_by_queue_and_priority.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();
        let queues = queues.clone();

        task::spawn_blocking(move || {
            // Acquire the write lock before reading, so no other writer
            // can take the same job between our read and the remove.
            let mut tx = db.write_tx();

            // Find the highest-priority, oldest ready job, returning
            // (priority_key, job_id) as raw bytes for index manipulation.
            let (priority_key, job_id): (Vec<u8>, Vec<u8>) = if queues.is_empty() {
                // Unfiltered: the first key in the global priority index.
                let entry = match ready_jobs_by_priority.first_key_value() {
                    Some(entry) => entry,
                    None => return Ok(None),
                };
                let (k, _) = entry.into_inner()?;
                let priority_key = k.to_vec();
                let job_id = priority_key[5..].to_vec();
                (priority_key, job_id)
            } else {
                // Queue-filtered: k-way merge across per-queue priority
                // ranges with limit 1 to find the single best candidate.
                // We create a read snapshot for range iteration; the write
                // lock we already hold ensures consistency.
                let snapshot = db.read_tx();
                let mut sources: Vec<MergeSource<'_>> = Vec::new();
                for queue_name in &queues {
                    let prefix_len = queue_name.len() + 1;
                    let mut start_key = Vec::with_capacity(prefix_len);
                    start_key.extend_from_slice(queue_name.as_bytes());
                    start_key.push(0);
                    let start = Bound::Included(start_key);
                    let mut end_key = Vec::with_capacity(prefix_len);
                    end_key.extend_from_slice(queue_name.as_bytes());
                    end_key.push(1);
                    let end = Bound::Excluded(end_key);

                    sources.push(range_source!(
                        snapshot
                            .range::<Vec<u8>, _>(&ready_jobs_by_queue_and_priority, (start, end),),
                        prefix_len
                    ));
                }

                let mut ids = kway_merge(sources, ScanDirection::Asc, 1)?;
                if ids.is_empty() {
                    return Ok(None);
                }

                // The merge result is {priority_b36:4}\0{job_id} — this is
                // also the key in ready_jobs_by_priority.
                let priority_key = ids.remove(0);
                let job_id = priority_key[5..].to_vec();
                (priority_key, job_id)
            };

            // Look up job metadata from the jobs keyspace (payload is Null
            // here — it lives in the payloads keyspace).
            let meta_bytes = jobs.get(&job_id)?.ok_or_else(|| {
                StoreError::Corruption(format!(
                    "job in ready_jobs_by_priority but missing from jobs keyspace: {:?}",
                    String::from_utf8_lossy(&job_id),
                ))
            })?;

            let mut job: Job = rmp_serde::from_slice(&meta_bytes)?;
            let old_status_key = make_status_key(JobStatus::Ready, &job.id);
            let old_queue_status_key = make_queue_status_key(&job.queue, JobStatus::Ready, &job.id);
            let queue_priority_key = make_queue_priority_key(&job.queue, job.priority, &job.id);
            job.status = JobStatus::Working.into();
            let new_status_key = make_status_key(JobStatus::Working, &job.id);
            let new_queue_status_key =
                make_queue_status_key(&job.queue, JobStatus::Working, &job.id);

            // Only write metadata back — payload is immutable and stays in
            // its own keyspace.
            let meta_bytes = rmp_serde::to_vec_named(&job)?;

            // Remove from priority indexes, update job record and status indexes.
            tx.remove(&ready_jobs_by_priority, &priority_key);
            tx.remove(&ready_jobs_by_queue_and_priority, &queue_priority_key);
            tx.insert(&jobs, &job_id, &meta_bytes);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");
            tx.remove(&jobs_by_queue_and_status, &old_queue_status_key);
            tx.insert(&jobs_by_queue_and_status, &new_queue_status_key, b"");
            tx.commit()?;

            // Hydrate the payload from the payloads keyspace for the caller.
            if let Some(payload_bytes) = payloads.get(&job_id)? {
                job.payload = rmp_serde::from_slice(&payload_bytes)?;
            }

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
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();
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

            // Can't mark a job as completed if it's no currently working.
            if job.status != JobStatus::Working as u8 {
                return Ok(false);
            }

            // Clean up all indexes.
            let status_key = make_status_key(JobStatus::Working, &id);
            let queue_key = make_queue_key(&job.queue, &id);
            let queue_status_key = make_queue_status_key(&job.queue, JobStatus::Working, &id);

            tx.remove(&jobs, &id);
            tx.remove(&payloads, &id);
            tx.remove(&jobs_by_status, &status_key);
            tx.remove(&jobs_by_queue, &queue_key);
            tx.remove(&jobs_by_queue_and_status, &queue_status_key);
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
                job.payload = rmp_serde::from_slice(&payload_bytes)?;
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
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let ready_jobs_by_queue_and_priority = self.ready_jobs_by_queue_and_priority.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();
        let id = id.to_string();

        // Returns Some(queue_name) if the job was requeued, None otherwise.
        let requeued_queue = task::spawn_blocking(move || -> Result<Option<String>, StoreError> {
            let mut tx = db.write_tx();

            let old_status_key = make_status_key(JobStatus::Working, &id);
            if jobs_by_status.get(&old_status_key)?.is_none() {
                return Ok(None);
            }

            // The job is still working, so it must still be in the jobs
            // keyspace (only mark_completed removes from both).
            let job_bytes = jobs.get(&id)?.ok_or_else(|| {
                StoreError::Corruption(format!("working job missing from jobs keyspace: {id:?}",))
            })?;
            let mut job: Job = rmp_serde::from_slice(&job_bytes)?;
            let queue = job.queue.clone();
            let old_queue_status_key = make_queue_status_key(&job.queue, JobStatus::Working, &id);
            let queue_priority_key = make_queue_priority_key(&job.queue, job.priority, &id);
            job.status = JobStatus::Ready.into();
            let new_status_key = make_status_key(JobStatus::Ready, &id);
            let new_queue_status_key = make_queue_status_key(&job.queue, JobStatus::Ready, &id);
            let job_bytes = rmp_serde::to_vec_named(&job)?;

            tx.insert(
                &ready_jobs_by_priority,
                &make_priority_key(job.priority, &id),
                b"",
            );
            tx.insert(&ready_jobs_by_queue_and_priority, &queue_priority_key, b"");
            tx.insert(&jobs, &id, &job_bytes);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");
            tx.remove(&jobs_by_queue_and_status, &old_queue_status_key);
            tx.insert(&jobs_by_queue_and_status, &new_queue_status_key, b"");
            tx.commit()?;

            Ok(Some(queue))
        })
        .await??;

        if let Some(queue) = requeued_queue {
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
    /// ```ignore
    /// let page = store.list_jobs(ListJobsOptions::new()).await?;
    /// for job in &page.jobs {
    ///     println!("{}: {}", job.id, job.queue);
    /// }
    /// ```
    ///
    /// Paginate through all jobs:
    ///
    /// ```ignore
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
    /// ```ignore
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
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();

        task::spawn_blocking(move || {
            let snapshot = db.read_tx();
            // We take 1 more job than requested so that we know if there's a
            // next page or not.
            let fetch = opts.limit + 1;
            let mut rows = Vec::with_capacity(fetch);

            if !opts.queues.is_empty() && !opts.statuses.is_empty() {
                // Combined queue + status filter: k-way merge across the
                // Cartesian product of (queues × statuses).
                let mut sources: Vec<MergeSource<'_>> = Vec::new();
                for queue_name in &opts.queues {
                    for status in &opts.statuses {
                        let prefix_len = queue_name.len() + 3;
                        let start = match &opts.from {
                            Some(cursor) => {
                                Bound::Excluded(make_queue_status_key(queue_name, *status, cursor))
                            }
                            None => {
                                let mut key = Vec::with_capacity(queue_name.len() + 3);
                                key.extend_from_slice(queue_name.as_bytes());
                                key.push(0);
                                key.push(*status as u8);
                                key.push(0);
                                Bound::Included(key)
                            }
                        };
                        let mut end_key = Vec::with_capacity(queue_name.len() + 2);
                        end_key.extend_from_slice(queue_name.as_bytes());
                        end_key.push(0);
                        end_key.push(*status as u8 + 1);
                        let end = Bound::Excluded(end_key);

                        sources.push(match opts.direction {
                            ScanDirection::Asc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_queue_and_status, (start, end),),
                                prefix_len
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_queue_and_status, (start, end),)
                                    .rev(),
                                prefix_len
                            ),
                        });
                    }
                }

                let ids = kway_merge(sources, opts.direction, fetch)?;
                load_jobs_by_ids(
                    &jobs_ks,
                    &payloads_ks,
                    &ids,
                    "jobs_by_queue_and_status",
                    &mut rows,
                )?;
            } else if !opts.queues.is_empty() {
                // Queue filter: k-way merge across per-queue ranges.
                let sources: Vec<MergeSource<'_>> = opts
                    .queues
                    .iter()
                    .map(|queue_name| {
                        let prefix_len = queue_name.len() + 1;
                        let start = match &opts.from {
                            Some(cursor) => Bound::Excluded(make_queue_key(queue_name, cursor)),
                            None => Bound::Included(make_queue_key(queue_name, "")),
                        };
                        let mut end_key = Vec::with_capacity(queue_name.len() + 1);
                        end_key.extend_from_slice(queue_name.as_bytes());
                        end_key.push(1);
                        let end = Bound::Excluded(end_key);

                        match opts.direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&jobs_by_queue, (start, end),),
                                prefix_len
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_queue, (start, end),)
                                    .rev(),
                                prefix_len
                            ),
                        }
                    })
                    .collect();

                let ids = kway_merge(sources, opts.direction, fetch)?;
                load_jobs_by_ids(&jobs_ks, &payloads_ks, &ids, "jobs_by_queue", &mut rows)?;
            } else if !opts.statuses.is_empty() {
                // Status filter: k-way merge across per-status ranges.
                let sources: Vec<MergeSource<'_>> = opts
                    .statuses
                    .iter()
                    .map(|status| {
                        let prefix = *status as u8;
                        let start = match &opts.from {
                            Some(cursor) => Bound::Excluded(make_status_key(*status, cursor)),
                            None => Bound::Included(vec![prefix, 0]),
                        };
                        let end = Bound::Excluded(vec![prefix + 1, 0]);

                        match opts.direction {
                            ScanDirection::Asc => range_source!(
                                snapshot.range::<Vec<u8>, _>(&jobs_by_status, (start, end),),
                                2
                            ),
                            ScanDirection::Desc => range_source!(
                                snapshot
                                    .range::<Vec<u8>, _>(&jobs_by_status, (start, end),)
                                    .rev(),
                                2
                            ),
                        }
                    })
                    .collect();

                let ids = kway_merge(sources, opts.direction, fetch)?;
                load_jobs_by_ids(&jobs_ks, &payloads_ks, &ids, "jobs_by_status", &mut rows)?;
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
                                job.payload = rmp_serde::from_slice(&payload_bytes)?;
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
                                job.payload = rmp_serde::from_slice(&payload_bytes)?;
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

    /// Return up to `limit` due scheduled jobs, plus the next wake-up time.
    ///
    /// Scans `scheduled_jobs_by_ready_at` from the front (earliest first)
    /// and collects jobs whose `ready_at <= now`. Stops as soon as it hits
    /// a future job or exhausts the limit.
    ///
    /// Returns `(due_jobs, next_ready_at)` where `next_ready_at` is the
    /// `ready_at` of the first future job (if any). The scheduler uses
    /// this to know how long to sleep before the next scan.
    pub async fn next_scheduled(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<(Vec<Job>, Option<u64>), StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let scheduled_jobs_by_ready_at = self.scheduled_jobs_by_ready_at.clone();

        task::spawn_blocking(move || -> Result<(Vec<Job>, Option<u64>), StoreError> {
            let snapshot = db.read_tx();
            let mut result = Vec::new();
            for entry in snapshot.range::<&[u8], _>(&scheduled_jobs_by_ready_at, ..) {
                let (key, _) = entry.into_inner()?;
                // First 8 bytes are the BE u64 ready_at timestamp.
                let ts_bytes: [u8; 8] = key[..8].try_into().map_err(|_| {
                    StoreError::Corruption(format!(
                        "scheduled index key too short ({} bytes, expected >= 9)",
                        key.len(),
                    ))
                })?;
                let ready_at = u64::from_be_bytes(ts_bytes);
                if ready_at > now {
                    // This job (and everything after) is in the future.
                    return Ok((result, Some(ready_at)));
                }
                if result.len() >= limit {
                    // Hit the batch cap but there are more due jobs. Return
                    // without a next_ready_at so the caller loops immediately.
                    return Ok((result, None));
                }
                // Job ID starts after the 8-byte timestamp + 1-byte separator.
                let job_id = &key[9..];
                let job_bytes = jobs.get(job_id)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "job in scheduled_jobs_by_ready_at but missing from jobs keyspace: {:?}",
                        String::from_utf8_lossy(job_id),
                    ))
                })?;
                result.push(rmp_serde::from_slice(&job_bytes)?);
            }
            // Exhausted the index — no more scheduled jobs at all.
            Ok((result, None))
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
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let ready_jobs_by_queue_and_priority = self.ready_jobs_by_queue_and_priority.clone();
        let scheduled_jobs_by_ready_at = self.scheduled_jobs_by_ready_at.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();

        let id = job.id.clone();
        let event_queue = job.queue.clone();
        let queue = job.queue.clone();
        let priority = job.priority;
        let ready_at = job.ready_at;

        task::spawn_blocking(move || -> Result<(), StoreError> {
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

            // Remove from scheduled index.
            let scheduled_key = make_scheduled_key(ready_at, &id);
            tx.remove(&scheduled_jobs_by_ready_at, &scheduled_key);

            // Swap status indexes: Scheduled -> Ready.
            let old_status_key = make_status_key(JobStatus::Scheduled, &id);
            let new_status_key = make_status_key(JobStatus::Ready, &id);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");

            let old_queue_status_key = make_queue_status_key(&queue, JobStatus::Scheduled, &id);
            let new_queue_status_key = make_queue_status_key(&queue, JobStatus::Ready, &id);
            tx.remove(&jobs_by_queue_and_status, &old_queue_status_key);
            tx.insert(&jobs_by_queue_and_status, &new_queue_status_key, b"");

            // Insert into priority indexes so the job is takeable.
            let priority_key = make_priority_key(priority, &id);
            let queue_priority_key = make_queue_priority_key(&queue, priority, &id);
            tx.insert(&ready_jobs_by_priority, &priority_key, b"");
            tx.insert(&ready_jobs_by_queue_and_priority, &queue_priority_key, b"");

            // Update the job record itself.
            let mut updated = current;
            updated.status = JobStatus::Ready.into();
            let updated_bytes = rmp_serde::to_vec_named(&updated)?;
            tx.insert(&jobs, &id, &updated_bytes);

            tx.commit()?;
            Ok(())
        })
        .await??;

        let _ = self.event_tx.send(StoreEvent::JobCreated {
            queue: event_queue,
            token: Arc::new(AtomicBool::new(false)),
        });

        Ok(())
    }

    /// Recover orphaned working jobs at startup.
    ///
    /// If the server crashes while jobs are in the `Working` state, those
    /// jobs are stuck — no client is processing them. This method scans the
    /// status index for all Working jobs and atomically moves them back to
    /// Ready so they can be retaken.
    ///
    /// Must be called before any subscribers exist (no events are fired).
    pub async fn recover_working_jobs(&self) -> Result<usize, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let ready_jobs_by_queue_and_priority = self.ready_jobs_by_queue_and_priority.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let jobs_by_queue_and_status = self.jobs_by_queue_and_status.clone();

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
                    let (key, _) = entry.into_inner().expect("status index read failed");
                    // Key layout: {status_u8}\0{job_id} — skip the 2-byte prefix.
                    String::from_utf8(key[2..].to_vec()).expect("job ID is not valid UTF-8")
                })
                .collect();
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

                let old_queue_status_key =
                    make_queue_status_key(&job.queue, JobStatus::Working, id);
                let new_queue_status_key = make_queue_status_key(&job.queue, JobStatus::Ready, id);
                tx.remove(&jobs_by_queue_and_status, &old_queue_status_key);
                tx.insert(&jobs_by_queue_and_status, &new_queue_status_key, b"");

                // Insert into priority indexes so the jobs are takeable.
                let priority_key = make_priority_key(job.priority, id);
                let queue_priority_key = make_queue_priority_key(&job.queue, job.priority, id);
                tx.insert(&ready_jobs_by_priority, &priority_key, b"");
                tx.insert(&ready_jobs_by_queue_and_priority, &queue_priority_key, b"");

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
}

/// Base-36 alphabet (0-9, a-z) for compact, lexicographically sortable keys.
const B36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

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

/// Build a compound queue + status index key: `{queue_name}\0{status_u8}\0{job_id}`.
fn make_queue_status_key(queue_name: &str, status: JobStatus, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(queue_name.len() + 3 + job_id.len());
    key.extend_from_slice(queue_name.as_bytes());
    key.push(0);
    key.push(status as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a per-queue priority index key: `{queue_name}\0{priority_b36:4}\0{job_id}`.
///
/// Within a single queue prefix, entries sort by priority then FIFO — the
/// same ordering as `ready_jobs_by_priority` but scoped to one queue.
fn make_queue_priority_key(queue_name: &str, priority: u16, job_id: &str) -> Vec<u8> {
    let priority_str = make_priority_key(priority, job_id);
    let mut key = Vec::with_capacity(queue_name.len() + 1 + priority_str.len());
    key.extend_from_slice(queue_name.as_bytes());
    key.push(0);
    key.extend_from_slice(priority_str.as_bytes());
    key
}

/// Build a priority index key that sorts by priority then by job ID.
///
/// The priority is encoded as a 4-character base-36 string (covers the full
/// u16 range: 65535 = "1ekf") so that lexicographic ordering matches numeric
/// ordering. The job ID (scru128, already base-36) provides uniqueness and
/// time-ordering within the same priority level.
fn make_priority_key(priority: u16, job_id: &str) -> String {
    let mut buf = [b'0'; 4];
    let mut n = priority as u32;
    for i in (0..4).rev() {
        buf[i] = B36[(n % 36) as usize];
        n /= 36;
    }
    let prefix = std::str::from_utf8(&buf).unwrap();
    format!("{}\0{}", prefix, job_id)
}

/// Build a scheduled index key: `{ready_at_be_u64}\0{job_id}`.
///
/// The 8-byte big-endian u64 gives chronological ordering so the background
/// scheduler can scan from the front and stop at the first future timestamp.
fn make_scheduled_key(ready_at: u64, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + 1 + job_id.len());
    key.extend_from_slice(&ready_at.to_be_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        std::mem::forget(dir);
        store
    }

    #[tokio::test]
    async fn enqueue_returns_job_with_id() {
        let store = test_store();
        let job = store
            .enqueue(EnqueueOptions::new(
                "default",
                serde_json::json!({"task": "test"}),
            ))
            .await
            .unwrap();

        assert!(!job.id.is_empty());
        assert_eq!(job.queue, "default");
        assert_eq!(job.priority, 0);
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, serde_json::json!({"task": "test"}));
    }

    #[tokio::test]
    async fn enqueue_generates_unique_ids() {
        let store = test_store();
        let job1 = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)))
            .await
            .unwrap();
        let job2 = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)))
            .await
            .unwrap();

        assert_ne!(job1.id, job2.id);
    }

    #[tokio::test]
    async fn enqueue_priority_key_reflects_priority() {
        let store = test_store();
        let low = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)).priority(10))
            .await
            .unwrap();
        let high = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)).priority(1))
            .await
            .unwrap();

        // Higher priority (lower number) should sort first lexicographically.
        assert!(
            make_priority_key(high.priority, &high.id) < make_priority_key(low.priority, &low.id)
        );
    }

    #[tokio::test]
    async fn enqueue_ids_are_fifo_ordered() {
        let store = test_store();
        let first = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)))
            .await
            .unwrap();
        let second = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)))
            .await
            .unwrap();
        let third = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!(null)))
            .await
            .unwrap();

        // scru128 IDs sort lexicographically in generation order.
        assert!(first.id < second.id);
        assert!(second.id < third.id);
    }

    #[tokio::test]
    async fn take_next_job_returns_none_when_empty() {
        let store = test_store();
        let job = store.take_next_job(&HashSet::new()).await.unwrap();
        assert!(job.is_none());
    }

    #[tokio::test]
    async fn take_next_job_returns_highest_priority() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("low")).priority(10))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("high")).priority(1))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("mid")).priority(5))
            .await
            .unwrap();

        let job = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(job.priority, 1);
        assert_eq!(job.status, u8::from(JobStatus::Working));
        assert_eq!(job.payload, serde_json::json!("high"));
    }

    #[tokio::test]
    async fn take_next_job_fifo_within_same_priority() {
        let store = test_store();
        let first = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("b")))
            .await
            .unwrap();

        let job = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(job.id, first.id);
    }

    #[tokio::test]
    async fn take_next_job_removes_from_queue() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("b")))
            .await
            .unwrap();

        let first = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        let second = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(first.payload, serde_json::json!("a"));
        assert_eq!(second.payload, serde_json::json!("b"));

        // Queue should now be empty.
        assert!(
            store
                .take_next_job(&HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn mark_completed_removes_job() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();

        let job = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert!(store.mark_completed(&job.id).await.unwrap());

        // Job should no longer be dequeue-able even if re-enqueue were attempted
        // via crash recovery — it's gone from the jobs keyspace entirely.
        assert!(
            store
                .take_next_job(&HashSet::new())
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
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();

        let job = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert!(store.mark_completed(&job.id).await.unwrap());
        assert!(!store.mark_completed(&job.id).await.unwrap());
    }

    #[tokio::test]
    async fn take_next_job_never_returns_duplicates_under_contention() {
        let store = Arc::new(test_store());
        let num_jobs = 50;

        for i in 0..num_jobs {
            store
                .enqueue(EnqueueOptions::new("default", serde_json::json!(i)))
                .await
                .unwrap();
        }

        // Spawn many concurrent workers all racing to take jobs.
        let mut handles = Vec::new();
        for _ in 0..20 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(job) = store.take_next_job(&HashSet::new()).await.unwrap() {
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

    // --- take_next_job queue filter tests ---

    #[tokio::test]
    async fn take_next_job_filters_by_single_queue() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("b")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("c")))
            .await
            .unwrap();

        let queues = HashSet::from(["reports".into()]);
        let job = store.take_next_job(&queues).await.unwrap().unwrap();
        assert_eq!(job.id, b.id);
        assert_eq!(job.queue, "reports");

        // No more reports jobs.
        assert!(store.take_next_job(&queues).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn take_next_job_filters_by_multiple_queues() {
        let store = test_store();
        let a = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("webhooks", serde_json::json!("c")))
            .await
            .unwrap();

        let queues = HashSet::from(["emails".into(), "webhooks".into()]);
        let first = store.take_next_job(&queues).await.unwrap().unwrap();
        assert_eq!(first.id, a.id);

        let second = store.take_next_job(&queues).await.unwrap().unwrap();
        assert_eq!(second.id, c.id);

        assert!(store.take_next_job(&queues).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_respects_priority() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("low")).priority(10))
            .await
            .unwrap();
        let high = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("high")).priority(1))
            .await
            .unwrap();

        let queues = HashSet::from(["emails".into()]);
        let job = store.take_next_job(&queues).await.unwrap().unwrap();
        assert_eq!(job.id, high.id);
        assert_eq!(job.priority, 1);
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_returns_none_for_empty_queue() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();

        let queues = HashSet::from(["reports".into()]);
        assert!(store.take_next_job(&queues).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_picks_best_across_queues() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("low")).priority(10))
            .await
            .unwrap();
        let high = store
            .enqueue(EnqueueOptions::new("webhooks", serde_json::json!("high")).priority(1))
            .await
            .unwrap();

        let queues = HashSet::from(["emails".into(), "webhooks".into()]);
        let job = store.take_next_job(&queues).await.unwrap().unwrap();
        assert_eq!(job.id, high.id);
        assert_eq!(job.queue, "webhooks");
        assert_eq!(job.priority, 1);
    }

    #[tokio::test]
    async fn requeue_returns_job_to_queue() {
        let store = test_store();
        let job = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(taken.id, job.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        let retaken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn requeue_preserves_priority() {
        let store = test_store();
        let high = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("high")).priority(1))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("low")).priority(10))
            .await
            .unwrap();

        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(taken.id, high.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        // Should get the high-priority job again, not the low-priority one.
        let retaken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(retaken.id, high.id);
        assert_eq!(retaken.priority, 1);
        assert_eq!(retaken.status, u8::from(JobStatus::Working));
    }

    #[tokio::test]
    async fn requeue_skips_already_acked_job() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert!(store.mark_completed(&taken.id).await.unwrap());

        assert!(!store.requeue(&taken.id).await.unwrap());

        // Queue should remain empty.
        assert!(
            store
                .take_next_job(&HashSet::new())
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
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
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
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();

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
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();

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
            .enqueue(EnqueueOptions::new("default", serde_json::json!("hello")))
            .await
            .unwrap();

        let job = store.get_job(&enqueued.id).await.unwrap().unwrap();
        assert_eq!(job.id, enqueued.id);
        assert_eq!(job.queue, "default");
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, serde_json::json!("hello"));
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
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();
        store.take_next_job(&HashSet::new()).await.unwrap();
        store.mark_completed(&enqueued.id).await.unwrap();

        assert!(store.get_job(&enqueued.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_asc_order() {
        let store = test_store();
        let a = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();

        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        store.mark_completed(&taken.id).await.unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_next_present_when_more_results() {
        let store = test_store();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_jobs_prev_present_when_cursor_provided() {
        let store = test_store();
        let a = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();

        // Take the first job so it becomes Working.
        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();

        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();

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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("webhooks", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("webhooks", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("c")))
            .await
            .unwrap();
        let d = store
            .enqueue(EnqueueOptions::new("webhooks", serde_json::json!("d")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("b")))
            .await
            .unwrap();

        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
            .await
            .unwrap();

        // Take a and b so they become Working.
        store.take_next_job(&HashSet::new()).await.unwrap();
        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap(); // a -> working

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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("b")))
            .await
            .unwrap();
        let _c = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("c")))
            .await
            .unwrap();

        // Take a so it becomes Working.
        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("webhooks", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("reports", serde_json::json!("c")))
            .await
            .unwrap();
        let d = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("d")))
            .await
            .unwrap();

        // Take a and b so they become Working.
        store.take_next_job(&HashSet::new()).await.unwrap();
        store.take_next_job(&HashSet::new()).await.unwrap();

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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("b")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap(); // a -> working

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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("c")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap(); // a -> working

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
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("emails", serde_json::json!("b")))
            .await
            .unwrap();

        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
            .await
            .unwrap();
        let b = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("b")))
            .await
            .unwrap();
        let c = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("c")))
            .await
            .unwrap();

        store.take_next_job(&HashSet::new()).await.unwrap(); // a -> working

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

    #[tokio::test]
    async fn enqueue_with_future_ready_at_creates_scheduled_job() {
        let store = test_store();
        let future_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000; // 1 minute from now

        let job = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(future_ms))
            .await
            .unwrap();

        assert_eq!(job.status, u8::from(JobStatus::Scheduled));
        assert_eq!(job.ready_at, future_ms);

        // Scheduled jobs should not be dequeue-able.
        let taken = store.take_next_job(&HashSet::new()).await.unwrap();
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(past_ms))
            .await
            .unwrap();

        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.ready_at, past_ms);

        // Should be dequeue-able immediately.
        let taken = store.take_next_job(&HashSet::new()).await.unwrap();
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(now + 60_000))
            .await
            .unwrap();
        assert_eq!(job.status, u8::from(JobStatus::Scheduled));

        // Pretend time has passed: ask for jobs due at ready_at.
        let (batch, _) = store.next_scheduled(now + 60_000, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        store.promote_scheduled(&batch[0]).await.unwrap();

        // It should now be takeable.
        let taken = store.take_next_job(&HashSet::new()).await.unwrap();
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(future_ms))
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
                .take_next_job(&HashSet::new())
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
                .enqueue(EnqueueOptions::new("q", serde_json::json!(i)).ready_at(future + i))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(future))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("later")).ready_at(base + 5000))
            .await
            .unwrap();
        let earlier = store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("earlier")).ready_at(base + 1000))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(future))
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("a")).ready_at(future))
            .await
            .unwrap();

        let (batch, _) = store.next_scheduled(future, 10).await.unwrap();
        store.promote_scheduled(&batch[0]).await.unwrap();

        // Promoting again should be a no-op (job is already Ready).
        store.promote_scheduled(&job).await.unwrap();

        // Should still be takeable exactly once.
        let taken = store.take_next_job(&HashSet::new()).await.unwrap();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().id, job.id);
        assert!(
            store
                .take_next_job(&HashSet::new())
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
            .enqueue(EnqueueOptions::new("q", serde_json::json!("due")).ready_at(now + 100))
            .await
            .unwrap();
        store
            .enqueue(EnqueueOptions::new("q", serde_json::json!("future")).ready_at(now + 60_000))
            .await
            .unwrap();

        // Ask as of now + 200: only the first job is due.
        let (batch, next) = store.next_scheduled(now + 200, 10).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, due.id);
        assert_eq!(next, Some(now + 60_000));
    }

    #[tokio::test]
    async fn recover_working_jobs_moves_working_to_ready() {
        let store = test_store();
        let job = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();

        // Take the job so it becomes Working.
        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(taken.id, job.id);

        // Nothing left to take.
        assert!(
            store
                .take_next_job(&HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // Recover should move it back to Ready.
        let recovered = store.recover_working_jobs().await.unwrap();
        assert_eq!(recovered, 1);

        // The job should be takeable again.
        let retaken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn recover_working_jobs_returns_zero_when_none_working() {
        let store = test_store();

        // Enqueue a job but don't take it — it stays Ready.
        store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("a")))
            .await
            .unwrap();

        let recovered = store.recover_working_jobs().await.unwrap();
        assert_eq!(recovered, 0);
    }

    #[tokio::test]
    async fn recover_working_jobs_preserves_priority() {
        let store = test_store();

        // Enqueue two jobs at different priorities.
        let low = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("low")).priority(10))
            .await
            .unwrap();
        let high = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("high")).priority(1))
            .await
            .unwrap();

        // Take both so they become Working.
        store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        store.take_next_job(&HashSet::new()).await.unwrap().unwrap();

        // Recover both.
        let recovered = store.recover_working_jobs().await.unwrap();
        assert_eq!(recovered, 2);

        // They should come back in priority order (high first, then low).
        let first = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        let second = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(first.id, high.id);
        assert_eq!(second.id, low.id);
    }

    #[tokio::test]
    async fn recover_working_jobs_ignores_other_statuses() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // A Ready job.
        let ready = store
            .enqueue(EnqueueOptions::new("default", serde_json::json!("ready")))
            .await
            .unwrap();

        // A Scheduled job (far in the future).
        let scheduled = store
            .enqueue(
                EnqueueOptions::new("default", serde_json::json!("scheduled"))
                    .ready_at(now + 600_000),
            )
            .await
            .unwrap();

        // Recover should find nothing.
        let recovered = store.recover_working_jobs().await.unwrap();
        assert_eq!(recovered, 0);

        // The ready job is still takeable.
        let taken = store.take_next_job(&HashSet::new()).await.unwrap().unwrap();
        assert_eq!(taken.id, ready.id);

        // The scheduled job is still in the scheduled index.
        let fetched = store.get_job(&scheduled.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Scheduled as u8);
    }
}
