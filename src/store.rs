// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Persistent storage layer to manage job queues.
//!
//! Wraps fjall (database) to provide transactional queue operations across
//! three keyspaces:
//!
//! - `jobs`: source of truth, keyed by job ID, stores full job metadata.
//! - `ready_jobs_by_priority`: partial priority index of ready jobs, keyed by `{priority_b36:4}\0{job_id}`.
//! - `jobs_by_status`: status index, keyed by `{status_u8}\0{job_id}`.

use std::fmt;
use std::ops::Bound;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    #[serde(rename = "p")]
    pub payload: serde_json::Value,

    /// Current lifecycle status, stored as a u8 which converts to `JobStatus`.
    #[serde(rename = "s")]
    pub status: u8,
}

impl Job {
    /// Derive the priority index key for this job.
    ///
    /// This is a pure function of `priority` and `id`, so it doesn't need
    /// to be stored.
    pub fn priority_key(&self) -> String {
        make_priority_key(self.priority, &self.id)
    }
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

    /// Optional status filter. When set, only jobs with this status are
    /// returned, using the status index for efficient lookup.
    pub status: Option<JobStatus>,
}

impl ListJobsOptions {
    /// Create default options (ascending, no cursor, no status filter, limit 50).
    pub fn new() -> Self {
        Self {
            from: None,
            direction: ScanDirection::Asc,
            limit: 50,
            status: None,
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
    pub fn status(mut self, status: JobStatus) -> Self {
        self.status = Some(status);
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

/// Events broadcast by the store when job state changes.
#[derive(Debug, Clone)]
pub enum StoreEvent {
    /// A new job was enqueued (or requeued) and is available for processing.
    JobEnqueued,

    /// A job was successfully completed and removed.
    JobCompleted(String),
}

/// Provides a handle to the persistent store.
pub struct Store {
    /// Connection to the underlying database.
    db: SingleWriterTxDatabase,

    /// Reference to the jobs keyspace.
    ///
    /// Jobs are keyed by their scru128 identifier which provides basic FIFO
    /// semantics. This is the authoritative source of job data, but it is not
    /// priority ordered.
    jobs: SingleWriterTxKeyspace,

    /// Partial index of ready jobs, ordered by priority then job ID.
    ///
    /// Only jobs in the `Ready` state appear here. Taking a job removes it
    /// from this index; requeueing adds it back.
    ready_jobs_by_priority: SingleWriterTxKeyspace,

    /// Reference to the status index keyspace.
    ///
    /// Keyed by `{status_u8}\0{job_id}` with empty values. Enables efficient
    /// range scans for jobs in a particular status without scanning the entire
    /// `jobs` keyspace.
    jobs_by_status: SingleWriterTxKeyspace,

    /// Broadcast channel for store events.
    ///
    /// Subscribers receive notifications when jobs are enqueued, completed,
    /// or otherwise change state. Take handlers use this both to wake up
    /// when new work is available and to prune their in-flight tracking.
    event_tx: broadcast::Sender<StoreEvent>,
}

impl Store {
    /// Open or create a store at the given path.
    ///
    /// The path refers to the directory in which fjall stores its keyspace
    /// data.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let db = SingleWriterTxDatabase::builder(path).open()?;
        let jobs = db.keyspace("jobs", fjall::KeyspaceCreateOptions::default)?;
        let ready_jobs_by_priority =
            db.keyspace("ready_jobs_by_priority", fjall::KeyspaceCreateOptions::default)?;
        let jobs_by_status =
            db.keyspace("jobs_by_status", fjall::KeyspaceCreateOptions::default)?;

        let (event_tx, _) = broadcast::channel(1024);

        Ok(Self {
            db,
            jobs,
            ready_jobs_by_priority,
            jobs_by_status,
            event_tx,
        })
    }

    /// Enqueue a new job.
    ///
    /// Generates a unique job ID, inserts the job into the `jobs` keyspace,
    /// and adds an entry to the `queue` keyspace for priority-ordered retrieval.
    ///
    /// Subscribers are notified.
    pub async fn enqueue(
        &self,
        queue_name: &str,
        priority: u16,
        payload: serde_json::Value,
    ) -> Result<Job, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let queue_name = queue_name.to_string();

        let job = task::spawn_blocking(move || -> Result<Job, StoreError> {
            // Generate a new lexicographically increasing id.
            let id = scru128::new_string();

            let job = Job {
                id: id.clone(),
                queue: queue_name,
                priority,
                payload,
                status: JobStatus::Ready.into(),
            };

            let priority_key = job.priority_key();
            let status_key = make_status_key(JobStatus::Ready, &id);
            let job_bytes = rmp_serde::to_vec_named(&job)?;

            // Atomically push the job into the jobs keyspace and indexes.
            let mut tx = db.write_tx();
            tx.insert(&jobs, &id, &job_bytes);
            tx.insert(&ready_jobs_by_priority, &priority_key, id.as_bytes());
            tx.insert(&jobs_by_status, &status_key, b"");
            tx.commit()?;

            Ok(job)
        })
        .await??;

        let _ = self.event_tx.send(StoreEvent::JobEnqueued);

        Ok(job)
    }

    /// Take the next job from the priority index.
    ///
    /// Atomically removes the highest-priority (lowest number), oldest job
    /// from the `ready_jobs_by_priority` keyspace and marks it as working. Returns
    /// `None` if the queue is empty.
    ///
    /// Subscribers are not notified.
    pub async fn take_next_job(&self) -> Result<Option<Job>, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let jobs_by_status = self.jobs_by_status.clone();

        task::spawn_blocking(move || {
            // Acquire the write lock before reading, so no other writer
            // can take the same job between our read and the remove.
            let mut tx = db.write_tx();

            // The first key in the priority index is the highest-priority,
            // oldest job, because keys sort by priority (base-36) then by
            // job ID (scru128, time-ordered).
            let entry = match ready_jobs_by_priority.first_key_value() {
                Some(entry) => entry,
                None => return Ok(None),
            };

            let (priority_key, job_id) = entry.into_inner()?;

            // Look up the full job data from the source of truth.
            let job_bytes = jobs.get(&job_id)?.ok_or_else(|| {
                StoreError::Corruption(format!(
                    "job in ready_jobs_by_priority but missing from jobs keyspace: {:?}",
                    String::from_utf8_lossy(&job_id),
                ))
            })?;

            let mut job: Job = rmp_serde::from_slice(&job_bytes)?;
            let old_status_key = make_status_key(JobStatus::Ready, &job.id);
            job.status = JobStatus::Working.into();
            let new_status_key = make_status_key(JobStatus::Working, &job.id);
            let job_bytes = rmp_serde::to_vec_named(&job)?;

            // Remove from priority index, update job record and status index.
            tx.remove(&ready_jobs_by_priority, &*priority_key);
            tx.insert(&jobs, &*job_id, &job_bytes);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");
            tx.commit()?;

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
        let jobs_by_status = self.jobs_by_status.clone();
        let id = id.to_string();
        let id_for_event = id.clone();

        let completed = task::spawn_blocking(move || {
            // Acquire the write lock before checking, so no other writer
            // can remove the job between our check and the remove.
            let mut tx = db.write_tx();

            let status_key = make_status_key(JobStatus::Working, &id);
            if jobs_by_status.get(&status_key)?.is_none() {
                return Ok(false);
            }

            tx.remove(&jobs, &id);
            tx.remove(&jobs_by_status, &status_key);
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
        let id = id.to_string();

        task::spawn_blocking(move || {
            let Some(bytes) = jobs.get(&id)? else {
                return Ok(None);
            };
            let job: Job = rmp_serde::from_slice(&bytes)?;
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
    /// Looks up the job from the `jobs` keyspace to reconstruct its queue key.
    /// Returns `true` if the job was actually requeued, `false` if it was
    /// already acked (no longer in the working state).
    ///
    /// Subscribers are notified.
    pub async fn requeue(&self, id: &str) -> Result<bool, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let ready_jobs_by_priority = self.ready_jobs_by_priority.clone();
        let jobs_by_status = self.jobs_by_status.clone();
        let id = id.to_string();

        let requeued = task::spawn_blocking(move || -> Result<bool, StoreError> {
            let mut tx = db.write_tx();

            let old_status_key = make_status_key(JobStatus::Working, &id);
            if jobs_by_status.get(&old_status_key)?.is_none() {
                return Ok(false);
            }

            // The job is still working, so it must still be in the jobs
            // keyspace (only mark_completed removes from both).
            let job_bytes = jobs.get(&id)?.ok_or_else(|| {
                StoreError::Corruption(format!("working job missing from jobs keyspace: {id:?}",))
            })?;
            let mut job: Job = rmp_serde::from_slice(&job_bytes)?;
            job.status = JobStatus::Ready.into();
            let new_status_key = make_status_key(JobStatus::Ready, &id);
            let job_bytes = rmp_serde::to_vec_named(&job)?;

            tx.insert(&ready_jobs_by_priority, &job.priority_key(), id.as_bytes());
            tx.insert(&jobs, &id, &job_bytes);
            tx.remove(&jobs_by_status, &old_status_key);
            tx.insert(&jobs_by_status, &new_status_key, b"");
            tx.commit()?;

            Ok(true)
        })
        .await??;

        if requeued {
            let _ = self.event_tx.send(StoreEvent::JobEnqueued);
        }

        Ok(requeued)
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
        let jobs_by_status = self.jobs_by_status.clone();

        task::spawn_blocking(move || {
            let snapshot = db.read_tx();
            // We take 1 more job than requested so that we know if there's a
            // next page or not.
            let fetch = opts.limit + 1;
            let mut rows = Vec::with_capacity(fetch);

            if let Some(status) = opts.status {
                // Use the status index: scan keys in
                // [{status}\0{cursor}..{status+1}\0) and look up each job.
                let prefix = status as u8;
                let next_prefix = prefix + 1;

                match opts.direction {
                    ScanDirection::Asc => {
                        let start = match &opts.from {
                            Some(cursor) => {
                                let key = make_status_key(status, cursor);
                                Bound::Excluded(key)
                            }
                            None => Bound::Included(vec![prefix, 0]),
                        };
                        let end = Bound::Excluded(vec![next_prefix, 0]);

                        let range = snapshot.range::<Vec<u8>, _>(&jobs_by_status, (start, end));

                        for entry in range.take(fetch) {
                            let (key, _) = entry.into_inner()?;
                            // Key is {status}\0{job_id} — extract job_id.
                            let job_id = &key[2..];
                            let job_bytes = jobs_ks.get(job_id)?.ok_or_else(|| {
                                StoreError::Corruption(format!(
                                    "job in jobs_by_status but missing from jobs keyspace: {:?}",
                                    String::from_utf8_lossy(job_id),
                                ))
                            })?;
                            let job: Job = rmp_serde::from_slice(&job_bytes)?;
                            rows.push(job);
                        }
                    }
                    ScanDirection::Desc => {
                        let end = match &opts.from {
                            Some(cursor) => {
                                let key = make_status_key(status, cursor);
                                Bound::Excluded(key)
                            }
                            None => Bound::Excluded(vec![next_prefix, 0]),
                        };
                        let start = Bound::Included(vec![prefix, 0]);

                        let range = snapshot.range::<Vec<u8>, _>(&jobs_by_status, (start, end));

                        for entry in range.rev().take(fetch) {
                            let (key, _) = entry.into_inner()?;
                            let job_id = &key[2..];
                            let job_bytes = jobs_ks.get(job_id)?.ok_or_else(|| {
                                StoreError::Corruption(format!(
                                    "job in jobs_by_status but missing from jobs keyspace: {:?}",
                                    String::from_utf8_lossy(job_id),
                                ))
                            })?;
                            let job: Job = rmp_serde::from_slice(&job_bytes)?;
                            rows.push(job);
                        }
                    }
                }
            } else {
                // No status filter — scan the jobs keyspace directly.
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
                            let (_, value) = entry.into_inner()?;
                            let job: Job = rmp_serde::from_slice(&value)?;
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
                            let (_, value) = entry.into_inner()?;
                            let job: Job = rmp_serde::from_slice(&value)?;
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

            // Build next/prev options, preserving the status filter.
            let make_opts = |cursor: &str, direction: ScanDirection| {
                let mut o = ListJobsOptions::new()
                    .from(cursor.to_string())
                    .direction(direction)
                    .limit(opts.limit);
                o.status = opts.status;
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
            .enqueue("default", 0, serde_json::json!({"task": "test"}))
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
            .enqueue("default", 0, serde_json::json!(null))
            .await
            .unwrap();
        let job2 = store
            .enqueue("default", 0, serde_json::json!(null))
            .await
            .unwrap();

        assert_ne!(job1.id, job2.id);
    }

    #[tokio::test]
    async fn enqueue_priority_key_reflects_priority() {
        let store = test_store();
        let low = store
            .enqueue("default", 10, serde_json::json!(null))
            .await
            .unwrap();
        let high = store
            .enqueue("default", 1, serde_json::json!(null))
            .await
            .unwrap();

        // Higher priority (lower number) should sort first lexicographically.
        assert!(high.priority_key() < low.priority_key());
    }

    #[tokio::test]
    async fn enqueue_ids_are_fifo_ordered() {
        let store = test_store();
        let first = store
            .enqueue("default", 0, serde_json::json!(null))
            .await
            .unwrap();
        let second = store
            .enqueue("default", 0, serde_json::json!(null))
            .await
            .unwrap();
        let third = store
            .enqueue("default", 0, serde_json::json!(null))
            .await
            .unwrap();

        // scru128 IDs sort lexicographically in generation order.
        assert!(first.id < second.id);
        assert!(second.id < third.id);
    }

    #[tokio::test]
    async fn take_next_job_returns_none_when_empty() {
        let store = test_store();
        let job = store.take_next_job().await.unwrap();
        assert!(job.is_none());
    }

    #[tokio::test]
    async fn take_next_job_returns_highest_priority() {
        let store = test_store();
        store
            .enqueue("default", 10, serde_json::json!("low"))
            .await
            .unwrap();
        store
            .enqueue("default", 1, serde_json::json!("high"))
            .await
            .unwrap();
        store
            .enqueue("default", 5, serde_json::json!("mid"))
            .await
            .unwrap();

        let job = store.take_next_job().await.unwrap().unwrap();
        assert_eq!(job.priority, 1);
        assert_eq!(job.status, u8::from(JobStatus::Working));
        assert_eq!(job.payload, serde_json::json!("high"));
    }

    #[tokio::test]
    async fn take_next_job_fifo_within_same_priority() {
        let store = test_store();
        let first = store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        store
            .enqueue("default", 0, serde_json::json!("b"))
            .await
            .unwrap();

        let job = store.take_next_job().await.unwrap().unwrap();
        assert_eq!(job.id, first.id);
    }

    #[tokio::test]
    async fn take_next_job_removes_from_queue() {
        let store = test_store();
        store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        store
            .enqueue("default", 0, serde_json::json!("b"))
            .await
            .unwrap();

        let first = store.take_next_job().await.unwrap().unwrap();
        let second = store.take_next_job().await.unwrap().unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(first.payload, serde_json::json!("a"));
        assert_eq!(second.payload, serde_json::json!("b"));

        // Queue should now be empty.
        assert!(store.take_next_job().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mark_completed_removes_job() {
        let store = test_store();
        store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();

        let job = store.take_next_job().await.unwrap().unwrap();
        assert!(store.mark_completed(&job.id).await.unwrap());

        // Job should no longer be dequeue-able even if re-enqueue were attempted
        // via crash recovery — it's gone from the jobs keyspace entirely.
        assert!(store.take_next_job().await.unwrap().is_none());
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
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();

        let job = store.take_next_job().await.unwrap().unwrap();
        assert!(store.mark_completed(&job.id).await.unwrap());
        assert!(!store.mark_completed(&job.id).await.unwrap());
    }

    #[tokio::test]
    async fn take_next_job_never_returns_duplicates_under_contention() {
        let store = Arc::new(test_store());
        let num_jobs = 50;

        for i in 0..num_jobs {
            store
                .enqueue("default", 0, serde_json::json!(i))
                .await
                .unwrap();
        }

        // Spawn many concurrent workers all racing to take jobs.
        let mut handles = Vec::new();
        for _ in 0..20 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(job) = store.take_next_job().await.unwrap() {
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

    #[tokio::test]
    async fn requeue_returns_job_to_queue() {
        let store = test_store();
        let job = store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        let taken = store.take_next_job().await.unwrap().unwrap();
        assert_eq!(taken.id, job.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        let retaken = store.take_next_job().await.unwrap().unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn requeue_preserves_priority() {
        let store = test_store();
        let high = store
            .enqueue("default", 1, serde_json::json!("high"))
            .await
            .unwrap();
        store
            .enqueue("default", 10, serde_json::json!("low"))
            .await
            .unwrap();

        let taken = store.take_next_job().await.unwrap().unwrap();
        assert_eq!(taken.id, high.id);

        assert!(store.requeue(&taken.id).await.unwrap());

        // Should get the high-priority job again, not the low-priority one.
        let retaken = store.take_next_job().await.unwrap().unwrap();
        assert_eq!(retaken.id, high.id);
        assert_eq!(retaken.priority, 1);
        assert_eq!(retaken.status, u8::from(JobStatus::Working));
    }

    #[tokio::test]
    async fn requeue_skips_already_acked_job() {
        let store = test_store();
        store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        let taken = store.take_next_job().await.unwrap().unwrap();
        assert!(store.mark_completed(&taken.id).await.unwrap());

        assert!(!store.requeue(&taken.id).await.unwrap());

        // Queue should remain empty.
        assert!(store.take_next_job().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn enqueue_broadcasts_job_enqueued() {
        let store = test_store();
        let mut rx = store.subscribe();

        store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();

        assert!(matches!(rx.recv().await.unwrap(), StoreEvent::JobEnqueued));
    }

    #[tokio::test]
    async fn mark_completed_broadcasts_job_completed() {
        let store = test_store();
        let job = store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        let taken = store.take_next_job().await.unwrap().unwrap();

        let mut rx = store.subscribe();
        store.mark_completed(&taken.id).await.unwrap();

        match rx.recv().await.unwrap() {
            StoreEvent::JobCompleted(id) => assert_eq!(id, job.id),
            other => panic!("expected JobCompleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn requeue_broadcasts_job_enqueued() {
        let store = test_store();
        store
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        let taken = store.take_next_job().await.unwrap().unwrap();

        let mut rx = store.subscribe();
        store.requeue(&taken.id).await.unwrap();

        assert!(matches!(rx.recv().await.unwrap(), StoreEvent::JobEnqueued));
    }

    #[tokio::test]
    async fn get_job_returns_enqueued_job() {
        let store = test_store();
        let enqueued = store
            .enqueue("default", 0, serde_json::json!("hello"))
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
            .enqueue("default", 0, serde_json::json!("a"))
            .await
            .unwrap();
        store.take_next_job().await.unwrap();
        store.mark_completed(&enqueued.id).await.unwrap();

        assert!(store.get_job(&enqueued.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_asc_order() {
        let store = test_store();
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_desc_order() {
        let store = test_store();
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
    }

    #[tokio::test]
    async fn list_jobs_cursor_is_exclusive_asc() {
        let store = test_store();
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();

        store.take_next_job().await.unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Working));
        assert_eq!(page.jobs[1].status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn list_jobs_excludes_completed_jobs() {
        let store = test_store();
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();

        let taken = store.take_next_job().await.unwrap().unwrap();
        store.mark_completed(&taken.id).await.unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_next_present_when_more_results() {
        let store = test_store();
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();

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
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_jobs_prev_present_when_cursor_provided() {
        let store = test_store();
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

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
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();

        // Take the first job so it becomes Working.
        store.take_next_job().await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Ready))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn list_jobs_filters_by_working_status() {
        let store = test_store();
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();

        store.take_next_job().await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Working))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Working));
    }

    #[tokio::test]
    async fn list_jobs_status_filter_empty_when_none_match() {
        let store = test_store();
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Working))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_status_filter_with_pagination() {
        let store = test_store();
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();
        let c = store.enqueue("q", 0, serde_json::json!("c")).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Ready).limit(2))
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
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();
        let b = store.enqueue("q", 0, serde_json::json!("b")).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .status(JobStatus::Ready)
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
        let a = store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();

        store.take_next_job().await.unwrap();

        // Now working.
        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Working))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        // Requeue — should move back to ready.
        store.requeue(&a.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Working))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Ready))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_status_filter_reflects_completion() {
        let store = test_store();
        store.enqueue("q", 0, serde_json::json!("a")).await.unwrap();

        let taken = store.take_next_job().await.unwrap().unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Working))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        store.mark_completed(&taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().status(JobStatus::Working))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }
}
