// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Persistent storage layer to manage job queues.
//!
//! Wraps fjall (database) to provide transactional queue operations across
//! three keyspaces:
//!
//! - `jobs`: source of truth, keyed by job ID, stores full job metadata.
//! - `queue`: sorted pending index, keyed by `{priority_b36:4}\0{job_id}`.
//! - `working`: tracks in-flight jobs, keyed by job ID.

use std::fmt;

use fjall::{SingleWriterTxDatabase, SingleWriterTxKeyspace};
use serde::{Deserialize, Serialize};
use tokio::task;

/// Error type returned by store operations.
#[derive(Debug)]
pub enum StoreError {
    /// The underlying database (fjall) returned an error.
    Db(fjall::Error),

    /// A job could not be serialized or deserialized.
    Serde(serde_json::Error),

    /// A blocking task was cancelled or panicked.
    TaskJoin(task::JoinError),

    /// Internal data inconsistency (e.g. queue references a missing job).
    Corruption(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Db(e) => write!(f, "database error: {e}"),
            StoreError::Serde(e) => write!(f, "serialization error: {e}"),
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

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Serde(e)
    }
}

impl From<task::JoinError> for StoreError {
    fn from(e: task::JoinError) -> Self {
        StoreError::TaskJoin(e)
    }
}

/// A job stored in the queue keyspace.
///
/// Jobs are identified using scru128 because it is time-sequenced and high
/// entropy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique job identifier (scru128).
    pub id: String,

    /// Queue this job belongs to.
    pub queue: String,

    /// Priority (lower number = higher priority).
    pub priority: u16,

    /// Arbitrary payload provided by the client.
    pub payload: serde_json::Value,
}

impl Job {
    /// Derive the queue keyspace key for this job.
    ///
    /// This is a pure function of `priority` and `id`, so it doesn't need
    /// to be stored.
    pub fn queue_key(&self) -> String {
        make_queue_key(self.priority, &self.id)
    }
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

    /// Reference to the queue keyspace.
    ///
    /// Entries in this keyspace are keyed by priority, then job ID, which
    /// naturally provides the priority ordering, then FIFO within each
    /// priority.
    queue: SingleWriterTxKeyspace,

    /// Reference to the working jobs keyspace.
    ///
    /// Jobs are atomically moved from the queue keyspace to the working
    /// keyspace when they are leased. In the event of crash recovery or worker
    /// disconnect jobs are returned to the queue keyspace again.
    working: SingleWriterTxKeyspace,
}

impl Store {
    /// Open or create a store at the given path.
    ///
    /// The path refers to the directory in which fjall stores its keyspace
    /// data.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let db = SingleWriterTxDatabase::builder(path).open()?;
        let jobs = db.keyspace("jobs", fjall::KeyspaceCreateOptions::default)?;
        let queue = db.keyspace("queue", fjall::KeyspaceCreateOptions::default)?;
        let working = db.keyspace("working", fjall::KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            jobs,
            queue,
            working: working,
        })
    }

    /// Enqueue a new job.
    ///
    /// Generates a unique job ID, inserts the job into the `jobs` keyspace,
    /// and adds an entry to the `queue` keyspace for priority-ordered retrieval.
    pub async fn enqueue(
        &self,
        queue_name: &str,
        priority: u16,
        payload: serde_json::Value,
    ) -> Result<Job, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let queue = self.queue.clone();
        let queue_name = queue_name.to_string();

        task::spawn_blocking(move || {
            let id = scru128::new_string();

            let job = Job {
                id: id.clone(),
                queue: queue_name,
                priority,
                payload,
            };

            let queue_key = job.queue_key();
            let job_bytes = serde_json::to_vec(&job)?;

            let mut tx = db.write_tx();
            tx.insert(&jobs, &id, &job_bytes);
            tx.insert(&queue, &queue_key, id.as_bytes());
            tx.commit()?;

            Ok(job)
        })
        .await?
    }

    /// Take the next job from the queue.
    ///
    /// Atomically removes the highest-priority (lowest number), oldest job
    /// from the `queue` keyspace and moves it to `working`. Returns `None`
    /// if the queue is empty.
    pub async fn take_next_job(&self) -> Result<Option<Job>, StoreError> {
        let db = self.db.clone();
        let jobs = self.jobs.clone();
        let queue = self.queue.clone();
        let working = self.working.clone();

        task::spawn_blocking(move || {
            // The first key in the queue keyspace is the highest-priority,
            // oldest job, because keys sort by priority (base-36) then by
            // job ID (scru128, time-ordered).
            let entry = match queue.first_key_value() {
                Some(entry) => entry,
                None => return Ok(None),
            };

            let (queue_key, job_id) = entry.into_inner()?;

            // Look up the full job data from the source of truth.
            let job_bytes = jobs.get(&job_id)?.ok_or_else(|| {
                StoreError::Corruption(format!(
                    "job in queue but missing from jobs keyspace: {:?}",
                    String::from_utf8_lossy(&job_id),
                ))
            })?;

            let job: Job = serde_json::from_slice(&job_bytes)?;

            // Atomically: remove from queue, add to working.
            let mut tx = db.write_tx();
            tx.remove(&queue, &*queue_key);
            tx.insert(&working, &*job_id, b"");
            tx.commit()?;

            Ok(Some(job))
        })
        .await?
    }
}

/// Base-36 alphabet (0-9, a-z) for compact, lexicographically sortable keys.
const B36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

/// Build a queue key that sorts by priority then by job ID.
///
/// The priority is encoded as a 4-character base-36 string (covers the full
/// u16 range: 65535 = "1ekf") so that lexicographic ordering matches numeric
/// ordering. The job ID (scru128, already base-36) provides uniqueness and
/// time-ordering within the same priority level.
fn make_queue_key(priority: u16, job_id: &str) -> String {
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
    async fn enqueue_queue_key_reflects_priority() {
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
        assert!(high.queue_key() < low.queue_key());
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
}
