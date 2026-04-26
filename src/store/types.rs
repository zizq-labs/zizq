// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Core domain types for the store layer.
//!
//! These are the fundamental data structures that represent jobs, errors,
//! statuses, and configuration. They are used throughout the store and
//! exposed in the public API.

use std::fmt;

use serde::{Deserialize, Serialize};
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

    /// The requested operation is not valid for the job's current state.
    InvalidOperation(String),

    /// Internal error (e.g. sync channel closed).
    Internal(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Db(e) => write!(f, "database error: {e}"),
            StoreError::Serialize(e) => write!(f, "serialization error: {e}"),
            StoreError::Deserialize(e) => write!(f, "deserialization error: {e}"),
            StoreError::TaskJoin(e) => write!(f, "blocking task failed: {e}"),
            StoreError::Corruption(msg) => write!(f, "data corruption: {msg}"),
            StoreError::InvalidOperation(msg) => write!(f, "invalid operation: {msg}"),
            StoreError::Internal(msg) => write!(f, "internal error: {msg}"),
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

/// Uniqueness scope — which job statuses constitute a conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UniqueWhile {
    Queued = 0,
    Active = 1,
    Exists = 2,
}

impl TryFrom<u8> for UniqueWhile {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(UniqueWhile::Queued),
            1 => Ok(UniqueWhile::Active),
            2 => Ok(UniqueWhile::Exists),
            other => Err(other),
        }
    }
}

impl UniqueWhile {
    pub(super) fn conflicts_with(self, status: JobStatus) -> bool {
        match self {
            UniqueWhile::Queued => matches!(status, JobStatus::Scheduled | JobStatus::Ready),
            UniqueWhile::Active => matches!(
                status,
                JobStatus::Scheduled | JobStatus::Ready | JobStatus::InFlight
            ),
            UniqueWhile::Exists => true,
        }
    }
}

/// A uniqueness constraint on a job, stored as a nested object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    /// The unique key string.
    #[serde(rename = "k")]
    pub key: String,

    /// The scope as a u8 (converts to `UniqueWhile`).
    #[serde(rename = "w")]
    pub scope: u8,
}

impl UniqueConstraint {
    /// The parsed uniqueness scope.
    pub fn unique_while(&self) -> UniqueWhile {
        UniqueWhile::try_from(self.scope).unwrap_or(UniqueWhile::Queued)
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

    /// Uniqueness constraint for deduplication. When set, new jobs cannot be
    /// enqueued with this key while the job is in any of the conflicting
    /// statuses.
    #[serde(rename = "u")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<UniqueConstraint>,
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
