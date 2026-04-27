// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Request, response, and API types for the HTTP layer.
//!
//! These are the data structures that flow through the API — request bodies,
//! response shapes, query parameters, and the conversions between HTTP and
//! store layer types.

use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::store::{self, ScanDirection, StoreError};

/// Lifecycle status of a job as returned in API responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// The job is in the store, but is scheduled to be queued at a later time.
    Scheduled,

    /// The job is in the priority queue ready to be worked.
    Ready,

    /// The job has been delivered to a client and is in-flight.
    #[serde(rename = "in_flight")]
    InFlight,

    /// The job was successfully completed by a worker.
    Completed,

    /// The job failed too many times and exhausted its retry policy.
    Dead,
}

impl From<store::JobStatus> for JobStatus {
    fn from(s: store::JobStatus) -> Self {
        match s {
            store::JobStatus::Scheduled => Self::Scheduled,
            store::JobStatus::Ready => Self::Ready,
            store::JobStatus::InFlight => Self::InFlight,
            store::JobStatus::Completed => Self::Completed,
            store::JobStatus::Dead => Self::Dead,
        }
    }
}

impl From<JobStatus> for store::JobStatus {
    fn from(s: JobStatus) -> Self {
        match s {
            JobStatus::Scheduled => Self::Scheduled,
            JobStatus::Ready => Self::Ready,
            JobStatus::InFlight => Self::InFlight,
            JobStatus::Completed => Self::Completed,
            JobStatus::Dead => Self::Dead,
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scheduled => f.write_str("scheduled"),
            Self::Ready => f.write_str("ready"),
            Self::InFlight => f.write_str("in_flight"),
            Self::Completed => f.write_str("completed"),
            Self::Dead => f.write_str("dead"),
        }
    }
}

impl FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(Self::Scheduled),
            "ready" => Ok(Self::Ready),
            "in_flight" => Ok(Self::InFlight),
            "completed" => Ok(Self::Completed),
            "dead" => Ok(Self::Dead),
            other => Err(format!("unknown status: {other}")),
        }
    }
}

// --- CommaSet ---

/// A set of values deserialized from a comma-delimited query parameter.
///
/// For example, `?status=ready,in_flight` deserializes into a
/// `CommaSet<JobStatus>` containing `{Ready, InFlight}`.
///
/// An absent or empty parameter deserializes as an empty set.
#[derive(Debug, Clone)]
pub struct CommaSet<T: Eq + Hash>(pub HashSet<T>);

impl<T: Eq + Hash> Default for CommaSet<T> {
    fn default() -> Self {
        Self(HashSet::new())
    }
}

impl<T: Eq + Hash> std::ops::Deref for CommaSet<T> {
    type Target = HashSet<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Eq + Hash + fmt::Display + Ord> fmt::Display for CommaSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<_> = self.0.iter().collect();
        parts.sort();
        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{part}")?;
        }
        Ok(())
    }
}

impl<'de, T> Deserialize<'de> for CommaSet<T>
where
    T: Eq + Hash + FromStr,
    T::Err: fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(Self(HashSet::new()));
        }
        let mut set = HashSet::new();
        for part in s.split(',') {
            if part.is_empty() {
                continue;
            }
            let value = part.parse().map_err(serde::de::Error::custom)?;
            set.insert(value);
        }
        Ok(Self(set))
    }
}

/// HTTP representation of a job.
///
/// Mirrors `store::Job` for now, but exists as a separate type so the HTTP
/// layer can evolve independently (e.g. adding virtual fields, renaming, or
/// changing serialization) without affecting storage.
#[derive(Debug, Clone, Serialize)]
pub struct Job {
    /// Unique job identifier.
    pub id: String,

    /// Job type.
    #[serde(rename = "type")]
    pub job_type: String,

    /// Queue this job belongs to.
    pub queue: String,

    /// Priority (lower number = higher priority).
    pub priority: u16,

    /// Current lifecycle status.
    pub status: JobStatus,

    /// Arbitrary payload provided by the client.
    ///
    /// Omitted from responses where the payload is not hydrated (e.g.
    /// enqueue confirmations and failure reports, which return metadata only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,

    /// When the job becomes eligible to run (milliseconds since Unix epoch).
    pub ready_at: u64,

    /// Number of times this job has failed.
    pub attempts: u32,

    /// Maximum retries before the job is killed. `None` means the server
    /// default applies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_limit: Option<u32>,

    /// Backoff configuration override. `None` means the server default applies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<BackoffConfig>,

    /// When the job was last dequeued (milliseconds since Unix epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dequeued_at: Option<u64>,

    /// When the job last failed (milliseconds since Unix epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u64>,

    /// Per-job retention configuration override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionConfig>,

    /// When the reaper will hard-delete this job (milliseconds since Unix epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purge_at: Option<u64>,

    /// When the job was completed (milliseconds since Unix epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,

    /// Unique key for deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_key: Option<String>,

    /// Uniqueness scope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_while: Option<String>,

    /// Whether this job was returned as a duplicate of an existing job.
    /// Only present on enqueue responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate: Option<bool>,
}

/// Convert a store job to an HTTP job, failing if the status byte is invalid.
impl TryFrom<store::Job> for Job {
    type Error = StoreError;

    fn try_from(job: store::Job) -> Result<Self, Self::Error> {
        Self::from_store_job(job, None)
    }
}

impl Job {
    /// Convert an enqueue result (created or duplicate) into an HTTP job.
    ///
    /// Sets the `duplicate` flag based on the result variant so the API
    /// response can distinguish new jobs from deduplication matches.
    pub fn try_from_result(result: store::EnqueueResult) -> Result<Self, StoreError> {
        let is_duplicate = result.is_duplicate();
        let job = result.into_job();
        Self::from_store_job(job, Some(is_duplicate))
    }

    /// Shared conversion from a store job to an HTTP job.
    ///
    /// Maps compact storage field names and u8 enums to the descriptive
    /// HTTP representation. The `duplicate` flag is `None` for normal
    /// lookups and `Some(bool)` for enqueue responses.
    fn from_store_job(job: store::Job, duplicate: Option<bool>) -> Result<Self, StoreError> {
        let unique_while = job.unique.as_ref().map(|uc| match uc.scope {
            0 => "queued".to_string(),
            1 => "active".to_string(),
            2 => "exists".to_string(),
            _ => "queued".to_string(),
        });

        let status = store::JobStatus::try_from(job.status).map_err(|v| {
            StoreError::Corruption(format!("job {} has unrecognized status byte: {v}", job.id))
        })?;

        Ok(Self {
            id: job.id,
            job_type: job.job_type,
            queue: job.queue,
            priority: job.priority,
            status: status.into(),
            payload: job.payload,
            ready_at: job.ready_at,
            attempts: job.attempts,
            retry_limit: job.retry_limit,
            backoff: job.backoff.map(Into::into),
            dequeued_at: job.dequeued_at,
            failed_at: job.failed_at,
            retention: job.retention.map(Into::into),
            purge_at: job.purge_at,
            completed_at: job.completed_at,
            unique_key: job.unique.map(|uc| uc.key),
            unique_while,
            duplicate,
        })
    }
}

/// Response shape for the health check.
#[derive(Serialize)]
pub struct HealthResponse {
    /// Status message.
    pub status: &'static str,
}

/// Response shape for the version endpoint.
#[derive(Serialize)]
pub struct VersionResponse {
    /// Server version from Cargo.toml.
    pub version: &'static str,
}

/// Response shape for the count jobs endpoint.
#[derive(Serialize)]
pub struct CountJobsResponse {
    /// Number of matching jobs.
    pub count: usize,
}

/// Response shape for the list queues endpoint.
#[derive(Serialize)]
pub struct ListQueuesResponse {
    pub queues: Vec<String>,
}

/// Response shape for all error responses.
#[derive(Serialize)]
pub struct ErrorResponse {
    /// Error message.
    pub error: String,
}

/// Response shape for content negotiation errors (406, 415).
#[derive(Serialize)]
pub struct UnsupportedFormatResponse {
    /// Error message.
    pub error: String,

    /// List of supported content types.
    pub supported: Vec<&'static str>,
}

/// Request shape for the job enqueue request.
///
/// Jobs are required to specify the queue that they target and a payload to be
/// provided when dequeued. Priority is optional, defaulting to the center of
/// the priority range.
#[derive(Deserialize)]
pub struct EnqueueRequest {
    /// The job type, e.g. "send_email" or "generate_report".
    #[serde(rename = "type")]
    pub job_type: String,

    /// The queue name onto which this job is pushed.
    pub queue: String,

    /// Optional job priority (lower is higher priority).
    pub priority: Option<u16>,

    /// Arbitrary job payload provided to the client on dequeue.
    pub payload: serde_json::Value,

    /// Optional timestamp (ms since epoch) when the job becomes eligible to
    /// run. If omitted or in the past, the job is immediately ready.
    pub ready_at: Option<u64>,

    /// Per-job maximum retries before the job is killed on failure. When
    /// omitted, the server default applies.
    pub retry_limit: Option<u32>,

    /// Per-job backoff configuration override. When omitted, the server
    /// default applies.
    pub backoff: Option<BackoffConfig>,

    /// Per-job retention configuration override. When omitted, the server
    /// default applies.
    pub retention: Option<RetentionConfig>,

    /// Unique key for deduplication. When present, the store checks for an
    /// existing job with the same key before inserting.
    pub unique_key: Option<String>,

    /// Uniqueness scope: "queued" (default), "active", or "exists".
    /// Only valid when `unique_key` is present.
    pub unique_while: Option<String>,
}

/// Request shape for bulk job enqueue.
#[derive(Deserialize)]
pub struct BulkEnqueueRequest {
    pub jobs: Vec<EnqueueRequest>,
}

/// Response shape for bulk job enqueue.
#[derive(Serialize)]
pub struct BulkEnqueueResponse {
    pub jobs: Vec<Job>,
}

/// Parse a unique_while string into the store enum.
pub fn parse_unique_while(s: &str) -> Result<store::UniqueWhile, String> {
    match s {
        "queued" => Ok(store::UniqueWhile::Queued),
        "active" => Ok(store::UniqueWhile::Active),
        "exists" => Ok(store::UniqueWhile::Exists),
        other => Err(format!(
            "invalid unique_while: {other:?} (expected \"queued\", \"active\", or \"exists\")"
        )),
    }
}

/// Request shape for bulk job success.
#[derive(Deserialize)]
pub struct BulkSuccessRequest {
    pub ids: Vec<String>,
}

/// Response shape for bulk success when some IDs were not found.
#[derive(Serialize)]
pub struct BulkSuccessNotFoundResponse {
    pub not_found: Vec<String>,
}

/// Client-facing backoff configuration with human-readable field names.
///
/// The store uses compact single-letter keys (`x`, `b`, `j`) for storage
/// efficiency, but the HTTP API exposes descriptive names for clarity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    /// Power curve steepness (attempts ** exponent).
    pub exponent: f32,

    /// Minimum delay in milliseconds.
    pub base_ms: u32,

    /// Max random milliseconds per attempt multiplier.
    pub jitter_ms: u32,
}

impl From<BackoffConfig> for store::BackoffConfig {
    fn from(b: BackoffConfig) -> Self {
        Self {
            exponent: b.exponent,
            base_ms: b.base_ms,
            jitter_ms: b.jitter_ms,
        }
    }
}

impl From<store::BackoffConfig> for BackoffConfig {
    fn from(b: store::BackoffConfig) -> Self {
        Self {
            exponent: b.exponent,
            base_ms: b.base_ms,
            jitter_ms: b.jitter_ms,
        }
    }
}

/// Client-facing retention configuration with human-readable field names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Retention period for completed jobs (milliseconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_ms: Option<u64>,

    /// Retention period for dead jobs (milliseconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_ms: Option<u64>,
}

impl From<RetentionConfig> for store::RetentionConfig {
    fn from(r: RetentionConfig) -> Self {
        Self {
            completed_ms: r.completed_ms,
            dead_ms: r.dead_ms,
        }
    }
}

impl From<store::RetentionConfig> for RetentionConfig {
    fn from(r: store::RetentionConfig) -> Self {
        Self {
            completed_ms: r.completed_ms,
            dead_ms: r.dead_ms,
        }
    }
}

/// Merge-patch body for retention config within `PATCH /jobs/{id}`.
///
/// Each sub-field distinguishes absent (leave unchanged) from `null`
/// (clear to server default) from a value (set).
#[derive(Deserialize)]
pub struct RetentionConfigPatchBody {
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub completed_ms: Option<Option<u64>>,

    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub dead_ms: Option<Option<u64>>,
}

impl From<RetentionConfigPatchBody> for store::RetentionConfigPatch {
    fn from(r: RetentionConfigPatchBody) -> Self {
        Self {
            completed_ms: r.completed_ms,
            dead_ms: r.dead_ms,
        }
    }
}

/// Request body for `POST /jobs/{id}/failure`.
///
/// Reports that a job has failed. The server decides whether to retry
/// (with backoff) or kill the job based on the retry limit and any
/// client-supplied overrides.
#[derive(Deserialize)]
pub struct FailureRequest {
    /// Error message describing what went wrong.
    pub message: String,

    /// Optional error class, e.g. "TimeoutError" or "ConnectionRefused".
    pub error_type: Option<String>,

    /// Optional stack trace or backtrace from the worker.
    pub backtrace: Option<String>,

    /// Force retry at a specific time (ms since epoch), bypassing backoff
    /// calculation. The job will be rescheduled even if max retries are
    /// exceeded.
    pub retry_at: Option<u64>,

    /// Kill the job immediately, regardless of retry limit.
    #[serde(default)]
    pub kill: bool,
}

/// Query parameters for `GET /jobs`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListJobsParams {
    /// Cursor: start after this job ID (exclusive).
    pub from: Option<String>,

    /// Sort order: "asc" (oldest first) or "desc" (newest first).
    #[serde(default)]
    pub order: Order,

    /// Maximum number of jobs to return (1–2000, default 50).
    pub limit: Option<u16>,

    /// Status filter, comma-delimited (e.g. "ready,in_flight").
    #[serde(default)]
    pub status: CommaSet<JobStatus>,

    /// Queue filter, comma-delimited (e.g. "emails,webhooks").
    #[serde(default)]
    pub queue: CommaSet<String>,

    /// Type filter, comma-delimited (e.g. "send_email,generate_report").
    #[serde(default, rename = "type")]
    pub job_type: CommaSet<String>,

    /// ID filter, comma-delimited.
    #[serde(default)]
    pub id: CommaSet<String>,

    /// jq expression to filter jobs by payload (e.g. ".user_id == 42").
    pub filter: Option<String>,
}

/// Query parameters for `GET /jobs/count`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountJobsParams {
    /// ID filter, comma-delimited.
    #[serde(default)]
    pub id: CommaSet<String>,

    /// Status filter, comma-delimited.
    #[serde(default)]
    pub status: CommaSet<JobStatus>,

    /// Queue filter, comma-delimited.
    #[serde(default)]
    pub queue: CommaSet<String>,

    /// Type filter, comma-delimited.
    #[serde(default, rename = "type")]
    pub job_type: CommaSet<String>,

    /// jq expression to filter jobs by payload.
    pub filter: Option<String>,
}

/// Query parameters for `DELETE /jobs`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteJobsParams {
    /// ID filter, comma-delimited.
    #[serde(default)]
    pub id: CommaSet<String>,

    /// Status filter, comma-delimited.
    #[serde(default)]
    pub status: CommaSet<JobStatus>,

    /// Queue filter, comma-delimited.
    #[serde(default)]
    pub queue: CommaSet<String>,

    /// Type filter, comma-delimited.
    #[serde(default, rename = "type")]
    pub job_type: CommaSet<String>,

    /// jq expression to filter jobs by payload.
    pub filter: Option<String>,
}

/// Query parameters for `PATCH /jobs` — bulk patch.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchJobsParams {
    /// ID filter, comma-delimited.
    #[serde(default)]
    pub id: CommaSet<String>,

    /// Status filter, comma-delimited.
    #[serde(default)]
    pub status: CommaSet<JobStatus>,

    /// Queue filter, comma-delimited.
    #[serde(default)]
    pub queue: CommaSet<String>,

    /// Type filter, comma-delimited.
    #[serde(default, rename = "type")]
    pub job_type: CommaSet<String>,

    /// jq expression to filter jobs by payload.
    pub filter: Option<String>,
}

/// Sort order for job listings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

impl From<Order> for ScanDirection {
    fn from(o: Order) -> Self {
        match o {
            Order::Asc => Self::Asc,
            Order::Desc => Self::Desc,
        }
    }
}

impl From<ScanDirection> for Order {
    fn from(d: ScanDirection) -> Self {
        match d {
            ScanDirection::Asc => Self::Asc,
            ScanDirection::Desc => Self::Desc,
        }
    }
}
/// Response shape for paginated job listings.
#[derive(Serialize)]
pub struct ListJobsResponse {
    /// The jobs on this page in the requested order.
    pub jobs: Vec<Job>,

    /// Information about self, next, prev pages.
    pub pages: ListJobsPages,
}

/// Pagination links in a job listing response.
#[derive(Serialize)]
pub struct ListJobsPages {
    /// URL for the current page.
    #[serde(rename = "self")]
    pub self_url: String,

    /// URL for the next page, or null if this is the last page.
    pub next: Option<String>,

    /// URL for the previous page, or null if this is the first page.
    pub prev: Option<String>,
}

// --- Error listing types ---

/// HTTP-facing error record, converted from `store::ErrorRecord`.
///
/// Uses human-readable field names instead of the compact single-letter keys
/// used in the store's MsgPack encoding.
#[derive(Debug, Serialize)]
pub struct ErrorRecord {
    /// Which attempt this error corresponds to (1-based).
    pub attempt: u32,

    /// Error message from the worker.
    pub message: String,

    /// Error class, e.g. "TimeoutError".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,

    /// Stack trace / backtrace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<String>,

    /// When the job was dequeued for this attempt (ms since epoch).
    pub dequeued_at: u64,

    /// When the job failed (ms since epoch).
    pub failed_at: u64,
}

impl From<store::ErrorRecord> for ErrorRecord {
    fn from(r: store::ErrorRecord) -> Self {
        Self {
            attempt: r.attempt,
            message: r.message,
            error_type: r.error_type,
            backtrace: r.backtrace,
            dequeued_at: r.dequeued_at,
            failed_at: r.failed_at,
        }
    }
}

/// Query parameters for `GET /jobs/{id}/errors`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListErrorsParams {
    /// Cursor: start after this attempt number (exclusive).
    pub from: Option<u32>,

    /// Sort order: "asc" (oldest first) or "desc" (newest first).
    #[serde(default)]
    pub order: Order,

    /// Maximum number of error records to return (1–200, default 50).
    pub limit: Option<u16>,
}

/// Response shape for paginated error listings.
#[derive(Serialize)]
pub struct ListErrorsResponse {
    /// The error records on this page.
    pub errors: Vec<ErrorRecord>,

    /// Information about self, next, prev pages.
    pub pages: ListErrorsPages,
}

/// Pagination links in an error listing response.
#[derive(Serialize)]
pub struct ListErrorsPages {
    /// URL for the current page.
    #[serde(rename = "self")]
    pub self_url: String,

    /// URL for the next page, or null if this is the last page.
    pub next: Option<String>,

    /// URL for the previous page, or null if this is the first page.
    pub prev: Option<String>,
}

/// Deserialize an optional-nullable field for JSON Merge Patch semantics:
/// - absent -> `None` (don't touch)
/// - `null` -> `Some(None)` (clear to default)
/// - value -> `Some(Some(T))` (apply)
pub fn deserialize_nullable<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::deserialize(deserializer)?))
}

/// Request body for `PATCH /jobs/{id}`.
///
/// Absent fields are left unchanged. Fields set to `null` are cleared
/// to their default (server-managed) value.
#[derive(Deserialize)]
pub struct PatchJobBody {
    /// Move the job to a different queue. Cannot be null.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub queue: Option<Option<String>>,

    /// Change the job's priority. Cannot be null.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub priority: Option<Option<u16>>,

    /// Change when the job becomes ready (milliseconds since epoch).
    /// `null` clears to immediately ready.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub ready_at: Option<Option<u64>>,

    /// Override the retry limit. `null` clears to server default.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub retry_limit: Option<Option<u32>>,

    /// Override the backoff config. `null` clears to server default.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub backoff: Option<Option<BackoffConfig>>,

    /// Override the retention config. `null` clears entirely.
    /// An object merge-patches individual sub-fields.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub retention: Option<Option<RetentionConfigPatchBody>>,
}

pub const DEFAULT_PREFETCH: usize = 1;

/// Query parameters for `GET /jobs/take`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakeParams {
    /// Maximum number of unacknowledged jobs to send before waiting.
    /// 0 means unlimited. Defaults to 1.
    #[serde(default = "default_prefetch")]
    pub prefetch: usize,

    /// Queue filter, comma-delimited (e.g. "emails,webhooks").
    /// Empty means all queues.
    #[serde(default)]
    pub queue: CommaSet<String>,
}

pub fn default_prefetch() -> usize {
    DEFAULT_PREFETCH
}
