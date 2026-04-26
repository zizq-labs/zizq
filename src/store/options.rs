// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Input/builder types for store operations.
//!
//! These are the options structs that callers construct to configure
//! queries, enqueue operations, patches, and failure handling.

use std::collections::HashSet;
use std::sync::Arc;

use crate::filter::PayloadFilter;

use super::types::{BackoffConfig, JobStatus, RetentionConfig, ScanDirection, UniqueWhile};

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

    /// Optional ID filter. When non-empty, only jobs with one of these
    /// IDs are returned. Intersected with other filters when combined.
    pub ids: HashSet<String>,

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

    /// Optional jq-style payload filter. When provided, only jobs whose
    /// payload matches the filter are returned. Payloads are hydrated and
    /// checked after the index scan narrows the candidate set.
    ///
    /// Wrapped in `Arc` because `ListJobsOptions` must be `Clone` for
    /// pagination, but `PayloadFilter` contains compiled state.
    pub filter: Option<Arc<PayloadFilter>>,
}

impl ListJobsOptions {
    /// Create default options (ascending, no cursor, no filters, limit 50).
    pub fn new() -> Self {
        Self {
            from: None,
            direction: ScanDirection::Asc,
            limit: 50,
            ids: HashSet::new(),
            statuses: HashSet::new(),
            queues: HashSet::new(),
            types: HashSet::new(),
            now: None,
            filter: None,
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

    /// Filter by job IDs and return `self`.
    ///
    /// An empty set means "all jobs". When combined with other filters,
    /// the result is the intersection.
    pub fn ids(mut self, ids: HashSet<String>) -> Self {
        self.ids = ids;
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
    pub fn now(mut self, now: impl Into<Option<u64>>) -> Self {
        self.now = now.into();
        self
    }

    /// Set the payload filter and return `self`.
    pub fn filter(mut self, filter: impl Into<Option<Arc<PayloadFilter>>>) -> Self {
        self.filter = filter.into();
        self
    }
}

/// Options for bulk-deleting jobs in `Store::delete_jobs`.
#[derive(Debug)]
pub struct BulkDeleteOptions {
    /// Optional ID filter. When non-empty, only these IDs are candidates.
    pub ids: HashSet<String>,

    /// Optional status filter.
    pub statuses: HashSet<JobStatus>,

    /// Optional queue filter.
    pub queues: HashSet<String>,

    /// Optional type filter.
    pub types: HashSet<String>,

    /// Optional jq payload filter.
    pub filter: Option<Arc<PayloadFilter>>,
}

impl BulkDeleteOptions {
    pub fn new() -> Self {
        Self {
            ids: HashSet::new(),
            statuses: HashSet::new(),
            queues: HashSet::new(),
            types: HashSet::new(),
            filter: None,
        }
    }
}

/// Options for bulk-patching jobs matching a set of filters.
///
/// Combines the same filter fields as `BulkDeleteOptions` with the
/// patch fields from `PatchJobOptions`.
pub struct BulkPatchOptions {
    /// Optional ID filter. When non-empty, only these IDs are candidates.
    pub ids: HashSet<String>,

    /// Optional status filter.
    pub statuses: HashSet<JobStatus>,

    /// Optional queue filter.
    pub queues: HashSet<String>,

    /// Optional type filter.
    pub types: HashSet<String>,

    /// Optional jq payload filter.
    pub filter: Option<Arc<PayloadFilter>>,

    /// The patch to apply to each matching job.
    pub patch: PatchJobOptions,
}

/// Request to patch a single job's mutable fields.
///
/// `None` = leave unchanged. `Some(None)` = clear to default/null.
/// `Some(Some(v))` = set to `v`.
///
/// Fields that are not nullable use plain `Option<T>`.
#[derive(Debug, Default)]
pub struct PatchJobOptions {
    /// Move the job to a different queue.
    pub queue: Option<String>,

    /// Change the job's priority.
    pub priority: Option<u16>,

    /// Change when the job becomes ready (milliseconds since epoch).
    /// `Some(None)` clears to "now" (immediately ready).
    pub ready_at: Option<Option<u64>>,

    /// Override the retry limit. `Some(None)` clears to server default.
    pub retry_limit: Option<Option<u32>>,

    /// Override the backoff config. `Some(None)` clears to server default.
    pub backoff: Option<Option<BackoffConfig>>,

    /// Override the retention config. `Some(None)` clears entirely.
    /// `Some(Some(patch))` merges individual sub-fields.
    pub retention: Option<Option<RetentionConfigPatch>>,
}

/// Merge-patch for `RetentionConfig`.
///
/// Each sub-field uses `Option<Option<u64>>`:
/// - `None` — leave unchanged
/// - `Some(None)` — clear to server default
/// - `Some(Some(v))` — set to `v`
#[derive(Debug, Default, Clone)]
pub struct RetentionConfigPatch {
    pub completed_ms: Option<Option<u64>>,
    pub dead_ms: Option<Option<u64>>,
}

impl RetentionConfigPatch {
    /// Apply this patch to an existing retention config (or `None`).
    ///
    /// Returns `None` if both sub-fields end up cleared — there is no
    /// point storing a `RetentionConfig` where every field is `None`.
    pub fn apply(self, current: Option<RetentionConfig>) -> Option<RetentionConfig> {
        let mut r = current.unwrap_or(RetentionConfig {
            completed_ms: None,
            dead_ms: None,
        });

        if let Some(v) = self.completed_ms {
            r.completed_ms = v;
        }
        if let Some(v) = self.dead_ms {
            r.dead_ms = v;
        }

        if r.completed_ms.is_none() && r.dead_ms.is_none() {
            None
        } else {
            Some(r)
        }
    }
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

    /// Unique key for deduplication. When set, the store checks for an
    /// existing job with the same key before inserting.
    pub unique_key: Option<String>,

    /// Uniqueness scope. Defaults to `Queued` when `unique_key` is set.
    pub unique_while: Option<UniqueWhile>,
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
            unique_key: None,
            unique_while: None,
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

    /// Set the unique key and return `self`.
    ///
    /// If `unique_while` is not yet set, defaults to `UniqueWhile::Queued`.
    pub fn unique_key(mut self, key: impl Into<String>) -> Self {
        self.unique_key = Some(key.into());
        if self.unique_while.is_none() {
            self.unique_while = Some(UniqueWhile::Queued);
        }
        self
    }

    /// Set the uniqueness scope and return `self`.
    pub fn unique_while(mut self, scope: UniqueWhile) -> Self {
        self.unique_while = Some(scope);
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
