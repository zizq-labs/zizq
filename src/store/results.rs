// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Result types returned by store operations.

use super::options::{ListErrorsOptions, ListJobsOptions};
use super::types::{ErrorRecord, Job};

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

/// Result of a single enqueue operation.
pub enum EnqueueResult {
    /// A new job was created.
    Created(Job),
    /// The job was a duplicate of an existing job in a conflicting state.
    Duplicate(Job),
}

impl EnqueueResult {
    /// Return a reference to the underlying job regardless of variant.
    pub fn job(&self) -> &Job {
        match self {
            EnqueueResult::Created(j) | EnqueueResult::Duplicate(j) => j,
        }
    }

    /// Return `true` if this result is a duplicate.
    pub fn is_duplicate(&self) -> bool {
        matches!(self, EnqueueResult::Duplicate(_))
    }

    /// Consume the result and return the underlying job.
    pub fn into_job(self) -> Job {
        match self {
            EnqueueResult::Created(j) | EnqueueResult::Duplicate(j) => j,
        }
    }
}

/// Result of a bulk completion operation.
pub struct BulkCompleteResult {
    /// IDs that were successfully marked as completed.
    pub completed: Vec<String>,
    /// IDs that were not found in the in-flight set.
    pub not_found: Vec<String>,
}
