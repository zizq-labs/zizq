// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
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
    /// The enqueue was folded into an existing pending batched job. The
    /// returned job is the batch's job (whose payload now reflects the
    /// merged result).
    Folded(Job),
}

impl EnqueueResult {
    /// Return a reference to the underlying job regardless of variant.
    pub fn job(&self) -> &Job {
        match self {
            EnqueueResult::Created(j) | EnqueueResult::Duplicate(j) | EnqueueResult::Folded(j) => j,
        }
    }

    /// Return `true` if this result is a duplicate.
    pub fn is_duplicate(&self) -> bool {
        matches!(self, EnqueueResult::Duplicate(_))
    }

    /// Return `true` if this enqueue was folded into an existing batched job.
    pub fn is_folded(&self) -> bool {
        matches!(self, EnqueueResult::Folded(_))
    }

    /// Consume the result and return the underlying job.
    pub fn into_job(self) -> Job {
        match self {
            EnqueueResult::Created(j) | EnqueueResult::Duplicate(j) | EnqueueResult::Folded(j) => j,
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
