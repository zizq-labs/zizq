// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

mod complete;
mod complete_batcher;
mod cron;
mod delete;
mod enqueue_batcher;
mod fail;
mod group_committer;
mod options;
mod ready_index;
mod requeue;
mod results;
mod scheduled;
mod scheduled_index;
mod store;
mod types;

#[cfg(test)]
mod test_support;

pub use options::{
    BulkDeleteOptions, BulkPatchOptions, CronEntryOptions, EnqueueOptions, FailureOptions,
    ListErrorsOptions, ListJobsOptions, PatchJobOptions, ReplaceCronGroupOptions,
    RetentionConfigPatch,
};

pub use results::{BulkCompleteResult, EnqueueResult, ListErrorsPage, ListJobsPage};

pub use store::{
    DEFAULT_BACKOFF_BASE_MS, DEFAULT_BACKOFF_EXPONENT, DEFAULT_BACKOFF_JITTER_MS,
    DEFAULT_CACHE_SIZE, DEFAULT_COMPLETE_BATCH_SIZE, DEFAULT_COMPLETED_RETENTION_MS,
    DEFAULT_DATA_TABLE_SIZE, DEFAULT_DEAD_RETENTION_MS, DEFAULT_ENQUEUE_BATCH_SIZE,
    DEFAULT_INDEX_TABLE_SIZE, DEFAULT_JOURNAL_SIZE, DEFAULT_L0_THRESHOLD, DEFAULT_RETRY_LIMIT,
    StorageConfig, Store, StoreEvent,
};

pub use cron::{CronEntry, CronGroup};

pub use types::{
    BackoffConfig, CommitMode, EnvConfigError, ErrorRecord, Job, JobStatus, RetentionConfig,
    ScanDirection, StoreError, UniqueConstraint, UniqueWhile,
};
