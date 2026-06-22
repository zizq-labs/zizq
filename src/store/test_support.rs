// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Shared test fixtures for the `store` module.
//!
//! Helpers live here so they can be reused across the per-operation test
//! modules (`complete::tests`, `enqueue::tests`, etc.) without drifting
//! from the originals that previously lived inline in `store.rs`.

#![cfg(test)]

use std::collections::HashSet;

use super::options::{EnqueueOptions, FailureOptions};
use super::store::{StorageConfig, Store};
use super::types::{BackoffConfig, Job};
use crate::time::now_millis;

/// Open a fresh store in a tempdir with default config.
///
/// The tempdir handle is leaked so the directory survives until the test
/// process exits — matches the original inline `test_store()`.
pub(super) fn test_store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
    std::mem::forget(dir);
    store
}

/// Open a fresh store with explicit completed/dead retention windows.
pub(super) fn test_store_with_retention(completed_ms: u64, dead_ms: u64) -> Store {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::default();
    config.default_completed_retention_ms = completed_ms;
    config.default_dead_retention_ms = dead_ms;
    let store = Store::open(dir.path().join("data"), config).unwrap();
    std::mem::forget(dir);
    store
}

/// Open a fresh store with a specific retry limit and zero-jitter backoff.
pub(super) fn test_store_with_retry_limit(retry_limit: u32) -> Store {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::default();
    config.default_retry_limit = retry_limit;
    config.default_backoff = BackoffConfig {
        exponent: 2.0,
        base_ms: 100,
        jitter_ms: 0, // deterministic
    };
    let store = Store::open(dir.path().join("data"), config).unwrap();
    std::mem::forget(dir);
    store
}

/// Enqueue a job on the default queue and immediately take it, returning
/// the InFlight job.
pub(super) async fn enqueue_and_take(store: &Store) -> Job {
    store
        .enqueue(
            now_millis(),
            EnqueueOptions::new("test", "default", serde_json::json!("payload")),
        )
        .await
        .unwrap()
        .into_job();
    store
        .take_next_job(now_millis(), &HashSet::new())
        .await
        .unwrap()
        .unwrap()
}

/// Build a `FailureOptions` with sensible defaults for testing.
pub(super) fn test_failure_opts() -> FailureOptions {
    FailureOptions {
        message: "something broke".into(),
        error_type: None,
        backtrace: None,
        retry_at: None,
        kill: false,
    }
}
