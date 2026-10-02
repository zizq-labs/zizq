// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Shared test fixtures for the `store` module.
//!
//! Helpers live here so they can be reused across the per-operation test
//! modules (`complete::tests`, `enqueue::tests`, etc.) without drifting
//! from the originals that previously lived inline in `store.rs`.

#![cfg(test)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use super::options::{EnqueueOptions, FailureOptions};
use super::storage_config::StorageConfig;
use super::store::Store;
use super::types::{BackoffConfig, Job};
use crate::time::now_millis;

/// The temporary directory a test store lives in, removed when the store
/// is dropped.
///
/// Every store preallocates a 64 MB journal. That costs nothing on disk on
/// Linux and macOS, where the file is sparse, but is fully allocated on
/// Windows, so leaving every test's store behind fills the disk.
///
/// Background threads holding the database can briefly outlive the store,
/// and Windows will not delete a file that is still open, so removal is
/// retried for a short while before giving up.
pub(crate) struct TempStoreDir(PathBuf);

impl Drop for TempStoreDir {
    fn drop(&mut self) {
        for _ in 0..50 {
            match std::fs::remove_dir_all(&self.0) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                _ => return,
            }
        }
    }
}

impl Store {
    /// Open a fresh store in a temporary directory, which is removed when
    /// the store's last handle to its keyspaces is dropped.
    pub(crate) fn open_temp(config: StorageConfig) -> Store {
        let dir = tempfile::tempdir().unwrap().keep();
        let store = Store::open(dir.join("data"), config).unwrap();
        let _ = store.ks.temp_dir.set(TempStoreDir(dir));
        store
    }
}

/// Open a fresh store in a tempdir with default config.
pub(super) fn test_store() -> Store {
    Store::open_temp(Default::default())
}

/// Open a fresh store with explicit completed/dead retention windows.
pub(super) fn test_store_with_retention(completed_ms: u64, dead_ms: u64) -> Store {
    let mut config = StorageConfig::default();
    config.default_completed_retention_ms = completed_ms;
    config.default_dead_retention_ms = dead_ms;
    Store::open_temp(config)
}

/// Open a fresh store with a specific cap on the number of budgets.
pub(super) fn test_store_with_max_budgets(max_budgets: usize) -> Store {
    let mut config = StorageConfig::default();
    config.max_budgets = max_budgets;
    Store::open_temp(config)
}

/// Open a fresh store with a specific retry limit and zero-jitter backoff.
pub(super) fn test_store_with_retry_limit(retry_limit: u32) -> Store {
    let mut config = StorageConfig::default();
    config.default_retry_limit = retry_limit;
    config.default_backoff = BackoffConfig {
        exponent: 2.0,
        base_ms: 100,
        jitter_ms: 0, // deterministic
    };
    Store::open_temp(config)
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
