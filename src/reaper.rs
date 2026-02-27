// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Background reaper that purges expired completed and dead jobs.
//!
//! Jobs that transition to Completed or Dead status get a `purge_at`
//! timestamp based on their retention configuration. This module provides
//! the async loop that scans the `jobs_by_purge_at` index and hard-deletes
//! jobs whose retention period has expired.

use std::time::Duration;

use tokio::sync::watch;

use crate::store::Store;

/// Default number of expired jobs to fetch per iteration.
pub const DEFAULT_BATCH_SIZE: usize = 1_000;

/// Default interval between reaper scans (milliseconds).
pub const DEFAULT_CHECK_INTERVAL_MS: u64 = 30_000;

/// Run the reaper loop until the shutdown signal is received.
///
/// Each iteration fetches a batch of expired jobs and purges them one at a
/// time. If the batch was full (more expired jobs remain), it loops
/// immediately without sleeping. Otherwise it sleeps for `check_interval`
/// before scanning again.
///
/// The `clock` parameter provides the current time in milliseconds since
/// the Unix epoch. In production, pass [`crate::time::now_millis`]; in tests,
/// an injectable clock enables deterministic time control.
pub async fn run(
    store: Store,
    clock: impl Fn() -> u64,
    batch_size: usize,
    check_interval: Duration,
    mut shutdown: watch::Receiver<()>,
) {
    loop {
        let now = clock();

        match store.next_expired(now, batch_size).await {
            Ok((batch, has_more)) => {
                if !batch.is_empty() {
                    match store.purge_batch(&batch).await {
                        Ok(count) => {
                            tracing::debug!(count, "jobs purged");
                        }
                        Err(e) => {
                            tracing::error!(%e, "purge_batch failed");
                        }
                    }
                }

                if has_more {
                    continue; // More expired jobs — loop immediately.
                }
            }
            Err(e) => {
                tracing::error!(%e, "next_expired failed");
            }
        }

        // Sleep for the check interval (or until shutdown).
        tokio::select! {
            _ = tokio::time::sleep(check_interval) => {}
            _ = shutdown.changed() => break,
        }
    }

    tracing::debug!("reaper stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EnqueueOptions, StorageConfig, Store};
    use crate::time::now_millis;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_store_with_retention(completed_ms: u64, dead_ms: u64) -> Store {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_completed_retention_ms = completed_ms;
        config.default_dead_retention_ms = dead_ms;
        let store = Store::open(dir.path().join("data"), config).unwrap();
        std::mem::forget(dir);
        store
    }

    fn test_clock() -> (Arc<AtomicU64>, impl Fn() -> u64) {
        let time = Arc::new(AtomicU64::new(now_millis()));
        let t = Arc::clone(&time);
        (time, move || t.load(Ordering::Relaxed))
    }

    async fn yield_reaper() {
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(1));
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn purges_expired_jobs_on_tick() {
        // retention=1000ms so purge_at = now + 1000
        let store = test_store_with_retention(1_000, 1_000);
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let t = clock.load(Ordering::Relaxed);

        // Enqueue, take, and complete a job.
        store
            .enqueue(t, EnqueueOptions::new("test", "q", serde_json::json!("a")))
            .await
            .unwrap();
        let job = store
            .take_next_job(t, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(t, &job.id).await.unwrap();

        // Job should still be visible (purge_at = t + 1000, now = t).
        assert!(store.get_job(t, &job.id).await.unwrap().is_some());

        // Start the reaper.
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            Duration::from_millis(100),
            shutdown_rx,
        ));

        // Advance past purge_at.
        clock.store(t + 1_001, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(101)).await;
        yield_reaper().await;

        // Job should be physically gone.
        // Use a far-future `now` to bypass the logical filter too.
        assert!(store.get_job(u64::MAX, &job.id).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_purge_future_jobs() {
        let store = test_store_with_retention(10_000, 10_000);
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let t = clock.load(Ordering::Relaxed);

        store
            .enqueue(t, EnqueueOptions::new("test", "q", serde_json::json!("a")))
            .await
            .unwrap();
        let job = store
            .take_next_job(t, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(t, &job.id).await.unwrap();

        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            Duration::from_millis(100),
            shutdown_rx,
        ));

        // Advance partway — not past the purge_at.
        clock.store(t + 5_000, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(101)).await;
        yield_reaper().await;

        // Job should still exist (purge_at = t + 10_000, now = t + 5_000).
        assert!(store.get_job(t + 5_000, &job.id).await.unwrap().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn loops_immediately_when_batch_is_full() {
        // Use retention=1 so mark_completed defers to the reaper instead
        // of synchronously deleting (retention=0 now hard-deletes inline).
        let store = test_store_with_retention(1, 1);
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let t = clock.load(Ordering::Relaxed);

        // Create 3 jobs, all completed immediately.
        let mut ids = Vec::new();
        for i in 0..3 {
            store
                .enqueue(t, EnqueueOptions::new("test", "q", serde_json::json!(i)))
                .await
                .unwrap();
            let job = store
                .take_next_job(t, &HashSet::new())
                .await
                .unwrap()
                .unwrap();
            store.mark_completed(t, &job.id).await.unwrap();
            ids.push(job.id);
        }

        // Advance clock past purge_at (t + 1ms retention).
        clock.store(t + 2, Ordering::Relaxed);

        // Use batch_size=2 to force multiple loops.
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            2,
            Duration::from_millis(100),
            shutdown_rx,
        ));

        tokio::time::advance(Duration::from_millis(101)).await;
        yield_reaper().await;
        yield_reaper().await;

        // All 3 should be purged.
        for id in &ids {
            assert!(store.get_job(u64::MAX, id).await.unwrap().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stops_on_shutdown_signal() {
        let store = test_store_with_retention(0, 0);
        let (_, clock_fn) = test_clock();
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let handle = tokio::spawn(run(
            store,
            clock_fn,
            DEFAULT_BATCH_SIZE,
            Duration::from_millis(100),
            shutdown_rx,
        ));

        yield_reaper().await;
        drop(shutdown_tx);
        yield_reaper().await;

        tokio::time::advance(Duration::from_secs(1)).await;
        handle.await.unwrap();
    }
}
