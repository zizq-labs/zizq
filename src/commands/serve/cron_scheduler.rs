// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Background cron scheduler that enqueues jobs from cron entries.
//!
//! Sleeps until the next due cron entry, waking early if a
//! `CronScheduleChanged` event arrives (which might have an earlier
//! `next_enqueue_at`). When entries are due, enqueues their jobs
//! and advances `next_enqueue_at` to the next occurrence.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::license::Feature;
use crate::state::AppState;
use crate::store::{Store, StoreEvent};

/// Run the cron scheduler loop until the shutdown signal is received.
///
/// The license is checked before promoting each batch of due entries.
/// When the license does not include `Feature::CronScheduling`, the
/// scheduler sleeps without enqueuing jobs until a schedule change
/// event arrives (which may indicate a license rotation).
pub async fn run(state: Arc<AppState>, clock: impl Fn() -> u64, mut shutdown: watch::Receiver<()>) {
    let store = &state.store;
    let mut event_rx = store.subscribe();

    loop {
        let now = clock();

        // Check license — if cron scheduling is not available, sleep
        // until a schedule change event (which may indicate a license
        // rotation has occurred).
        let licensed = state
            .license
            .read()
            .unwrap()
            .require(now, Feature::CronScheduling)
            .is_ok();

        if !licensed {
            // License not valid — wait briefly and re-check. The license
            // may be rotated at any moment via the file watcher.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => {
                    tracing::debug!("cron scheduler stopped");
                    return;
                }
            }
            continue;
        }

        // Peek at the in-memory cron index for the next due entry.
        let next_due = store.cron_next_due_at();

        // Process any entries that are due now.
        if let Some(due_at) = next_due {
            if due_at <= now {
                if let Err(e) = process_due_entries(&store, &clock).await {
                    tracing::error!(%e, "cron scheduler: error processing due entries");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = shutdown.changed() => break,
                    }
                }
                // Loop immediately to check for more due entries.
                continue;
            }
        }

        // Sleep until the next due entry, or indefinitely if there are none.
        let sleep_until = next_due.unwrap_or(u64::MAX);
        let delay = Duration::from_millis(sleep_until.saturating_sub(clock()));
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep => break,
                event = event_rx.recv() => {
                    match event {
                        Ok(StoreEvent::CronScheduleChanged) => {
                            // Schedule changed — break to re-check the
                            // index from the top of the outer loop.
                            break;
                        }
                        Ok(StoreEvent::IndexRebuilt) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => break,
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!("cron scheduler stopped");
                            return;
                        }
                        _ => {} // Ignore job events.
                    }
                }
                _ = shutdown.changed() => {
                    tracing::debug!("cron scheduler stopped");
                    return;
                }
            }
        }
    }

    tracing::debug!("cron scheduler stopped");
}

/// Process all due cron entries and enqueue their jobs.
async fn process_due_entries(
    store: &Store,
    clock: &impl Fn() -> u64,
) -> Result<(), crate::store::StoreError> {
    let now = clock();
    let due = store.next_due_cron_entries(now);

    for (_, group, entry_name) in due {
        match store.promote_cron_entry(&group, &entry_name, clock()).await {
            Ok(Some(entry)) => {
                tracing::info!(
                    group = %group,
                    entry = %entry_name,
                    next_enqueue_at = ?entry.next_enqueue_at,
                    "cron job enqueued"
                );
            }
            Ok(None) => {
                // Entry was paused, not due, or deleted — skip silently.
            }
            Err(e) => {
                tracing::error!(
                    group = %group,
                    entry = %entry_name,
                    %e,
                    "failed to enqueue cron job"
                );
                return Err(e);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::license::{License, Tier};
    use crate::state::{AppState, DEFAULT_GLOBAL_IN_FLIGHT_LIMIT, DEFAULT_HEARTBEAT_INTERVAL_MS};
    use crate::store::{CronEntryOptions, EnqueueOptions, ListJobsOptions, Store, StoreEvent};
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        store
    }

    fn test_clock() -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        // Fixed base: 2023-11-14 22:13:20 UTC
        let time = Arc::new(AtomicU64::new(1_700_000_000_000));
        (Arc::clone(&time), time)
    }

    fn clock_fn(clock: &Arc<AtomicU64>) -> impl Fn() -> u64 + Send + Sync + 'static {
        let t = Arc::clone(clock);
        move || t.load(Ordering::Relaxed)
    }

    fn pro_license() -> License {
        License::Licensed {
            licensee_id: "test".into(),
            licensee_name: "Test".into(),
            tier: Tier::Pro,
            expires_at: u64::MAX,
        }
    }

    fn test_state(store: Store, clock: &Arc<AtomicU64>) -> Arc<AppState> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        std::mem::forget(_shutdown_tx);
        let (admin_events, _) = broadcast::channel(64);
        Arc::new(AppState {
            license: RwLock::new(License::Free),
            store,
            heartbeat_interval_ms: Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS),
            global_in_flight_limit: DEFAULT_GLOBAL_IN_FLIGHT_LIMIT,
            shutdown: shutdown_rx,
            clock: Arc::new(clock_fn(clock)),
            admin_events,
            start_time: std::time::Instant::now(),
        })
    }

    fn cron_entry(name: &str, expression: &str) -> CronEntryOptions {
        CronEntryOptions {
            name: name.to_string(),
            expression: expression.to_string(),
            paused: None,
            job: EnqueueOptions::new("test_job", "cron-q", serde_json::json!({})),
        }
    }

    /// Wait for a `JobCreated` event on the store's broadcast channel.
    /// Times out after 5 seconds (real time) to avoid hanging on failure.
    async fn wait_for_job_created(store: &Store) {
        let mut rx = store.subscribe();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Ok(StoreEvent::JobCreated { .. }) => return,
                        Err(broadcast::error::RecvError::Closed) => {
                            panic!("store event channel closed while waiting for JobCreated");
                        }
                        _ => continue,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    panic!("timed out waiting for JobCreated event");
                }
            }
        }
    }

    /// Brief yield to let the scheduler process. Used for negative tests
    /// (asserting nothing happened) where we can't wait for a specific event.
    async fn yield_scheduler() {
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(2));
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn promotes_due_cron_entry() {
        let store = test_store();
        let (clock, _) = test_clock();
        let state = test_state(store.clone(), &clock);
        *state.license.write().unwrap() = pro_license();

        let t = clock.load(Ordering::Relaxed);

        store
            .replace_cron_group("default", vec![cron_entry("e1", "* * * * *")], t)
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(state.clone(), clock_fn(&clock), shutdown_rx));

        // Advance past the due time and wait for the job to be created.
        clock.store(due_at + 1, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(due_at - t + 1)).await;
        wait_for_job_created(&store).await;

        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["cron-q".to_string()].into()))
            .await
            .unwrap();
        assert_eq!(jobs.jobs.len(), 1);
        assert_eq!(jobs.jobs[0].job_type, "test_job");

        let updated = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        assert!(updated.next_enqueue_at.unwrap() > due_at);
        assert_eq!(updated.last_enqueue_at, Some(due_at + 1));
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_promote_before_due() {
        let store = test_store();
        let (clock, _) = test_clock();
        let state = test_state(store.clone(), &clock);
        *state.license.write().unwrap() = pro_license();

        let t = clock.load(Ordering::Relaxed);

        store
            .replace_cron_group("default", vec![cron_entry("e1", "* * * * *")], t)
            .await
            .unwrap();

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(state.clone(), clock_fn(&clock), shutdown_rx));

        // Advance only a small amount — not past the entry's due time.
        clock.store(t + 5_000, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(5_000)).await;
        yield_scheduler().await;

        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["cron-q".to_string()].into()))
            .await
            .unwrap();
        assert!(jobs.jobs.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn wakes_on_schedule_changed_event() {
        let store = test_store();
        let (clock, _) = test_clock();
        let state = test_state(store.clone(), &clock);
        *state.license.write().unwrap() = pro_license();

        let t = clock.load(Ordering::Relaxed);

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(state.clone(), clock_fn(&clock), shutdown_rx));

        // Let the scheduler start and go to sleep (no entries yet).
        yield_scheduler().await;

        // Add a cron entry — this emits CronScheduleChanged.
        store
            .replace_cron_group("default", vec![cron_entry("e1", "* * * * *")], t)
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        // Advance past the due time. The CronScheduleChanged event should
        // have already woken the scheduler.
        clock.store(due_at + 1, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(due_at - t + 1)).await;
        wait_for_job_created(&store).await;

        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["cron-q".to_string()].into()))
            .await
            .unwrap();
        assert_eq!(jobs.jobs.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_on_shutdown_signal() {
        let store = test_store();
        let (clock, _) = test_clock();
        let state = test_state(store, &clock);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let handle = tokio::spawn(run(state, clock_fn(&clock), shutdown_rx));

        yield_scheduler().await;

        drop(shutdown_tx);
        yield_scheduler().await;

        tokio::time::advance(Duration::from_secs(2)).await;
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn skips_when_license_not_valid() {
        let store = test_store();
        let (clock, _) = test_clock();
        let state = test_state(store.clone(), &clock);
        // License is Free (default) — cron scheduling not available.

        let t = clock.load(Ordering::Relaxed);

        store
            .replace_cron_group("default", vec![cron_entry("e1", "* * * * *")], t)
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(state.clone(), clock_fn(&clock), shutdown_rx));

        // Advance past the due time.
        clock.store(due_at + 1, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(due_at - t + 1)).await;
        yield_scheduler().await;

        // No jobs should be enqueued — license doesn't permit cron scheduling.
        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["cron-q".to_string()].into()))
            .await
            .unwrap();
        assert!(jobs.jobs.is_empty());
    }
}
