// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Background scheduler that promotes scheduled jobs to the Ready state.
//!
//! Jobs enqueued with a future `ready_at` timestamp sit in the
//! `scheduled_jobs_by_ready_at` index until their time arrives. This module
//! provides the async loop that scans that index and promotes due jobs.

use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::store::{Store, StoreEvent};

/// Default number of due jobs to fetch per iteration.
///
/// Override with the `ZANXIO_SCHEDULER_BATCH_SIZE` environment variable.
pub const DEFAULT_BATCH_SIZE: usize = 200;

/// Run the scheduler loop until the shutdown signal is received.
///
/// Each iteration fetches a batch of due scheduled jobs and promotes them
/// one at a time. If no jobs are due, the loop sleeps until the next
/// scheduled `ready_at` timestamp, waking early if a new `JobScheduled`
/// event arrives (which might be due sooner).
///
/// The `clock` parameter provides the current time in milliseconds since
/// the Unix epoch. In production, pass [`crate::time::now_millis`]; in tests, an
/// injectable clock enables deterministic time control.
///
/// `batch_size` controls how many due jobs are fetched per iteration.
pub async fn run(
    store: Store,
    clock: impl Fn() -> u64,
    batch_size: usize,
    mut shutdown: watch::Receiver<()>,
) {
    let mut event_rx = store.subscribe();

    loop {
        let now = clock();

        let (batch, next_ready_at) = match store.next_scheduled(now, batch_size).await {
            // Got a batch of scheduled jobs to promote. Move to the next phase.
            Ok(result) => result,
            // In the error case we just wait 1 second and try again.
            Err(e) => {
                tracing::error!(%e, "next_scheduled failed");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.changed() => break,
                }
                continue;
            }
        };

        // Promote all due jobs in this batch.
        let mut promoted_any = false;
        for job in &batch {
            // If anything goes wrong we wait 1 second and try again.
            if let Err(e) = store.promote_scheduled(job).await {
                tracing::error!(job_id = %job.id, %e, "promote_scheduled failed");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.changed() => break,
                }
                // Break out of the inner for loop; the outer loop will
                // retry on the next iteration.
                break;
            }

            tracing::debug!(job_id = %job.id, job_type = %job.job_type, queue = %job.queue, "job promoted");
            promoted_any = true;
        }

        if promoted_any && next_ready_at.is_none() {
            // We promoted jobs and there might be more due beyond our batch
            // limit (next_ready_at is None because we hit the cap, not
            // because the index is empty). Loop immediately to check.
            continue;
        }

        // Sleep until the next scheduled job is due. If a JobScheduled
        // event arrives with an earlier ready_at, reset the timer — this
        // avoids a database round-trip when the new job isn't due any
        // sooner than what we're already waiting for. We ignore
        // JobCreated and JobCompleted events since they're only relevant
        // to workers.
        //
        // When there are no scheduled jobs at all (next_ready_at is
        // None and we didn't promote anything), we use u64::MAX as the
        // sleep target so that any JobScheduled event is "earlier" and
        // resets the timer.
        let mut sleep_until = match next_ready_at {
            Some(ready_at) => ready_at,
            None if !promoted_any => u64::MAX,
            None => {
                // We promoted jobs and hit the batch limit — loop
                // immediately (handled by the continue above, but this
                // arm is here for exhaustiveness).
                continue;
            }
        };

        let delay = Duration::from_millis(sleep_until.saturating_sub(clock()));
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep => break,
                event = event_rx.recv() => {
                    match event {
                        Ok(StoreEvent::JobScheduled { ready_at })
                            if ready_at < sleep_until =>
                        {
                            // New job is due sooner — reset the timer.
                            sleep_until = ready_at;
                            let delay = Duration::from_millis(
                                ready_at.saturating_sub(clock()),
                            );
                            sleep.as_mut().reset(
                                tokio::time::Instant::now() + delay,
                            );
                        }
                        Ok(StoreEvent::JobScheduled { .. }) => {
                            // New job is due later — keep sleeping.
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => break,
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!("scheduler stopped");
                            return;
                        }
                        _ => {} // Ignore JobCreated, JobCompleted
                    }
                }
                _ = shutdown.changed() => {
                    tracing::debug!("scheduler stopped");
                    return;
                }
            }
        }
    }

    tracing::debug!("scheduler stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EnqueueOptions, JobStatus, Store};
    use crate::time::now_millis;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        std::mem::forget(dir);
        store
    }

    /// Build a controllable clock backed by an `AtomicU64`.
    ///
    /// Returns the atomic (for advancing time) and a closure suitable for
    /// passing to `run()`.
    fn test_clock() -> (Arc<AtomicU64>, impl Fn() -> u64) {
        let time = Arc::new(AtomicU64::new(now_millis()));
        let t = Arc::clone(&time);
        (time, move || t.load(Ordering::Relaxed))
    }

    /// Yield to let the scheduler process events. The scheduler uses
    /// `spawn_blocking` for store operations, which run on real threads.
    /// We alternate between brief real sleeps (so blocking tasks finish)
    /// and async yields (so the scheduler task gets polled and processes
    /// results). Multiple rounds handle the multi-step scheduler loop
    /// (next_scheduled → promote → next_scheduled again).
    async fn yield_scheduler() {
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(1));
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn promotes_single_due_job() {
        let store = test_store();
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            shutdown_rx,
        ));

        let t = clock.load(Ordering::Relaxed);

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(t + 1_000),
            )
            .await
            .unwrap();

        // Advance the clock past the job's ready_at and let the scheduler
        // timer fire.
        clock.store(t + 1_001, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(1_001)).await;
        yield_scheduler().await;

        let job = store.get_job(&job.id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready as u8);
    }

    #[tokio::test(start_paused = true)]
    async fn promotes_batch_of_due_jobs() {
        let store = test_store();
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            shutdown_rx,
        ));

        let t = clock.load(Ordering::Relaxed);

        // Enqueue three jobs all due at the same time.
        let mut ids = Vec::new();
        for i in 0..3 {
            let job = store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "q", serde_json::json!(i)).ready_at(t + 1_000),
                )
                .await
                .unwrap();
            ids.push(job.id);
        }

        clock.store(t + 1_001, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(1_001)).await;
        yield_scheduler().await;

        for id in &ids {
            let job = store.get_job(id).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Ready as u8);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_promote_future_jobs() {
        let store = test_store();
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            shutdown_rx,
        ));

        let t = clock.load(Ordering::Relaxed);

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")).ready_at(t + 10_000),
            )
            .await
            .unwrap();

        // Advance partway — not past the job's ready_at.
        clock.store(t + 5_000, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(5_000)).await;
        yield_scheduler().await;

        let job = store.get_job(&job.id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Scheduled as u8);
    }

    #[tokio::test(start_paused = true)]
    async fn wakes_early_when_earlier_job_arrives() {
        let store = test_store();
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            shutdown_rx,
        ));

        let t = clock.load(Ordering::Relaxed);

        // Enqueue a job due in 10s — scheduler starts sleeping until then.
        let late = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("late")).ready_at(t + 10_000),
            )
            .await
            .unwrap();

        // Now enqueue a job due in 2s — should reset the timer.
        let early = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("early")).ready_at(t + 2_000),
            )
            .await
            .unwrap();

        // Advance past the early job's ready_at but not the late one.
        clock.store(t + 2_001, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(2_001)).await;
        yield_scheduler().await;

        let early_job = store.get_job(&early.id).await.unwrap().unwrap();
        assert_eq!(early_job.status, JobStatus::Ready as u8);

        let late_job = store.get_job(&late.id).await.unwrap().unwrap();
        assert_eq!(late_job.status, JobStatus::Scheduled as u8);
    }

    /// Regression test for a bug where `sleep_until` was not updated when
    /// the timer was reset by a `JobScheduled` event. Each successive event
    /// passed the `ready_at < sleep_until` guard (because `sleep_until` was
    /// still `u64::MAX`), pushing the timer forward to the latest job's
    /// `ready_at` instead of keeping it pinned to the earliest.
    #[tokio::test(start_paused = true)]
    async fn does_not_reset_timer_when_later_job_arrives() {
        let store = test_store();
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run(
            store.clone(),
            clock_fn,
            DEFAULT_BATCH_SIZE,
            shutdown_rx,
        ));

        let t = clock.load(Ordering::Relaxed);

        // Enqueue a job due in 2s — scheduler resets timer from u64::MAX
        // to t+2000.
        let early = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("early")).ready_at(t + 2_000),
            )
            .await
            .unwrap();

        // Enqueue a job due in 10s — must NOT push the timer forward.
        let late = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("late")).ready_at(t + 10_000),
            )
            .await
            .unwrap();

        // Advance past the early job only.
        clock.store(t + 2_001, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(2_001)).await;
        yield_scheduler().await;

        let early_job = store.get_job(&early.id).await.unwrap().unwrap();
        assert_eq!(
            early_job.status,
            JobStatus::Ready as u8,
            "early job should be promoted — timer must not have been pushed to late job's ready_at"
        );

        let late_job = store.get_job(&late.id).await.unwrap().unwrap();
        assert_eq!(late_job.status, JobStatus::Scheduled as u8);
    }

    #[tokio::test(start_paused = true)]
    async fn loops_immediately_when_batch_is_full() {
        let store = test_store();
        let (clock, clock_fn) = test_clock();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // Use batch size of 2 so we can trigger the "more due" path.
        tokio::spawn(run(store.clone(), clock_fn, 2, shutdown_rx));

        let t = clock.load(Ordering::Relaxed);

        // Enqueue 3 jobs all due at the same time.
        let mut ids = Vec::new();
        for i in 0..3 {
            let job = store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "q", serde_json::json!(i)).ready_at(t + 1_000),
                )
                .await
                .unwrap();
            ids.push(job.id);
        }

        clock.store(t + 1_001, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(1_001)).await;
        yield_scheduler().await;

        // All 3 should be promoted even though batch_size is 2 — the
        // scheduler loops immediately when the batch was full.
        for id in &ids {
            let job = store.get_job(id).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Ready as u8);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stops_on_shutdown_signal() {
        let store = test_store();
        let (_, clock_fn) = test_clock();
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let handle = tokio::spawn(run(store, clock_fn, DEFAULT_BATCH_SIZE, shutdown_rx));

        // Give the scheduler a moment to enter its loop.
        yield_scheduler().await;

        // Send shutdown.
        drop(shutdown_tx);
        yield_scheduler().await;

        // The task should complete promptly.
        tokio::time::advance(Duration::from_secs(1)).await;
        handle.await.unwrap();
    }
}
