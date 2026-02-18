// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Background scheduler that promotes scheduled jobs to the Ready state.
//!
//! Jobs enqueued with a future `ready_at` timestamp sit in the
//! `scheduled_jobs_by_ready_at` index until their time arrives. This module
//! provides the async loop that scans that index and promotes due jobs.

use std::time::{Duration, SystemTime};

use tokio::sync::{broadcast, watch};

use crate::store::{Store, StoreEvent};

/// Default number of due jobs to fetch per iteration.
///
/// Override with the `ZANXIO_SCHEDULER_BATCH_SIZE` environment variable.
const DEFAULT_BATCH_SIZE: usize = 200;

/// Returns the current time as milliseconds since the Unix epoch.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Run the scheduler loop until the shutdown signal is received.
///
/// Each iteration fetches a batch of due scheduled jobs and promotes them
/// one at a time. If no jobs are due, the loop sleeps until the next
/// scheduled `ready_at` timestamp, waking early if a new `JobScheduled`
/// event arrives (which might be due sooner).
pub async fn run(store: Store, mut shutdown: watch::Receiver<()>) {
    let batch_size = std::env::var("ZANXIO_SCHEDULER_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);

    let mut event_rx = store.subscribe();

    loop {
        let now = now_millis();

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

            tracing::debug!(job_id = %job.id, queue = %job.queue, "job promoted");
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
        let sleep_until = match next_ready_at {
            Some(ready_at) => ready_at,
            None if !promoted_any => u64::MAX,
            None => {
                // We promoted jobs and hit the batch limit — loop
                // immediately (handled by the continue above, but this
                // arm is here for exhaustiveness).
                continue;
            }
        };

        let delay = Duration::from_millis(sleep_until.saturating_sub(now_millis()));
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
                            let delay = Duration::from_millis(
                                ready_at.saturating_sub(now_millis()),
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
