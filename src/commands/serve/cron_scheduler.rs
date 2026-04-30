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
