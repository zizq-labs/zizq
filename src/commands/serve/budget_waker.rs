// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Background task that tells workers when a throttled job can run.
//!
//! A job held back by a budget is already Ready — it sits in its
//! budget's group waiting for capacity, not for anything about itself to
//! change. Nothing announces it when that capacity arrives, because the
//! events that rouse a blocked take stream are all about jobs
//! *arriving*, and no job arrives when a bucket refills or a slot frees.
//! Without this task the work waits for the next unrelated enqueue to
//! knock on the door: imperceptible on a busy server, unbounded on a
//! quiet one.
//!
//! # Two ways capacity comes back, and they need different waking
//!
//! A `time_based` budget refills on a clock, so the answer is a timer.
//! A `while_in_flight` budget refills when a job stops running, which no
//! clock predicts — that one has to be woken by the event.
//!
//! Both funnel through the same pass. `Budgets::wakeup` reports what is
//! affordable now and, separately, the earliest moment a still-parked
//! job becomes affordable *on the clock alone*. `None` for the latter
//! means no timer would help and the loop should wait on events instead.
//!
//! # Why the events, and not a fixed tick
//!
//! Polling on an interval would work and be simpler, but it puts a floor
//! under how late a freed slot is noticed and a ceiling on how idle the
//! server can be. Listening to the events that actually correspond to
//! capacity returning costs nothing on a quiet server and reacts
//! immediately on a busy one.
//!
//! Lagging the broadcast channel is harmless here: the response to
//! missing events is the same as the response to receiving them — look
//! again — so a lag is treated as a wake rather than an error.

use tokio::sync::{broadcast, watch};

use crate::store::{Store, StoreEvent};

/// How many jobs one pass will announce.
///
/// A pass considers the head of each budget that has work, so this only
/// binds when a great many budgets free up at the same instant. It is
/// there to keep one pass from flooding the broadcast channel and
/// lagging every subscriber; whatever it skips is picked up on the next
/// pass, which the same events will provoke.
pub const DEFAULT_BATCH_SIZE: usize = 128;

/// Run the budget waker until the shutdown signal is received.
///
/// The `clock` parameter provides the current time in milliseconds since
/// the Unix epoch. In production, pass [`crate::time::now_millis`]; in
/// tests, an injectable clock enables deterministic time control.
pub async fn run(
    store: Store,
    clock: impl Fn() -> u64,
    batch_size: usize,
    mut shutdown: watch::Receiver<()>,
) {
    let mut event_rx = store.subscribe();

    'wake: loop {
        // Announces whatever is affordable and hands back the next
        // clock-driven opportunity, if there is one.
        let next_refill = store.wake_budgeted_jobs(clock(), batch_size);

        // With no timer to set, the only thing that can help is an
        // event. `u64::MAX` stands in for "not on any clock" so the two
        // cases share one select below rather than duplicating it.
        let sleep_until = next_refill.unwrap_or(u64::MAX);
        let delay = std::time::Duration::from_millis(sleep_until.saturating_sub(clock()));
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);

        // Inner loop so that an event carrying no news is genuinely
        // ignored. Falling through to the outer loop instead would
        // rescan on *every* event, and since this task hears its own
        // announcements on the same channel, an announced job that no
        // worker has claimed yet would provoke another announcement,
        // and another — a spin that only ends when someone takes the
        // job.
        loop {
            tokio::select! {
                _ = &mut sleep => {
                    // A budget has dripped back up to affordable.
                    break;
                }
                _ = shutdown.changed() => break 'wake,
                event = event_rx.recv() => {
                    match event {
                        // Every way a job can stop occupying a slot.
                        // Each may have freed capacity that something
                        // parked is waiting on, so each is worth
                        // another look.
                        Ok(StoreEvent::JobCompleted { .. })
                        | Ok(StoreEvent::JobFailed { .. })
                        | Ok(StoreEvent::JobDeleted { .. }) => break,

                        // The registry was rebuilt from disk, so
                        // whatever was last computed described a
                        // different world.
                        Ok(StoreEvent::IndexRebuilt) => break,

                        // Missing events cannot be recovered, but the
                        // remedy for having missed them is the same as
                        // for hearing them.
                        Err(broadcast::error::RecvError::Lagged(_)) => break,

                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!("budget waker stopped");
                            return;
                        }

                        // Leaves budget capacity untouched, so there is
                        // nothing new to say. Keep waiting rather than
                        // rescanning.
                        Ok(_) => {}
                    }
                }
            }
        }
    }

    tracing::debug!("budget waker stopped");
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::sync::watch;

    use super::*;
    use crate::store::{BudgetBinding, BudgetStrategy, EnqueueOptions};
    use crate::time::now_millis;

    fn test_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        store
    }

    fn test_clock() -> (Arc<AtomicU64>, impl Fn() -> u64) {
        let time = Arc::new(AtomicU64::new(now_millis()));
        let t = Arc::clone(&time);
        (time, move || t.load(Ordering::Relaxed))
    }

    /// Wait for one `JobDispatchable` naming `id`, or give up.
    ///
    /// Bounded by a timeout rather than a fixed sleep so a passing run
    /// is fast and a failing one still terminates.
    async fn expect_dispatchable(store: &Store, id: &str) -> bool {
        let mut rx = store.subscribe();
        let wait = async {
            loop {
                match rx.recv().await {
                    Ok(StoreEvent::JobDispatchable { id: got, .. }) if got == id => return true,
                    Ok(_) => continue,
                    Err(_) => return false,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap_or(false)
    }

    /// The `while_in_flight` half: a slot freed by one worker has to
    /// rouse a stream blocked waiting for it. Nothing else will —
    /// `JobCompleted` only prunes the completing worker's own in-flight
    /// set.
    #[tokio::test]
    async fn a_freed_slot_announces_the_job_waiting_on_it() {
        let store = test_store();
        let now = now_millis();
        let (_time, clock) = test_clock();
        let (_tx, shutdown) = watch::channel(());

        store
            .create_budget("solo", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();

        let running = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!("a"))
                    .budget(BudgetBinding::new("solo")),
            )
            .await
            .unwrap()
            .into_job();
        let waiting = store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!("b"))
                    .budget(BudgetBinding::new("solo")),
            )
            .await
            .unwrap()
            .into_job();

        // Occupy the only slot.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, running.id);

        tokio::spawn(run(store.clone(), clock, DEFAULT_BATCH_SIZE, shutdown));

        // Subscribe before completing, so the announcement cannot be
        // missed between the two.
        let listening = tokio::spawn({
            let store = store.clone();
            let id = waiting.id.clone();
            async move { expect_dispatchable(&store, &id).await }
        });
        tokio::task::yield_now().await;

        store
            .mark_completed(now_millis(), &running.id)
            .await
            .unwrap();

        assert!(
            listening.await.unwrap(),
            "no announcement for the waiting job"
        );
    }

    /// The `time_based` half, and the case with no other moving parts:
    /// nothing is completed, nothing is enqueued, nothing disconnects.
    /// The job goes out purely because its bucket refilled, which is
    /// exactly what no other event covers.
    ///
    /// Runs on the real clock with a short window rather than a paused
    /// one with a long window. The task sleeps on a tokio timer derived
    /// from the injected clock, so a paused runtime needs both advanced
    /// together — and advancing an hour also expires the listener's own
    /// timeout, failing the test for the wrong reason. A 100ms window
    /// against a 5s timeout is a wide enough margin to be honest.
    #[tokio::test]
    async fn a_refilled_bucket_announces_without_any_other_activity() {
        let store = test_store();
        let now = now_millis();
        let (_tx, shutdown) = watch::channel(());

        store
            .create_budget(
                "brief",
                1,
                BudgetStrategy::TimeBased {
                    duration_ms: 100,
                    burst: None,
                },
                now,
            )
            .await
            .unwrap();

        // Spend the window's whole allowance.
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!("a"))
                    .budget(BudgetBinding::new("brief")),
            )
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let parked = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!("b"))
                    .budget(BudgetBinding::new("brief")),
            )
            .await
            .unwrap()
            .into_job();

        // Not takeable yet — the allowance is spent.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        let listening = tokio::spawn({
            let store = store.clone();
            let id = parked.id.clone();
            async move { expect_dispatchable(&store, &id).await }
        });
        tokio::task::yield_now().await;

        tokio::spawn(run(
            store.clone(),
            crate::time::now_millis,
            DEFAULT_BATCH_SIZE,
            shutdown,
        ));

        assert!(listening.await.unwrap(), "no announcement after the refill");
    }

    /// The task hears its own announcements on the same channel it
    /// publishes to. If an event that changes no capacity provoked a
    /// rescan, an announced job that no worker has claimed would be
    /// announced again immediately, and again — a spin that burns a core
    /// and floods the broadcast channel until someone takes the job.
    ///
    /// Counts rather than asserting exactly one, because a completion or
    /// a genuine refill may legitimately re-announce. The gap between
    /// "a few" and "as many as fit in 200ms" is several orders of
    /// magnitude, so the threshold does not need to be precise.
    #[tokio::test]
    async fn an_unclaimed_announcement_does_not_provoke_another() {
        let store = test_store();
        let now = now_millis();
        let (_time, clock) = test_clock();
        let (_tx, shutdown) = watch::channel(());

        store
            .create_budget("solo", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!("a"))
                    .budget(BudgetBinding::new("solo")),
            )
            .await
            .unwrap();

        // Affordable and parked, and deliberately never claimed.
        let mut rx = store.subscribe();
        tokio::spawn(run(store.clone(), clock, DEFAULT_BATCH_SIZE, shutdown));

        let mut announcements = 0usize;
        let counting = async {
            loop {
                match rx.recv().await {
                    Ok(StoreEvent::JobDispatchable { .. }) => announcements += 1,
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        announcements += n as usize;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), counting).await;

        assert!(
            announcements < 10,
            "waker spun: {announcements} announcements in 200ms"
        );
    }

    #[tokio::test]
    async fn shutdown_stops_the_waker() {
        let store = test_store();
        let (_time, clock) = test_clock();
        let (tx, shutdown) = watch::channel(());

        let handle = tokio::spawn(run(store, clock, DEFAULT_BATCH_SIZE, shutdown));
        drop(tx);

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("waker did not stop")
            .unwrap();
    }
}
