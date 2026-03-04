// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! WebSocket event stream for admin dashboard clients.
//!
//! Each `GET /events` request upgrades to a WebSocket connection,
//! sends an initial `JobSnapshot`, then streams incremental
//! `JobChanged` events driven by `StoreEvent` subscriptions.
//!
//! Both Ready and Working panes use a snapshot-diff strategy: the
//! server holds a full ordered set of IDs and diffs a capped window
//! (top N) against the previous window to produce add/remove events.
//! Only genuinely new IDs trigger a `get_job` disk read.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;

use super::{AdminEvent, AdminJobSummary, JobChangeStatus};
use crate::http::AppState;
use crate::store::{self, StoreEvent};
use crate::time::now_millis;

/// Maximum number of ready jobs in the capped window.
const READY_CAP: usize = 200;

/// Maximum number of working jobs in the capped window.
const WORKING_CAP: usize = 200;

/// Per-connection state tracking the previous capped windows.
struct ConnectionState {
    /// Previous ready window: sorted `(priority, id)` keys.
    prev_ready: Vec<(u16, String)>,

    /// Full ordered set of working jobs: `(dequeued_at, id)`.
    /// Maintained from `StoreEvent`s — append on `JobWorking`,
    /// remove on `JobCompleted`/`JobFailed`.
    working_ids: BTreeSet<(u64, String)>,

    /// Previous working window: first `WORKING_CAP` entries
    /// from `working_ids`.
    prev_working: Vec<(u64, String)>,
}

/// WebSocket endpoint that streams admin events to dashboard clients.
pub async fn event_stream(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let mut admin_rx = state.admin_events.subscribe();
    let mut store_rx = state.store.subscribe();
    let (mut sender, mut receiver) = socket.split();

    // Spawn a task that handles sending events to the WebSocket.
    let store = state.store.clone();

    let send_task = tokio::spawn(async move {
        // Query initial snapshot and build connection state.
        let mut conn = match send_initial_snapshot(&store, &mut sender).await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!(%e, "admin ws: initial snapshot failed");
                return;
            }
        };

        loop {
            tokio::select! {
                // Admin-specific events (currently just heartbeats).
                event = admin_rx.recv() => {
                    match event {
                        Ok(AdminEvent::Heartbeat { .. }) => {
                            let json = match serde_json::to_string(&event.unwrap()) {
                                Ok(json) => json,
                                Err(e) => {
                                    tracing::error!(%e, "admin ws: failed to serialize heartbeat");
                                    continue;
                                }
                            };

                            if sender.send(Message::Text(json.into())).await.is_err() {
                                // Client disconnected.
                                break;
                            }
                        }
                        // Job events are tracked via store_rx, not admin_rx.
                        Ok(_) => {}
                        // Only heartbeats flow through admin_rx — missing
                        // some is harmless since the next one self-corrects.
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                // Store-specific events (job state changes).
                event = store_rx.recv() => {
                    match event {
                        Ok(store_event) => {
                            let events = process_store_event(
                                &store,
                                store_event,
                                &mut conn,
                            ).await;

                            for event in events {
                                let json = match serde_json::to_string(&event) {
                                    Ok(json) => json,
                                    Err(e) => {
                                        tracing::error!(%e, "admin ws: failed to serialize event");
                                        continue;
                                    }
                                };

                                if sender.send(Message::Text(json.into())).await.is_err() {
                                    // Client disconnected.
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Missed events — re-query full snapshot.
                            match send_initial_snapshot(&store, &mut sender).await {
                                Ok(new_conn) => conn = new_conn,
                                Err(e) => {
                                    tracing::error!(%e, "admin ws: resync snapshot failed");
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    });

    // Read loop — detect client disconnect.
    while let Some(Ok(_msg)) = receiver.next().await {}

    send_task.abort();
}

/// Query ready + working jobs, send a `JobSnapshot`, and return
/// initial `ConnectionState`.
async fn send_initial_snapshot(
    store: &store::Store,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Result<ConnectionState, String> {
    let mut working_jobs = store
        .list_jobs(
            store::ListJobsOptions::new()
                .statuses([store::JobStatus::Working].into())
                .limit(usize::MAX),
        )
        .await
        .map(|page| page.jobs)
        .unwrap_or_else(|e| {
            tracing::error!(%e, "admin ws: failed to list working jobs for snapshot");
            Vec::new()
        });

    working_jobs.sort_by_key(|j| j.dequeued_at.unwrap_or(0));

    // Build full ordered working ID set from all working jobs.
    let working_ids: BTreeSet<(u64, String)> = working_jobs
        .iter()
        .map(|j| (j.dequeued_at.unwrap_or(0), j.id.clone()))
        .collect();

    // Capped windows of jobs in each state for the snapshot.
    let capped_ready = store.list_ready_jobs(READY_CAP).await.unwrap_or_else(|e| {
        tracing::error!(%e, "admin ws: failed to list ready jobs for snapshot");
        Vec::new()
    });

    let capped_working: Vec<store::Job> = working_jobs.into_iter().take(WORKING_CAP).collect();

    // Collect keys for diffing logic.
    let prev_ready: Vec<(u16, String)> = capped_ready
        .iter()
        .map(|j| (j.priority, j.id.clone()))
        .collect();

    let prev_working: Vec<(u64, String)> = capped_working
        .iter()
        .map(|j| (j.dequeued_at.unwrap_or(0), j.id.clone()))
        .collect();

    // Prepare the windowed snapshot of jobs in each state to send to the client.
    let snapshot = AdminEvent::JobSnapshot {
        ready: capped_ready
            .into_iter()
            .map(AdminJobSummary::from)
            .collect(),
        working: capped_working
            .into_iter()
            .map(AdminJobSummary::from)
            .collect(),
    };

    let json = serde_json::to_string(&snapshot).map_err(|e| e.to_string())?;

    sender
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| e.to_string())?;

    Ok(ConnectionState {
        prev_ready,
        working_ids,
        prev_working,
    })
}

/// Process a store event, returning zero or more `AdminEvent`s to send.
async fn process_store_event(
    store: &store::Store,
    event: StoreEvent,
    conn: &mut ConnectionState,
) -> Vec<AdminEvent> {
    match event {
        StoreEvent::JobCreated { .. } => diff_ready(store, conn).await,
        StoreEvent::JobWorking { id } => {
            let mut events = Vec::new();
            // Insert into working set.
            if let Ok(Some(job)) = store.get_job(now_millis(), &id).await {
                conn.working_ids
                    .insert((job.dequeued_at.unwrap_or(0), id.clone()));
            }
            // Diff working window (new entry may appear).
            events.extend(diff_working(store, conn).await);
            // Diff ready window (job left ready, backfill may be needed).
            events.extend(diff_ready(store, conn).await);
            events
        }
        StoreEvent::JobCompleted { id } => {
            let mut events = Vec::new();
            // Remove from working set.
            conn.working_ids.retain(|(_, wid)| wid != &id);
            // Emit semantic "completed" event.
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Completed,
                job: None,
            });
            // Diff working window for backfill.
            events.extend(diff_working(store, conn).await);
            events
        }
        StoreEvent::JobFailed { id, .. } => {
            let mut events = Vec::new();
            // Remove from working set.
            conn.working_ids.retain(|(_, wid)| wid != &id);
            // Emit semantic "dead" event.
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Dead,
                job: None,
            });
            // Diff working window for backfill.
            events.extend(diff_working(store, conn).await);
            events
        }
        StoreEvent::JobScheduled { .. } => Vec::new(),
        StoreEvent::IndexRebuilt => diff_ready(store, conn).await,
    }
}

/// Diff the ready window: scan top READY_CAP IDs from the ReadyIndex,
/// compare against `prev_ready`, emit adds/removes.
async fn diff_ready(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let current = store.scan_ready_ids(READY_CAP).await;
    let (adds, removes) = diff_sorted(&current, &conn.prev_ready);
    let mut events = Vec::with_capacity(adds.len() + removes.len());

    // Emit removals first so the TUI frees space before inserts.
    for (_priority, id) in removes {
        events.push(AdminEvent::JobChanged {
            id,
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });
    }

    // Emit additions — fetch metadata only for genuinely new IDs.
    for (_priority, id) in adds {
        if let Ok(Some(job)) = store.get_job(now_millis(), &id).await {
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Ready,
                job: Some(AdminJobSummary::from(job)),
            });
        }
    }

    conn.prev_ready = current;
    events
}

/// Diff the working window: take first WORKING_CAP entries from
/// `working_ids`, compare against `prev_working`, emit adds/removes.
async fn diff_working(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let current: Vec<(u64, String)> = conn.working_ids.iter().take(WORKING_CAP).cloned().collect();
    let (adds, removes) = diff_sorted(&current, &conn.prev_working);
    let mut events = Vec::with_capacity(adds.len() + removes.len());

    // Emit removals.
    for (_dequeued_at, id) in removes {
        events.push(AdminEvent::JobChanged {
            id,
            status: JobChangeStatus::WorkingRemoved,
            job: None,
        });
    }

    // Emit additions — fetch metadata for new IDs.
    for (_dequeued_at, id) in adds {
        if let Ok(Some(job)) = store.get_job(now_millis(), &id).await {
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Working,
                job: Some(AdminJobSummary::from(job)),
            });
        }
    }

    conn.prev_working = current;
    events
}

/// Diff two sorted slices, returning `(added, removed)` elements.
///
/// Both slices must be sorted by their natural `Ord`. Elements present
/// in `current` but not `previous` are "added"; elements in `previous`
/// but not `current` are "removed".
///
/// Used by both `diff_ready` and `diff_working` to compare capped
/// windows against their previous state.
fn diff_sorted<K: Ord + Clone>(
    current: &[(K, String)],
    previous: &[(K, String)],
) -> (Vec<(K, String)>, Vec<(K, String)>) {
    let mut adds = Vec::new();
    let mut removes = Vec::new();

    let mut cur_idx = 0;
    let mut prev_idx = 0;

    while cur_idx < current.len() && prev_idx < previous.len() {
        match current[cur_idx].cmp(&previous[prev_idx]) {
            std::cmp::Ordering::Equal => {
                cur_idx += 1;
                prev_idx += 1;
            }
            std::cmp::Ordering::Less => {
                adds.push(current[cur_idx].clone());
                cur_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                removes.push(previous[prev_idx].clone());
                prev_idx += 1;
            }
        }
    }

    adds.extend_from_slice(&current[cur_idx..]);
    removes.extend_from_slice(&previous[prev_idx..]);

    (adds, removes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, watch};

    use crate::http::AppState;
    use crate::license::License;
    use crate::store::{EnqueueOptions, Store};
    use crate::time::now_millis;

    // ── diff_sorted unit tests ──────────────────────────────────────

    #[test]
    fn diff_sorted_identical() {
        let a = vec![(0u16, "a".into()), (1, "b".into())];
        let (adds, removes) = diff_sorted(&a, &a);
        assert!(adds.is_empty());
        assert!(removes.is_empty());
    }

    #[test]
    fn diff_sorted_all_new() {
        let current = vec![(0u16, "a".into()), (1, "b".into())];
        let previous: Vec<(u16, String)> = vec![];
        let (adds, removes) = diff_sorted(&current, &previous);
        assert_eq!(adds, current);
        assert!(removes.is_empty());
    }

    #[test]
    fn diff_sorted_all_removed() {
        let current: Vec<(u16, String)> = vec![];
        let previous = vec![(0u16, "a".into()), (1, "b".into())];
        let (adds, removes) = diff_sorted(&current, &previous);
        assert!(adds.is_empty());
        assert_eq!(removes, previous);
    }

    #[test]
    fn diff_sorted_mixed_adds_and_removes() {
        let previous = vec![(0u16, "a".into()), (1, "b".into()), (2, "c".into())];
        let current = vec![(0u16, "a".into()), (1, "x".into()), (2, "c".into())];
        let (adds, removes) = diff_sorted(&current, &previous);
        assert_eq!(adds, vec![(1, "x".into())]);
        assert_eq!(removes, vec![(1, "b".into())]);
    }

    #[test]
    fn diff_sorted_interleaved() {
        let previous = vec![(1u16, "a".into()), (3, "c".into()), (5, "e".into())];
        let current = vec![(2u16, "b".into()), (3, "c".into()), (4, "d".into())];
        let (adds, removes) = diff_sorted(&current, &previous);
        assert_eq!(adds, vec![(2, "b".into()), (4, "d".into())]);
        assert_eq!(removes, vec![(1, "a".into()), (5, "e".into())]);
    }

    #[test]
    fn diff_sorted_u64_keys() {
        let previous = vec![(100u64, "j1".into()), (200, "j2".into())];
        let current = vec![(200u64, "j2".into()), (300, "j3".into())];
        let (adds, removes) = diff_sorted(&current, &previous);
        assert_eq!(adds, vec![(300, "j3".into())]);
        assert_eq!(removes, vec![(100, "j1".into())]);
    }

    // ── Integration tests: snapshot + incremental events ────────────

    fn test_state() -> Arc<AppState> {
        let (admin_events, _) = broadcast::channel(64);
        let (_, shutdown_rx) = watch::channel(());
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        Arc::new(AppState {
            license: License::Free,
            store,
            heartbeat_interval_ms: Duration::from_millis(500),
            global_working_limit: 0,
            global_in_flight: AtomicU64::new(0),
            shutdown: shutdown_rx,
            clock: Arc::new(now_millis),
            admin_events,
        })
    }

    /// Connect a WebSocket client to the admin endpoint and return the
    /// receive half of the stream.
    async fn connect(
        state: Arc<AppState>,
    ) -> (
        std::net::SocketAddr,
        futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, super::super::app(state))
                .await
                .unwrap();
        });
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
            .await
            .unwrap();
        let (_, rx) = ws.split();
        (addr, rx)
    }

    /// Read the next WebSocket message as parsed JSON.
    async fn next_json(
        rx: &mut futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    ) -> serde_json::Value {
        let msg = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("timed out waiting for message")
            .expect("stream ended")
            .expect("read error");
        serde_json::from_str(&msg.into_text().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn snapshot_contains_ready_and_working_jobs() {
        let state = test_state();
        let now = now_millis();

        // Enqueue two ready jobs.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("type_a", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("type_b", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        // Take one so it becomes working.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state).await;
        let snapshot = next_json(&mut rx).await;

        assert_eq!(snapshot["event"], "job_snapshot");
        assert_eq!(snapshot["ready"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["working"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn enqueue_sends_ready_event() {
        let state = test_state();
        let now = now_millis();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume initial empty snapshot.
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["event"], "job_snapshot");

        // Enqueue a job — should produce a "ready" event.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "emails", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let msg = next_json(&mut rx).await;
        assert_eq!(msg["event"], "job_changed");
        assert_eq!(msg["status"], "ready");
        assert!(msg["job"].is_object());
        assert_eq!(msg["job"]["queue"], "emails");
    }

    #[tokio::test]
    async fn take_job_sends_working_and_ready_removed() {
        let state = test_state();
        let now = now_millis();

        // Enqueue a job before connecting so it appears in the snapshot.
        let job = state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume snapshot (has 1 ready, 0 working).
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["ready"].as_array().unwrap().len(), 1);

        // Take the job — triggers JobWorking.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // We should get events for the working window add and ready
        // window removal (order may vary, collect both).
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let msg = next_json(&mut rx).await;
            assert_eq!(msg["event"], "job_changed");
            assert_eq!(msg["id"], job.id);
            statuses.push(msg["status"].as_str().unwrap().to_string());
        }
        statuses.sort();
        assert_eq!(statuses, vec!["ready_removed", "working"]);
    }

    #[tokio::test]
    async fn complete_job_sends_completed_and_working_removed() {
        let state = test_state();
        let now = now_millis();

        // Enqueue and take a job.
        let job = state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume snapshot (0 ready, 1 working).
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["working"].as_array().unwrap().len(), 1);

        // Complete the job.
        state.store.mark_completed(now, &job.id).await.unwrap();

        // Should get "completed" + "working_removed" for the same job.
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let msg = next_json(&mut rx).await;
            assert_eq!(msg["event"], "job_changed");
            assert_eq!(msg["id"], job.id);
            statuses.push(msg["status"].as_str().unwrap().to_string());
        }
        statuses.sort();
        assert_eq!(statuses, vec!["completed", "working_removed"]);
    }
}
