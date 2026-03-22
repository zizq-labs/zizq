// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! WebSocket event stream (currently) for Terminal UI clients.
//!
//! Each `GET /events` request upgrades to a WebSocket connection, sends an
//! initial `JobSnapshot`, then streams incremental `JobChanged` events driven
//! by `StoreEvent` subscriptions.
//!
//! Each event is wrapped in a consistently shaped `AdminEvent` which provides
//! some common server status info such as the total number of jobs in the
//! queue and the uptime of the server.
//!
//! Each tab in the frontend app use a snapshot-diff strategy: the server holds
//! a full ordered set of IDs and diffs a capped window (top N) against the
//! previous window to produce add/remove events. Only genuinely new IDs
//! trigger a `get_job` disk read.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;

use super::{
    AdminEvent, AdminJobSummary, AdminMessage, ClientMessage, JobChangeStatus, JobWindow, ListName,
    ServerStatus,
};
use crate::state::AppState;
use crate::store::{self, StoreEvent};
use crate::time::now_millis;

/// Default subscription window size.
const DEFAULT_LIMIT: usize = 200;

/// Maximum subscription window for free-tier connections.
const FREE_TIER_CAP: usize = 10;

/// Per-list subscription parameters.
#[derive(Clone)]
struct Subscription {
    offset: usize,
    limit: usize,
}

impl Default for Subscription {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: DEFAULT_LIMIT,
        }
    }
}

/// Per-connection state tracking the previous capped windows.
struct ConnectionState {
    /// Previous ready window: sorted `(priority, id)` keys.
    prev_ready: Vec<(u16, String)>,

    /// Full ordered set of in-flight jobs: `(dequeued_at, id)`.
    /// Maintained from `StoreEvent`s — append on `JobInFlight`,
    /// remove on `JobCompleted`/`JobFailed`.
    in_flight_ids: BTreeSet<(u64, String)>,

    /// Previous in-flight window entries from `in_flight_ids`.
    prev_in_flight: Vec<(u64, String)>,

    /// Previous scheduled window: sorted `(ready_at, id)` keys.
    prev_scheduled: Vec<(u64, String)>,

    /// Per-list subscription state.
    ready_sub: Subscription,
    in_flight_sub: Subscription,
    scheduled_sub: Subscription,
}

/// WebSocket endpoint that streams admin events to connected clients.
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

    // Channel for client messages from the read loop to the send task.
    let (client_tx, mut client_rx) = tokio::sync::mpsc::channel::<ClientMessage>(16);

    // Spawn a task that handles sending events to the WebSocket.
    let send_state = state.clone();
    let send_task = tokio::spawn(async move {
        let store = &send_state.store;

        // Query initial snapshot and build connection state.
        let mut conn = match send_initial_snapshot(&send_state, &mut sender).await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!(%e, "admin ws: initial snapshot failed");
                return;
            }
        };

        loop {
            tokio::select! {
                // Client subscribe messages.
                client_msg = client_rx.recv() => {
                    match client_msg {
                        Some(ClientMessage::Subscribe { list, offset, limit }) => {
                            handle_subscribe(
                                &send_state,
                                store,
                                &mut conn,
                                &mut sender,
                                list,
                                offset,
                                limit,
                            )
                            .await;
                        }
                        None => break,
                    }
                }
                // Admin-specific events (currently just heartbeats).
                event = admin_rx.recv() => {
                    match event {
                        Ok(AdminEvent::Heartbeat) => {
                            let msg = AdminMessage {
                                server: server_status(&send_state),
                                event: AdminEvent::Heartbeat,
                            };
                            let json = match serde_json::to_string(&msg) {
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
                                store,
                                store_event,
                                &mut conn,
                            ).await;

                            for event in events {
                                let msg = AdminMessage {
                                    server: server_status(&send_state),
                                    event,
                                };
                                let json = match serde_json::to_string(&msg) {
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
                            match send_initial_snapshot(&send_state, &mut sender).await {
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

    // Read loop — parse client messages and forward to send task.
    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                if client_tx.send(client_msg).await.is_err() {
                    break;
                }
            }
        }
    }

    send_task.abort();
}

/// Build a `ServerStatus` snapshot from the current app state.
fn server_status(state: &AppState) -> ServerStatus {
    let tier = match state.license.tier() {
        Some(tier) => tier.to_string(),
        None => "free".to_string(),
    };
    let subscription_limit = if can_show_live_queue(state) {
        None
    } else {
        Some(FREE_TIER_CAP)
    };
    ServerStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_ms: state.start_time.elapsed().as_millis() as u64,
        tier,
        total_ready: state.store.ready_count(),
        total_in_flight: state.store.in_flight_count(),
        total_scheduled: state.store.scheduled_count(),
        subscription_limit,
    }
}

/// Check whether the server license permits live queue detail.
fn can_show_live_queue(state: &AppState) -> bool {
    let now_ms = (state.clock)();
    state
        .license
        .require(now_ms, crate::license::Feature::TopLiveQueue)
        .is_ok()
}

/// Query each set: ready, in-flight, scheduled jobs, send a `JobSnapshot`, and
/// return initial `ConnectionState`.
///
/// When the license does not permit live queue detail, job lists are empty —
/// the header totals are still populated via `ServerStatus`.
async fn send_initial_snapshot(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Result<ConnectionState, String> {
    let live_queue = can_show_live_queue(state);
    let make_sub = || {
        if live_queue {
            Subscription::default()
        } else {
            Subscription {
                offset: 0,
                limit: FREE_TIER_CAP,
            }
        }
    };

    send_snapshot_with_subs(state, sender, make_sub(), make_sub(), make_sub()).await
}

async fn send_snapshot_with_subs(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    ready_sub: Subscription,
    in_flight_sub: Subscription,
    scheduled_sub: Subscription,
) -> Result<ConnectionState, String> {
    let store = &state.store;

    // In-flight data is always available (free tier).
    let mut in_flight_jobs = store
        .list_jobs(
            store::ListJobsOptions::new()
                .statuses([store::JobStatus::InFlight].into())
                .limit(usize::MAX),
        )
        .await
        .map(|page| page.jobs)
        .unwrap_or_else(|e| {
            tracing::error!(%e, "admin ws: failed to list in-flight jobs for snapshot");
            Vec::new()
        });

    in_flight_jobs.sort_by_key(|j| j.dequeued_at.unwrap_or(0));

    // Build full ordered in-flight ID set from all in-flight jobs.
    let in_flight_ids: BTreeSet<(u64, String)> = in_flight_jobs
        .iter()
        .map(|j| (j.dequeued_at.unwrap_or(0), j.id.clone()))
        .collect();

    // Windowed slices of jobs in each state for the snapshot.
    let capped_ready = store
        .list_ready_jobs(ready_sub.offset, ready_sub.limit)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, "admin ws: failed to list ready jobs for snapshot");
            Vec::new()
        });

    let capped_in_flight: Vec<store::Job> = in_flight_jobs
        .into_iter()
        .skip(in_flight_sub.offset)
        .take(in_flight_sub.limit)
        .collect();

    let capped_scheduled = store
        .list_scheduled_jobs(scheduled_sub.offset, scheduled_sub.limit)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(%e, "admin ws: failed to list scheduled jobs for snapshot");
            Vec::new()
        });

    // Collect keys for diffing logic.
    let prev_ready: Vec<(u16, String)> = capped_ready
        .iter()
        .map(|j| (j.priority, j.id.clone()))
        .collect();

    let prev_in_flight: Vec<(u64, String)> = capped_in_flight
        .iter()
        .map(|j| (j.dequeued_at.unwrap_or(0), j.id.clone()))
        .collect();

    let prev_scheduled: Vec<(u64, String)> = capped_scheduled
        .iter()
        .map(|j| (j.ready_at, j.id.clone()))
        .collect();

    // Prepare the windowed snapshot of jobs in each state to send to the client.
    let snapshot = AdminMessage {
        server: server_status(state),
        event: AdminEvent::JobSnapshot {
            ready: JobWindow {
                offset: ready_sub.offset,
                items: capped_ready
                    .into_iter()
                    .map(AdminJobSummary::from)
                    .collect(),
            },
            in_flight: JobWindow {
                offset: in_flight_sub.offset,
                items: capped_in_flight
                    .into_iter()
                    .map(AdminJobSummary::from)
                    .collect(),
            },
            scheduled: JobWindow {
                offset: scheduled_sub.offset,
                items: capped_scheduled
                    .into_iter()
                    .map(AdminJobSummary::from)
                    .collect(),
            },
        },
    };

    let json = serde_json::to_string(&snapshot).map_err(|e| e.to_string())?;

    sender
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| e.to_string())?;

    Ok(ConnectionState {
        prev_ready,
        in_flight_ids,
        prev_in_flight,
        prev_scheduled,
        ready_sub,
        in_flight_sub,
        scheduled_sub,
    })
}

/// Process a store event, returning zero or more `AdminEvent`s to send.
///
/// In-flight events are always emitted. Ready and scheduled diffs are
/// only emitted when `live_queue` is true (Pro license or higher).
async fn process_store_event(
    store: &store::Store,
    event: StoreEvent,
    conn: &mut ConnectionState,
) -> Vec<AdminEvent> {
    match event {
        StoreEvent::JobCreated { .. } => {
            let mut events = diff_ready(store, conn).await;
            events.extend(diff_scheduled(store, conn).await);
            events
        }
        StoreEvent::JobInFlight { id } => {
            let mut events = Vec::new();
            // Insert into in-flight set.
            if let Ok(Some(job)) = store.get_job(now_millis(), &id).await {
                conn.in_flight_ids
                    .insert((job.dequeued_at.unwrap_or(0), id.clone()));
            }
            // Diff in-flight window (new entry may appear).
            events.extend(diff_in_flight(store, conn).await);
            // Diff ready window (job left ready, backfill may be needed).
            events.extend(diff_ready(store, conn).await);
            events
        }
        StoreEvent::JobCompleted { id } => {
            let mut events = Vec::new();
            // Remove from in-flight set.
            conn.in_flight_ids.retain(|(_, wid)| wid != &id);
            // Emit semantic "completed" event.
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Completed,
                job: None,
            });
            // Diff in-flight window for backfill.
            events.extend(diff_in_flight(store, conn).await);
            events
        }
        StoreEvent::JobFailed { id, .. } => {
            let mut events = Vec::new();
            // Remove from in-flight set.
            conn.in_flight_ids.retain(|(_, wid)| wid != &id);
            // Emit semantic "dead" event.
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Dead,
                job: None,
            });
            // Diff in-flight window for backfill.
            events.extend(diff_in_flight(store, conn).await);
            events
        }
        StoreEvent::JobScheduled { .. } => diff_scheduled(store, conn).await,
        StoreEvent::IndexRebuilt => {
            let mut events = diff_ready(store, conn).await;
            events.extend(diff_scheduled(store, conn).await);
            events
        }
    }
}

/// Handle a subscribe message: update subscription, reset diff state, re-send snapshot.
async fn handle_subscribe(
    state: &AppState,
    store: &store::Store,
    conn: &mut ConnectionState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    list: ListName,
    offset: usize,
    limit: usize,
) {
    let sub = if can_show_live_queue(state) {
        Subscription { offset, limit }
    } else {
        Subscription {
            offset: 0,
            limit: FREE_TIER_CAP,
        }
    };
    match list {
        ListName::Ready => {
            conn.ready_sub = sub;
            conn.prev_ready.clear();
        }
        ListName::InFlight => {
            conn.in_flight_sub = sub;
            conn.prev_in_flight.clear();
        }
        ListName::Scheduled => {
            conn.scheduled_sub = sub;
            conn.prev_scheduled.clear();
        }
    }

    // Re-send a full snapshot at the new window.
    let ready_window = {
        let jobs = store
            .list_ready_jobs(conn.ready_sub.offset, conn.ready_sub.limit)
            .await
            .unwrap_or_default();
        let keys: Vec<(u16, String)> = jobs.iter().map(|j| (j.priority, j.id.clone())).collect();
        conn.prev_ready = keys;
        JobWindow {
            offset: conn.ready_sub.offset,
            items: jobs.into_iter().map(AdminJobSummary::from).collect(),
        }
    };

    let in_flight_items: Vec<store::Job> = {
        let mut jobs = Vec::new();
        for (_dequeued_at, id) in conn
            .in_flight_ids
            .iter()
            .skip(conn.in_flight_sub.offset)
            .take(conn.in_flight_sub.limit)
        {
            if let Ok(Some(job)) = store.get_job(now_millis(), id).await {
                jobs.push(job);
            }
        }
        jobs
    };
    conn.prev_in_flight = in_flight_items
        .iter()
        .map(|j| (j.dequeued_at.unwrap_or(0), j.id.clone()))
        .collect();
    let in_flight_window = JobWindow {
        offset: conn.in_flight_sub.offset,
        items: in_flight_items
            .into_iter()
            .map(AdminJobSummary::from)
            .collect(),
    };

    let scheduled_window = {
        let jobs = store
            .list_scheduled_jobs(conn.scheduled_sub.offset, conn.scheduled_sub.limit)
            .await
            .unwrap_or_default();
        let keys: Vec<(u64, String)> = jobs.iter().map(|j| (j.ready_at, j.id.clone())).collect();
        conn.prev_scheduled = keys;
        JobWindow {
            offset: conn.scheduled_sub.offset,
            items: jobs.into_iter().map(AdminJobSummary::from).collect(),
        }
    };

    let snapshot = AdminMessage {
        server: server_status(state),
        event: AdminEvent::JobSnapshot {
            ready: ready_window,
            in_flight: in_flight_window,
            scheduled: scheduled_window,
        },
    };

    if let Ok(json) = serde_json::to_string(&snapshot) {
        let _ = sender.send(Message::Text(json.into())).await;
    }
}

/// Diff the ready window: scan IDs from the ReadyIndex using subscription
/// offset/limit, compare against `prev_ready`, emit adds/removes.
async fn diff_ready(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let current = store
        .scan_ready_ids(conn.ready_sub.offset, conn.ready_sub.limit)
        .await;
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

/// Diff the in-flight window: take entries from `in_flight_ids` using
/// subscription offset/limit, compare against `prev_in_flight`, emit adds/removes.
async fn diff_in_flight(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let current: Vec<(u64, String)> = conn
        .in_flight_ids
        .iter()
        .skip(conn.in_flight_sub.offset)
        .take(conn.in_flight_sub.limit)
        .cloned()
        .collect();
    let (adds, removes) = diff_sorted(&current, &conn.prev_in_flight);
    let mut events = Vec::with_capacity(adds.len() + removes.len());

    // Emit removals.
    for (_dequeued_at, id) in removes {
        events.push(AdminEvent::JobChanged {
            id,
            status: JobChangeStatus::InFlightRemoved,
            job: None,
        });
    }

    // Emit additions — fetch metadata for new IDs.
    for (_dequeued_at, id) in adds {
        if let Ok(Some(job)) = store.get_job(now_millis(), &id).await {
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::InFlight,
                job: Some(AdminJobSummary::from(job)),
            });
        }
    }

    conn.prev_in_flight = current;
    events
}

/// Diff the scheduled window: scan IDs from the ScheduledIndex using
/// subscription offset/limit, compare against `prev_scheduled`, emit adds/removes.
async fn diff_scheduled(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let current = store
        .scan_scheduled_ids(conn.scheduled_sub.offset, conn.scheduled_sub.limit)
        .await;
    let (adds, removes) = diff_sorted(&current, &conn.prev_scheduled);
    let mut events = Vec::with_capacity(adds.len() + removes.len());

    // Emit removals first so the TUI frees space before inserts.
    for (_ready_at, id) in removes {
        events.push(AdminEvent::JobChanged {
            id,
            status: JobChangeStatus::ScheduledRemoved,
            job: None,
        });
    }

    // Emit additions — fetch metadata only for genuinely new IDs.
    for (_ready_at, id) in adds {
        if let Ok(Some(job)) = store.get_job(now_millis(), &id).await {
            events.push(AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary::from(job)),
            });
        }
    }

    conn.prev_scheduled = current;
    events
}

/// Diff two sorted slices, returning `(added, removed)` elements.
///
/// Both slices must be sorted by their natural `Ord`. Elements present
/// in `current` but not `previous` are "added"; elements in `previous`
/// but not `current` are "removed".
///
/// Used by both `diff_ready` and `diff_in_flight` to compare capped
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
    use std::time::Duration;

    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, watch};

    use crate::license::License;
    use crate::state::AppState;
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

    fn test_state_with_license(license: License) -> Arc<AppState> {
        let (admin_events, _) = broadcast::channel(64);
        let (_, shutdown_rx) = watch::channel(());
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        Arc::new(AppState {
            license,
            store,
            heartbeat_interval_ms: Duration::from_millis(500),
            global_in_flight_limit: 0,
            shutdown: shutdown_rx,
            clock: Arc::new(now_millis),
            admin_events,
            start_time: std::time::Instant::now(),
        })
    }

    fn test_state() -> Arc<AppState> {
        test_state_with_license(pro_license())
    }

    fn free_state() -> Arc<AppState> {
        test_state_with_license(License::Free)
    }

    fn pro_license() -> License {
        License::Licensed {
            licensee_id: "lic_test".into(),
            licensee_name: "Test Org".into(),
            tier: crate::license::Tier::Pro,
            expires_at: u64::MAX,
        }
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

    /// Assert that a message contains valid server status fields.
    fn assert_server_status(msg: &serde_json::Value) {
        let server = &msg["server"];
        assert!(server.is_object(), "message should contain 'server' object");
        assert!(
            server["version"].is_string(),
            "server.version should be a string"
        );
        assert!(
            server["uptime_ms"].is_u64(),
            "server.uptime_ms should be a u64"
        );
        assert!(server["tier"].is_string(), "server.tier should be a string");
        assert!(
            server["total_ready"].is_u64(),
            "server.total_ready should be a u64"
        );
        assert!(
            server["total_in_flight"].is_u64(),
            "server.total_in_flight should be a u64"
        );
        assert!(
            server["total_scheduled"].is_u64(),
            "server.total_scheduled should be a u64"
        );
    }

    #[tokio::test]
    async fn snapshot_contains_all_job_states_and_server_status() {
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

        // Take one so it becomes in-flight.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Enqueue a scheduled job (ready in the future).
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("type_c", "q", serde_json::json!(null)).ready_at(now + 60_000),
            )
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state).await;
        let snapshot = next_json(&mut rx).await;

        assert_eq!(snapshot["event"], "job_snapshot");
        assert_server_status(&snapshot);
        assert_eq!(snapshot["ready"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["in_flight"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["scheduled"]["items"].as_array().unwrap().len(), 1);
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
        assert_server_status(&msg);
        assert!(msg["job"].is_object());
        assert_eq!(msg["job"]["queue"], "emails");
    }

    #[tokio::test]
    async fn take_job_sends_in_flight_and_ready_removed() {
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
            .unwrap()
            .into_job();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume snapshot (has 1 ready, 0 in-flight).
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["ready"]["items"].as_array().unwrap().len(), 1);

        // Take the job — triggers JobInFlight.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // We should get events for the in-flight window add and ready
        // window removal (order may vary, collect both).
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let msg = next_json(&mut rx).await;
            assert_eq!(msg["event"], "job_changed");
            assert_eq!(msg["id"], job.id);
            assert_server_status(&msg);
            statuses.push(msg["status"].as_str().unwrap().to_string());
        }
        statuses.sort();
        assert_eq!(statuses, vec!["in_flight", "ready_removed"]);
    }

    #[tokio::test]
    async fn complete_job_sends_completed_and_in_flight_removed() {
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
            .unwrap()
            .into_job();
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume snapshot (0 ready, 1 in-flight).
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["in_flight"]["items"].as_array().unwrap().len(), 1);

        // Complete the job.
        state.store.mark_completed(now, &job.id).await.unwrap();

        // Should get "completed" + "in_flight_removed" for the same job.
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let msg = next_json(&mut rx).await;
            assert_eq!(msg["event"], "job_changed");
            assert_eq!(msg["id"], job.id);
            assert_server_status(&msg);
            statuses.push(msg["status"].as_str().unwrap().to_string());
        }
        statuses.sort();
        assert_eq!(statuses, vec!["completed", "in_flight_removed"]);
    }

    #[tokio::test]
    async fn server_status_reflects_actual_counts() {
        let state = test_state();
        let now = now_millis();

        // Enqueue 3 ready jobs.
        for typ in ["a", "b", "c"] {
            state
                .store
                .enqueue(now, EnqueueOptions::new(typ, "q", serde_json::json!(null)))
                .await
                .unwrap();
        }

        // Take one so it becomes in-flight (3 ready -> 2 ready + 1 in-flight).
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Enqueue 2 scheduled jobs.
        for typ in ["d", "e"] {
            state
                .store
                .enqueue(
                    now,
                    EnqueueOptions::new(typ, "q", serde_json::json!(null)).ready_at(now + 60_000),
                )
                .await
                .unwrap();
        }

        let (_addr, mut rx) = connect(state).await;
        let snapshot = next_json(&mut rx).await;

        let server = &snapshot["server"];
        assert_eq!(server["version"], env!("CARGO_PKG_VERSION"));
        assert!(server["uptime_ms"].as_u64().unwrap() < 5_000);
        assert_eq!(server["tier"], "pro");
        assert!(
            server["subscription_limit"].is_null(),
            "pro tier should not have subscription_limit"
        );
        assert_eq!(server["total_ready"], 2);
        assert_eq!(server["total_in_flight"], 1);
        assert_eq!(server["total_scheduled"], 2);
    }

    #[tokio::test]
    async fn enqueue_scheduled_job_sends_scheduled_event() {
        let state = test_state();
        let now = now_millis();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume initial empty snapshot.
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["event"], "job_snapshot");

        // Enqueue a scheduled job (ready in the future).
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "emails", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap();

        let msg = next_json(&mut rx).await;
        assert_eq!(msg["event"], "job_changed");
        assert_eq!(msg["status"], "scheduled");
        assert_server_status(&msg);
        assert!(msg["job"].is_object());
        assert_eq!(msg["job"]["queue"], "emails");
    }

    #[tokio::test]
    async fn promote_scheduled_sends_ready_and_scheduled_removed() {
        let state = test_state();
        let now = now_millis();

        // Enqueue a scheduled job before connecting.
        let job = state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)).ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume snapshot (0 ready, 1 scheduled).
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["ready"]["items"].as_array().unwrap().len(), 0);
        assert_eq!(snapshot["scheduled"]["items"].as_array().unwrap().len(), 1);

        // Promote the job (simulates the scheduler firing).
        state.store.promote_scheduled(&job).await.unwrap();

        // Should get "ready" + "scheduled_removed" (order may vary).
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let msg = next_json(&mut rx).await;
            assert_eq!(msg["event"], "job_changed");
            assert_server_status(&msg);
            statuses.push(msg["status"].as_str().unwrap().to_string());
        }
        statuses.sort();
        assert_eq!(statuses, vec!["ready", "scheduled_removed"]);
    }

    #[tokio::test]
    async fn free_tier_snapshot_has_capped_job_lists() {
        let state = free_state();
        let now = now_millis();

        // Enqueue jobs in all states.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("ready_type", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("ready_type2", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("sched_type", "q", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state).await;
        let snapshot = next_json(&mut rx).await;

        assert_eq!(snapshot["event"], "job_snapshot");
        assert_server_status(&snapshot);
        assert_eq!(snapshot["server"]["tier"], "free");
        assert_eq!(snapshot["server"]["subscription_limit"], FREE_TIER_CAP);
        // Totals are still populated.
        assert_eq!(snapshot["server"]["total_ready"], 1);
        assert_eq!(snapshot["server"]["total_in_flight"], 1);
        assert_eq!(snapshot["server"]["total_scheduled"], 1);
        // All lists are now populated (capped to FREE_TIER_CAP).
        assert_eq!(snapshot["in_flight"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["ready"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["scheduled"]["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn free_tier_streams_ready_events_within_cap() {
        let state = free_state();
        let now = now_millis();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume initial snapshot.
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["event"], "job_snapshot");

        // Enqueue a job — free tier now receives ready events within the cap.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let msg = next_json(&mut rx).await;
        assert_eq!(msg["event"], "job_changed");
        assert_eq!(msg["status"], "ready");
        assert_eq!(msg["server"]["tier"], "free");
    }

    #[tokio::test]
    async fn free_tier_streams_in_flight_events() {
        let state = free_state();
        let now = now_millis();

        // Enqueue a job before connecting.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("task", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume snapshot.
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["event"], "job_snapshot");

        // Take the job — should produce in_flight event even on free tier.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Collect events until we see the in_flight status.
        let msg = next_json(&mut rx).await;
        assert_eq!(msg["event"], "job_changed");
        assert_eq!(msg["status"], "in_flight");
        assert_eq!(msg["server"]["tier"], "free");
    }
}
