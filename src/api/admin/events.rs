// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
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

use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;

use super::types::status_name;
use super::{
    AdminEvent, AdminJob, AdminJobError, AdminMessage, ClientMessage, Direction, Fallback,
    FindAnchor, JobChangeStatus, JobWindow, ListName, SearchQuery, ServerStatus, SubscribeAnchor,
};
use crate::state::AppState;
use crate::store::{self, StoreEvent};

/// Default subscription window size.
const DEFAULT_LIMIT: usize = 200;

/// How many of a job's failures a `JobDetail` carries, newest first, and
/// how many a `GetErrors` page holds unless the request asks otherwise.
const ERROR_PAGE_SIZE: usize = 10;

/// The most failures one `GetErrors` page can hold.
const MAX_ERROR_PAGE_SIZE: usize = 50;

/// Per-list subscription parameters. The dashboard addresses rows by
/// job id, not by numeric offset — see `store::WindowAnchor` docs.
#[derive(Clone)]
struct Subscription {
    anchor: store::WindowAnchor,
    fallback: store::WindowFallback,
    limit: usize,
}

impl Default for Subscription {
    fn default() -> Self {
        Self {
            anchor: store::WindowAnchor::Head,
            fallback: store::WindowFallback::Head,
            limit: DEFAULT_LIMIT,
        }
    }
}

/// Per-connection state tracking the set of ids currently in each
/// list's window. Position is irrelevant for change detection under
/// anchor-based addressing — we just diff id sets.
struct ConnectionState {
    /// IDs currently in the ready window (sorted by priority).
    prev_ready: Vec<String>,
    /// IDs currently in the in-flight window (sorted by dequeued_at).
    prev_in_flight: Vec<String>,
    /// IDs currently in the scheduled window (sorted by ready_at).
    prev_scheduled: Vec<String>,

    /// Per-list subscription state.
    ready_sub: Subscription,
    in_flight_sub: Subscription,
    scheduled_sub: Subscription,

    /// Whether to include full job details (payload, retry_limit, etc.).
    detail: bool,
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
                        Some(ClientMessage::Subscribe {
                            list,
                            anchor,
                            limit,
                        }) => {
                            handle_subscribe(
                                &send_state,
                                store,
                                &mut conn,
                                &mut sender,
                                list,
                                anchor,
                                limit,
                            )
                            .await;
                        }
                        Some(ClientMessage::SetDetailLevel { detail }) => {
                            conn.detail = detail;
                            // Resend a snapshot so the client gets the new detail level.
                            let event = build_snapshot(store, &mut conn).await;
                            let msg = AdminMessage {
                                server: server_status(&send_state),
                                event,
                            };
                            if let Ok(json) = serde_json::to_string(&msg) {
                                if sender.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Some(ClientMessage::DeleteJob { id }) => {
                            // Fire-and-forget — the deletion's JobDeleted
                            // store event will fan out through the normal
                            // event path and remove the row on every
                            // connected client (including this one).
                            if let Err(e) = send_state.store.delete_job(&id).await {
                                tracing::warn!(%e, %id, "admin ws: delete_job failed");
                            }
                        }
                        Some(ClientMessage::GetJob { id }) => {
                            let msg = AdminMessage {
                                server: server_status(&send_state),
                                event: handle_get_job(&send_state, id).await,
                            };
                            if let Ok(json) = serde_json::to_string(&msg) {
                                if sender.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Some(ClientMessage::GetErrors { id, before, limit }) => {
                            let limit = limit.unwrap_or(ERROR_PAGE_SIZE).clamp(1, MAX_ERROR_PAGE_SIZE);
                            let (errors, more) = list_errors(&send_state, &id, before, limit).await;
                            let msg = AdminMessage {
                                server: server_status(&send_state),
                                event: AdminEvent::JobErrors { id, errors, more },
                            };
                            if let Ok(json) = serde_json::to_string(&msg) {
                                if sender.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Some(ClientMessage::Find {
                            list,
                            anchor,
                            direction,
                            query,
                            limit,
                        }) => {
                            let event =
                                handle_find(&send_state, list, anchor, direction, query, limit)
                                    .await;
                            let msg = AdminMessage {
                                server: server_status(&send_state),
                                event,
                            };
                            if let Ok(json) = serde_json::to_string(&msg) {
                                if sender.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
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
                            // Missed events — re-query full snapshot, but
                            // preserve the client's existing subscriptions
                            // and detail flag so the resync doesn't silently
                            // demote them to defaults.
                            match resync_snapshot(&send_state, &mut sender, &conn).await {
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
    let tier = match state.license.read().unwrap().tier() {
        Some(tier) => tier.to_string(),
        None => "free".to_string(),
    };
    ServerStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_ms: state.start_time.elapsed().as_millis() as u64,
        tier,
        total_ready: state.store.ready_count(),
        total_in_flight: state.store.in_flight_count(),
        total_scheduled: state.store.scheduled_count(),
        subscription_limit: None,
    }
}

/// Send the initial `JobSnapshot` covering all three lists (each
/// anchored at `Head` by default) and build the connection state.
async fn send_initial_snapshot(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Result<ConnectionState, String> {
    send_snapshot_with_subs(
        state,
        sender,
        Subscription::default(),
        Subscription::default(),
        Subscription::default(),
        false,
    )
    .await
}

/// Re-send a full snapshot using the existing connection's subscription
/// windows and detail flag, used to recover from a `broadcast` lag
/// without losing the client's prior `SetDetailLevel` / `Subscribe`
/// choices.
async fn resync_snapshot(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    conn: &ConnectionState,
) -> Result<ConnectionState, String> {
    send_snapshot_with_subs(
        state,
        sender,
        conn.ready_sub.clone(),
        conn.in_flight_sub.clone(),
        conn.scheduled_sub.clone(),
        conn.detail,
    )
    .await
}

async fn send_snapshot_with_subs(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    ready_sub: Subscription,
    in_flight_sub: Subscription,
    scheduled_sub: Subscription,
    detail: bool,
) -> Result<ConnectionState, String> {
    // Seed a ConnectionState with empty diff baselines — build_snapshot
    // will populate them from the initial `list_window_*` responses.
    let mut conn = ConnectionState {
        prev_ready: Vec::new(),
        prev_in_flight: Vec::new(),
        prev_scheduled: Vec::new(),
        ready_sub,
        in_flight_sub,
        scheduled_sub,
        detail,
    };

    let event = build_snapshot(&state.store, &mut conn).await;

    let msg = AdminMessage {
        server: server_status(state),
        event,
    };
    let json = serde_json::to_string(&msg).map_err(|e| e.to_string())?;

    sender
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| e.to_string())?;

    Ok(conn)
}

/// Process a store event, returning zero or more `AdminEvent`s to send.
async fn process_store_event(
    store: &store::Store,
    event: StoreEvent,
    conn: &mut ConnectionState,
) -> Vec<AdminEvent> {
    match event {
        StoreEvent::JobDispatchable { .. } => {
            // Covers both fresh enqueues and `requeue` calls. A requeue
            // also dropped an entry from the server-side InFlightIndex,
            // so we re-diff all three windows. Fresh enqueues will see
            // an unchanged in-flight scan, so the diff is a cheap no-op.
            let mut events = diff_ready(store, conn).await;
            events.extend(diff_in_flight(store, conn).await);
            events.extend(diff_scheduled(store, conn).await);
            events
        }
        StoreEvent::JobInFlight { .. } => {
            // The store already inserted into InFlightIndex; re-diff
            // the in-flight window to pick up the new entry, and the
            // ready window since the job left it.
            let mut events = diff_in_flight(store, conn).await;
            events.extend(diff_ready(store, conn).await);
            events
        }
        StoreEvent::JobCompleted { id } => {
            // Emit semantic "completed" event, then re-diff in-flight
            // for the removal + any backfill. The server-side index
            // already removed this id.
            let mut events = vec![AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Completed,
                job: None,
            }];
            events.extend(diff_in_flight(store, conn).await);
            events
        }
        StoreEvent::JobFailed { id, .. } => {
            let mut events = vec![AdminEvent::JobChanged {
                id,
                status: JobChangeStatus::Dead,
                job: None,
            }];
            events.extend(diff_in_flight(store, conn).await);
            events
        }
        StoreEvent::JobScheduled { .. } => diff_scheduled(store, conn).await,
        StoreEvent::JobPatched { .. } => {
            // Patches can change any field (queue, priority, ready_at, status),
            // so ID-based diffing won't detect field-level changes within the
            // visible window. Send a fresh snapshot instead.
            vec![build_snapshot(store, conn).await]
        }
        StoreEvent::JobDeleted { .. } => {
            let mut events = diff_ready(store, conn).await;
            events.extend(diff_in_flight(store, conn).await);
            events.extend(diff_scheduled(store, conn).await);
            events
        }
        StoreEvent::IndexRebuilt => {
            let mut events = diff_ready(store, conn).await;
            events.extend(diff_scheduled(store, conn).await);
            events
        }
        StoreEvent::CronScheduleChanged => {
            // TODO: emit cron-specific admin events for `zizq top`.
            Vec::new()
        }
        StoreEvent::BudgetPolicyChanged => {
            // Nothing to diff: a policy change moves no job between the
            // ready, scheduled and in-flight windows this connection
            // tracks. Per-budget stats are their own admin surface and
            // do not exist yet.
            Vec::new()
        }
    }
}

/// Handle a subscribe message: update the per-list subscription and
/// re-emit a snapshot for all three lists.
///
/// Only the list named in the request has its anchor swapped — the
/// other two lists' subs are preserved and re-scanned so the client
/// receives a fully-consistent snapshot.
async fn handle_subscribe(
    state: &AppState,
    store: &store::Store,
    conn: &mut ConnectionState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    list: ListName,
    anchor: SubscribeAnchor,
    limit: usize,
) {
    let (store_anchor, store_fallback) = to_store_anchor(anchor);
    let sub = Subscription {
        anchor: store_anchor,
        fallback: store_fallback,
        limit,
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

    let event = build_snapshot(store, conn).await;
    let snapshot = AdminMessage {
        server: server_status(state),
        event,
    };
    if let Ok(json) = serde_json::to_string(&snapshot) {
        let _ = sender.send(Message::Text(json.into())).await;
    }
}

/// Look up one job in full, with its most recent failures, for a
/// `GetJob` request. A job that no longer exists comes back without
/// `job` or `status`; so does one that can't be read, which is logged.
async fn handle_get_job(state: &AppState, id: String) -> AdminEvent {
    let job = match state.store.get_job((state.clock)(), &id).await {
        Ok(job) => job,
        Err(e) => {
            tracing::warn!(%e, %id, "admin ws: get_job failed");
            None
        }
    };

    let (errors, more_errors) = if job.is_some() {
        list_errors(state, &id, None, ERROR_PAGE_SIZE).await
    } else {
        (Vec::new(), false)
    };

    let status = job
        .as_ref()
        .and_then(|job| store::JobStatus::try_from(job.status).ok())
        .map(|status| status_name(status).to_string());

    AdminEvent::JobDetail {
        id,
        job: job.map(|job| AdminJob::from_store(job, true)),
        status,
        errors,
        more_errors,
    }
}

/// A page of `id`'s failures, newest first, from the one before attempt
/// `before` (or the newest), and whether there are older ones. A failure to
/// read them is logged and treated as there being none.
async fn list_errors(
    state: &AppState,
    id: &str,
    before: Option<u32>,
    limit: usize,
) -> (Vec<AdminJobError>, bool) {
    let mut opts = store::ListErrorsOptions::new(id)
        .direction(store::ScanDirection::Desc)
        .limit(limit);
    if let Some(attempt) = before {
        opts = opts.from(attempt);
    }
    match state.store.list_errors(opts).await {
        Ok(page) => (
            page.errors.into_iter().map(AdminJobError::from).collect(),
            page.next.is_some(),
        ),
        Err(e) => {
            tracing::warn!(%e, %id, "admin ws: list_errors failed");
            (Vec::new(), false)
        }
    }
}

/// Translate the wire `SubscribeAnchor` into the store's
/// `(WindowAnchor, WindowFallback)` pair. `Head` and `Tail` don't have
/// a meaningful fallback — the value we pick is only consulted when
/// the anchor's job id is missing, and neither of these can miss.
fn to_store_anchor(anchor: SubscribeAnchor) -> (store::WindowAnchor, store::WindowFallback) {
    match anchor {
        SubscribeAnchor::Head => (store::WindowAnchor::Head, store::WindowFallback::Head),
        SubscribeAnchor::Tail => (store::WindowAnchor::Tail, store::WindowFallback::Tail),
        SubscribeAnchor::Offset { offset } => (
            store::WindowAnchor::Offset(offset),
            store::WindowFallback::Tail,
        ),
        SubscribeAnchor::Around { id, fallback } => {
            let f = match fallback {
                Fallback::Head => store::WindowFallback::Head,
                Fallback::Tail => store::WindowFallback::Tail,
                Fallback::Nowhere => store::WindowFallback::Nowhere,
            };
            (store::WindowAnchor::Around(id), f)
        }
    }
}

/// Handle a `Find` client message: translate the wire enums into the
/// store's shape, dispatch to the appropriate `Store::find_*`, and
/// build a `FindResult` event. See `store::find` module docs for the
/// walk semantics and race-free rationale.
async fn handle_find(
    state: &AppState,
    list: ListName,
    anchor: FindAnchor,
    direction: Direction,
    query: SearchQuery,
    limit: usize,
) -> AdminEvent {
    // Nonsense direction/anchor combos short-circuit to a null result:
    // `Start + Backward` and `End + Forward` have nothing to search.
    let nonsense = matches!(
        (&anchor, direction),
        (FindAnchor::Start, Direction::Backward) | (FindAnchor::End, Direction::Forward)
    );
    if nonsense {
        return AdminEvent::FindResult {
            list,
            matched_position: None,
            window: empty_window(),
        };
    }

    let anchor_id = match anchor {
        FindAnchor::JobId { id } => Some(id),
        FindAnchor::Start | FindAnchor::End => None,
    };
    let store_direction = match direction {
        Direction::Forward => store::FindDirection::Forward,
        Direction::Backward => store::FindDirection::Backward,
        Direction::Locate => store::FindDirection::Locate,
    };

    let outcome = match list {
        ListName::Ready => {
            state
                .store
                .find_ready(anchor_id, store_direction, query.queues, query.types, limit)
                .await
        }
        ListName::InFlight => {
            state
                .store
                .find_in_flight(anchor_id, store_direction, query.queues, query.types, limit)
                .await
        }
        ListName::Scheduled => {
            state
                .store
                .find_scheduled(anchor_id, store_direction, query.queues, query.types, limit)
                .await
        }
    };

    // Look up the list's total so the client can render its depth-bar
    // marker. Under the anchor-based Subscribe model the depth-bar is
    // decoupled from the window shape, so we consult the index length
    // here rather than piggybacking on `FindOutcome`.
    let total = match list {
        ListName::Ready => state.store.ready_count(),
        ListName::InFlight => state.store.in_flight_count(),
        ListName::Scheduled => state.store.scheduled_count(),
    };

    match outcome {
        Ok(Some(outcome)) => {
            let matched_id = outcome
                .window_items
                .get(
                    outcome
                        .matched_position
                        .saturating_sub(outcome.window_offset),
                )
                .map(|j| j.id.clone());
            AdminEvent::FindResult {
                list,
                matched_position: Some(outcome.matched_position),
                window: JobWindow {
                    items: outcome
                        .window_items
                        .into_iter()
                        .map(|j| AdminJob::from_store(j, false))
                        .collect(),
                    first_position: outcome.window_offset,
                    total,
                    resolved_anchor: matched_id,
                },
            }
        }
        Ok(None) => AdminEvent::FindResult {
            list,
            matched_position: None,
            window: empty_window(),
        },
        Err(e) => {
            tracing::error!(%e, "admin ws: find failed");
            AdminEvent::FindResult {
                list,
                matched_position: None,
                window: empty_window(),
            }
        }
    }
}

/// Build an empty `JobWindow` for "no match" / error paths.
fn empty_window() -> JobWindow {
    JobWindow {
        items: Vec::new(),
        first_position: 0,
        total: 0,
        resolved_anchor: None,
    }
}

/// Diff the ready window: re-fetch it via the subscription's anchor
/// and emit `JobChanged` events for ids that entered or left the
/// window since last diff.
async fn diff_ready(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let outcome = store
        .list_window_ready(
            conn.ready_sub.anchor.clone(),
            conn.ready_sub.fallback,
            conn.ready_sub.limit,
        )
        .await
        .unwrap_or_else(|_| store::WindowOutcome {
            items: Vec::new(),
            first_position: 0,
            total: 0,
            resolved_anchor: None,
        });
    diff_list_window(
        conn,
        outcome,
        ListSelector::Ready,
        JobChangeStatus::Ready,
        JobChangeStatus::ReadyRemoved,
    )
    .await
}

/// Diff the in-flight window.
async fn diff_in_flight(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let outcome = store
        .list_window_in_flight(
            conn.in_flight_sub.anchor.clone(),
            conn.in_flight_sub.fallback,
            conn.in_flight_sub.limit,
        )
        .await
        .unwrap_or_else(|_| store::WindowOutcome {
            items: Vec::new(),
            first_position: 0,
            total: 0,
            resolved_anchor: None,
        });
    diff_list_window(
        conn,
        outcome,
        ListSelector::InFlight,
        JobChangeStatus::InFlight,
        JobChangeStatus::InFlightRemoved,
    )
    .await
}

/// Diff the scheduled window.
async fn diff_scheduled(store: &store::Store, conn: &mut ConnectionState) -> Vec<AdminEvent> {
    let outcome = store
        .list_window_scheduled(
            conn.scheduled_sub.anchor.clone(),
            conn.scheduled_sub.fallback,
            conn.scheduled_sub.limit,
        )
        .await
        .unwrap_or_else(|_| store::WindowOutcome {
            items: Vec::new(),
            first_position: 0,
            total: 0,
            resolved_anchor: None,
        });
    diff_list_window(
        conn,
        outcome,
        ListSelector::Scheduled,
        JobChangeStatus::Scheduled,
        JobChangeStatus::ScheduledRemoved,
    )
    .await
}

/// Which of the three list buffers a diff/refresh operates over.
#[derive(Copy, Clone)]
enum ListSelector {
    Ready,
    InFlight,
    Scheduled,
}

/// Shared diff body. Given a freshly-scanned window and a selector
/// for which `prev_*` list to compare/update, emits `JobChanged`
/// events for the added and removed ids.
async fn diff_list_window(
    conn: &mut ConnectionState,
    outcome: store::WindowOutcome,
    selector: ListSelector,
    added_status: JobChangeStatus,
    removed_status: JobChangeStatus,
) -> Vec<AdminEvent> {
    let current_ids: Vec<String> = outcome.items.iter().map(|j| j.id.clone()).collect();
    let prev = match selector {
        ListSelector::Ready => &mut conn.prev_ready,
        ListSelector::InFlight => &mut conn.prev_in_flight,
        ListSelector::Scheduled => &mut conn.prev_scheduled,
    };
    let (added_ids, removed_ids) = diff_ids(&current_ids, prev);
    *prev = current_ids;

    let mut events = Vec::with_capacity(added_ids.len() + removed_ids.len());
    // Emit removals first so the TUI frees space before inserts.
    for id in removed_ids {
        events.push(AdminEvent::JobChanged {
            id,
            status: removed_status,
            job: None,
        });
    }
    for id in added_ids {
        // The `WindowOutcome` already contains the hydrated job, so no
        // extra `get_job` round-trip is needed.
        if let Some(job) = outcome.items.iter().find(|j| j.id == id) {
            events.push(AdminEvent::JobChanged {
                id: id.clone(),
                status: added_status,
                job: Some(AdminJob::from_store(job.clone(), conn.detail)),
            });
        }
    }
    events
}

/// Build a fresh `JobSnapshot` event and reset the connection's diff
/// baseline. Called after connect, resubscribe, and `SetDetailLevel`.
async fn build_snapshot(store: &store::Store, conn: &mut ConnectionState) -> AdminEvent {
    let ready = fetch_window(store, conn, ListSelector::Ready).await;
    let in_flight = fetch_window(store, conn, ListSelector::InFlight).await;
    let scheduled = fetch_window(store, conn, ListSelector::Scheduled).await;

    AdminEvent::JobSnapshot {
        ready,
        in_flight,
        scheduled,
    }
}

/// Fetch a `JobWindow` for one list and update the connection's
/// `prev_*` baseline to match. Shared by `build_snapshot` (all three
/// lists) and `handle_subscribe` (one list at a time via
/// `build_snapshot` too).
async fn fetch_window(
    store: &store::Store,
    conn: &mut ConnectionState,
    selector: ListSelector,
) -> JobWindow {
    let (outcome, prev) = match selector {
        ListSelector::Ready => (
            store
                .list_window_ready(
                    conn.ready_sub.anchor.clone(),
                    conn.ready_sub.fallback,
                    conn.ready_sub.limit,
                )
                .await,
            &mut conn.prev_ready,
        ),
        ListSelector::InFlight => (
            store
                .list_window_in_flight(
                    conn.in_flight_sub.anchor.clone(),
                    conn.in_flight_sub.fallback,
                    conn.in_flight_sub.limit,
                )
                .await,
            &mut conn.prev_in_flight,
        ),
        ListSelector::Scheduled => (
            store
                .list_window_scheduled(
                    conn.scheduled_sub.anchor.clone(),
                    conn.scheduled_sub.fallback,
                    conn.scheduled_sub.limit,
                )
                .await,
            &mut conn.prev_scheduled,
        ),
    };
    let outcome = outcome.unwrap_or_else(|_| store::WindowOutcome {
        items: Vec::new(),
        first_position: 0,
        total: 0,
        resolved_anchor: None,
    });
    *prev = outcome.items.iter().map(|j| j.id.clone()).collect();

    let admin_items: Vec<AdminJob> = outcome
        .items
        .into_iter()
        .map(|j| AdminJob::from_store(j, conn.detail))
        .collect();
    JobWindow {
        items: admin_items,
        first_position: outcome.first_position,
        total: outcome.total,
        resolved_anchor: outcome.resolved_anchor,
    }
}

/// Diff two id lists (each sorted per the list's ordering key),
/// returning `(added, removed)` ids.
fn diff_ids(current: &[String], previous: &[String]) -> (Vec<String>, Vec<String>) {
    let prev_set: std::collections::HashSet<&String> = previous.iter().collect();
    let curr_set: std::collections::HashSet<&String> = current.iter().collect();
    let added: Vec<String> = current
        .iter()
        .filter(|id| !prev_set.contains(id))
        .cloned()
        .collect();
    let removed: Vec<String> = previous
        .iter()
        .filter(|id| !curr_set.contains(id))
        .cloned()
        .collect();
    (added, removed)
}

/// Legacy sorted-slice diff (unused by the new anchor-based flow but
/// retained for the tests that touched it directly). Elements
/// present in `current` but not `previous` are "added"; elements in
/// `previous` but not `current` are "removed".
#[allow(dead_code)]
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
        let store = Store::open_temp(Default::default());
        Arc::new(AppState {
            license: std::sync::RwLock::new(license),
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
            axum::serve(listener, super::super::app(state, Default::default()))
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

    /// Regression: when a worker disconnects, the take handler calls
    /// `store.requeue(id)` for each in-flight job, which emits a
    /// `JobDispatchable` event. The admin handler used to only diff ready and
    /// scheduled on `JobDispatchable`, leaving stale in-flight rows in the
    /// `zizq top` tab even though `store.requeue` had removed them from
    /// the server-side `InFlightIndex`. The handler now always re-runs
    /// `diff_in_flight` on `JobDispatchable`, so the removal surfaces as an
    /// `in_flight_removed` event.
    #[tokio::test]
    async fn requeue_emits_in_flight_removed_and_ready() {
        let state = test_state();
        let now = now_millis();

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

        // Consume the initial snapshot (1 in-flight, 0 ready).
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["in_flight"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["ready"]["items"].as_array().unwrap().len(), 0);

        // Requeue — same path the take loop uses for disconnect cleanup.
        state.store.requeue(&job.id).await.unwrap();

        // Should observe both `in_flight_removed` and `ready` for the
        // same id (order may vary).
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let msg = next_json(&mut rx).await;
            assert_eq!(msg["event"], "job_changed");
            assert_eq!(msg["id"], job.id);
            statuses.push(msg["status"].as_str().unwrap().to_string());
        }
        statuses.sort();
        assert_eq!(statuses, vec!["in_flight_removed", "ready"]);
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
    async fn free_tier_snapshot_is_not_capped() {
        let state = free_state();
        let now = now_millis();

        for _ in 0..15 {
            state
                .store
                .enqueue(
                    now,
                    EnqueueOptions::new("task", "q", serde_json::json!(null)),
                )
                .await
                .unwrap();
        }

        let (_addr, mut rx) = connect(state).await;
        let snapshot = next_json(&mut rx).await;

        assert_eq!(snapshot["event"], "job_snapshot");
        assert_server_status(&snapshot);
        assert_eq!(snapshot["server"]["tier"], "free");
        assert!(snapshot["server"]["subscription_limit"].is_null());
        assert_eq!(snapshot["server"]["total_ready"], 15);
        assert_eq!(snapshot["ready"]["items"].as_array().unwrap().len(), 15);
    }

    #[tokio::test]
    async fn free_tier_streams_ready_events() {
        let state = free_state();
        let now = now_millis();

        let (_addr, mut rx) = connect(state.clone()).await;

        // Consume initial snapshot.
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["event"], "job_snapshot");

        // Enqueue a job.
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
    async fn delete_job_message_deletes_the_job_and_emits_event() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let state = test_state();
        let now = now_millis();

        // Enqueue a job so there's something to delete.
        let job = state
            .store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Connect a bidirectional WebSocket.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state_clone = state.clone();
        tokio::spawn(async move {
            axum::serve(listener, super::super::app(state_clone, Default::default()))
                .await
                .unwrap();
        });
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
            .await
            .unwrap();
        let (mut tx, mut rx) = ws.split();

        // Drain the initial snapshot.
        let snapshot = next_json(&mut rx).await;
        assert_eq!(snapshot["event"], "job_snapshot");

        // Send the delete request.
        let delete_msg = serde_json::json!({
            "type": "delete_job",
            "id": job.id,
        })
        .to_string();
        tx.send(Message::Text(delete_msg.into())).await.unwrap();

        // We should observe a `job_changed` event with the deletion.
        // Status is `completed` or `dead` (existing JobDeleted store path
        // doesn't use a distinct semantic status — the row simply leaves
        // every list).
        let mut got_removal = false;
        for _ in 0..5 {
            let msg = next_json(&mut rx).await;
            if msg["event"] == "job_changed"
                && (msg["status"] == "ready_removed"
                    || msg["status"] == "in_flight_removed"
                    || msg["status"] == "scheduled_removed")
                && msg["id"] == job.id
            {
                got_removal = true;
                break;
            }
        }
        assert!(
            got_removal,
            "expected a *_removed event for the deleted job"
        );

        // The job should be gone from the store.
        let gone = state.store.get_job(now_millis(), &job.id).await.unwrap();
        assert!(
            gone.is_none(),
            "job should have been deleted from the store"
        );
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

    /// Connect a WebSocket client that can send as well as receive, and
    /// consume the initial snapshot.
    async fn connect_both(
        state: Arc<AppState>,
    ) -> (
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tokio_tungstenite::tungstenite::Message,
        >,
        futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, super::super::app(state, Default::default()))
                .await
                .unwrap();
        });
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
            .await
            .unwrap();
        let (tx, mut rx) = ws.split();
        assert_eq!(next_json(&mut rx).await["event"], "job_snapshot");
        (tx, rx)
    }

    /// Send `request` and return the first `event` that comes back,
    /// skipping any other events that arrive first.
    async fn request_event(
        tx: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tokio_tungstenite::tungstenite::Message,
        >,
        rx: &mut futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
        request: serde_json::Value,
        event: &str,
    ) -> serde_json::Value {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        tx.send(Message::Text(request.to_string().into()))
            .await
            .unwrap();
        loop {
            let msg = next_json(rx).await;
            if msg["event"] == event {
                return msg;
            }
        }
    }

    fn attempts(errors: &serde_json::Value) -> Vec<u64> {
        errors
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["attempt"].as_u64().unwrap())
            .collect()
    }

    /// Take the next job and fail it with `message`, making it ready again
    /// at once, as the scheduler would when its retry falls due.
    async fn take_and_fail(state: &AppState, message: &str) -> String {
        let now = now_millis();
        let job = state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .expect("a job to take");
        let failed = state
            .store
            .record_failure(
                now,
                &job.id,
                crate::store::FailureOptions {
                    message: message.into(),
                    error_type: Some("ForkError".into()),
                    backtrace: None,
                    retry_at: Some(now),
                    kill: false,
                },
            )
            .await
            .unwrap()
            .expect("the failed job");
        state.store.promote_scheduled(&failed).await.unwrap();
        job.id
    }

    #[tokio::test]
    async fn get_job_returns_the_job_in_full_with_its_failures() {
        let state = free_state();
        state
            .store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("spawn", "q", serde_json::json!({ "cmd": "ls" })),
            )
            .await
            .unwrap();
        let id = take_and_fail(&state, "first failure").await;
        take_and_fail(&state, "second failure").await;

        let (mut tx, mut rx) = connect_both(state).await;
        let detail = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_job", "id": id }),
            "job_detail",
        )
        .await;

        assert_server_status(&detail);
        assert_eq!(detail["id"], id);
        assert_eq!(detail["status"], "ready");
        assert_eq!(detail["job"]["job_type"], "spawn");
        assert_eq!(detail["job"]["payload"], serde_json::json!({ "cmd": "ls" }));
        assert_eq!(detail["job"]["attempts"], 2);

        let errors = detail["errors"].as_array().unwrap();
        let attempts: Vec<_> = errors
            .iter()
            .map(|e| e["attempt"].as_u64().unwrap())
            .collect();
        assert_eq!(attempts, vec![2, 1], "newest first");
        assert_eq!(errors[0]["message"], "second failure");
        assert_eq!(errors[0]["error_type"], "ForkError");
        assert!(errors[0]["failed_at"].as_u64().unwrap() > 0);
        assert_eq!(detail["more_errors"], false);
    }

    #[tokio::test]
    async fn get_job_for_a_missing_job_has_no_job_or_status() {
        let (mut tx, mut rx) = connect_both(free_state()).await;
        let detail = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_job", "id": "no-such-job" }),
            "job_detail",
        )
        .await;

        assert_eq!(detail["id"], "no-such-job");
        assert!(detail.get("job").is_none());
        assert!(detail.get("status").is_none());
        assert_eq!(detail["errors"], serde_json::json!([]));
        assert_eq!(detail["more_errors"], false);
    }

    #[tokio::test]
    async fn get_job_says_when_there_are_more_failures_than_it_sent() {
        let state = free_state();
        let id = job_failed_times(&state, ERROR_PAGE_SIZE + 1).await;

        let (mut tx, mut rx) = connect_both(state).await;
        let detail = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_job", "id": id }),
            "job_detail",
        )
        .await;

        let errors = detail["errors"].as_array().unwrap();
        assert_eq!(errors.len(), ERROR_PAGE_SIZE);
        assert_eq!(errors[0]["attempt"], ERROR_PAGE_SIZE + 1);
        assert_eq!(detail["more_errors"], true);
    }

    #[tokio::test]
    async fn get_job_finds_a_completed_job_while_it_is_retained() {
        let state = free_state();
        state
            .store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("spawn", "q", serde_json::json!(null)).retention(
                    crate::store::RetentionConfig {
                        completed_ms: Some(60_000),
                        dead_ms: None,
                    },
                ),
            )
            .await
            .unwrap();
        let now = now_millis();
        let job = state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        state.store.mark_completed(now, &job.id).await.unwrap();

        let (mut tx, mut rx) = connect_both(state).await;
        let detail = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_job", "id": job.id }),
            "job_detail",
        )
        .await;

        assert_eq!(detail["status"], "completed");
        assert_eq!(detail["job"]["id"], job.id);
    }

    /// Enqueue a job that can fail `n` times without dying, and fail it
    /// that many times.
    async fn job_failed_times(state: &AppState, n: usize) -> String {
        state
            .store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("spawn", "q", serde_json::json!(null)).retry_limit(100),
            )
            .await
            .unwrap();
        let mut id = String::new();
        for attempt in 1..=n {
            id = take_and_fail(state, &format!("failure {attempt}")).await;
        }
        id
    }

    #[tokio::test]
    async fn get_errors_pages_back_beyond_the_job_detail() {
        let state = free_state();
        let id = job_failed_times(&state, 13).await;

        let (mut tx, mut rx) = connect_both(state).await;
        let detail = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_job", "id": id }),
            "job_detail",
        )
        .await;
        assert_eq!(
            attempts(&detail["errors"]),
            (4..=13).rev().collect::<Vec<_>>()
        );
        assert_eq!(detail["more_errors"], true);

        let page = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_errors", "id": id, "before": 4 }),
            "job_errors",
        )
        .await;
        assert_server_status(&page);
        assert_eq!(page["id"], id);
        assert_eq!(attempts(&page["errors"]), vec![3, 2, 1]);
        assert_eq!(page["errors"][0]["message"], "failure 3");
        assert_eq!(page["more"], false);
    }

    #[tokio::test]
    async fn get_errors_pages_are_ten_long_unless_asked_and_at_most_fifty() {
        let state = free_state();
        let id = job_failed_times(&state, 60).await;

        let (mut tx, mut rx) = connect_both(state).await;
        let default = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_errors", "id": id }),
            "job_errors",
        )
        .await;
        assert_eq!(
            attempts(&default["errors"]),
            (51..=60).rev().collect::<Vec<_>>()
        );
        assert_eq!(default["more"], true);

        let capped = request_event(
            &mut tx,
            &mut rx,
            serde_json::json!({ "type": "get_errors", "id": id, "limit": 1000 }),
            "job_errors",
        )
        .await;
        assert_eq!(
            capped["errors"].as_array().unwrap().len(),
            MAX_ERROR_PAGE_SIZE
        );
        assert_eq!(capped["more"], true);
    }
}
