// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Event sources for the TUI.
//!
//! Merges terminal input and WebSocket network events into a single
//! channel that the main event loop reads from.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc;

use crate::api::admin::{
    AdminEvent, AdminJob, AdminMessage, ClientMessage, Direction, FindAnchor, JobChangeStatus,
    JobWindow, ListName, SearchQuery, ServerStatus,
};

/// Unified event type for the TUI event loop.
pub enum Event {
    /// User requested quit (e.g. pressed 'q').
    Quit,
    /// Switch to the next tab.
    NextTab,
    /// Switch to the previous tab.
    PrevTab,
    /// Scroll the table view left.
    ScrollLeft,
    /// Scroll the table view right.
    ScrollRight,
    /// Scroll the cursor up one row.
    ScrollUp,
    /// Scroll the cursor down one row.
    ScrollDown,
    /// Scroll the cursor up one page.
    PageUp,
    /// Scroll the cursor down one page.
    PageDown,
    /// Jump to the first row.
    GoToStart,
    /// Jump to the last row.
    GoToEnd,
    /// Toggle the detail panel.
    ToggleDetail,
    /// Toggle paused state — freeze/unfreeze the live job lists.
    TogglePause,
    /// Begin a delete-confirmation prompt for the cursor row.
    RequestDelete,
    /// Confirm the pending delete (sends the WS message + removes the row).
    ConfirmDelete,
    /// Dismiss the pending delete prompt without doing anything.
    CancelDelete,
    /// Suspend the process (Ctrl-Z).
    Suspend,
    /// User pressed `/` — enter search input mode.
    SearchOpen,
    /// A key event while the search prompt is open. Forwarded verbatim
    /// to the active field so `tui-input` can handle cursor motion,
    /// backspace/delete, home/end etc. Special keys (Enter, Esc, Tab,
    /// Ctrl-C) are diverted to their own events by the reader before
    /// they reach this variant.
    SearchKey(crossterm::event::KeyEvent),
    /// Tab pressed while search input is active — switch between the
    /// `Type` and `Queue` fields.
    SearchFieldSwitch,
    /// Enter pressed while search input is active — parse the query
    /// and dispatch a forward `Find`.
    SearchSubmit,
    /// Esc pressed while search input is active — dismiss the prompt
    /// without searching.
    SearchCancel,
    /// `n` — advance to the next match with the last-used query.
    SearchNext,
    /// `N` — step back to the previous match with the last-used query.
    SearchPrev,
    /// Esc outside of any modal prompt — clear a pinned search
    /// result if one is active.
    Unpin,
    /// Server connection attempt in progress.
    ServerConnecting,
    /// Server connection established.
    ServerConnected { url: String },
    /// Heartbeat received from server.
    ServerHeartbeat { server: ServerStatus },
    /// Snapshot of ready, in-flight, and scheduled job queues.
    ServerJobSnapshot {
        server: ServerStatus,
        ready: JobWindow,
        in_flight: JobWindow,
        scheduled: JobWindow,
    },
    /// Incremental change to a single job's status.
    ServerJobChanged {
        server: ServerStatus,
        id: String,
        status: JobChangeStatus,
        job: Option<AdminJob>,
    },
    /// Response to a `Find` request — search hit, backward search hit,
    /// or `Locate` result for a pinned job. `matched_position: None`
    /// means no match / anchor no longer exists.
    ServerFindResult {
        server: ServerStatus,
        list: crate::api::admin::ListName,
        matched_position: Option<usize>,
        window: JobWindow,
    },
    /// Server connection lost.
    ServerDisconnected,
}

impl Event {
    /// Returns `true` for events that should trigger an immediate render.
    pub fn is_user_input(&self) -> bool {
        matches!(
            self,
            Event::Quit
                | Event::NextTab
                | Event::PrevTab
                | Event::ScrollLeft
                | Event::ScrollRight
                | Event::ToggleDetail
                | Event::TogglePause
                | Event::RequestDelete
                | Event::ConfirmDelete
                | Event::CancelDelete
                | Event::SearchOpen
                | Event::SearchKey(_)
                | Event::SearchFieldSwitch
                | Event::SearchSubmit
                | Event::SearchCancel
                | Event::SearchNext
                | Event::SearchPrev
                | Event::Unpin
        )
    }

    /// Returns `true` for events that should trigger a deferred (batched) render.
    pub fn is_scroll(&self) -> bool {
        matches!(
            self,
            Event::ScrollUp
                | Event::ScrollDown
                | Event::PageUp
                | Event::PageDown
                | Event::GoToStart
                | Event::GoToEnd
        )
    }
}

/// Shared mode flag between the App and the terminal reader. When set,
/// the reader forwards raw character input as `SearchChar` events
/// instead of interpreting keys as command shortcuts.
pub type InputModeFlag = Arc<AtomicBool>;

/// Spawn a blocking thread that reads terminal input events and sends
/// them to the event channel. `search_mode` is toggled by the App when
/// entering / leaving the `/` search input prompt — the reader routes
/// keystrokes accordingly.
pub fn read_terminal_events(tx: mpsc::Sender<Event>, search_mode: InputModeFlag) {
    tokio::task::spawn_blocking(move || {
        use crossterm::event::{self, Event as CtEvent, KeyCode, KeyEventKind, KeyModifiers};

        loop {
            if let Ok(CtEvent::Key(key)) = event::read() {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                if search_mode.load(Ordering::Acquire) {
                    // Search input mode: peel off Enter/Esc/Tab/Ctrl-C
                    // as high-level prompt actions; forward everything
                    // else as a raw `SearchKey` so the active `Input`
                    // field can handle it (character insertion, cursor
                    // motion, backspace, delete, home, end, etc.).
                    match key.code {
                        KeyCode::Enter => {
                            let _ = tx.blocking_send(Event::SearchSubmit);
                        }
                        KeyCode::Esc => {
                            let _ = tx.blocking_send(Event::SearchCancel);
                        }
                        KeyCode::Tab | KeyCode::BackTab => {
                            let _ = tx.blocking_send(Event::SearchFieldSwitch);
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            // Ctrl-C aborts the whole app even from
                            // inside the prompt — matches the outer
                            // shortcut so users don't get stuck.
                            let _ = tx.blocking_send(Event::Quit);
                            break;
                        }
                        _ => {
                            let _ = tx.blocking_send(Event::SearchKey(key));
                        }
                    }
                    continue;
                }

                match (key.code, key.modifiers) {
                    (KeyCode::Char('q'), _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                        let _ = tx.blocking_send(Event::Quit);
                        break;
                    }
                    (KeyCode::Char('z'), KeyModifiers::CONTROL) => {
                        let _ = tx.blocking_send(Event::Suspend);
                    }
                    (KeyCode::Tab, _) => {
                        let _ = tx.blocking_send(Event::NextTab);
                    }
                    (KeyCode::BackTab, _) => {
                        let _ = tx.blocking_send(Event::PrevTab);
                    }
                    (KeyCode::Right, _) => {
                        let _ = tx.blocking_send(Event::ScrollRight);
                    }
                    (KeyCode::Left, _) => {
                        let _ = tx.blocking_send(Event::ScrollLeft);
                    }
                    (KeyCode::Up, _) | (KeyCode::Char('k'), _) => {
                        let _ = tx.blocking_send(Event::ScrollUp);
                    }
                    (KeyCode::Down, _) | (KeyCode::Char('j'), _) => {
                        let _ = tx.blocking_send(Event::ScrollDown);
                    }
                    (KeyCode::PageUp, _) => {
                        let _ = tx.blocking_send(Event::PageUp);
                    }
                    (KeyCode::PageDown, _) => {
                        let _ = tx.blocking_send(Event::PageDown);
                    }
                    (KeyCode::Char('i'), _) => {
                        let _ = tx.blocking_send(Event::ToggleDetail);
                    }
                    (KeyCode::Char('p'), _) => {
                        let _ = tx.blocking_send(Event::TogglePause);
                    }
                    (KeyCode::Char('g'), _) => {
                        let _ = tx.blocking_send(Event::GoToStart);
                    }
                    (KeyCode::Char('G'), _) => {
                        let _ = tx.blocking_send(Event::GoToEnd);
                    }
                    (KeyCode::Char('D'), _) => {
                        let _ = tx.blocking_send(Event::RequestDelete);
                    }
                    (KeyCode::Char('y'), _) => {
                        let _ = tx.blocking_send(Event::ConfirmDelete);
                    }
                    (KeyCode::Char('/'), _) => {
                        let _ = tx.blocking_send(Event::SearchOpen);
                    }
                    (KeyCode::Char('n'), _) => {
                        let _ = tx.blocking_send(Event::SearchNext);
                    }
                    (KeyCode::Char('N'), _) => {
                        let _ = tx.blocking_send(Event::SearchPrev);
                    }
                    (KeyCode::Esc, _) => {
                        // Multiplexed: cancel a delete prompt if one is
                        // pending, otherwise unpin the current search
                        // result. The App decides based on its state.
                        let _ = tx.blocking_send(Event::CancelDelete);
                        let _ = tx.blocking_send(Event::Unpin);
                    }
                    (KeyCode::Home, _) => {
                        let _ = tx.blocking_send(Event::GoToStart);
                    }
                    (KeyCode::End, _) => {
                        let _ = tx.blocking_send(Event::GoToEnd);
                    }
                    _ => {}
                }
            }
        }
    });
}

/// Spawn an async task that manages the WebSocket connection,
/// automatically reconnecting on failure.
pub fn manage_ws_connection(
    tx: mpsc::Sender<Event>,
    mut ws_out_rx: mpsc::Receiver<String>,
    base_url: String,
    http_client: reqwest::Client,
) {
    tokio::spawn(async move {
        let url = format!("{base_url}/events");

        loop {
            let _ = tx.send(Event::ServerConnecting).await;

            // Error is intentionally ignored — the UI shows "Connecting"
            // status until a connection succeeds, so the retry loop
            // handles failures gracefully without console output.
            let _ = connect_ws(&url, &tx, &mut ws_out_rx, &http_client).await;

            let _ = tx.send(Event::ServerDisconnected).await;

            // Wait before reconnecting.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });
}

/// Connect to the WebSocket endpoint and stream events until disconnected.
async fn connect_ws(
    url: &str,
    tx: &mpsc::Sender<Event>,
    ws_out_rx: &mut mpsc::Receiver<String>,
    http_client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use futures_util::{SinkExt, StreamExt};
    use reqwest_websocket::{Message, Upgrade};

    let response = http_client.get(url).upgrade().send().await?;
    let connected_url = response.url().origin().ascii_serialization();
    let mut websocket = response.into_websocket().await?;

    let _ = tx.send(Event::ServerConnected { url: connected_url }).await;

    loop {
        tokio::select! {
            msg = websocket.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(event) = parse_ws_message(&text) {
                            if tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Some(Ok(_)) => {} // ignore binary/ping/pong
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),
                }
            }
            outbound = ws_out_rx.recv() => {
                match outbound {
                    Some(msg) => {
                        websocket.send(Message::Text(msg)).await?;
                    }
                    None => return Ok(()),
                }
            }
        }
    }
}

/// Parse a WebSocket JSON text message into a TUI Event.
fn parse_ws_message(text: &str) -> Option<Event> {
    let msg: AdminMessage = serde_json::from_str(text).ok()?;

    match msg.event {
        AdminEvent::Heartbeat => Some(Event::ServerHeartbeat { server: msg.server }),
        AdminEvent::JobSnapshot {
            ready,
            in_flight,
            scheduled,
        } => Some(Event::ServerJobSnapshot {
            server: msg.server,
            ready,
            in_flight,
            scheduled,
        }),
        AdminEvent::JobChanged { id, status, job } => Some(Event::ServerJobChanged {
            server: msg.server,
            id,
            status,
            job,
        }),
        AdminEvent::FindResult {
            list,
            matched_position,
            window,
        } => Some(Event::ServerFindResult {
            server: msg.server,
            list,
            matched_position,
            window,
        }),
    }
}

/// Serialize a detail-level message for sending over WebSocket.
pub fn detail_level_message(detail: bool) -> String {
    serde_json::to_string(&ClientMessage::SetDetailLevel { detail })
        .expect("ClientMessage serialization cannot fail")
}

/// Serialize a delete-job message for sending over WebSocket.
pub fn delete_job_message(id: String) -> String {
    serde_json::to_string(&ClientMessage::DeleteJob { id })
        .expect("ClientMessage serialization cannot fail")
}

/// Serialize a subscribe message for sending over WebSocket.
pub fn subscribe_message(
    list: crate::api::admin::ListName,
    anchor: crate::api::admin::SubscribeAnchor,
    limit: usize,
) -> String {
    serde_json::to_string(&crate::api::admin::ClientMessage::Subscribe {
        list,
        anchor,
        limit,
    })
    .expect("ClientMessage serialization cannot fail")
}

/// Serialize a `Find` message for sending over WebSocket.
pub fn find_message(
    list: ListName,
    anchor: FindAnchor,
    direction: Direction,
    query: SearchQuery,
    limit: usize,
) -> String {
    serde_json::to_string(&ClientMessage::Find {
        list,
        anchor,
        direction,
        query,
        limit,
    })
    .expect("ClientMessage serialization cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_heartbeat() {
        let json = r#"{"server":{"version":"1.0.0","uptime_ms":5000,"tier":"pro","total_ready":0,"total_in_flight":0,"total_scheduled":0},"event":"heartbeat"}"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerHeartbeat { server } => {
                assert_eq!(server.version, "1.0.0");
                assert_eq!(server.uptime_ms, 5000);
                assert_eq!(server.tier, "pro");
                assert_eq!(server.total_ready, 0);
                assert_eq!(server.total_in_flight, 0);
                assert_eq!(server.total_scheduled, 0);
            }
            _ => panic!("expected ServerHeartbeat"),
        }
    }

    #[test]
    fn parses_job_snapshot() {
        let json = r#"{
            "server": {"version":"1.0.0","uptime_ms":5000,"tier":"pro","total_ready":1,"total_in_flight":1,"total_scheduled":1},
            "event": "job_snapshot",
            "ready": {"first_position":0,"total":1,"items":[{"id":"r1","queue":"q","job_type":"t","priority":0,"ready_at":1000,"attempts":0}]},
            "in_flight": {"first_position":0,"total":1,"items":[{"id":"w1","queue":"q","job_type":"t","priority":0,"ready_at":1000,"attempts":0,"dequeued_at":2000}]},
            "scheduled": {"first_position":0,"total":1,"items":[{"id":"s1","queue":"q","job_type":"t","priority":0,"ready_at":5000,"attempts":0}]}
        }"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobSnapshot {
                server,
                ready,
                in_flight,
                scheduled,
            } => {
                assert_eq!(server.version, "1.0.0");
                assert_eq!(server.total_ready, 1);
                assert_eq!(server.total_in_flight, 1);
                assert_eq!(server.total_scheduled, 1);
                assert_eq!(ready.items.len(), 1);
                assert_eq!(ready.items[0].id, "r1");
                assert_eq!(in_flight.items.len(), 1);
                assert_eq!(in_flight.items[0].id, "w1");
                assert_eq!(scheduled.items.len(), 1);
                assert_eq!(scheduled.items[0].id, "s1");
            }
            _ => panic!("expected ServerJobSnapshot"),
        }
    }

    #[test]
    fn parses_job_changed_scheduled() {
        let json = r#"{
            "server": {"version":"1.0.0","uptime_ms":5000,"tier":"pro","total_ready":0,"total_in_flight":0,"total_scheduled":1},
            "event": "job_changed",
            "id": "s1",
            "status": "scheduled",
            "job": {"id":"s1","queue":"q","job_type":"t","priority":0,"ready_at":5000,"attempts":0}
        }"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobChanged {
                server,
                id,
                status,
                job,
            } => {
                assert_eq!(server.version, "1.0.0");
                assert_eq!(server.total_scheduled, 1);
                assert_eq!(id, "s1");
                assert_eq!(status, JobChangeStatus::Scheduled);
                assert!(job.is_some());
            }
            _ => panic!("expected ServerJobChanged"),
        }
    }

    #[test]
    fn parses_job_changed_with_job() {
        let json = r#"{
            "server": {"version":"1.0.0","uptime_ms":5000,"tier":"pro","total_ready":1,"total_in_flight":0,"total_scheduled":0},
            "event": "job_changed",
            "id": "j1",
            "status": "ready",
            "job": {"id":"j1","queue":"q","job_type":"t","priority":5,"ready_at":1000,"attempts":0}
        }"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobChanged {
                server,
                id,
                status,
                job,
            } => {
                assert_eq!(server.version, "1.0.0");
                assert_eq!(server.total_ready, 1);
                assert_eq!(id, "j1");
                assert_eq!(status, JobChangeStatus::Ready);
                let job = job.unwrap();
                assert_eq!(job.priority, 5);
            }
            _ => panic!("expected ServerJobChanged"),
        }
    }

    #[test]
    fn parses_job_changed_without_job() {
        let json = r#"{"server":{"version":"1.0.0","uptime_ms":5000,"tier":"pro","total_ready":0,"total_in_flight":0,"total_scheduled":0},"event":"job_changed","id":"j1","status":"completed"}"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobChanged {
                server,
                id,
                status,
                job,
            } => {
                assert_eq!(server.version, "1.0.0");
                assert_eq!(id, "j1");
                assert_eq!(status, JobChangeStatus::Completed);
                assert!(job.is_none());
            }
            _ => panic!("expected ServerJobChanged"),
        }
    }

    #[test]
    fn returns_none_for_invalid_json() {
        assert!(parse_ws_message("not json").is_none());
    }

    #[test]
    fn returns_none_for_unknown_event_type() {
        let json = r#"{"server":{"version":"1.0.0","uptime_ms":0,"tier":"free","total_ready":0,"total_in_flight":0,"total_scheduled":0},"event":"unknown","data":123}"#;
        assert!(parse_ws_message(json).is_none());
    }

    #[test]
    fn parses_find_result_match() {
        let json = r#"{
            "server":{"version":"1.0.0","uptime_ms":0,"tier":"pro","total_ready":10,"total_in_flight":0,"total_scheduled":0},
            "event":"find_result",
            "list":"ready",
            "matched_position": 3,
            "window": {"first_position":1,"total":10,"items":[
                {"id":"a","queue":"q","job_type":"t","priority":0,"ready_at":0,"attempts":0},
                {"id":"b","queue":"q","job_type":"t","priority":0,"ready_at":0,"attempts":0},
                {"id":"c","queue":"q","job_type":"t","priority":0,"ready_at":0,"attempts":0}
            ]}
        }"#;
        let event = parse_ws_message(json).unwrap();
        match event {
            Event::ServerFindResult {
                list,
                matched_position,
                window,
                ..
            } => {
                assert!(matches!(list, ListName::Ready));
                assert_eq!(matched_position, Some(3));
                assert_eq!(window.first_position, 1);
                assert_eq!(window.items.len(), 3);
            }
            _ => panic!("expected ServerFindResult"),
        }
    }

    #[test]
    fn parses_find_result_none() {
        let json = r#"{
            "server":{"version":"1.0.0","uptime_ms":0,"tier":"pro","total_ready":0,"total_in_flight":0,"total_scheduled":0},
            "event":"find_result",
            "list":"in_flight",
            "window": {"first_position":0,"total":0,"items":[]}
        }"#;
        let event = parse_ws_message(json).unwrap();
        match event {
            Event::ServerFindResult {
                matched_position,
                window,
                ..
            } => {
                assert_eq!(matched_position, None);
                assert!(window.items.is_empty());
            }
            _ => panic!("expected ServerFindResult"),
        }
    }

    #[test]
    fn find_message_serializes_expected_shape() {
        let msg = find_message(
            ListName::Ready,
            FindAnchor::JobId {
                id: "job123".into(),
            },
            Direction::Forward,
            SearchQuery {
                queues: vec!["emails".into()],
                types: vec!["send".into()],
            },
            60,
        );
        // Full-shape check via parse-back-to-value.
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["type"], "find");
        assert_eq!(v["list"], "ready");
        assert_eq!(v["direction"], "forward");
        assert_eq!(v["limit"], 60);
        assert_eq!(v["anchor"]["kind"], "job_id");
        assert_eq!(v["anchor"]["id"], "job123");
        assert_eq!(v["query"]["queues"][0], "emails");
        assert_eq!(v["query"]["types"][0], "send");
    }
}
