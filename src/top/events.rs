// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Event sources for the TUI.
//!
//! Merges terminal input and WebSocket network events into a single
//! channel that the main event loop reads from.

use tokio::sync::mpsc;

use crate::admin::{
    AdminEvent, AdminJobSummary, AdminMessage, JobChangeStatus, JobWindow, ServerStatus,
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
    /// Server connection attempt in progress.
    ServerConnecting,
    /// Server connection established.
    ServerConnected,
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
        job: Option<AdminJobSummary>,
    },
    /// Server connection lost.
    ServerDisconnected,
}

impl Event {
    /// Returns `true` for events that should trigger an immediate render.
    pub fn is_user_input(&self) -> bool {
        matches!(
            self,
            Event::Quit | Event::NextTab | Event::PrevTab | Event::ScrollLeft | Event::ScrollRight
        )
    }

    /// Returns `true` for events that should trigger a deferred (batched) render.
    pub fn is_scroll(&self) -> bool {
        matches!(self, Event::ScrollUp | Event::ScrollDown)
    }
}

/// Spawn a blocking thread that reads terminal input events and sends
/// them to the event channel.
pub fn read_terminal_events(tx: mpsc::Sender<Event>) {
    tokio::task::spawn_blocking(move || {
        use crossterm::event::{self, Event as CtEvent, KeyCode, KeyEventKind};

        loop {
            if let Ok(CtEvent::Key(key)) = event::read() {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => {
                            let _ = tx.blocking_send(Event::Quit);
                            break;
                        }
                        KeyCode::Char('n') => {
                            let _ = tx.blocking_send(Event::NextTab);
                        }
                        KeyCode::Char('p') => {
                            let _ = tx.blocking_send(Event::PrevTab);
                        }
                        KeyCode::Right => {
                            let _ = tx.blocking_send(Event::ScrollRight);
                        }
                        KeyCode::Left => {
                            let _ = tx.blocking_send(Event::ScrollLeft);
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            let _ = tx.blocking_send(Event::ScrollUp);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            let _ = tx.blocking_send(Event::ScrollDown);
                        }
                        _ => {}
                    }
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
) {
    tokio::spawn(async move {
        let url = format!("{base_url}/events");

        loop {
            let _ = tx.send(Event::ServerConnecting).await;

            // Error is intentionally ignored — the UI shows "Connecting"
            // status until a connection succeeds, so the retry loop
            // handles failures gracefully without console output.
            let _ = connect_ws(&url, &tx, &mut ws_out_rx).await;

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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use futures_util::{SinkExt, StreamExt};
    use reqwest_websocket::{Message, RequestBuilderExt};

    let response = reqwest::Client::new().get(url).upgrade().send().await?;
    let mut websocket = response.into_websocket().await?;

    let _ = tx.send(Event::ServerConnected).await;

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
    }
}

/// Serialize a subscribe message for sending over WebSocket.
pub fn subscribe_message(list: crate::admin::ListName, offset: usize, limit: usize) -> String {
    serde_json::to_string(&crate::admin::ClientMessage::Subscribe {
        list,
        offset,
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
            "ready": {"offset":0,"items":[{"id":"r1","queue":"q","job_type":"t","priority":0,"ready_at":1000,"attempts":0}]},
            "in_flight": {"offset":0,"items":[{"id":"w1","queue":"q","job_type":"t","priority":0,"ready_at":1000,"attempts":0,"dequeued_at":2000}]},
            "scheduled": {"offset":0,"items":[{"id":"s1","queue":"q","job_type":"t","priority":0,"ready_at":5000,"attempts":0}]}
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
}
