// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Event sources for the TUI.
//!
//! Merges terminal input and WebSocket network events into a single
//! channel that the main event loop reads from.

use tokio::sync::mpsc;

use crate::admin::{AdminEvent, AdminJobSummary, JobChangeStatus};

/// Unified event type for the TUI event loop.
pub enum Event {
    /// User requested quit (e.g. pressed 'q').
    Quit,
    /// Server connection attempt in progress.
    ServerConnecting,
    /// Server connection established.
    ServerConnected,
    /// Heartbeat received from server.
    ServerHeartbeat { version: String, uptime_ms: u64 },
    /// Snapshot of ready and working job queues.
    ServerJobSnapshot {
        ready: Vec<AdminJobSummary>,
        working: Vec<AdminJobSummary>,
    },
    /// Incremental change to a single job's status.
    ServerJobChanged {
        id: String,
        status: JobChangeStatus,
        job: Option<AdminJobSummary>,
    },
    /// Server connection lost.
    ServerDisconnected,
}

/// Spawn a blocking thread that reads terminal input events and sends
/// them to the event channel.
pub fn read_terminal_events(tx: mpsc::Sender<Event>) {
    tokio::task::spawn_blocking(move || {
        use crossterm::event::{self, Event as CtEvent, KeyCode, KeyEventKind};

        loop {
            if let Ok(CtEvent::Key(key)) = event::read() {
                if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                    let _ = tx.blocking_send(Event::Quit);
                    break;
                }
            }
        }
    });
}

/// Spawn an async task that manages the WebSocket connection,
/// automatically reconnecting on failure.
pub fn manage_ws_connection(tx: mpsc::Sender<Event>, base_url: String) {
    tokio::spawn(async move {
        let url = format!("{base_url}/events");

        loop {
            let _ = tx.send(Event::ServerConnecting).await;

            // Error is intentionally ignored — the UI shows "Connecting"
            // status until a connection succeeds, so the retry loop
            // handles failures gracefully without console output.
            let _ = connect_ws(&url, &tx).await;

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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use futures_util::StreamExt;
    use reqwest_websocket::{Message, RequestBuilderExt};

    let response = reqwest::Client::new().get(url).upgrade().send().await?;
    let mut websocket = response.into_websocket().await?;

    let _ = tx.send(Event::ServerConnected).await;

    while let Some(msg) = websocket.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            if let Some(event) = parse_ws_message(&text) {
                if tx.send(event).await.is_err() {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

/// Parse a WebSocket JSON text message into a TUI Event.
fn parse_ws_message(text: &str) -> Option<Event> {
    let admin_event: AdminEvent = serde_json::from_str(text).ok()?;

    match admin_event {
        AdminEvent::Heartbeat { version, uptime_ms } => {
            Some(Event::ServerHeartbeat { version, uptime_ms })
        }
        AdminEvent::JobSnapshot { ready, working } => {
            Some(Event::ServerJobSnapshot { ready, working })
        }
        AdminEvent::JobChanged { id, status, job } => {
            Some(Event::ServerJobChanged { id, status, job })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_heartbeat() {
        let json = r#"{"event":"heartbeat","version":"1.0.0","uptime_ms":5000}"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerHeartbeat { version, uptime_ms } => {
                assert_eq!(version, "1.0.0");
                assert_eq!(uptime_ms, 5000);
            }
            _ => panic!("expected ServerHeartbeat"),
        }
    }

    #[test]
    fn parses_job_snapshot() {
        let json = r#"{
            "event": "job_snapshot",
            "ready": [{"id":"r1","queue":"q","job_type":"t","priority":0,"ready_at":1000,"attempts":0}],
            "working": [{"id":"w1","queue":"q","job_type":"t","priority":0,"ready_at":1000,"attempts":0,"dequeued_at":2000}]
        }"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobSnapshot { ready, working } => {
                assert_eq!(ready.len(), 1);
                assert_eq!(ready[0].id, "r1");
                assert_eq!(working.len(), 1);
                assert_eq!(working[0].id, "w1");
            }
            _ => panic!("expected ServerJobSnapshot"),
        }
    }

    #[test]
    fn parses_job_changed_with_job() {
        let json = r#"{
            "event": "job_changed",
            "id": "j1",
            "status": "ready",
            "job": {"id":"j1","queue":"q","job_type":"t","priority":5,"ready_at":1000,"attempts":0}
        }"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobChanged { id, status, job } => {
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
        let json = r#"{"event":"job_changed","id":"j1","status":"completed"}"#;
        let event = parse_ws_message(json).unwrap();

        match event {
            Event::ServerJobChanged { id, status, job } => {
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
        let json = r#"{"event":"unknown","data":123}"#;
        assert!(parse_ws_message(json).is_none());
    }
}
