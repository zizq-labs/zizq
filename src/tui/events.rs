// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Event sources for the TUI.
//!
//! Merges terminal input and WebSocket network events into a single
//! channel that the main event loop reads from.

use tokio::sync::mpsc;

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
        // Convert http(s):// to ws(s)://.
        let ws_url = if base_url.starts_with("https://") {
            format!("wss://{}/events", &base_url["https://".len()..])
        } else if base_url.starts_with("http://") {
            format!("ws://{}/events", &base_url["http://".len()..])
        } else {
            format!("{base_url}/events")
        };

        loop {
            let _ = tx.send(Event::ServerConnecting).await;

            match connect_ws(&ws_url, &tx).await {
                Ok(()) => {}
                Err(_) => {}
            }

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
    use tokio_tungstenite::tungstenite::Message;

    let (stream, _response) = tokio_tungstenite::connect_async(url).await?;

    let _ = tx.send(Event::ServerConnected).await;

    let (_sender, mut receiver) = stream.split();

    while let Some(msg) = receiver.next().await {
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

/// Parse a WebSocket text message into a TUI Event.
fn parse_ws_message(text: &str) -> Option<Event> {
    let parsed: serde_json::Value = serde_json::from_str(text).ok()?;
    let event_type = parsed["event"].as_str()?;

    match event_type {
        "heartbeat" => {
            let version = parsed["version"].as_str()?.to_string();
            let uptime_ms = parsed["uptime_ms"].as_u64()?;
            Some(Event::ServerHeartbeat { version, uptime_ms })
        }
        _ => None,
    }
}
