// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! WebSocket event stream for admin dashboard clients.
//!
//! Each `GET /events` request upgrades to a WebSocket connection and
//! subscribes to the admin broadcast channel. Multiple concurrent
//! clients each get their own independent subscription.

use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};

use crate::admin::AdminEvent;
use crate::http::AppState;

/// WebSocket endpoint that streams admin events to dashboard clients.
pub async fn event_stream(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.admin_events.subscribe();
    let (mut sender, mut receiver) = socket.split();

    // Spawn a task that forwards broadcast events to the WebSocket.
    let send_task = tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            let json = match event {
                AdminEvent::Heartbeat { version, uptime_ms } => {
                    format!(
                        r#"{{"event":"heartbeat","version":"{version}","uptime_ms":{uptime_ms}}}"#
                    )
                }
            };

            if sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // Read loop — detect client disconnect (future: handle viewport/filter messages).
    while let Some(Ok(_msg)) = receiver.next().await {}

    send_task.abort();
}
