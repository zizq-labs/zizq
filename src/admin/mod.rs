// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Admin API for the Zizq dashboard.
//!
//! Provides a separate HTTP listener for monitoring and management.
//! Clients connect via WebSocket to `/events` for live updates.
//! The admin API is available on all tiers — only the TUI client
//! that connects to it requires a license.

pub mod events;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use serde::{Deserialize, Serialize};

use crate::http::AppState;
use crate::store;

/// Events broadcast to admin dashboard clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum AdminEvent {
    /// Periodic heartbeat with server metadata.
    Heartbeat { version: String, uptime_ms: u64 },

    /// Snapshot of ready and working job queues.
    JobSnapshot {
        ready: Vec<AdminJobSummary>,
        working: Vec<AdminJobSummary>,
    },

    /// Incremental change to a single job's status.
    JobChanged {
        id: String,
        status: JobChangeStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        job: Option<AdminJobSummary>,
    },
}

/// The kind of change that occurred for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobChangeStatus {
    Ready,
    ReadyRemoved,
    Working,
    WorkingRemoved,
    Completed,
    Dead,
}

/// Summary of a job for the admin dashboard (no payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminJobSummary {
    pub id: String,
    pub queue: String,
    pub job_type: String,
    pub priority: u16,
    pub ready_at: u64,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dequeued_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u64>,
}

impl From<store::Job> for AdminJobSummary {
    fn from(job: store::Job) -> Self {
        Self {
            id: job.id,
            queue: job.queue,
            job_type: job.job_type,
            priority: job.priority,
            ready_at: job.ready_at,
            attempts: job.attempts,
            dequeued_at: job.dequeued_at,
            failed_at: job.failed_at,
        }
    }
}

/// Build the admin API router.
pub fn app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/events", get(events::event_stream))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, watch};

    use crate::http::AppState;
    use crate::license::License;
    use crate::store::Store;
    use crate::time::now_millis;

    fn test_state() -> (broadcast::Sender<AdminEvent>, Arc<AppState>) {
        let (admin_events, _) = broadcast::channel(64);
        let (_, shutdown_rx) = watch::channel(());
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        let state = Arc::new(AppState {
            license: License::Free,
            store,
            heartbeat_interval_ms: Duration::from_millis(500),
            global_working_limit: 0,
            global_in_flight: AtomicU64::new(0),
            shutdown: shutdown_rx,
            clock: Arc::new(now_millis),
            admin_events: admin_events.clone(),
        });
        (admin_events, state)
    }

    /// Consume the initial job_snapshot that every connection receives.
    async fn consume_initial_snapshot(
        ws: &mut futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    ) {
        let msg = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timed out waiting for initial snapshot")
            .expect("stream ended")
            .expect("read error");
        let parsed: serde_json::Value = serde_json::from_str(&msg.into_text().unwrap()).unwrap();
        assert_eq!(parsed["event"], "job_snapshot");
    }

    #[tokio::test]
    async fn websocket_connects_to_events_endpoint() {
        let (admin_events, state) = test_state();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });

        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
            .await
            .unwrap();
        let (_, mut ws_rx) = ws.split();

        // Consume the initial job_snapshot.
        consume_initial_snapshot(&mut ws_rx).await;

        admin_events
            .send(AdminEvent::Heartbeat {
                version: "1.0.0".to_string(),
                uptime_ms: 5000,
            })
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
            .await
            .expect("timed out waiting for message")
            .expect("stream ended")
            .expect("read error");

        let text = msg.into_text().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(parsed["event"], "heartbeat");
        assert_eq!(parsed["version"], "1.0.0");
        assert_eq!(parsed["uptime_ms"], 5000);
    }

    #[tokio::test]
    async fn websocket_receives_multiple_events() {
        let (admin_events, state) = test_state();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });

        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
            .await
            .unwrap();
        let (_, mut ws_rx) = ws.split();

        // Consume the initial job_snapshot.
        consume_initial_snapshot(&mut ws_rx).await;

        for i in 1..=3 {
            admin_events
                .send(AdminEvent::Heartbeat {
                    version: "1.0.0".to_string(),
                    uptime_ms: i * 1000,
                })
                .unwrap();
        }

        for i in 1..=3 {
            let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
                .await
                .expect("timed out")
                .expect("stream ended")
                .expect("read error");

            let parsed: serde_json::Value =
                serde_json::from_str(&msg.into_text().unwrap()).unwrap();
            assert_eq!(parsed["uptime_ms"], i * 1000);
        }
    }

    #[tokio::test]
    async fn websocket_handles_client_disconnect() {
        let (admin_events, state) = test_state();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });

        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
            .await
            .unwrap();
        let (mut ws_tx, mut ws_rx) = ws.split();

        // Consume the initial job_snapshot.
        consume_initial_snapshot(&mut ws_rx).await;

        // Verify connection works.
        admin_events
            .send(AdminEvent::Heartbeat {
                version: "1.0.0".to_string(),
                uptime_ms: 1000,
            })
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(msg.into_text().unwrap().contains("heartbeat"));

        // Close the connection gracefully.
        ws_tx.close().await.unwrap();

        // Server should not panic — sending into the closed connection
        // just silently fails.
        let _ = admin_events.send(AdminEvent::Heartbeat {
            version: "1.0.0".to_string(),
            uptime_ms: 2000,
        });
    }
}
