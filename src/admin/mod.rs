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

use crate::state::AppState;
use crate::store;

/// Server-wide status sent with every admin message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatus {
    pub version: String,
    pub uptime_ms: u64,
    pub tier: String,
    pub total_ready: usize,
    pub total_in_flight: usize,
    pub total_scheduled: usize,
    /// When present, the server caps each subscription list to this many items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_limit: Option<usize>,
}

/// Wrapper that pairs a `ServerStatus` snapshot with an `AdminEvent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMessage {
    pub server: ServerStatus,
    #[serde(flatten)]
    pub event: AdminEvent,
}

/// Events broadcast to admin dashboard clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum AdminEvent {
    /// Periodic heartbeat (server metadata is in the `ServerStatus` wrapper).
    Heartbeat,

    /// Snapshot of ready, in-flight, and scheduled job queues.
    JobSnapshot {
        ready: JobWindow,
        in_flight: JobWindow,
        scheduled: JobWindow,
    },

    /// Incremental change to a single job's status.
    JobChanged {
        id: String,
        status: JobChangeStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        job: Option<AdminJobSummary>,
    },
}

/// A windowed slice of jobs with offset metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobWindow {
    pub offset: usize,
    pub items: Vec<AdminJobSummary>,
}

/// Client-to-server message for controlling subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe {
        list: ListName,
        offset: usize,
        limit: usize,
    },
}

/// Which job list a subscription targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListName {
    Ready,
    InFlight,
    Scheduled,
}

/// The kind of change that occurred for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobChangeStatus {
    Ready,
    ReadyRemoved,
    InFlight,
    InFlightRemoved,
    Scheduled,
    ScheduledRemoved,
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
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, watch};

    use crate::license::License;
    use crate::state::AppState;
    use crate::store::Store;
    use crate::time::now_millis;

    fn test_state() -> (broadcast::Sender<AdminEvent>, Arc<AppState>) {
        let (admin_events, _) = broadcast::channel(64);
        let (_, shutdown_rx) = watch::channel(());
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        let state = Arc::new(AppState {
            license: std::sync::RwLock::new(License::Free),
            store,
            heartbeat_interval_ms: Duration::from_millis(500),
            global_in_flight_limit: 0,
            shutdown: shutdown_rx,
            clock: Arc::new(now_millis),
            admin_events: admin_events.clone(),
            start_time: std::time::Instant::now(),
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

        admin_events.send(AdminEvent::Heartbeat).unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
            .await
            .expect("timed out waiting for message")
            .expect("stream ended")
            .expect("read error");

        let text = msg.into_text().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(parsed["event"], "heartbeat");
        assert!(parsed["server"]["version"].is_string());
        assert!(parsed["server"]["uptime_ms"].is_number());
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

        for _ in 1..=3 {
            admin_events.send(AdminEvent::Heartbeat).unwrap();
        }

        for _ in 1..=3 {
            let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
                .await
                .expect("timed out")
                .expect("stream ended")
                .expect("read error");

            let parsed: serde_json::Value =
                serde_json::from_str(&msg.into_text().unwrap()).unwrap();
            assert_eq!(parsed["event"], "heartbeat");
            assert!(parsed["server"]["uptime_ms"].is_number());
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
        admin_events.send(AdminEvent::Heartbeat).unwrap();

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
        let _ = admin_events.send(AdminEvent::Heartbeat);
    }
}
