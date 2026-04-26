// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Admin API request handlers.
//!
//! Provides the router for the admin HTTP listener, which serves the
//! WebSocket event stream and backup endpoint.

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use super::{backup, events};
use crate::state::AppState;

/// Build the admin API router.
pub fn app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/events", get(events::event_stream))
        .route("/backup", post(backup::handle_backup))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::super::types::AdminEvent;
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

    #[tokio::test]
    async fn backup_returns_valid_tar_gz() {
        let (_, state) = test_state();

        // Enqueue a job so the backup has data.
        state
            .store
            .enqueue(
                now_millis(),
                crate::store::EnqueueOptions::new("test", "q", serde_json::json!("hello")),
            )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{addr}/backup"))
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/gzip"
        );

        let bytes = res.bytes().await.unwrap();
        assert!(bytes.len() > 0);

        // Decompress and verify the archive structure.
        let gz = flate2::read::GzDecoder::new(&bytes[..]);
        let mut archive = tar::Archive::new(gz);

        let entries: Vec<String> = archive
            .entries()
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().unwrap().to_string_lossy().to_string())
            .collect();

        // Should contain zizq-root/data/ and files underneath it.
        assert!(
            entries.iter().any(|p| p.starts_with("zizq-root/data")),
            "archive should contain zizq-root/data/, got: {entries:?}"
        );
    }

    #[tokio::test]
    async fn backup_produces_restorable_database() {
        let (_, state) = test_state();

        state
            .store
            .enqueue(
                now_millis(),
                crate::store::EnqueueOptions::new("test", "q", serde_json::json!("backup_me")),
            )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });

        let client = reqwest::Client::new();
        let bytes = client
            .post(format!("http://{addr}/backup"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // Extract to a temp directory.
        let restore_dir = tempfile::tempdir().unwrap();
        let gz = flate2::read::GzDecoder::new(&bytes[..]);
        let mut archive = tar::Archive::new(gz);

        let prefix = format!("{}/", crate::BACKUP_ARCHIVE_PREFIX);
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_path_buf();
            let Some(stripped) = path.strip_prefix(&prefix).ok() else {
                continue;
            };
            let dest = restore_dir.path().join(stripped);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            if entry.header().entry_type().is_dir() {
                std::fs::create_dir_all(&dest).unwrap();
            } else {
                let mut file = std::fs::File::create(&dest).unwrap();
                std::io::copy(&mut entry, &mut file).unwrap();
            }
        }

        // Open the restored database and verify the job is there.
        let restored = Store::open(restore_dir.path().join("data"), Default::default()).unwrap();

        let now = now_millis();
        let opts = crate::store::ListJobsOptions::new().limit(100).now(now);
        let page = restored.list_jobs(opts).await.unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!("backup_me")));
    }
}
