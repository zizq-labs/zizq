// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Admin backup endpoint.
//!
//! `POST /backup` creates a consistent snapshot of the database and streams
//! it back as a `.tar.gz` archive.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::state::AppState;

/// Handle `POST /backup` — create and stream a database backup.
pub async fn handle_backup(State(state): State<Arc<AppState>>) -> Response {
    let temp_dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(%e, "failed to create temp directory for backup");
            return (StatusCode::INTERNAL_SERVER_ERROR, "backup failed").into_response();
        }
    };

    let backup_path = temp_dir.path().join("data");
    if let Err(e) = state.store.backup_snapshot(backup_path.clone()).await {
        tracing::error!(%e, "backup snapshot failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "backup failed").into_response();
    }

    // Stream a tar.gz archive with structure: zizq-root/data/...
    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);

    tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter(tx);
        let gz = flate2::write::GzEncoder::new(writer, flate2::Compression::fast());
        let mut archive = tar::Builder::new(gz);

        // Append the backup data directory inside the archive.
        let archive_data_path = format!("{}/data", crate::BACKUP_ARCHIVE_PREFIX);
        if let Err(e) = archive.append_dir_all(&archive_data_path, &backup_path) {
            tracing::error!(%e, "failed to write tar archive");
            return;
        }

        if let Err(e) = archive.finish() {
            tracing::error!(%e, "failed to finish tar archive");
            return;
        }

        // Flush the gzip stream.
        if let Err(e) = archive.into_inner().and_then(|gz| gz.finish().map(|_| ())) {
            tracing::error!(%e, "failed to finish gzip stream");
        }

        // temp_dir is dropped here, cleaning up the temporary backup.
        drop(temp_dir);
    });

    let stream = ReceiverStream::new(rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/gzip")
        .header(
            CONTENT_DISPOSITION,
            "attachment; filename=\"backup.tar.gz\"",
        )
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Adapter that implements `std::io::Write` by sending chunks through an mpsc channel.
struct ChannelWriter(mpsc::Sender<Result<Vec<u8>, std::io::Error>>);

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .blocking_send(Ok(buf.to_vec()))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "receiver dropped"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
