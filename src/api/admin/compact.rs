// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Admin compaction endpoint.
//!
//! `POST /compact` forces a full LSM compaction of both keyspaces,
//! reclaiming tombstone space that leveled compaction would otherwise
//! leave behind on quiet databases. See `Store::compact_all`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::state::AppState;

/// Handle `POST /compact` — force a full compaction of all keyspaces.
///
/// Blocks until compaction finishes. May take many seconds on large
/// databases.
pub async fn handle_compact(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.store.compact_all().await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!(%e, "compact_all failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
