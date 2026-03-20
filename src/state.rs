// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Shared server state.
//!
//! [`AppState`] holds the runtime dependencies shared across the HTTP API,
//! admin WebSocket handlers, and background tasks.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::admin::AdminEvent;
use crate::license::License;
use crate::store::Store;

/// Default interval between heartbeat frames on idle take connections (milliseconds).
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 3_000;

/// Default maximum number of in-flight jobs across all connections.
pub const DEFAULT_GLOBAL_IN_FLIGHT_LIMIT: u64 = 1024;

/// Shared server state, passed to all request handlers.
pub struct AppState {
    /// Validated license determining which paid features are available.
    pub license: License,

    /// Persistent store for job queue operations.
    pub store: Store,

    /// Interval between heartbeat frames on idle take connections.
    pub heartbeat_interval_ms: Duration,

    /// Maximum number of in-flight jobs across all connections.
    /// 0 means no limit.
    pub global_in_flight_limit: u64,

    /// Cloneable receiver for graceful shutdown signaling.
    pub shutdown: watch::Receiver<()>,

    /// Clock function for the current time in milliseconds since Unix epoch.
    ///
    /// In production, this is `time::now_millis`; tests can inject a
    /// custom clock for deterministic timestamps.
    pub clock: Arc<dyn Fn() -> u64 + Send + Sync>,

    /// Broadcast channel for admin dashboard events (heartbeats, etc.).
    pub admin_events: broadcast::Sender<AdminEvent>,

    /// Server start time, used to compute uptime for admin heartbeats.
    pub start_time: std::time::Instant,
}
