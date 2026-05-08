// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Types for the admin WebSocket API.
//!
//! These are the message types exchanged between the admin server and
//! dashboard clients (e.g. `zizq top`).

use serde::{Deserialize, Serialize};

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
        job: Option<AdminJob>,
    },
}

/// A windowed slice of jobs with offset metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobWindow {
    pub offset: usize,
    pub items: Vec<AdminJob>,
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
    SetDetailLevel {
        detail: bool,
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

/// A job as seen by the admin API.
///
/// Always includes the core fields needed for the dashboard table. When
/// detail mode is enabled, additional fields (payload, retry_limit, etc.)
/// are populated. Extra fields are silently ignored by older clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminJob {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<AdminBackoff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<AdminRetention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_while: Option<String>,
}

/// Backoff configuration for the admin API (human-readable field names).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminBackoff {
    pub base_ms: u32,
    pub exponent: f32,
    pub jitter_ms: u32,
}

/// Retention configuration for the admin API (human-readable field names).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRetention {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_ms: Option<u64>,
}

impl AdminJob {
    /// Convert a store job to an admin job.
    ///
    /// When `detail` is true, includes payload and other extended fields.
    pub fn from_store(job: store::Job, detail: bool) -> Self {
        let (unique_key, unique_while) = if detail {
            match job.unique {
                Some(ref uc) => (
                    Some(uc.key.clone()),
                    Some(
                        match uc.scope {
                            0 => "queued",
                            1 => "active",
                            2 => "exists",
                            _ => "queued",
                        }
                        .to_string(),
                    ),
                ),
                None => (None, None),
            }
        } else {
            (None, None)
        };

        Self {
            id: job.id,
            queue: job.queue,
            job_type: job.job_type,
            priority: job.priority,
            ready_at: job.ready_at,
            attempts: job.attempts,
            dequeued_at: job.dequeued_at,
            failed_at: job.failed_at,
            payload: if detail { job.payload } else { None },
            retry_limit: if detail { job.retry_limit } else { None },
            backoff: if detail {
                job.backoff.map(|b| AdminBackoff {
                    base_ms: b.base_ms,
                    exponent: b.exponent,
                    jitter_ms: b.jitter_ms,
                })
            } else {
                None
            },
            retention: if detail {
                job.retention.map(|r| AdminRetention {
                    completed_ms: r.completed_ms,
                    dead_ms: r.dead_ms,
                })
            } else {
                None
            },
            unique_key,
            unique_while,
        }
    }
}
