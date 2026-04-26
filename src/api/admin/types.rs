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
