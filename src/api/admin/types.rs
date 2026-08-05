// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
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

    /// Response to a `ClientMessage::Find` request.
    ///
    /// Carries the matched job's absolute position within the unfiltered
    /// index at the moment the server walked, plus a `JobWindow` of
    /// surrounding items so the dashboard can render the match's
    /// neighbourhood without a follow-up `Subscribe`. Under
    /// `Direction::Locate` a `matched_position: None` means the anchor
    /// id no longer exists (job completed / requeued out) and the
    /// dashboard should unpin.
    FindResult {
        list: ListName,
        matched_position: Option<usize>,
        window: JobWindow,
    },
}

/// A windowed slice of jobs centered on (or clamped near) an anchor.
///
/// The dashboard renders the job's row as `items[cursor_local_idx]`,
/// where `cursor_local_idx` is derived by looking up its currently
/// pinned job id inside `items`. Position is descriptive metadata,
/// not the primary index: the client cursor lives in job-id space.
///
/// `first_position` and `total` power the depth-bar (they answer
/// "where in the queue is the first item and how deep is the queue"),
/// but the cursor does not use them.
///
/// `resolved_anchor` reflects whether the server found the requested
/// `Around` anchor in the list at scan time:
///
/// - `Some(id)`: the anchor id is present; `items` is centered on it.
/// - `None`: the anchor was requested but wasn't found — `items` was
///   filled per the client's `fallback` (or is empty for `Nowhere`).
///   The client should treat its previously-pinned cursor as lost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobWindow {
    pub items: Vec<AdminJob>,
    pub first_position: usize,
    pub total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_anchor: Option<String>,
}

/// Client-to-server message for controlling subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Ask the server for a window of jobs around an anchor. See
    /// `SubscribeAnchor` for the anchor semantics.
    Subscribe {
        list: ListName,
        anchor: SubscribeAnchor,
        limit: usize,
    },
    SetDetailLevel {
        detail: bool,
    },
    /// Request deletion of a single job. The server doesn't ack — the
    /// resulting `JobDeleted` store event flows back through the normal
    /// admin event stream and removes the row on every connected client.
    DeleteJob {
        id: String,
    },
    /// Search for a job in one of the lists. Behaves like `less`'s `/`,
    /// `n`, `N`: the server walks the appropriate in-memory index in
    /// the requested `direction` starting from `anchor`, tests each
    /// entry against `query`, and returns the first match — plus a
    /// window of surrounding items so the dashboard can render the
    /// match's neighbourhood without a second round-trip.
    ///
    /// Under `Direction::Locate` the server just resolves the anchor
    /// id's current position and returns a window centered on it —
    /// used by the dashboard to re-center on a pinned job that has
    /// drifted out of the local buffer.
    Find {
        list: ListName,
        anchor: FindAnchor,
        direction: Direction,
        #[serde(default)]
        query: SearchQuery,
        limit: usize,
    },
}

/// Anchor point for a `Find` walk. The server resolves an anchor to
/// an absolute position in the appropriate list's in-memory index
/// (priority for `Ready`, `dequeued_at` for `InFlight`, `ready_at`
/// for `Scheduled`), then walks from there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FindAnchor {
    /// Anchor at the current position of a specific job id. If the id
    /// is not in the index, the walk falls back to `Start` (for
    /// `Forward`) or `End` (for `Backward`), and `Locate` returns
    /// `matched_position: None`.
    JobId { id: String },
    /// Walk from position 0.
    Start,
    /// Walk from the last position (`len - 1`).
    End,
}

/// Direction of a `Find` walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Skip the anchor, walk forward, return first match.
    Forward,
    /// Skip the anchor, walk backward, return first match.
    Backward,
    /// Return the anchor's current position. `query` is ignored.
    /// Used by the dashboard's row-sticky follow when a pinned job
    /// has drifted out of the local buffer.
    Locate,
}

/// Predicate applied to each candidate during a `Find` walk. Empty
/// vectors mean "no constraint on this dimension." Multiple values in
/// a vector are OR'd; different fields are AND'd.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    pub queues: Vec<String>,
    #[serde(default)]
    pub types: Vec<String>,
}

/// Anchor of a `Subscribe` request.
///
/// The dashboard addresses jobs by their (stable) job id rather than
/// by (drifting) numeric position: a queue shifts every time a worker
/// takes the head job, so "position 500" points at a different job
/// each round. `Around` scopes the returned window to the neighbourhood
/// of a specific job so the client's cursor can stay pinned even as
/// the queue churns underneath it. `Head` and `Tail` are natural
/// starting anchors (e.g. `G` in `zizq top` sends `Tail`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubscribeAnchor {
    /// Follow a specific job id. Used by search / pin — the cursor
    /// tracks a specific matched job as the queue churns around it.
    Around {
        id: String,
        /// What the server should do when `id` is no longer in the
        /// list. See `Fallback`.
        #[serde(default)]
        fallback: Fallback,
    },
    /// Numeric depth anchor. Used by arrow-key navigation and
    /// paging: the cursor cares about being at depth N of the queue,
    /// not about a specific job. The server clamps to
    /// `max(0, total - limit)` when `offset >= total`, so under drain
    /// the client always receives real tail items instead of an
    /// empty window.
    Offset {
        offset: usize,
    },
    Head,
    Tail,
}

/// Fallback behaviour for `SubscribeAnchor::Around` when the anchor
/// id is not in the list at scan time.
///
/// - `Head`: return the head-most `limit` items. Used when the client
///   was following the top of the list.
/// - `Tail`: return the last `limit` items. Used when the client was
///   following the bottom (e.g. after `G`).
/// - `Nowhere`: return an empty window. Used when the client would
///   prefer to render a "cursor lost" state and wait for the next
///   navigation action rather than teleport somewhere unexpected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fallback {
    Head,
    Tail,
    #[default]
    Nowhere,
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
