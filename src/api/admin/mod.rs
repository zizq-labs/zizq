// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Admin API for the Zizq dashboard.
//!
//! Provides a separate HTTP listener for monitoring and management.
//! Clients connect via WebSocket to `/events` for live updates, and a
//! browser pointed at the listener gets the web UI that does the same.
//! The admin API is available on all tiers.

pub mod backup;
pub mod compact;
pub mod events;
mod handlers;
mod origin;
mod types;
mod web;

pub use handlers::app;
pub use origin::AllowedHosts;
pub use types::{
    AdminBackoff, AdminEvent, AdminJob, AdminMessage, AdminRetention, ClientMessage, Direction,
    Fallback, FindAnchor, JobChangeStatus, JobWindow, ListName, SearchQuery, ServerStatus,
    SubscribeAnchor,
};
