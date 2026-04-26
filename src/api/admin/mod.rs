// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Admin API for the Zizq dashboard.
//!
//! Provides a separate HTTP listener for monitoring and management.
//! Clients connect via WebSocket to `/events` for live updates.
//! The admin API is available on all tiers — only the TUI client
//! that connects to it requires a license.

pub mod backup;
pub mod events;
mod handlers;
mod types;

pub use handlers::app;
pub use types::{
    AdminEvent, AdminJobSummary, AdminMessage, ClientMessage, JobChangeStatus, JobWindow, ListName,
    ServerStatus,
};
