// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TUI application state model.

use super::events::Event;

/// Connection status to the admin API.
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

/// Top-level application state for the TUI.
pub struct App {
    pub status: ConnectionStatus,
    pub server_version: Option<String>,
    pub server_uptime_ms: Option<u64>,
}

impl App {
    pub fn new() -> Self {
        Self {
            status: ConnectionStatus::Connecting,
            server_version: None,
            server_uptime_ms: None,
        }
    }

    /// Handle an incoming event, updating state.
    ///
    /// Returns `true` if the application should quit.
    pub fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Quit => return true,
            Event::ServerConnecting => {
                self.status = ConnectionStatus::Connecting;
            }
            Event::ServerConnected => {
                self.status = ConnectionStatus::Connected;
            }
            Event::ServerHeartbeat { version, uptime_ms } => {
                self.status = ConnectionStatus::Connected;
                self.server_version = Some(version);
                self.server_uptime_ms = Some(uptime_ms);
            }
            Event::ServerDisconnected => {
                self.status = ConnectionStatus::Disconnected;
                self.server_version = None;
                self.server_uptime_ms = None;
            }
        }
        false
    }
}
