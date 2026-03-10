// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TUI application state model.

use tokio::sync::mpsc;

use crate::admin::{AdminJobSummary, JobChangeStatus, JobWindow, ServerStatus};
use crate::license::Tier;

use super::events::{self, Event};

/// Active tab in the TUI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    Ready,
    InFlight,
    Scheduled,
}

impl Tab {
    const ALL: [Tab; 3] = [Tab::InFlight, Tab::Ready, Tab::Scheduled];

    pub fn next(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap();
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    pub fn idx(self) -> usize {
        Self::ALL.iter().position(|&t| t == self).unwrap()
    }

    pub fn prev(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap();
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    fn list_name(self) -> crate::admin::ListName {
        match self {
            Tab::Ready => crate::admin::ListName::Ready,
            Tab::InFlight => crate::admin::ListName::InFlight,
            Tab::Scheduled => crate::admin::ListName::Scheduled,
        }
    }
}

/// Connection status to the admin API.
#[derive(Clone)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

/// Per-list scroll and buffering state.
#[derive(Clone)]
pub struct ListState {
    /// Selected row index in the full (server-side) list.
    pub cursor: usize,
    /// First visible row in the viewport.
    pub scroll_pos: usize,
    /// Offset of buffered data from server.
    pub buffer_offset: usize,
    /// Whether the cursor should follow the bottom of the list.
    pub follow_bottom: bool,
    /// Last subscribe (offset, limit) sent — used to avoid duplicate requests.
    last_subscribe: Option<(usize, usize)>,
}

impl Default for ListState {
    fn default() -> Self {
        Self {
            cursor: 0,
            scroll_pos: 0,
            buffer_offset: 0,
            follow_bottom: false,
            last_subscribe: None,
        }
    }
}

/// Top-level application state for the TUI.
pub struct App {
    pub host: String,
    pub status: ConnectionStatus,
    pub server_version: Option<String>,
    pub server_uptime_ms: Option<u64>,
    pub server_tier: Option<Tier>,
    pub subscription_limit: Option<usize>,
    pub total_ready: usize,
    pub total_in_flight: usize,
    pub total_scheduled: usize,
    pub ready_jobs: Vec<AdminJobSummary>,
    pub in_flight_jobs: Vec<AdminJobSummary>,
    pub scheduled_jobs: Vec<AdminJobSummary>,
    pub now_ms: u64,
    pub active_tab: Tab,
    pub h_scroll: [u16; 3],
    pub list_states: [ListState; 3],
    pub viewport_height: usize,
    pub ws_tx: Option<mpsc::Sender<String>>,
}

impl Clone for App {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            status: self.status.clone(),
            server_version: self.server_version.clone(),
            server_uptime_ms: self.server_uptime_ms,
            server_tier: self.server_tier,
            subscription_limit: self.subscription_limit,
            total_ready: self.total_ready,
            total_in_flight: self.total_in_flight,
            total_scheduled: self.total_scheduled,
            ready_jobs: self.ready_jobs.clone(),
            in_flight_jobs: self.in_flight_jobs.clone(),
            scheduled_jobs: self.scheduled_jobs.clone(),
            now_ms: self.now_ms,
            active_tab: self.active_tab,
            h_scroll: self.h_scroll,
            list_states: self.list_states.clone(),
            viewport_height: self.viewport_height,
            ws_tx: self.ws_tx.clone(),
        }
    }
}

impl App {
    pub fn new(host: String) -> Self {
        Self {
            host,
            status: ConnectionStatus::Connecting,
            server_version: None,
            server_uptime_ms: None,
            server_tier: None,
            subscription_limit: None,
            total_ready: 0,
            total_in_flight: 0,
            total_scheduled: 0,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            now_ms: 0,
            active_tab: Tab::InFlight,
            h_scroll: [0; 3],
            list_states: Default::default(),
            viewport_height: 0,
            ws_tx: None,
        }
    }

    pub fn set_ws_tx(&mut self, tx: mpsc::Sender<String>) {
        self.ws_tx = Some(tx);
    }

    /// Get the total count for the active tab.
    fn active_total(&self) -> usize {
        match self.active_tab {
            Tab::Ready => self.total_ready,
            Tab::InFlight => self.total_in_flight,
            Tab::Scheduled => self.total_scheduled,
        }
    }

    /// Effective total for scroll clamping — capped when server sets a limit.
    fn effective_total(&self) -> usize {
        let total = self.active_total();
        match self.subscription_limit {
            Some(cap) => total.min(cap),
            None => total,
        }
    }

    /// Send a subscribe message over WebSocket.
    fn send_subscribe(&self, tab: Tab, offset: usize, limit: usize) {
        if let Some(tx) = &self.ws_tx {
            let msg = events::subscribe_message(tab.list_name(), offset, limit);
            let _ = tx.try_send(msg);
        }
    }

    /// Re-subscribe all tabs with a window sized to the current viewport.
    /// Called when the terminal is resized.
    pub fn resubscribe_all(&mut self) {
        if self.subscription_limit.is_some() {
            return;
        }
        let vh = self.viewport_height;
        if vh == 0 {
            return;
        }
        let limit = vh * 3;
        for tab in [Tab::InFlight, Tab::Ready, Tab::Scheduled] {
            let ls = &mut self.list_states[tab.idx()];
            let offset = ls.cursor.saturating_sub(limit / 2);
            ls.last_subscribe = Some((offset, limit));
            if let Some(tx) = &self.ws_tx {
                let msg = events::subscribe_message(tab.list_name(), offset, limit);
                let _ = tx.try_send(msg);
            }
        }
    }

    /// Check if prefetch is needed and send subscribe if so.
    /// Deduplicates: won't re-send the same (offset, limit) for the same tab.
    fn maybe_prefetch(&mut self) {
        if self.subscription_limit.is_some() {
            return;
        }
        let vh = self.viewport_height;
        if vh == 0 {
            return;
        }
        let tab = self.active_tab;
        let ls = &self.list_states[tab.idx()];
        let buffer_size = match tab {
            Tab::Ready => self.ready_jobs.len(),
            Tab::InFlight => self.in_flight_jobs.len(),
            Tab::Scheduled => self.scheduled_jobs.len(),
        };
        let buffer_end = ls.buffer_offset + buffer_size;

        // Prefetch when cursor is within one viewport of buffer edges.
        let near_top = ls.cursor < ls.buffer_offset + vh;
        let near_bottom = ls.cursor + vh >= buffer_end;

        if (near_top && ls.buffer_offset > 0) || (near_bottom && buffer_end < self.active_total()) {
            let new_limit = vh * 3;
            let new_offset = ls.cursor.saturating_sub(new_limit / 2);
            let key = (new_offset, new_limit);

            if self.list_states[tab.idx()].last_subscribe == Some(key) {
                return;
            }
            self.list_states[tab.idx()].last_subscribe = Some(key);
            self.send_subscribe(tab, new_offset, new_limit);
        }
    }

    /// Apply server status fields from any message.
    fn apply_server_status(&mut self, server: ServerStatus) {
        self.status = ConnectionStatus::Connected;
        self.server_version = Some(server.version);
        self.server_uptime_ms = Some(server.uptime_ms);
        self.server_tier = Tier::parse(&server.tier);
        self.subscription_limit = server.subscription_limit;
        self.total_ready = server.total_ready;
        self.total_in_flight = server.total_in_flight;
        self.total_scheduled = server.total_scheduled;
    }

    /// Handle an incoming event, updating state.
    ///
    /// Returns `true` if the application should quit.
    pub fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Quit => return true,
            Event::NextTab => {
                self.active_tab = self.active_tab.next();
            }
            Event::PrevTab => {
                self.active_tab = self.active_tab.prev();
            }
            Event::ScrollLeft => {
                let i = self.active_tab.idx();
                self.h_scroll[i] = self.h_scroll[i].saturating_sub(4);
            }
            Event::ScrollRight => {
                let i = self.active_tab.idx();
                self.h_scroll[i] = self.h_scroll[i].saturating_add(4);
            }
            Event::ScrollUp => {
                self.scroll_up();
            }
            Event::ScrollDown => {
                self.scroll_down();
            }
            Event::PageUp => {
                self.page_up();
            }
            Event::PageDown => {
                self.page_down();
            }
            Event::ServerConnecting => {
                self.status = ConnectionStatus::Connecting;
            }
            Event::ServerConnected => {
                self.status = ConnectionStatus::Connected;
            }
            Event::ServerHeartbeat { server } => {
                self.apply_server_status(server);
                self.apply_follow_bottom();
            }
            Event::ServerJobSnapshot {
                server,
                ready,
                in_flight,
                scheduled,
            } => {
                self.apply_server_status(server);
                self.apply_job_window(Tab::Ready, ready);
                self.apply_job_window(Tab::InFlight, in_flight);
                self.apply_job_window(Tab::Scheduled, scheduled);
                self.apply_follow_bottom();
            }
            Event::ServerJobChanged {
                server,
                id,
                status,
                job,
            } => {
                self.apply_server_status(server);
                match status {
                    JobChangeStatus::Ready => {
                        if let Some(job) = job {
                            let key = (job.priority, &job.id);
                            let pos = self
                                .ready_jobs
                                .partition_point(|j| (j.priority, &j.id) < key);
                            if self.ready_jobs.get(pos).is_none_or(|j| j.id != id) {
                                self.ready_jobs.insert(pos, job);
                            }
                        }
                    }
                    JobChangeStatus::InFlight => {
                        self.ready_jobs.retain(|j| j.id != id);
                        if let Some(job) = job {
                            let dequeued = job.dequeued_at.unwrap_or(0);
                            let pos = self
                                .in_flight_jobs
                                .partition_point(|j| j.dequeued_at.unwrap_or(0) < dequeued);
                            if self.in_flight_jobs.get(pos).is_none_or(|j| j.id != id) {
                                self.in_flight_jobs.insert(pos, job);
                            }
                        }
                    }
                    JobChangeStatus::ReadyRemoved => {
                        self.ready_jobs.retain(|j| j.id != id);
                    }
                    JobChangeStatus::InFlightRemoved => {
                        self.in_flight_jobs.retain(|j| j.id != id);
                    }
                    JobChangeStatus::Scheduled => {
                        if let Some(job) = job {
                            let ready_at = job.ready_at;
                            let pos = self
                                .scheduled_jobs
                                .partition_point(|j| (j.ready_at, &j.id) < (ready_at, &job.id));
                            if self.scheduled_jobs.get(pos).is_none_or(|j| j.id != id) {
                                self.scheduled_jobs.insert(pos, job);
                            }
                        }
                    }
                    JobChangeStatus::ScheduledRemoved => {
                        self.scheduled_jobs.retain(|j| j.id != id);
                    }
                    JobChangeStatus::Completed | JobChangeStatus::Dead => {
                        self.ready_jobs.retain(|j| j.id != id);
                        self.in_flight_jobs.retain(|j| j.id != id);
                        self.scheduled_jobs.retain(|j| j.id != id);
                    }
                }
                self.apply_follow_bottom();
            }
            Event::ServerDisconnected => {
                self.status = ConnectionStatus::Disconnected;
                self.server_version = None;
                self.server_uptime_ms = None;
                self.server_tier = None;
                self.subscription_limit = None;
                self.total_ready = 0;
                self.total_in_flight = 0;
                self.total_scheduled = 0;
                self.ready_jobs.clear();
                self.in_flight_jobs.clear();
                self.scheduled_jobs.clear();
                self.list_states = Default::default();
            }
        }
        false
    }

    /// Apply a JobWindow snapshot to the corresponding list.
    fn apply_job_window(&mut self, tab: Tab, window: JobWindow) {
        let ls = &mut self.list_states[tab.idx()];
        ls.buffer_offset = window.offset;
        ls.last_subscribe = None; // Allow new prefetch after data arrives.
        match tab {
            Tab::Ready => self.ready_jobs = window.items,
            Tab::InFlight => self.in_flight_jobs = window.items,
            Tab::Scheduled => self.scheduled_jobs = window.items,
        }
    }

    /// Clamp cursor/scroll positions and apply follow-bottom tracking.
    fn apply_follow_bottom(&mut self) {
        let cap = self.subscription_limit;
        for tab in [Tab::InFlight, Tab::Ready, Tab::Scheduled] {
            let raw_total = match tab {
                Tab::Ready => self.total_ready,
                Tab::InFlight => self.total_in_flight,
                Tab::Scheduled => self.total_scheduled,
            };
            let total = match cap {
                Some(c) => raw_total.min(c),
                None => raw_total,
            };
            let ls = &mut self.list_states[tab.idx()];

            if total == 0 {
                ls.cursor = 0;
                ls.scroll_pos = 0;
                continue;
            }

            // Follow-bottom: stick cursor to end.
            if ls.follow_bottom {
                ls.cursor = total - 1;
            }

            // Clamp cursor if total shrunk below it.
            if ls.cursor >= total {
                ls.cursor = total - 1;
            }

            // Clamp scroll_pos so cursor stays visible.
            if self.viewport_height > 0 {
                if ls.cursor < ls.scroll_pos {
                    ls.scroll_pos = ls.cursor;
                } else if ls.cursor >= ls.scroll_pos + self.viewport_height {
                    ls.scroll_pos = ls.cursor - self.viewport_height + 1;
                }
            }
        }
    }

    fn scroll_up(&mut self) {
        let total = self.effective_total();
        if total == 0 {
            return;
        }
        let ls = &mut self.list_states[self.active_tab.idx()];
        if ls.cursor > 0 {
            ls.cursor -= 1;
        }
        if ls.cursor < ls.scroll_pos {
            ls.scroll_pos = ls.cursor;
        }
        ls.follow_bottom = false;
        self.maybe_prefetch();
    }

    fn scroll_down(&mut self) {
        let total = self.effective_total();
        if total == 0 {
            return;
        }
        let ls = &mut self.list_states[self.active_tab.idx()];
        if ls.cursor < total - 1 {
            ls.cursor += 1;
        }
        if self.viewport_height > 0 && ls.cursor >= ls.scroll_pos + self.viewport_height {
            ls.scroll_pos = ls.cursor - self.viewport_height + 1;
        }
        ls.follow_bottom = ls.cursor == total - 1;
        self.maybe_prefetch();
    }

    fn page_up(&mut self) {
        let total = self.effective_total();
        if total == 0 || self.viewport_height == 0 {
            return;
        }
        let ls = &mut self.list_states[self.active_tab.idx()];
        let jump = self.viewport_height.saturating_sub(1).max(1);
        ls.cursor = ls.cursor.saturating_sub(jump);
        if ls.cursor < ls.scroll_pos {
            ls.scroll_pos = ls.cursor;
        }
        ls.follow_bottom = false;
        self.maybe_prefetch();
    }

    fn page_down(&mut self) {
        let total = self.effective_total();
        if total == 0 || self.viewport_height == 0 {
            return;
        }
        let ls = &mut self.list_states[self.active_tab.idx()];
        let jump = self.viewport_height.saturating_sub(1).max(1);
        ls.cursor = (ls.cursor + jump).min(total - 1);
        if ls.cursor >= ls.scroll_pos + self.viewport_height {
            ls.scroll_pos = ls.cursor - self.viewport_height + 1;
        }
        ls.follow_bottom = ls.cursor == total - 1;
        self.maybe_prefetch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_server() -> crate::admin::ServerStatus {
        crate::admin::ServerStatus {
            version: "1.0.0".to_string(),
            uptime_ms: 5000,
            tier: "pro".to_string(),
            total_ready: 0,
            total_in_flight: 0,
            total_scheduled: 0,
            subscription_limit: None,
        }
    }

    fn job(id: &str, priority: u16, dequeued_at: Option<u64>) -> AdminJobSummary {
        AdminJobSummary {
            id: id.to_string(),
            queue: "q".to_string(),
            job_type: "t".to_string(),
            priority,
            ready_at: 1000,
            attempts: 0,
            dequeued_at,
            failed_at: None,
        }
    }

    fn ready_event(id: &str, priority: u16) -> Event {
        Event::ServerJobChanged {
            server: default_server(),
            id: id.to_string(),
            status: JobChangeStatus::Ready,
            job: Some(job(id, priority, None)),
        }
    }

    fn in_flight_event(id: &str, dequeued_at: u64) -> Event {
        Event::ServerJobChanged {
            server: default_server(),
            id: id.to_string(),
            status: JobChangeStatus::InFlight,
            job: Some(job(id, 0, Some(dequeued_at))),
        }
    }

    fn ids(jobs: &[AdminJobSummary]) -> Vec<&str> {
        jobs.iter().map(|j| j.id.as_str()).collect()
    }

    // ── Quit ────────────────────────────────────────────────────────

    #[test]
    fn quit_returns_true() {
        let mut app = App::new("127.0.0.1:8901".into());
        assert!(app.handle_event(Event::Quit));
    }

    // ── Connection lifecycle ────────────────────────────────────────

    #[test]
    fn heartbeat_sets_connected_and_metadata() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::ServerHeartbeat {
            server: default_server(),
        });
        assert!(matches!(app.status, ConnectionStatus::Connected));
        assert_eq!(app.server_version.as_deref(), Some("1.0.0"));
        assert_eq!(app.server_uptime_ms, Some(5000));
    }

    #[test]
    fn disconnect_clears_state() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::ServerHeartbeat {
            server: default_server(),
        });
        app.handle_event(ready_event("j1", 0));
        app.handle_event(Event::ServerDisconnected);

        assert!(matches!(app.status, ConnectionStatus::Disconnected));
        assert!(app.server_version.is_none());
        assert!(app.server_uptime_ms.is_none());
        assert_eq!(app.total_ready, 0);
        assert_eq!(app.total_in_flight, 0);
        assert_eq!(app.total_scheduled, 0);
        assert!(app.ready_jobs.is_empty());
        assert!(app.in_flight_jobs.is_empty());
        assert!(app.scheduled_jobs.is_empty());
    }

    // ── Snapshot ────────────────────────────────────────────────────

    #[test]
    fn snapshot_replaces_jobs() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("old", 0));

        app.handle_event(Event::ServerJobSnapshot {
            server: default_server(),
            ready: JobWindow {
                offset: 0,
                items: vec![job("r1", 0, None)],
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![job("w1", 0, Some(100))],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![job("s1", 0, None)],
            },
        });

        assert_eq!(ids(&app.ready_jobs), vec!["r1"]);
        assert_eq!(ids(&app.in_flight_jobs), vec!["w1"]);
        assert_eq!(ids(&app.scheduled_jobs), vec!["s1"]);
    }

    // ── Ready insertion ─────────────────────────────────────────────

    #[test]
    fn ready_inserts_in_priority_order() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("low", 5));
        app.handle_event(ready_event("high", 1));
        app.handle_event(ready_event("mid", 3));

        assert_eq!(ids(&app.ready_jobs), vec!["high", "mid", "low"]);
    }

    #[test]
    fn ready_deduplicates() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        app.handle_event(ready_event("j1", 0));

        assert_eq!(app.ready_jobs.len(), 1);
    }

    // ── In-flight insertion ──────────────────────────────────────────

    #[test]
    fn in_flight_inserts_in_dequeued_order() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(in_flight_event("w2", 200));
        app.handle_event(in_flight_event("w1", 100));
        app.handle_event(in_flight_event("w3", 300));

        assert_eq!(ids(&app.in_flight_jobs), vec!["w1", "w2", "w3"]);
    }

    #[test]
    fn in_flight_removes_from_ready() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        assert_eq!(app.ready_jobs.len(), 1);

        app.handle_event(in_flight_event("j1", 100));
        assert!(app.ready_jobs.is_empty());
        assert_eq!(ids(&app.in_flight_jobs), vec!["j1"]);
    }

    #[test]
    fn in_flight_deduplicates() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(in_flight_event("j1", 100));
        app.handle_event(in_flight_event("j1", 100));

        assert_eq!(app.in_flight_jobs.len(), 1);
    }

    // ── Removals ────────────────────────────────────────────────────

    #[test]
    fn ready_removed_removes_from_ready() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        app.handle_event(ready_event("j2", 1));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "j1".to_string(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j2"]);
    }

    #[test]
    fn in_flight_removed_removes_from_in_flight() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(in_flight_event("j1", 100));
        app.handle_event(in_flight_event("j2", 200));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "j1".to_string(),
            status: JobChangeStatus::InFlightRemoved,
            job: None,
        });

        assert_eq!(ids(&app.in_flight_jobs), vec!["j2"]);
    }

    #[test]
    fn completed_removes_from_both() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("r1", 0));
        app.handle_event(in_flight_event("w1", 100));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "r1".to_string(),
            status: JobChangeStatus::Completed,
            job: None,
        });
        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "w1".to_string(),
            status: JobChangeStatus::Dead,
            job: None,
        });

        assert!(app.ready_jobs.is_empty());
        assert!(app.in_flight_jobs.is_empty());
    }

    #[test]
    fn removal_of_unknown_id_is_noop() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "nonexistent".to_string(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
    }

    // ── Tab switching ───────────────────────────────────────────────

    #[test]
    fn default_tab_is_in_flight() {
        let app = App::new("127.0.0.1:8901".into());
        assert_eq!(app.active_tab, Tab::InFlight);
    }

    #[test]
    fn next_tab_cycles_forward() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        assert_eq!(app.active_tab, Tab::Ready);
    }

    #[test]
    fn next_tab_cycles_through_scheduled() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        app.handle_event(Event::NextTab);
        assert_eq!(app.active_tab, Tab::Scheduled);
    }

    #[test]
    fn next_tab_wraps_around() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        app.handle_event(Event::NextTab);
        app.handle_event(Event::NextTab);
        assert_eq!(app.active_tab, Tab::InFlight);
    }

    #[test]
    fn prev_tab_cycles_backward() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        app.handle_event(Event::PrevTab);
        assert_eq!(app.active_tab, Tab::InFlight);
    }

    #[test]
    fn prev_tab_wraps_around() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::PrevTab);
        assert_eq!(app.active_tab, Tab::Scheduled);
    }

    // ── Scheduled insertion ────────────────────────────────────────

    #[test]
    fn scheduled_inserts_in_ready_at_order() {
        let mut app = App::new("127.0.0.1:8901".into());
        let scheduled_event = |id: &str, ready_at: u64| -> Event {
            Event::ServerJobChanged {
                server: default_server(),
                id: id.to_string(),
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                }),
            }
        };

        app.handle_event(scheduled_event("s2", 2000));
        app.handle_event(scheduled_event("s1", 1000));
        app.handle_event(scheduled_event("s3", 3000));

        assert_eq!(ids(&app.scheduled_jobs), vec!["s1", "s2", "s3"]);
    }

    #[test]
    fn scheduled_removed_removes_from_scheduled() {
        let mut app = App::new("127.0.0.1:8901".into());
        let scheduled_event = |id: &str, ready_at: u64| -> Event {
            Event::ServerJobChanged {
                server: default_server(),
                id: id.to_string(),
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                }),
            }
        };

        app.handle_event(scheduled_event("s1", 1000));
        app.handle_event(scheduled_event("s2", 2000));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "s1".to_string(),
            status: JobChangeStatus::ScheduledRemoved,
            job: None,
        });

        assert_eq!(ids(&app.scheduled_jobs), vec!["s2"]);
    }

    #[test]
    fn completed_removes_from_scheduled() {
        let mut app = App::new("127.0.0.1:8901".into());
        let scheduled_event = |id: &str, ready_at: u64| -> Event {
            Event::ServerJobChanged {
                server: default_server(),
                id: id.to_string(),
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                }),
            }
        };

        app.handle_event(scheduled_event("s1", 1000));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "s1".to_string(),
            status: JobChangeStatus::Completed,
            job: None,
        });

        assert!(app.scheduled_jobs.is_empty());
    }
}
