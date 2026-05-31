// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TUI application state model.

use tokio::sync::mpsc;

use crate::api::admin::{AdminJob, JobChangeStatus, JobWindow, ServerStatus};
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

    fn list_name(self) -> crate::api::admin::ListName {
        match self {
            Tab::Ready => crate::api::admin::ListName::Ready,
            Tab::InFlight => crate::api::admin::ListName::InFlight,
            Tab::Scheduled => crate::api::admin::ListName::Scheduled,
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
    pub ready_jobs: Vec<AdminJob>,
    pub in_flight_jobs: Vec<AdminJob>,
    pub scheduled_jobs: Vec<AdminJob>,
    pub now_ms: u64,
    pub active_tab: Tab,
    pub h_scroll: [u16; 3],
    pub list_states: [ListState; 3],
    pub viewport_height: usize,
    pub show_detail: bool,
    /// When true, the displayed job lists are frozen at the snapshot they
    /// held at the moment pause was toggled on. Header totals/heartbeat
    /// fields keep updating; incoming `JobChanged` / `JobSnapshot` events
    /// don't mutate the row buffers. Toggled by `p`.
    pub paused: bool,
    /// When `Some`, the user has pressed `D` on a row and the help bar
    /// is showing a `Delete job …? [y/N]` prompt. While set, user input
    /// is restricted to confirming (`y`) or cancelling (`n` / Esc / `q`).
    pub pending_delete: Option<String>,
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
            show_detail: self.show_detail,
            paused: self.paused,
            pending_delete: self.pending_delete.clone(),
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
            show_detail: false,
            paused: false,
            pending_delete: None,
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
        // While paused the visible buffer is intentionally frozen — no
        // prefetching, so scrolling clamps at whatever rows were already
        // present at the moment of pause.
        if self.paused {
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

    /// Send the current detail level to the server.
    fn send_detail_level(&self) {
        if let Some(tx) = &self.ws_tx {
            let msg = events::detail_level_message(self.show_detail);
            let _ = tx.try_send(msg);
        }
    }

    /// Return the id of the job under the cursor in the active tab,
    /// or `None` if the buffer doesn't cover that row.
    fn selected_job_id(&self) -> Option<String> {
        let ls = &self.list_states[self.active_tab.idx()];
        let idx = ls.cursor.checked_sub(ls.buffer_offset)?;
        let jobs = match self.active_tab {
            Tab::Ready => &self.ready_jobs,
            Tab::InFlight => &self.in_flight_jobs,
            Tab::Scheduled => &self.scheduled_jobs,
        };
        jobs.get(idx).map(|j| j.id.clone())
    }

    /// Send a delete-job message over WebSocket and remove the row from
    /// every local buffer. The server will also broadcast a `JobDeleted`
    /// store event, but applying the removal eagerly keeps the cursor
    /// behaving sensibly even while paused (when incoming events are
    /// otherwise ignored).
    fn confirm_pending_delete(&mut self) {
        let Some(id) = self.pending_delete.take() else {
            return;
        };
        if let Some(tx) = &self.ws_tx {
            let msg = events::delete_job_message(id.clone());
            let _ = tx.try_send(msg);
        }
        self.ready_jobs.retain(|j| j.id != id);
        self.in_flight_jobs.retain(|j| j.id != id);
        self.scheduled_jobs.retain(|j| j.id != id);
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
        // While a delete prompt is pending, gate user input down to
        // confirm/cancel. Server-pushed events still flow through and
        // update state — only keypresses are restricted. `is_user_input`
        // and `is_scroll` together cover every keyboard-originated event.
        if self.pending_delete.is_some() && (event.is_user_input() || event.is_scroll()) {
            match event {
                Event::ConfirmDelete => self.confirm_pending_delete(),
                // Quit while prompting cancels the prompt rather than
                // exiting; a second `q` (or any other quit gesture) after
                // cancellation will quit normally.
                Event::CancelDelete | Event::Quit => self.pending_delete = None,
                _ => {}
            }
            return false;
        }

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
            Event::GoToStart => {
                self.go_to_start();
            }
            Event::GoToEnd => {
                self.go_to_end();
            }
            Event::ToggleDetail => {
                // Detail level can't be changed while paused — the visible
                // payloads are whatever was in the frozen snapshot.
                if self.paused {
                    return false;
                }
                self.show_detail = !self.show_detail;
                self.send_detail_level();
            }
            Event::RequestDelete => {
                if let Some(id) = self.selected_job_id() {
                    self.pending_delete = Some(id);
                }
            }
            // Outside of an active prompt these are no-ops — the prompt
            // path above is the only thing that interprets them.
            Event::ConfirmDelete | Event::CancelDelete => {}
            Event::TogglePause => {
                self.paused = !self.paused;
                if !self.paused {
                    // Resync the active tab on unpause so the visible rows
                    // reflect current truth. Other tabs will refresh lazily
                    // on next prefetch or when they become active.
                    let tab = self.active_tab;
                    let ls = &self.list_states[tab.idx()];
                    if let Some((offset, limit)) = ls.last_subscribe {
                        self.send_subscribe(tab, offset, limit);
                    } else if self.viewport_height > 0 {
                        let limit = self.viewport_height * 3;
                        let offset = ls.cursor.saturating_sub(limit / 2);
                        self.send_subscribe(tab, offset, limit);
                    }
                }
            }
            Event::ServerConnecting => {
                self.status = ConnectionStatus::Connecting;
            }
            Event::ServerConnected { url } => {
                self.status = ConnectionStatus::Connected;
                self.host = url;
                // Resend detail level so the server knows our preference
                // after a reconnect.
                if self.show_detail {
                    self.send_detail_level();
                }
            }
            Event::ServerHeartbeat { server } => {
                self.apply_server_status(server);
                self.apply_follow_bottom();
                // The follow-bottom clamp may have moved the cursor outside
                // the current buffer (e.g. after G during list churn), so
                // re-check whether we need a fresh window.
                self.maybe_prefetch();
            }
            Event::ServerJobSnapshot {
                server,
                ready,
                in_flight,
                scheduled,
            } => {
                self.apply_server_status(server);
                // Header totals/server status always update; the row
                // windows are held frozen while paused.
                if self.paused {
                    return false;
                }
                self.apply_job_window(Tab::Ready, ready);
                self.apply_job_window(Tab::InFlight, in_flight);
                self.apply_job_window(Tab::Scheduled, scheduled);
                self.apply_follow_bottom();
                // A snapshot can land with an offset that pre-dates rapid
                // server-side churn (jobs were enqueued/drained while the
                // Subscribe was in flight), leaving the cursor outside the
                // new buffer. Trigger another prefetch round so we converge.
                self.maybe_prefetch();
            }
            Event::ServerJobChanged {
                server,
                id,
                status,
                job,
            } => {
                self.apply_server_status(server);
                if self.paused {
                    return false;
                }
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
                // Each JobChanged can move the follow-bottom cursor; same
                // reasoning as the snapshot/heartbeat paths.
                self.maybe_prefetch();
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
            Event::Suspend => {
                // Handled by the main event loop before reaching here.
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

    /// Cursor navigation bounds for the active tab.
    ///
    /// While live this is the full server-side range. While paused it
    /// clamps to the rows currently in the buffer so the user can only
    /// scroll through the frozen snapshot. Returns `None` when there's
    /// nothing to navigate.
    fn cursor_bounds(&self) -> Option<(usize, usize)> {
        if !self.paused {
            let total = self.effective_total();
            if total == 0 {
                return None;
            }
            return Some((0, total - 1));
        }
        let tab = self.active_tab;
        let ls = &self.list_states[tab.idx()];
        let buffer_size = match tab {
            Tab::Ready => self.ready_jobs.len(),
            Tab::InFlight => self.in_flight_jobs.len(),
            Tab::Scheduled => self.scheduled_jobs.len(),
        };
        if buffer_size == 0 {
            return None;
        }
        Some((ls.buffer_offset, ls.buffer_offset + buffer_size - 1))
    }

    /// Clamp cursor/scroll positions and apply follow-bottom tracking.
    fn apply_follow_bottom(&mut self) {
        // While paused, the cursor is bounded to the frozen buffer (see
        // `cursor_bounds`) — letting `follow_bottom` and the live total
        // drag it around would break that contract.
        if self.paused {
            return;
        }
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
        let Some((min, _)) = self.cursor_bounds() else {
            return;
        };
        let ls = &mut self.list_states[self.active_tab.idx()];
        if ls.cursor > min {
            ls.cursor -= 1;
        }
        if ls.cursor < ls.scroll_pos {
            ls.scroll_pos = ls.cursor;
        }
        ls.follow_bottom = false;
        self.maybe_prefetch();
    }

    fn scroll_down(&mut self) {
        let Some((_, max)) = self.cursor_bounds() else {
            return;
        };
        let ls = &mut self.list_states[self.active_tab.idx()];
        if ls.cursor < max {
            ls.cursor += 1;
        }
        if self.viewport_height > 0 && ls.cursor >= ls.scroll_pos + self.viewport_height {
            ls.scroll_pos = ls.cursor - self.viewport_height + 1;
        }
        // `follow_bottom` only makes sense while live — paused scrolling
        // never wants the cursor pulled toward the server-side bottom.
        ls.follow_bottom = !self.paused && ls.cursor == max;
        self.maybe_prefetch();
    }

    fn page_up(&mut self) {
        let Some((min, _)) = self.cursor_bounds() else {
            return;
        };
        if self.viewport_height == 0 {
            return;
        }
        let ls = &mut self.list_states[self.active_tab.idx()];
        let jump = self.viewport_height.saturating_sub(1).max(1);
        ls.cursor = ls.cursor.saturating_sub(jump).max(min);
        if ls.cursor < ls.scroll_pos {
            ls.scroll_pos = ls.cursor;
        }
        ls.follow_bottom = false;
        self.maybe_prefetch();
    }

    fn page_down(&mut self) {
        let Some((_, max)) = self.cursor_bounds() else {
            return;
        };
        if self.viewport_height == 0 {
            return;
        }
        let ls = &mut self.list_states[self.active_tab.idx()];
        let jump = self.viewport_height.saturating_sub(1).max(1);
        ls.cursor = (ls.cursor + jump).min(max);
        if ls.cursor >= ls.scroll_pos + self.viewport_height {
            ls.scroll_pos = ls.cursor - self.viewport_height + 1;
        }
        ls.follow_bottom = !self.paused && ls.cursor == max;
        self.maybe_prefetch();
    }

    fn go_to_start(&mut self) {
        let Some((min, _)) = self.cursor_bounds() else {
            return;
        };
        let ls = &mut self.list_states[self.active_tab.idx()];
        ls.cursor = min;
        ls.scroll_pos = min;
        ls.follow_bottom = false;
        self.maybe_prefetch();
    }

    fn go_to_end(&mut self) {
        let Some((_, max)) = self.cursor_bounds() else {
            return;
        };
        let ls = &mut self.list_states[self.active_tab.idx()];
        ls.cursor = max;
        if self.viewport_height > 0 && ls.cursor + 1 >= self.viewport_height {
            ls.scroll_pos = ls.cursor + 1 - self.viewport_height;
        } else {
            ls.scroll_pos = 0;
        }
        ls.follow_bottom = !self.paused;
        self.maybe_prefetch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_server() -> crate::api::admin::ServerStatus {
        crate::api::admin::ServerStatus {
            version: "1.0.0".to_string(),
            uptime_ms: 5000,
            tier: "pro".to_string(),
            total_ready: 0,
            total_in_flight: 0,
            total_scheduled: 0,
            subscription_limit: None,
        }
    }

    fn job(id: &str, priority: u16, dequeued_at: Option<u64>) -> AdminJob {
        AdminJob {
            id: id.to_string(),
            queue: "q".to_string(),
            job_type: "t".to_string(),
            priority,
            ready_at: 1000,
            attempts: 0,
            dequeued_at,
            failed_at: None,
            payload: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
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

    fn ids(jobs: &[AdminJob]) -> Vec<&str> {
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
                job: Some(AdminJob {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                    payload: None,
                    retry_limit: None,
                    backoff: None,
                    retention: None,
                    unique_key: None,
                    unique_while: None,
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
                job: Some(AdminJob {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                    payload: None,
                    retry_limit: None,
                    backoff: None,
                    retention: None,
                    unique_key: None,
                    unique_while: None,
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
                job: Some(AdminJob {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                    payload: None,
                    retry_limit: None,
                    backoff: None,
                    retention: None,
                    unique_key: None,
                    unique_while: None,
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

    // ── Stale-snapshot prefetch ─────────────────────────────────────

    /// When a snapshot lands with an offset that no longer covers the
    /// cursor (because the list shifted while a Subscribe was in flight),
    /// the app must immediately request another window. Otherwise the
    /// render falls through to empty until the user keypresses again.
    /// This is the bug behind G producing a blank list under churn.
    #[tokio::test(flavor = "current_thread")]
    async fn stale_snapshot_triggers_followup_prefetch() {
        let (tx, mut rx) = mpsc::channel::<String>(16);
        let mut app = App::new("127.0.0.1:8901".into());
        app.set_ws_tx(tx);
        app.viewport_height = 20;
        app.active_tab = Tab::Ready;

        // Pretend the user pressed G with the server reporting 1000 ready
        // jobs but our local buffer holding rows 100..160.
        let mut srv = default_server();
        srv.total_ready = 1000;
        app.handle_event(Event::ServerHeartbeat {
            server: srv.clone(),
        });
        app.handle_event(Event::ServerJobSnapshot {
            server: srv.clone(),
            ready: JobWindow {
                offset: 100,
                items: (100..160).map(|i| job(&format!("j{i}"), 0, None)).collect(),
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });
        app.handle_event(Event::GoToEnd);

        // Drain whatever subscribes the events above produced.
        while rx.try_recv().is_ok() {}

        // Now simulate a stale snapshot landing: server total has dropped
        // to 500, and the requested window came back as offset=969 with
        // zero items because that range no longer exists.
        srv.total_ready = 500;
        app.handle_event(Event::ServerJobSnapshot {
            server: srv,
            ready: JobWindow {
                offset: 969,
                items: vec![],
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });

        // The fix should have queued another Subscribe so the buffer can
        // catch up with the new cursor position (499, the new bottom).
        let msg = rx.try_recv().expect("expected a follow-up Subscribe");
        assert!(
            msg.contains("\"type\":\"subscribe\"") && msg.contains("\"list\":\"ready\""),
            "expected a Subscribe to the ready list, got: {msg}"
        );
    }

    // ── Pause ───────────────────────────────────────────────────────

    #[test]
    fn toggle_pause_flips_paused_flag() {
        let mut app = App::new("127.0.0.1:8901".into());
        assert!(!app.paused);

        app.handle_event(Event::TogglePause);
        assert!(app.paused);

        app.handle_event(Event::TogglePause);
        assert!(!app.paused);
    }

    #[test]
    fn paused_ignores_job_changed_mutations() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);

        app.handle_event(Event::TogglePause);

        // These should not mutate the ready list.
        app.handle_event(ready_event("j2", 0));
        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "j1".into(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
    }

    #[test]
    fn paused_ignores_snapshot_windows_but_keeps_server_status() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        app.handle_event(Event::TogglePause);

        let mut srv = default_server();
        srv.total_ready = 999;
        srv.uptime_ms = 12345;

        app.handle_event(Event::ServerJobSnapshot {
            server: srv,
            ready: JobWindow {
                offset: 0,
                items: vec![job("snap", 0, None)],
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });

        // Row list stays frozen.
        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
        // Header / totals continue to update.
        assert_eq!(app.total_ready, 999);
        assert_eq!(app.server_uptime_ms, Some(12345));
    }

    #[test]
    fn paused_heartbeat_still_updates_server_status() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::TogglePause);

        let mut srv = default_server();
        srv.uptime_ms = 99_999;
        app.handle_event(Event::ServerHeartbeat { server: srv });

        assert_eq!(app.server_uptime_ms, Some(99_999));
    }

    /// While paused, navigation must stay inside whatever rows are
    /// currently in the buffer. The server-side total is irrelevant —
    /// the user is looking at a frozen subset.
    #[test]
    fn paused_g_jumps_to_buffer_end_not_server_total() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.viewport_height = 20;
        app.active_tab = Tab::Ready;

        // Server claims 1000 ready jobs but the local buffer holds only
        // 20 of them, anchored at offset 100.
        let mut srv = default_server();
        srv.total_ready = 1000;
        app.handle_event(Event::ServerJobSnapshot {
            server: srv,
            ready: JobWindow {
                offset: 100,
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });

        app.handle_event(Event::TogglePause);
        app.handle_event(Event::GoToEnd);

        // Last buffer row is index 119, not 999.
        let ls = &app.list_states[Tab::Ready.idx()];
        assert_eq!(ls.cursor, 119);
        assert!(
            !ls.follow_bottom,
            "follow_bottom should not stick when paused"
        );
    }

    #[test]
    fn paused_g_then_g_lower_jumps_to_buffer_start() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.viewport_height = 20;
        app.active_tab = Tab::Ready;

        let mut srv = default_server();
        srv.total_ready = 1000;
        app.handle_event(Event::ServerJobSnapshot {
            server: srv,
            ready: JobWindow {
                offset: 100,
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });

        app.handle_event(Event::TogglePause);
        app.handle_event(Event::GoToStart);

        // First buffer row is index 100, not 0.
        let ls = &app.list_states[Tab::Ready.idx()];
        assert_eq!(ls.cursor, 100);
    }

    #[test]
    fn paused_scroll_down_clamps_at_buffer_end() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.viewport_height = 20;
        app.active_tab = Tab::Ready;

        let mut srv = default_server();
        srv.total_ready = 1000;
        app.handle_event(Event::ServerJobSnapshot {
            server: srv,
            ready: JobWindow {
                offset: 100,
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });

        // Position cursor at the last buffer row, then pause.
        app.handle_event(Event::GoToEnd);
        // GoToEnd while live sets cursor=999; clamp at buffer once we pause.
        app.handle_event(Event::TogglePause);
        app.list_states[Tab::Ready.idx()].cursor = 119;

        // j past the buffer must not move the cursor.
        app.handle_event(Event::ScrollDown);
        let ls = &app.list_states[Tab::Ready.idx()];
        assert_eq!(ls.cursor, 119);
    }

    #[test]
    fn paused_heartbeat_does_not_drag_cursor_to_server_bottom() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.viewport_height = 20;
        app.active_tab = Tab::Ready;

        // Buffer covers 100..120.
        let mut srv = default_server();
        srv.total_ready = 1000;
        app.handle_event(Event::ServerJobSnapshot {
            server: srv.clone(),
            ready: JobWindow {
                offset: 100,
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
            },
            in_flight: JobWindow {
                offset: 0,
                items: vec![],
            },
            scheduled: JobWindow {
                offset: 0,
                items: vec![],
            },
        });

        // Press G live (sets follow_bottom = true, cursor near server end).
        app.handle_event(Event::GoToEnd);
        app.handle_event(Event::TogglePause);
        // Snap cursor back inside buffer to simulate the user navigating.
        app.list_states[Tab::Ready.idx()].cursor = 110;

        // Heartbeat with churn — server now has 2000 ready jobs.
        srv.total_ready = 2000;
        app.handle_event(Event::ServerHeartbeat { server: srv });

        // The follow-bottom logic must NOT have fired while paused.
        let ls = &app.list_states[Tab::Ready.idx()];
        assert_eq!(ls.cursor, 110);
    }

    #[test]
    fn paused_toggle_detail_is_noop() {
        let mut app = App::new("127.0.0.1:8901".into());
        assert!(!app.show_detail);
        app.handle_event(Event::TogglePause);

        app.handle_event(Event::ToggleDetail);

        // Detail level didn't change because the snapshot was frozen.
        assert!(!app.show_detail);
    }

    // ── Delete prompt ───────────────────────────────────────────────

    /// Seed the app with a ready job, cursor pointing at it.
    fn seed_ready_with_cursor(id: &str) -> App {
        let mut app = App::new("127.0.0.1:8901".into());
        app.viewport_height = 20;
        app.active_tab = Tab::Ready;
        app.total_ready = 1;
        app.ready_jobs = vec![job(id, 0, None)];
        app.list_states[Tab::Ready.idx()].buffer_offset = 0;
        app.list_states[Tab::Ready.idx()].cursor = 0;
        app
    }

    #[test]
    fn d_opens_delete_prompt_for_cursor_row() {
        let mut app = seed_ready_with_cursor("j1");
        app.handle_event(Event::RequestDelete);
        assert_eq!(app.pending_delete.as_deref(), Some("j1"));
    }

    #[test]
    fn d_on_empty_buffer_does_not_open_prompt() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::RequestDelete);
        assert!(app.pending_delete.is_none());
    }

    #[test]
    fn confirm_outside_prompt_is_noop() {
        let mut app = seed_ready_with_cursor("j1");
        app.handle_event(Event::ConfirmDelete);
        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
    }

    #[test]
    fn cancel_clears_pending_delete() {
        let mut app = seed_ready_with_cursor("j1");
        app.handle_event(Event::RequestDelete);
        app.handle_event(Event::CancelDelete);
        assert!(app.pending_delete.is_none());
        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
    }

    #[test]
    fn quit_during_prompt_cancels_instead_of_quitting() {
        let mut app = seed_ready_with_cursor("j1");
        app.handle_event(Event::RequestDelete);
        let quit = app.handle_event(Event::Quit);
        assert!(!quit, "Quit during prompt must not exit the app");
        assert!(app.pending_delete.is_none());

        // Second quit, no prompt active — should now exit.
        assert!(app.handle_event(Event::Quit));
    }

    #[test]
    fn confirm_sends_delete_message_and_removes_row() {
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let mut app = seed_ready_with_cursor("j_to_delete");
        app.set_ws_tx(tx);

        app.handle_event(Event::RequestDelete);
        app.handle_event(Event::ConfirmDelete);

        assert!(app.pending_delete.is_none());
        assert!(app.ready_jobs.is_empty(), "row should be removed locally");

        let msg = rx.try_recv().expect("expected a delete_job message");
        assert!(
            msg.contains("\"type\":\"delete_job\"") && msg.contains("j_to_delete"),
            "expected DeleteJob with id j_to_delete, got: {msg}"
        );
    }

    #[test]
    fn navigation_keys_are_swallowed_while_prompt_pending() {
        let mut app = seed_ready_with_cursor("j1");
        // Add a second job so scrolling has somewhere to go.
        app.ready_jobs.push(job("j2", 0, None));
        app.total_ready = 2;

        app.handle_event(Event::RequestDelete);
        let cursor_before = app.list_states[Tab::Ready.idx()].cursor;

        app.handle_event(Event::ScrollDown);
        app.handle_event(Event::TogglePause);

        assert_eq!(
            app.list_states[Tab::Ready.idx()].cursor,
            cursor_before,
            "navigation should be ignored while prompt is pending"
        );
        assert!(
            !app.paused,
            "pause toggle should be ignored while prompt is pending"
        );
        assert!(
            app.pending_delete.is_some(),
            "prompt should still be pending"
        );
    }
}
