// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TUI application state model.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;

use crate::api::admin::{
    AdminJob, Direction, Fallback, FindAnchor, JobChangeStatus, JobWindow, ListName, SearchQuery,
    ServerStatus, SubscribeAnchor,
};
use crate::license::Tier;

use super::events::{self, Event, InputModeFlag};

/// Which of the two search prompt fields currently receives input.
/// Type takes focus first — it's the more common initial filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SearchField {
    #[default]
    Type,
    Queue,
}

impl SearchField {
    fn toggle(self) -> Self {
        match self {
            SearchField::Type => SearchField::Queue,
            SearchField::Queue => SearchField::Type,
        }
    }
}

/// State of the `/` search prompt: two independent single-line input
/// fields (Type and Queue) plus which one is currently focused. Enter
/// combines both into a `SearchQuery`; Tab switches fields.
#[derive(Clone, Default)]
pub struct SearchPrompt {
    pub type_input: Input,
    pub queue_input: Input,
    pub active: SearchField,
}

impl SearchPrompt {
    pub fn active_input(&self) -> &Input {
        match self.active {
            SearchField::Type => &self.type_input,
            SearchField::Queue => &self.queue_input,
        }
    }

    fn active_input_mut(&mut self) -> &mut Input {
        match self.active {
            SearchField::Type => &mut self.type_input,
            SearchField::Queue => &mut self.queue_input,
        }
    }

    fn switch_field(&mut self) {
        self.active = self.active.toggle();
    }

    /// Split a field's value on commas or whitespace into non-empty
    /// tokens. Lets users filter by multiple queues or types from a
    /// single field (e.g. `emails, billing`).
    fn tokens(value: &str) -> Vec<String> {
        value
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    pub fn to_query(&self) -> SearchQuery {
        SearchQuery {
            queues: Self::tokens(self.queue_input.value()),
            types: Self::tokens(self.type_input.value()),
        }
    }

    /// Build a prompt pre-filled from a previously-submitted query.
    /// Used when `/` is pressed with an active search so the user can
    /// tweak the existing filters instead of retyping from scratch.
    /// Multi-value fields are joined with `, ` (the same delimiter
    /// `to_query` accepts on the way back in).
    pub fn from_query(query: &SearchQuery) -> Self {
        Self {
            type_input: query.types.join(", ").into(),
            queue_input: query.queues.join(", ").into(),
            active: SearchField::default(),
        }
    }
}

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
///
/// Hybrid model: the *authoritative* cursor is `cursor_id` (a job id),
/// but a numeric mirror (`cursor`) is derived from it on each buffer
/// update. Scroll math still works numerically — moves compute a
/// new numeric position, look up the id at that position, and re-pin
/// `cursor_id`. When the queue churns underneath us, `cursor_id`
/// stays anchored to the same *job*; on the next diff/snapshot we
/// recompute the numeric `cursor` from wherever that job now sits.
/// If the job leaves the buffer entirely we fire a `Find { Locate }`
/// to relocate — same mechanism as the pinned-search path.
#[derive(Clone)]
pub struct ListState {
    /// Job id of the selected row — authoritative. `None` when there
    /// is no selection (empty list or transient "cursor lost" state).
    pub cursor_id: Option<String>,
    /// Numeric mirror of `cursor_id`, re-derived on every buffer
    /// update. Kept for the existing scroll math + rendering paths.
    pub cursor: usize,
    /// First visible row in the viewport (numeric position).
    pub scroll_pos: usize,
    /// Position of `items[0]` in the underlying sorted list, as of
    /// the last received `JobWindow`. Zero when there is no buffer.
    pub buffer_offset: usize,
    /// Total items in the list, as of the last received `JobWindow`.
    /// Server's `ServerStatus.total_*` counters are the same value in
    /// steady state but land via a different code path.
    pub total: usize,
    /// Whether the cursor should track the bottom of the list.
    pub follow_bottom: bool,
    /// Last `(SubscribeAnchor, limit)` sent — used to avoid duplicate
    /// requests when `maybe_prefetch` fires repeatedly on the same
    /// underlying state.
    last_subscribe: Option<(SubscribeAnchor, usize)>,
    /// When the last `maybe_prefetch` subscribe was sent for this tab.
    /// Used to rate-limit prefetches so held-down arrow keys and
    /// drain-driven `JobChanged` bursts don't spam the server with
    /// near-identical subscribes at key-repeat / event-arrival cadence.
    last_prefetch_at: Option<Instant>,
}

impl Default for ListState {
    fn default() -> Self {
        Self {
            cursor_id: None,
            cursor: 0,
            scroll_pos: 0,
            buffer_offset: 0,
            total: 0,
            follow_bottom: false,
            last_subscribe: None,
            last_prefetch_at: None,
        }
    }
}

/// Minimum interval between prefetch subscribes for a single tab.
/// The key-based dedup handles held-down arrows while the cursor is
/// stuck at the buffer edge, but once responses start arriving (or
/// drain events start eating into the buffer) the anchor moves and
/// dedup no longer helps. This throttle covers those cases.
const PREFETCH_MIN_INTERVAL: Duration = Duration::from_millis(100);

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
    /// Current search state:
    /// - `search_input`: `Some(text)` while the `/` prompt is open,
    ///   holding the in-progress query string.
    /// - `search_query`: the last query submitted, remembered so `n`
    ///   and `N` can step through subsequent matches.
    /// - `pinned_job_id`: id of the job the cursor is following. When
    ///   set, the cursor tracks this specific job's row wherever it
    ///   moves in the list; on each buffer update we re-locate its
    ///   local index. Cleared when the user presses Esc, when the
    ///   server reports the job no longer exists, or when the user
    ///   opens a fresh search.
    pub search_input: Option<SearchPrompt>,
    pub search_query: Option<SearchQuery>,
    pub pinned_job_id: Option<String>,
    /// True while a `Locate` request for the pinned job is in flight —
    /// suppresses duplicate follow-up requests when a burst of
    /// `JobChanged` events lands before the response.
    pub pinned_locate_pending: bool,
    /// Shared with the terminal input reader — set while the search
    /// prompt is open so the reader forwards raw characters instead
    /// of interpreting shortcut keys.
    pub search_mode_flag: Option<InputModeFlag>,
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
            search_input: self.search_input.clone(),
            search_query: self.search_query.clone(),
            pinned_job_id: self.pinned_job_id.clone(),
            pinned_locate_pending: self.pinned_locate_pending,
            search_mode_flag: self.search_mode_flag.clone(),
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
            search_input: None,
            search_query: None,
            pinned_job_id: None,
            pinned_locate_pending: false,
            search_mode_flag: None,
        }
    }

    pub fn set_ws_tx(&mut self, tx: mpsc::Sender<String>) {
        self.ws_tx = Some(tx);
    }

    pub fn set_search_mode_flag(&mut self, flag: InputModeFlag) {
        self.search_mode_flag = Some(flag);
    }

    fn set_search_mode(&self, on: bool) {
        if let Some(flag) = &self.search_mode_flag {
            flag.store(on, Ordering::Release);
        }
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

    /// Send a subscribe message over WebSocket with the given anchor.
    fn send_subscribe(&self, tab: Tab, anchor: SubscribeAnchor, limit: usize) {
        if let Some(tx) = &self.ws_tx {
            let msg = events::subscribe_message(tab.list_name(), anchor, limit);
            let _ = tx.try_send(msg);
        }
    }

    /// Re-subscribe all tabs with a fresh window centered on each
    /// tab's cursor. Called when the terminal is resized.
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
            let anchor = self.anchor_for_prefetch(tab);
            self.list_states[tab.idx()].last_subscribe = Some((anchor.clone(), limit));
            self.send_subscribe(tab, anchor, limit);
        }
    }

    /// Choose the anchor to use for a `Subscribe` prefetch on `tab`.
    ///
    /// Two navigation intents map to two anchor kinds:
    ///
    /// - **Pinned / following a specific job** (search, `n`/`N`): use
    ///   `Around(pinned_id, Nowhere)` so the cursor tracks that job as
    ///   the queue churns. If the job has been processed the server
    ///   returns an empty window and the client unpins.
    /// - **Depth-based navigation** (arrow keys, page down, plain
    ///   scrolling): use `Offset(cursor_depth - vh)` so the returned
    ///   window sits roughly around the cursor's current depth. Under
    ///   drain the server clamps to the current tail rather than
    ///   returning an empty window — no dead space, no teleporting
    ///   to Head because a specific job vanished.
    ///
    /// `G` and `g` are handled outside this helper: they send `Tail`
    /// and `Head` directly.
    fn anchor_for_prefetch(&self, tab: Tab) -> SubscribeAnchor {
        if let Some(id) = &self.pinned_job_id {
            return SubscribeAnchor::Around {
                id: id.clone(),
                fallback: Fallback::Nowhere,
            };
        }
        let ls = &self.list_states[tab.idx()];
        // Center the requested window on the cursor's depth.
        let half = self.viewport_height.saturating_mul(3) / 2;
        let offset = ls.cursor.saturating_sub(half);
        SubscribeAnchor::Offset { offset }
    }

    /// Check if prefetch is needed and send subscribe if so.
    /// Deduplicates: won't re-send the same anchor + limit for the same tab.
    fn maybe_prefetch(&mut self) {
        if self.subscription_limit.is_some() {
            return;
        }
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

        // Prefetch when cursor is within one viewport of a buffer edge
        // *and* there are more items to fetch in that direction.
        let near_top = ls.cursor < ls.buffer_offset + vh;
        let near_bottom = ls.cursor + vh >= buffer_end;
        let needs_top = near_top && ls.buffer_offset > 0;
        let needs_bottom = near_bottom && buffer_end < self.active_total();

        if !(needs_top || needs_bottom) {
            return;
        }

        let new_limit = vh * 3;
        let anchor = self.anchor_for_prefetch(tab);
        let key = (anchor.clone(), new_limit);
        if self.list_states[tab.idx()].last_subscribe == Some(key.clone()) {
            return;
        }
        // Time-based throttle: even when the anchor has shifted (held
        // arrow key, or drain events chewing the buffer edge), don't
        // fire more than one prefetch per `PREFETCH_MIN_INTERVAL`. The
        // skipped subscribe isn't queued — the next `maybe_prefetch`
        // call after the window elapses will recompute with the
        // current cursor and send a fresh, up-to-date anchor.
        let now = Instant::now();
        if let Some(last) = self.list_states[tab.idx()].last_prefetch_at
            && now.duration_since(last) < PREFETCH_MIN_INTERVAL
        {
            return;
        }
        let ls = &mut self.list_states[tab.idx()];
        ls.last_subscribe = Some(key);
        ls.last_prefetch_at = Some(now);
        self.send_subscribe(tab, anchor, new_limit);
    }

    /// Send the current detail level to the server.
    fn send_detail_level(&self) {
        if let Some(tx) = &self.ws_tx {
            let msg = events::detail_level_message(self.show_detail);
            let _ = tx.try_send(msg);
        }
    }

    /// Send a `Find` over the WebSocket. `limit` is derived from the
    /// current viewport so the returned window fills the visible area.
    fn send_find(&self, tab: Tab, anchor: FindAnchor, direction: Direction, query: SearchQuery) {
        let Some(tx) = &self.ws_tx else {
            return;
        };
        let limit = if self.viewport_height > 0 {
            self.viewport_height * 3
        } else {
            60
        };
        let msg = events::find_message(tab.list_name(), anchor, direction, query, limit);
        let _ = tx.try_send(msg);
    }

    /// Anchor to use for a fresh `/` search, and the fallback anchor
    /// for `n`/`N` when there's no pin.
    ///
    /// Prefers the cursor row's job id — so `/` and `n` continue the
    /// walk forward from wherever the user's eye is, rather than
    /// snapping back to the top of the list. This is what makes
    /// `scroll down → n` feel natural instead of jarring.
    ///
    /// Falls back to `Start` when the cursor row's id isn't known —
    /// initial launch (no data / no scroll yet) and after `g`. That
    /// last case is the escape hatch: `g` then `n` searches from the
    /// beginning of the list.
    fn anchor_for_initial_search(&self) -> FindAnchor {
        if let Some(id) = &self.list_states[self.active_tab.idx()].cursor_id {
            return FindAnchor::JobId { id: id.clone() };
        }
        FindAnchor::Start
    }

    /// Anchor to use for `n`/`N` stepping: the pinned job (so we skip
    /// it), falling back to the cursor's row if the pin is missing.
    fn anchor_for_step(&self) -> FindAnchor {
        if let Some(id) = &self.pinned_job_id {
            return FindAnchor::JobId { id: id.clone() };
        }
        self.anchor_for_initial_search()
    }

    /// Re-anchor the cursor to the currently pinned job (if any). Called
    /// after any buffer mutation (JobSnapshot, JobChanged) so the
    /// cursor "follows" the pinned row as the queue churns around it.
    ///
    /// If the pinned job is in the local buffer, we simply update the
    /// cursor's numeric position to its current local index. If it's
    /// no longer in the buffer, we ask the server to `Locate` it and
    /// send back a fresh window centered on it — cursor and buffer
    /// re-anchor when that response arrives. The `Locate` request is
    /// gated by `pinned_locate_pending` so we don't spam duplicate
    /// requests while one is already in flight.
    fn follow_pinned_job(&mut self) {
        let Some(pinned) = self.pinned_job_id.clone() else {
            return;
        };
        let tab = self.active_tab;
        let jobs = match tab {
            Tab::Ready => &self.ready_jobs,
            Tab::InFlight => &self.in_flight_jobs,
            Tab::Scheduled => &self.scheduled_jobs,
        };
        let buffer_offset = self.list_states[tab.idx()].buffer_offset;
        if let Some(local) = jobs.iter().position(|j| j.id == pinned) {
            let ls = &mut self.list_states[tab.idx()];
            ls.cursor = buffer_offset + local;
            return;
        }
        // Pinned job left the buffer — ask the server where it is now.
        if self.pinned_locate_pending {
            return;
        }
        self.pinned_locate_pending = true;
        self.send_find(
            tab,
            FindAnchor::JobId { id: pinned },
            Direction::Locate,
            SearchQuery::default(),
        );
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

        // Same gating pattern for the `/` search prompt.
        if self.search_input.is_some() {
            match event {
                Event::SearchKey(key) => {
                    if let Some(prompt) = self.search_input.as_mut() {
                        prompt
                            .active_input_mut()
                            .handle_event(&crossterm::event::Event::Key(key));
                    }
                }
                Event::SearchFieldSwitch => {
                    if let Some(prompt) = self.search_input.as_mut() {
                        prompt.switch_field();
                    }
                }
                Event::SearchSubmit => {
                    let prompt = self.search_input.take().unwrap_or_default();
                    self.set_search_mode(false);
                    let query = prompt.to_query();
                    self.search_query = Some(query.clone());
                    self.send_find(
                        self.active_tab,
                        self.anchor_for_initial_search(),
                        Direction::Forward,
                        query,
                    );
                }
                Event::SearchCancel | Event::Quit => {
                    self.search_input = None;
                    self.set_search_mode(false);
                }
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
            Event::SearchOpen => {
                // Prefill from the currently-active query if there is
                // one — lets the user tweak an existing filter instead
                // of retyping. A fresh prompt (no prior query) starts
                // empty.
                self.search_input = Some(
                    self.search_query
                        .as_ref()
                        .map(SearchPrompt::from_query)
                        .unwrap_or_default(),
                );
                self.set_search_mode(true);
            }
            // Outside the search prompt these are no-ops; the prompt
            // path above interprets them.
            Event::SearchKey(_)
            | Event::SearchFieldSwitch
            | Event::SearchSubmit
            | Event::SearchCancel => {}
            Event::SearchNext => {
                if let Some(query) = self.search_query.clone() {
                    self.send_find(
                        self.active_tab,
                        self.anchor_for_step(),
                        Direction::Forward,
                        query,
                    );
                }
            }
            Event::SearchPrev => {
                if let Some(query) = self.search_query.clone() {
                    self.send_find(
                        self.active_tab,
                        self.anchor_for_step(),
                        Direction::Backward,
                        query,
                    );
                }
            }
            Event::Unpin => {
                self.pinned_job_id = None;
            }
            Event::TogglePause => {
                self.paused = !self.paused;
                if !self.paused {
                    // Resync the active tab on unpause so the visible rows
                    // reflect current truth. Other tabs will refresh lazily
                    // on next prefetch or when they become active.
                    let tab = self.active_tab;
                    let ls = &self.list_states[tab.idx()];
                    if let Some((anchor, limit)) = ls.last_subscribe.clone() {
                        self.send_subscribe(tab, anchor, limit);
                    } else if self.viewport_height > 0 {
                        let limit = self.viewport_height * 3;
                        let anchor = self.anchor_for_prefetch(tab);
                        self.send_subscribe(tab, anchor, limit);
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
                self.follow_pinned_job();
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
                self.follow_pinned_job();
                // Each JobChanged can move the follow-bottom cursor; same
                // reasoning as the snapshot/heartbeat paths.
                self.maybe_prefetch();
            }
            Event::ServerFindResult {
                server,
                list,
                matched_position,
                window,
            } => {
                self.apply_server_status(server);
                // Any FindResult clears the "waiting for locate" gate —
                // even a `None` (job gone), since we treat that as an
                // authoritative unpin below.
                self.pinned_locate_pending = false;
                if self.paused {
                    return false;
                }
                let tab = match list {
                    ListName::Ready => Tab::Ready,
                    ListName::InFlight => Tab::InFlight,
                    ListName::Scheduled => Tab::Scheduled,
                };
                match matched_position {
                    Some(pos) => {
                        // The matched job id is whatever sits at the
                        // matched position within the returned window.
                        let local = pos.saturating_sub(window.first_position);
                        let matched_id = window.items.get(local).map(|j| j.id.clone());
                        // Pin the cursor to the match itself, then
                        // apply the window — `sync_cursor_from_id`
                        // will land the numeric cursor on it.
                        self.pinned_job_id = matched_id.clone();
                        self.list_states[tab.idx()].cursor_id = matched_id;
                        self.list_states[tab.idx()].follow_bottom = false;
                        self.apply_job_window(tab, window);
                        if self.viewport_height > 0 {
                            let ls = &mut self.list_states[tab.idx()];
                            let half = self.viewport_height / 2;
                            ls.scroll_pos = ls.cursor.saturating_sub(half);
                        }
                        self.active_tab = tab;
                    }
                    None => {
                        // No match found, or Locate returned "anchor
                        // gone." Either way, drop the pin so the user
                        // isn't chasing a ghost.
                        self.pinned_job_id = None;
                    }
                }
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

    /// Apply a JobWindow snapshot to the corresponding list. Updates
    /// buffer, `first_position`, and `total`; re-derives the numeric
    /// `cursor` from the (authoritative) `cursor_id` so drift under
    /// churn resolves.
    fn apply_job_window(&mut self, tab: Tab, window: JobWindow) {
        let ls = &mut self.list_states[tab.idx()];
        ls.buffer_offset = window.first_position;
        ls.total = window.total;
        ls.last_subscribe = None;
        // Response landed — reopen the prefetch throttle. The rate
        // limit exists to prevent spam *within* the request/response
        // cycle; the natural round-trip cadence is our real ceiling.
        ls.last_prefetch_at = None;
        match tab {
            Tab::Ready => self.ready_jobs = window.items,
            Tab::InFlight => self.in_flight_jobs = window.items,
            Tab::Scheduled => self.scheduled_jobs = window.items,
        }
        self.sync_cursor_from_id(tab);
    }

    /// Sync the numeric cursor to `pinned_job_id`'s current position
    /// in the buffer — but **only** when the user has actively pinned
    /// via search (`/`, `n`, `N`).
    ///
    /// For plain scroll navigation the cursor is a numeric *depth*
    /// that should stay put when the buffer's contents shift
    /// underneath. Vim's model: cursor sits at row N of the viewport,
    /// row N's content may change as items shift, but the cursor row
    /// itself doesn't move. Pulling the cursor onto a specific job
    /// every buffer update is what makes it appear to "jump around"
    /// under heavy churn.
    ///
    /// `follow_bottom` (from `G`) is handled by `apply_follow_bottom`
    /// clamping the numeric cursor to `total - 1` — no id chase
    /// needed there either.
    fn sync_cursor_from_id(&mut self, tab: Tab) {
        let Some(id) = self.pinned_job_id.clone() else {
            return;
        };
        let buf: &[AdminJob] = match tab {
            Tab::Ready => &self.ready_jobs,
            Tab::InFlight => &self.in_flight_jobs,
            Tab::Scheduled => &self.scheduled_jobs,
        };
        if let Some(local) = buf.iter().position(|j| j.id == id) {
            let ls = &mut self.list_states[tab.idx()];
            ls.cursor = ls.buffer_offset + local;
            ls.cursor_id = Some(id);
        }
    }

    /// Cursor navigation bounds for the active tab.
    ///
    /// While live this is the full server-side range. While paused it
    /// clamps to the rows currently in the buffer so the user can only
    /// scroll through the frozen snapshot. Returns `None` when there's
    /// nothing to navigate.
    fn cursor_bounds(&self) -> Option<(usize, usize)> {
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
        let buffer_lo = ls.buffer_offset;
        let buffer_hi = ls.buffer_offset + buffer_size - 1;
        if self.paused {
            return Some((buffer_lo, buffer_hi));
        }
        // Live: the cursor is bounded by (a) what exists on the server
        // and (b) what we currently have in the buffer. Scrolling past
        // the buffered range would render empty rows until the prefetch
        // response arrives; instead we clamp here, fire the prefetch,
        // and let the cursor advance once the new items are in hand.
        let total = self.effective_total();
        if total == 0 {
            return None;
        }
        let max = (total - 1).min(buffer_hi);
        Some((buffer_lo, max))
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
            let buffer_size = match tab {
                Tab::Ready => self.ready_jobs.len(),
                Tab::InFlight => self.in_flight_jobs.len(),
                Tab::Scheduled => self.scheduled_jobs.len(),
            };
            let ls = &mut self.list_states[tab.idx()];

            if total == 0 {
                ls.cursor = 0;
                ls.scroll_pos = 0;
                continue;
            }

            // Remember the pre-clamp viewport row so we can preserve it
            // if we end up dragging the cursor upward below.
            let prev_visual_row = ls.cursor.saturating_sub(ls.scroll_pos);

            // Follow-bottom: stick cursor to end.
            if ls.follow_bottom {
                ls.cursor = total - 1;
            }

            // Clamp cursor if total shrunk below it.
            if ls.cursor >= total {
                ls.cursor = total - 1;
            }

            // Clamp cursor to the buffered range too: parking on an
            // unloaded row would render empties until the prefetch
            // response arrives. Keep the cursor at the buffer edge and
            // let it advance once the new items land.
            let buffer_hi = ls.buffer_offset + buffer_size;
            let cursor_was_clamped_down = buffer_size > 0 && ls.cursor >= buffer_hi;
            if cursor_was_clamped_down {
                ls.cursor = buffer_hi - 1;
            }

            // Position the viewport so the cursor is visible.
            //
            // Under follow-bottom we always park the cursor at the
            // last viewport row: as `total` shrinks (queue drains),
            // this keeps the tail rows visible above the cursor
            // instead of leaving a screenful of blank space below it
            // (which is what happens if we naively pin `scroll_pos`
            // to the cursor's numeric value).
            //
            // When we've had to drag the cursor upward because the
            // buffer's tail was chewed away by JobChanged removals,
            // keep the cursor at the same viewport row it was on
            // before — otherwise the highlighted row visibly "jumps
            // up" every time the buffer shrinks. `scroll_pos` slides
            // up in lockstep with the cursor so the visual row is
            // preserved.
            //
            // Otherwise we just clamp `scroll_pos` around the cursor
            // in the usual bidirectional way.
            if self.viewport_height > 0 {
                if ls.follow_bottom {
                    ls.scroll_pos = ls.cursor.saturating_sub(self.viewport_height - 1);
                } else if cursor_was_clamped_down {
                    ls.scroll_pos = ls
                        .cursor
                        .saturating_sub(prev_visual_row)
                        .max(ls.buffer_offset);
                } else if ls.cursor < ls.scroll_pos {
                    ls.scroll_pos = ls.cursor;
                } else if ls.cursor >= ls.scroll_pos + self.viewport_height {
                    ls.scroll_pos = ls.cursor - self.viewport_height + 1;
                }
            }
        }
    }

    /// Release a search pin. Called at the start of every explicit
    /// navigation action so `follow_pinned_job` doesn't drag the
    /// cursor back to the pinned row on the next tick.
    fn release_pin(&mut self) {
        self.pinned_job_id = None;
        self.pinned_locate_pending = false;
    }

    /// Sync `cursor_id` to whichever row currently sits under the
    /// numeric cursor. Called at the end of every explicit scroll so
    /// a subsequent buffer refresh doesn't drag the cursor back to
    /// whatever `cursor_id` was left over from a prior search — that
    /// would produce the "snap back to the pinned row" bug when
    /// scrolling past the buffer edge.
    fn refresh_cursor_id_from_position(&mut self, tab: Tab) {
        let ls = &mut self.list_states[tab.idx()];
        let buf: &[AdminJob] = match tab {
            Tab::Ready => &self.ready_jobs,
            Tab::InFlight => &self.in_flight_jobs,
            Tab::Scheduled => &self.scheduled_jobs,
        };
        let local = ls.cursor.checked_sub(ls.buffer_offset);
        ls.cursor_id = local.and_then(|i| buf.get(i)).map(|j| j.id.clone());
    }

    fn scroll_up(&mut self) {
        self.release_pin();
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
        let tab = self.active_tab;
        self.refresh_cursor_id_from_position(tab);
        self.maybe_prefetch();
    }

    fn scroll_down(&mut self) {
        self.release_pin();
        let Some((_, max)) = self.cursor_bounds() else {
            return;
        };
        let total = self.effective_total();
        let ls = &mut self.list_states[self.active_tab.idx()];
        if ls.cursor < max {
            ls.cursor += 1;
        }
        if self.viewport_height > 0 && ls.cursor >= ls.scroll_pos + self.viewport_height {
            ls.scroll_pos = ls.cursor - self.viewport_height + 1;
        }
        // `follow_bottom` only makes sense while live and only when
        // the cursor has genuinely reached the server-side tail —
        // stopping at the buffer edge because prefetch hasn't landed
        // yet shouldn't turn on follow-bottom mid-list.
        ls.follow_bottom = !self.paused && total > 0 && ls.cursor == total - 1;
        let tab = self.active_tab;
        self.refresh_cursor_id_from_position(tab);
        self.maybe_prefetch();
    }

    fn page_up(&mut self) {
        self.release_pin();
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
        let tab = self.active_tab;
        self.refresh_cursor_id_from_position(tab);
        self.maybe_prefetch();
    }

    fn page_down(&mut self) {
        self.release_pin();
        let Some((_, max)) = self.cursor_bounds() else {
            return;
        };
        if self.viewport_height == 0 {
            return;
        }
        let total = self.effective_total();
        let ls = &mut self.list_states[self.active_tab.idx()];
        let jump = self.viewport_height.saturating_sub(1).max(1);
        ls.cursor = (ls.cursor + jump).min(max);
        if ls.cursor >= ls.scroll_pos + self.viewport_height {
            ls.scroll_pos = ls.cursor - self.viewport_height + 1;
        }
        ls.follow_bottom = !self.paused && total > 0 && ls.cursor == total - 1;
        let tab = self.active_tab;
        self.refresh_cursor_id_from_position(tab);
        self.maybe_prefetch();
    }

    fn go_to_start(&mut self) {
        self.release_pin();
        let Some((min, _)) = self.cursor_bounds() else {
            return;
        };
        let ls = &mut self.list_states[self.active_tab.idx()];
        ls.cursor = min;
        ls.scroll_pos = min;
        ls.follow_bottom = false;
        // Clear cursor_id so sync_cursor_from_id derives it from the
        // freshly-arriving Head window rather than trying to preserve
        // whatever the cursor was on before.
        ls.cursor_id = None;
        let tab = self.active_tab;
        let limit = self.viewport_height.saturating_mul(3).max(1);
        // `g` explicitly asks for the head, not a depth-anchored
        // window. Bypass the usual `anchor_for_prefetch` heuristic.
        self.send_subscribe(tab, SubscribeAnchor::Head, limit);
    }

    fn go_to_end(&mut self) {
        self.release_pin();
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
        // Clear cursor_id — sync_cursor_from_id will re-derive it
        // (from `buf.last()` since follow_bottom is set).
        ls.cursor_id = None;
        let tab = self.active_tab;
        let limit = self.viewport_height.saturating_mul(3).max(1);
        // `G` explicitly asks for the tail. Under drain, subsequent
        // `maybe_prefetch` calls will use `Offset(cursor - vh)` and
        // clamp naturally.
        self.send_subscribe(tab, SubscribeAnchor::Tail, limit);
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
                items: vec![job("r1", 0, None)],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![job("w1", 0, Some(100))],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![job("s1", 0, None)],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: (100..160).map(|i| job(&format!("j{i}"), 0, None)).collect(),
                first_position: 100,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: vec![],
                first_position: 969,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: vec![job("snap", 0, None)],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
                first_position: 100,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
                first_position: 100,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
                first_position: 100,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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
                items: (100..120).map(|i| job(&format!("j{i}"), 0, None)).collect(),
                first_position: 100,
                total: 0,
                resolved_anchor: None,
            },
            in_flight: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
            scheduled: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
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

    // ── SearchPrompt.to_query ─────────────────────────────────────

    #[test]
    fn search_prompt_empty_is_empty_query() {
        let prompt = SearchPrompt::default();
        let q = prompt.to_query();
        assert!(q.queues.is_empty());
        assert!(q.types.is_empty());
    }

    #[test]
    fn search_prompt_queue_and_type_from_fields() {
        let mut prompt = SearchPrompt::default();
        prompt.type_input = "send_welcome".into();
        prompt.queue_input = "emails".into();
        let q = prompt.to_query();
        assert_eq!(q.queues, vec!["emails".to_string()]);
        assert_eq!(q.types, vec!["send_welcome".to_string()]);
    }

    #[test]
    fn search_prompt_splits_on_comma_and_whitespace() {
        let mut prompt = SearchPrompt::default();
        prompt.queue_input = "emails, billing  reports".into();
        let q = prompt.to_query();
        assert_eq!(
            q.queues,
            vec![
                "emails".to_string(),
                "billing".to_string(),
                "reports".to_string()
            ]
        );
    }

    #[test]
    fn search_prompt_ignores_empty_tokens() {
        let mut prompt = SearchPrompt::default();
        prompt.type_input = " , ,send, ".into();
        let q = prompt.to_query();
        assert_eq!(q.types, vec!["send".to_string()]);
    }

    // ── search flow end-to-end ────────────────────────────────────

    fn search_char_key(c: char) -> crossterm::event::KeyEvent {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)
    }

    #[test]
    fn search_open_sets_input_state() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::SearchOpen);
        assert!(app.search_input.is_some());
    }

    #[test]
    fn search_cancel_clears_input_without_running_search() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::SearchOpen);
        for c in "abc".chars() {
            app.handle_event(Event::SearchKey(search_char_key(c)));
        }
        app.handle_event(Event::SearchCancel);
        assert!(app.search_input.is_none());
        assert!(app.search_query.is_none());
    }

    #[test]
    fn search_submit_captures_query() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::SearchOpen);
        // Default active field is Type — type "welcome", switch to
        // Queue, type "emails". Enter combines them.
        for c in "welcome".chars() {
            app.handle_event(Event::SearchKey(search_char_key(c)));
        }
        app.handle_event(Event::SearchFieldSwitch);
        for c in "emails".chars() {
            app.handle_event(Event::SearchKey(search_char_key(c)));
        }
        app.handle_event(Event::SearchSubmit);
        assert!(app.search_input.is_none());
        let q = app.search_query.expect("query should be captured");
        assert_eq!(q.queues, vec!["emails".to_string()]);
        assert_eq!(q.types, vec!["welcome".to_string()]);
    }

    #[test]
    fn initial_search_anchor_uses_cursor_id_when_set() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.active_tab = Tab::Ready;
        app.list_states[Tab::Ready.idx()].cursor_id = Some("j_current".into());
        assert!(matches!(
            app.anchor_for_initial_search(),
            FindAnchor::JobId { ref id } if id == "j_current"
        ));
    }

    #[test]
    fn initial_search_anchor_falls_back_to_start_without_cursor_id() {
        let app = App::new("127.0.0.1:8901".into());
        assert!(matches!(app.anchor_for_initial_search(), FindAnchor::Start));
    }

    #[test]
    fn step_anchor_prefers_pin_over_cursor_id() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.active_tab = Tab::Ready;
        app.list_states[Tab::Ready.idx()].cursor_id = Some("j_cursor".into());
        app.pinned_job_id = Some("j_pinned".into());
        assert!(matches!(
            app.anchor_for_step(),
            FindAnchor::JobId { ref id } if id == "j_pinned"
        ));
    }

    #[test]
    fn step_anchor_falls_back_to_cursor_id_when_unpinned() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.active_tab = Tab::Ready;
        app.list_states[Tab::Ready.idx()].cursor_id = Some("j_cursor".into());
        assert!(matches!(
            app.anchor_for_step(),
            FindAnchor::JobId { ref id } if id == "j_cursor"
        ));
    }

    #[test]
    fn search_open_prefills_from_active_query() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.search_query = Some(SearchQuery {
            queues: vec!["emails".into(), "billing".into()],
            types: vec!["audit".into()],
        });
        app.handle_event(Event::SearchOpen);
        let prompt = app.search_input.as_ref().expect("prompt should be open");
        assert_eq!(prompt.type_input.value(), "audit");
        assert_eq!(prompt.queue_input.value(), "emails, billing");
    }

    #[test]
    fn search_open_without_active_query_starts_empty() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::SearchOpen);
        let prompt = app.search_input.as_ref().expect("prompt should be open");
        assert!(prompt.type_input.value().is_empty());
        assert!(prompt.queue_input.value().is_empty());
    }

    #[test]
    fn search_prompt_from_query_round_trip() {
        let query = SearchQuery {
            queues: vec!["a".into(), "b".into()],
            types: vec!["x".into(), "y".into()],
        };
        let prompt = SearchPrompt::from_query(&query);
        assert_eq!(prompt.to_query(), query);
    }

    #[test]
    fn search_field_switch_toggles_active_field() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::SearchOpen);
        assert_eq!(app.search_input.as_ref().unwrap().active, SearchField::Type);
        app.handle_event(Event::SearchFieldSwitch);
        assert_eq!(
            app.search_input.as_ref().unwrap().active,
            SearchField::Queue
        );
        app.handle_event(Event::SearchFieldSwitch);
        assert_eq!(app.search_input.as_ref().unwrap().active, SearchField::Type);
    }

    #[test]
    fn find_result_pins_job_and_snaps_cursor() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.active_tab = Tab::Ready;
        app.viewport_height = 20;
        // window.offset=1 → items[0] is at filtered position 1,
        // items[2] at position 3. matched_position=3 must land on
        // items[3 - offset] = items[2] = "target".
        app.handle_event(Event::ServerFindResult {
            server: default_server(),
            list: ListName::Ready,
            matched_position: Some(3),
            window: JobWindow {
                items: vec![
                    job("a", 0, None),
                    job("b", 0, None),
                    job("target", 0, None),
                    job("d", 0, None),
                    job("e", 0, None),
                ],
                first_position: 1,
                total: 0,
                resolved_anchor: None,
            },
        });
        assert_eq!(app.pinned_job_id.as_deref(), Some("target"));
        let ls = &app.list_states[Tab::Ready.idx()];
        assert_eq!(ls.cursor, 3);
        assert_eq!(ls.buffer_offset, 1);
    }

    #[test]
    fn find_result_none_clears_pin() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.pinned_job_id = Some("stale".into());
        app.pinned_locate_pending = true;
        app.handle_event(Event::ServerFindResult {
            server: default_server(),
            list: ListName::Ready,
            matched_position: None,
            window: JobWindow {
                items: vec![],
                first_position: 0,
                total: 0,
                resolved_anchor: None,
            },
        });
        assert!(app.pinned_job_id.is_none());
        assert!(!app.pinned_locate_pending);
    }

    #[test]
    fn unpin_event_clears_pinned_job() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.pinned_job_id = Some("some-id".into());
        app.handle_event(Event::Unpin);
        assert!(app.pinned_job_id.is_none());
    }
}
