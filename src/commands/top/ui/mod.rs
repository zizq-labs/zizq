// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Ratatui rendering for the TUI dashboard.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use super::app::{App, ConnectionStatus};

mod depth_bar;
mod detail_panel;
mod format;
mod header;
mod help_bar;
mod job_table;
mod tabs;

/// Render the current application state to a terminal frame.
pub fn render(app: &mut App, frame: &mut Frame) {
    // Clamp horizontal scroll so it never exceeds what the current
    // viewport actually needs. This prevents accumulating scroll
    // offset while the viewport is wide enough not to scroll, which
    // would cause a confusing jump when the window is later resized
    // smaller.
    let viewport_width = frame.area().width;
    let max_scroll = MIN_TABLE_WIDTH.saturating_sub(viewport_width);
    for s in &mut app.h_scroll {
        *s = (*s).min(max_scroll);
    }
    let chunks = Layout::vertical([
        Constraint::Length(7), // header
        Constraint::Length(1), // tab bar
        Constraint::Min(0),    // content table
        Constraint::Length(1), // help bar
    ])
    .split(frame.area());

    header::render(app, frame, chunks[0]);

    tabs::render(app, frame, chunks[1]);

    // Content area — render only the active tab's table, with the detail
    // panel beside it at full height when the terminal is wide enough.
    let side_width = side_panel_width(chunks[2].width).filter(|_| app.show_detail);
    let (content_area, side_area) = match side_width {
        Some(width) => {
            let [table, panel] =
                Layout::horizontal([Constraint::Min(0), Constraint::Length(width)])
                    .spacing(1)
                    .areas(chunks[2]);
            (table, Some(panel))
        }
        None => (chunks[2], None),
    };

    let subscription_limit = if matches!(app.status, ConnectionStatus::Connected) {
        app.subscription_limit
    } else {
        None
    };

    // Split content area into table + optional detail panel + optional cap message.
    // Panel height: min(DETAIL_PANEL_MAX, 30% of content area).
    // Suppressed if the table would get fewer than MIN_TABLE_ROWS body rows.
    let detail_height = DETAIL_PANEL_MAX.min((content_area.height * 30 / 100).max(1));
    let show_detail_panel = app.show_detail
        && side_area.is_none()
        && content_area.height > detail_height + MIN_TABLE_ROWS + 1;

    let (table_area, detail_area, msg_area) = {
        let mut constraints = vec![Constraint::Min(0)]; // table always first

        if show_detail_panel {
            constraints.push(Constraint::Length(detail_height));
        }
        if subscription_limit.is_some() {
            constraints.push(Constraint::Length(4));
        }

        let split = Layout::vertical(constraints).split(content_area);
        let mut idx = 1;

        let detail = if show_detail_panel {
            let area = split[idx];
            idx += 1;
            Some(area)
        } else {
            None
        };

        let msg = if subscription_limit.is_some() {
            Some(split[idx])
        } else {
            None
        };

        (split[0], detail, msg)
    };

    // Store the viewport height (minus 1 for the header row) so
    // scroll logic can reference it. If it changed (terminal resize),
    // re-subscribe with an updated window size.
    let table_body_height = table_area.height.saturating_sub(1) as usize;
    if table_body_height != app.viewport_height {
        app.viewport_height = table_body_height;
        app.resubscribe_all();
    }

    job_table::render_active(app, frame, table_area, table_body_height);

    // The scroll position belongs to one job; another starts at the top.
    let selected = app.selected_job_id();
    if app.detail_view.job != selected {
        app.detail_view.job = selected;
        app.detail_view.scroll = 0;
    }
    let panel = detail_area
        .map(|area| (area, detail_panel::Placement::Below))
        .or(side_area.map(|area| (area, detail_panel::Placement::Beside)));
    // A terminal too short for the panel leaves focus with the list.
    if panel.is_none() {
        app.detail_view.focused = false;
    }
    if let Some((area, placement)) = panel {
        // Below the table the panel's headings stay put as it scrolls.
        app.detail_view.page = match placement {
            detail_panel::Placement::Below => area.height.saturating_sub(1),
            detail_panel::Placement::Beside => area.height,
        } as usize;
        if let Some(scroll) = detail_panel::render(app, frame, area, placement) {
            app.detail_view.scroll = scroll;
        }
    }

    if let Some(cap) = subscription_limit {
        let msg = Paragraph::new(vec![
            Line::default(),
            Line::from(format!("Display is currently capped at {cap} jobs.")),
            Line::from("Upgrade to a pro license to see everything."),
            Line::default(),
        ])
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Center);
        frame.render_widget(msg, msg_area.unwrap());
    }

    help_bar::render(app, frame, chunks[3], panel.is_some());
}

/// Minimum width for the table content area. When the viewport is narrower,
/// the table renders at this width and the visible portion is determined by
/// the horizontal scroll offset.
pub(super) const MIN_TABLE_WIDTH: u16 = 120;

/// The narrowest a detail panel beside the table can be.
const SIDE_PANEL_MIN_WIDTH: u16 = 50;

/// The width of a detail panel beside the table, or `None` when the
/// terminal is too narrow to fit one and the table at `MIN_TABLE_WIDTH`, so
/// the panel goes below the table instead. About a third of the width,
/// within those limits.
fn side_panel_width(width: u16) -> Option<u16> {
    // One column separates the table from the panel.
    let room = width.checked_sub(MIN_TABLE_WIDTH + 1)?;
    (room >= SIDE_PANEL_MIN_WIDTH).then(|| (width * 35 / 100).clamp(SIDE_PANEL_MIN_WIDTH, room))
}

/// Maximum height of the detail panel (heading + all possible fields).
const DETAIL_PANEL_MAX: u16 = 13;

/// Minimum table body rows required before showing the detail panel.
const MIN_TABLE_ROWS: u16 = 3;

/// Foreground color for tab labels and table header text.
pub(super) const fn tab_fg() -> Color {
    Color::Black
}

/// Background color of headings: the active tab and the table header row,
/// or the detail panel's section headings. Green marks the pane with focus
/// (the one the movement keys act on), yellow the other.
pub(super) const fn header_bg(focused: bool) -> Color {
    if focused { Color::Green } else { Color::Yellow }
}

/// Whether the job list has focus, rather than the detail panel.
pub(super) fn list_has_focus(app: &App) -> bool {
    !(app.show_detail && app.detail_view.focused)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::admin::{AdminJob, AdminJobError};
    use crate::commands::top::app::Tab;
    use crate::license::Tier;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    /// Render the app state to a fixed-size terminal and return the
    /// buffer contents as a string suitable for snapshot comparison.
    fn render_to_string(app: &App, width: u16, height: u16) -> String {
        let mut app = app.clone();
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        let frame = terminal.draw(|f| render(&mut app, f)).unwrap();
        let buf = &frame.buffer;
        let mut lines = Vec::new();
        for y in 0..buf.area.height {
            let mut line = String::new();
            for x in 0..buf.area.width {
                line.push_str(buf[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }
        lines.join("\n")
    }

    /// Assert a snapshot with filters that normalise dynamic content.
    /// The client version is replaced with a stable value so version
    /// bumps don't break snapshot tests.
    macro_rules! assert_ui_snapshot {
        ($($arg:tt)*) => {{
            let mut settings = insta::Settings::clone_current();
            settings.add_filter(r"Zizq \d+\.\d+\.\d+", "Zizq 0.1.0");
            // Timestamps are shown in the local time zone, which differs
            // between machines. The placeholder keeps the same width so the
            // columns after it stay put.
            settings.add_filter(
                r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{2}:\d{2}",
                "YYYY-MM-DD HH:MM:SS +HH:MM",
            );
            settings.bind(|| {
                insta::assert_snapshot!($($arg)*);
            });
        }};
    }

    fn new_app() -> App {
        App {
            host: "127.0.0.1:8901".to_string(),
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        }
    }

    #[test]
    fn render_connecting() {
        let app = new_app();
        assert_ui_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_paused_shows_indicator_and_swaps_help_labels() {
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        app.paused = true;
        assert_ui_snapshot!(render_to_string(&app, 60, 10));
    }

    /// Too short a terminal for the detail panel asks in the help bar,
    /// naming the job, since it isn't on screen.
    #[test]
    fn render_delete_prompt_in_the_help_bar_without_a_panel() {
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        app.pending_delete = Some("j_abc123".into());
        assert_ui_snapshot!(render_to_string(&app, 80, 10));
    }

    #[test]
    fn render_search_prompt_empty() {
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        app.search_input = Some(crate::commands::top::app::SearchPrompt::default());
        assert_ui_snapshot!(render_to_string(&app, 80, 10));
    }

    #[test]
    fn render_search_prompt_with_typed_type_field() {
        use crate::commands::top::app::SearchPrompt;
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        let mut prompt = SearchPrompt::default();
        prompt.type_input = "send_welcome".into();
        app.search_input = Some(prompt);
        assert_ui_snapshot!(render_to_string(&app, 80, 10));
    }

    #[test]
    fn render_search_prompt_active_queue_field() {
        use crate::commands::top::app::{SearchField, SearchPrompt};
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        let mut prompt = SearchPrompt::default();
        prompt.type_input = "send_welcome".into();
        prompt.queue_input = "emails".into();
        prompt.active = SearchField::Queue;
        app.search_input = Some(prompt);
        assert_ui_snapshot!(render_to_string(&app, 80, 10));
    }

    #[test]
    fn render_paused_at_buffer_edge_shows_resume_hint() {
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        app.paused = true;
        app.active_tab = Tab::Ready;
        app.total_ready = 2;
        app.ready_jobs = vec![
            job_admin("j1", "queue1", "type1"),
            job_admin("j2", "queue1", "type1"),
        ];
        app.list_states[Tab::Ready.idx()].buffer_offset = 0;
        // Cursor at last row → bottom edge → "↓ Resume to scroll further".
        app.list_states[Tab::Ready.idx()].cursor = 1;
        assert_ui_snapshot!(render_to_string(&app, 90, 12));
    }

    fn job_admin(id: &str, queue: &str, job_type: &str) -> AdminJob {
        AdminJob {
            id: id.into(),
            queue: queue.into(),
            job_type: job_type.into(),
            priority: 0,
            ready_at: 0,
            attempts: 0,
            dequeued_at: None,
            failed_at: None,
            payload: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
        }
    }

    #[test]
    fn render_connected_with_server_info() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(65_000),
            server_tier: Some(Tier::Pro),
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        };
        assert_ui_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_disconnected() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Disconnected,
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        };
        assert_ui_snapshot!(render_to_string(&app, 60, 10));
    }

    fn realistic_job(
        id: &str,
        queue: &str,
        job_type: &str,
        priority: u16,
        ready_at: u64,
        attempts: u32,
        dequeued_at: Option<u64>,
        failed_at: Option<u64>,
    ) -> AdminJob {
        AdminJob {
            id: id.to_string(),
            queue: queue.to_string(),
            job_type: job_type.to_string(),
            priority,
            ready_at,
            attempts,
            dequeued_at,
            failed_at,
            payload: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
        }
    }

    fn sample_app(active_tab: Tab) -> App {
        let now_ms = 1_700_000_000_000u64;
        App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(120_000),
            server_tier: Some(Tier::Pro),
            subscription_limit: None,
            total_ready: 2,
            total_in_flight: 2,
            total_scheduled: 2,
            ready_jobs: vec![
                realistic_job(
                    "0195b70a3cc87a1f0890da08e8b3cc86",
                    "default",
                    "generate_annual_report_email",
                    0,
                    now_ms - 5_200,
                    0,
                    None,
                    None,
                ),
                realistic_job(
                    "0195b70a3dd47c2308a1fb09e9c4dd97",
                    "default",
                    "send_welcome_email",
                    5,
                    now_ms - 62_000,
                    2,
                    None,
                    None,
                ),
            ],
            in_flight_jobs: vec![
                realistic_job(
                    "0195b70a2bb46e1f07a0c908d7a2bb40",
                    "default",
                    "generate_annual_report_email",
                    0,
                    now_ms - 10_000,
                    1,
                    Some(now_ms - 3_100),
                    None,
                ),
                realistic_job(
                    "0195b70a1aa35d0e06b0b807c691aa30",
                    "billing",
                    "charge_subscription",
                    0,
                    now_ms - 200_000,
                    3,
                    Some(now_ms - 150_000),
                    Some(now_ms - 60_000),
                ),
            ],
            scheduled_jobs: vec![
                realistic_job(
                    "0195b70a4ee89f2f09a1eb0af9d5ee80",
                    "default",
                    "send_reminder_email",
                    0,
                    now_ms + 232_000,
                    0,
                    None,
                    None,
                ),
                realistic_job(
                    "0195b70a5ff9a03f0ab1fc0b0ae6ff90",
                    "reports",
                    "generate_quarterly_report",
                    0,
                    now_ms + 3_600_000,
                    1,
                    None,
                    None,
                ),
            ],
            now_ms,
            active_tab,
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        }
    }

    #[test]
    fn render_ready_tab_wide() {
        let app = sample_app(Tab::Ready);
        assert_ui_snapshot!(render_to_string(&app, 160, 12));
    }

    #[test]
    fn render_ready_tab_narrow() {
        let app = sample_app(Tab::Ready);
        assert_ui_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_in_flight_tab_wide() {
        let app = sample_app(Tab::InFlight);
        assert_ui_snapshot!(render_to_string(&app, 160, 12));
    }

    #[test]
    fn render_in_flight_tab_narrow() {
        let app = sample_app(Tab::InFlight);
        assert_ui_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_in_flight_tab_narrow_scrolled() {
        let mut app = sample_app(Tab::InFlight);
        app.h_scroll[Tab::InFlight.idx()] = 20;
        assert_ui_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_in_flight_tab_narrow_scrolled_max() {
        let mut app = sample_app(Tab::InFlight);
        app.h_scroll[Tab::InFlight.idx()] = 40;
        assert_ui_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_scheduled_tab_wide() {
        let app = sample_app(Tab::Scheduled);
        assert_ui_snapshot!(render_to_string(&app, 160, 12));
    }

    #[test]
    fn render_scheduled_tab_narrow() {
        let app = sample_app(Tab::Scheduled);
        assert_ui_snapshot!(render_to_string(&app, 80, 12));
    }

    /// A failure of the sample jobs, `minutes_ago` before the fixtures' now.
    fn failure(attempt: u32, minutes_ago: u64, message: &str) -> AdminJobError {
        let failed_at = 1_700_000_000_000u64 - minutes_ago * 60_000;
        AdminJobError {
            attempt,
            message: message.to_string(),
            error_type: Some("ForkError".to_string()),
            backtrace: Some("worker.sh:12 in spawn\nworker.sh:40 in run".to_string()),
            dequeued_at: failed_at - 5_000,
            failed_at,
        }
    }

    /// The sample app on the in-flight tab with the detail panel open on
    /// its first job, as the server describes it with `errors`.
    fn detail_app(errors: Vec<AdminJobError>, more_errors: bool) -> App {
        use crate::api::admin::{AdminBackoff, AdminRetention};
        use crate::commands::top::app::JobDetail;

        let mut app = sample_app(Tab::InFlight);
        app.show_detail = true;

        let mut job = app.in_flight_jobs[0].clone();
        job.payload = Some(serde_json::json!({
            "to": "user@example.com",
            "subject": "Welcome!",
            "urgent": true,
        }));
        job.retry_limit = Some(25);
        job.backoff = Some(AdminBackoff {
            base_ms: 15000,
            exponent: 4.0,
            jitter_ms: 30000,
        });
        job.retention = Some(AdminRetention {
            completed_ms: Some(604_800_000),
            dead_ms: Some(2_592_000_000),
        });
        job.unique_key = Some("report:annual:2026".to_string());
        job.unique_while = Some("active".to_string());

        app.job_detail = Some(JobDetail {
            id: job.id.clone(),
            job: Some(job),
            status: Some("in_flight".to_string()),
            errors,
            more_errors,
        });
        app
    }

    fn three_failures() -> Vec<AdminJobError> {
        vec![
            failure(
                3,
                2,
                "fork failed: cannot allocate memory\nwhile spawning `convert`",
            ),
            failure(2, 9, "fork failed: cannot allocate memory"),
            failure(1, 14, "process exited with status 141"),
        ]
    }

    #[test]
    fn render_detail_panel() {
        let app = detail_app(three_failures(), false);
        assert_ui_snapshot!(render_to_string(&app, 160, 30));
    }

    #[test]
    fn render_detail_panel_narrow_shows_failures_over_the_payload() {
        let app = detail_app(three_failures(), false);
        assert_ui_snapshot!(render_to_string(&app, 130, 30));
    }

    #[test]
    fn render_detail_panel_narrow_without_failures_shows_the_payload() {
        let app = detail_app(Vec::new(), false);
        assert_ui_snapshot!(render_to_string(&app, 130, 30));
    }

    #[test]
    fn render_detail_panel_with_more_failures_than_fit() {
        let errors = (1..=10)
            .rev()
            .map(|n| failure(n, 20 - n as u64, "boom"))
            .collect();
        let app = detail_app(errors, true);
        assert_ui_snapshot!(render_to_string(&app, 160, 30));
    }

    #[test]
    fn side_panel_width_leaves_the_table_its_minimum() {
        assert_eq!(side_panel_width(170), None);
        assert_eq!(side_panel_width(171), Some(50));
        assert_eq!(side_panel_width(200), Some(70));
        assert_eq!(side_panel_width(250), Some(87));
    }

    #[test]
    fn render_detail_panel_beside_the_table() {
        let app = detail_app(three_failures(), false);
        assert_ui_snapshot!(render_to_string(&app, 200, 40));
    }

    #[test]
    fn render_detail_panel_beside_the_table_with_a_long_payload() {
        let mut app = detail_app(three_failures(), false);
        let job = app.job_detail.as_mut().unwrap().job.as_mut().unwrap();
        job.payload = Some(serde_json::json!({
            "recipients": (1..=30).map(|n| format!("user{n}@example.com")).collect::<Vec<_>>(),
        }));
        assert_ui_snapshot!(render_to_string(&app, 200, 40));
    }

    /// `detail_app`, with the panel focused and scrolled `scroll` lines,
    /// and a payload long enough to need it.
    fn focused_detail_app(scroll: usize) -> App {
        let mut app = detail_app(three_failures(), false);
        let job = app.job_detail.as_mut().unwrap().job.as_mut().unwrap();
        job.payload = Some(serde_json::json!({
            "recipients": (1..=30).map(|n| format!("user{n}@example.com")).collect::<Vec<_>>(),
        }));
        app.detail_view.focused = true;
        app.detail_view.scroll = scroll;
        app.detail_view.job = Some(app.in_flight_jobs[0].id.clone());
        app
    }

    #[test]
    fn render_focused_detail_panel_beside_the_table_shows_everything() {
        // Scrolled past the attributes to the payload, which is uncapped,
        // down to every failure in full.
        let app = focused_detail_app(30);
        assert_ui_snapshot!(render_to_string(&app, 200, 40));
    }

    #[test]
    fn render_focused_detail_panel_below_the_table_scrolls_under_its_headings() {
        let app = focused_detail_app(4);
        assert_ui_snapshot!(render_to_string(&app, 160, 40));
    }

    #[test]
    fn scrolling_the_detail_panel_stops_at_its_end() {
        let mut app = focused_detail_app(usize::MAX);
        let mut terminal = Terminal::new(TestBackend::new(200, 40)).unwrap();

        terminal.draw(|f| render(&mut app, f)).unwrap();

        // Scrolled so the panel's last line is on its bottom row: further
        // would only leave rows blank.
        let last_row = 40 - 2;
        let mut terminal = Terminal::new(TestBackend::new(200, 40)).unwrap();
        let mut at_end = app.clone();
        let frame = terminal.draw(|f| render(&mut at_end, f)).unwrap();
        let last_line: String = (130..200)
            .map(|x| frame.buffer[(x, last_row)].symbol())
            .collect();
        assert_eq!(
            last_line.trim(),
            "worker.sh:40 in run",
            "the oldest backtrace"
        );
        assert_eq!(at_end.detail_view.scroll, app.detail_view.scroll);
    }

    #[test]
    fn another_job_starts_the_detail_panel_at_the_top() {
        let mut app = focused_detail_app(10);
        app.detail_view.job = Some("another".into());
        let mut terminal = Terminal::new(TestBackend::new(200, 40)).unwrap();

        terminal.draw(|f| render(&mut app, f)).unwrap();

        assert_eq!(app.detail_view.scroll, 0);
    }

    #[test]
    fn green_headings_mark_the_pane_with_focus() {
        use ratatui::style::Color;

        // The active tab's label and the panel's first heading.
        let colours = |app: &App| {
            let mut app = app.clone();
            let mut terminal = Terminal::new(TestBackend::new(200, 40)).unwrap();
            let frame = terminal.draw(|f| render(&mut app, f)).unwrap();
            let buf = &frame.buffer;
            let tab = (0..200)
                .map(|x| &buf[(x, 7)])
                .find(|c| c.symbol() == "I")
                .unwrap()
                .bg;
            let panel = buf[(132, 8)].bg;
            (tab, panel)
        };

        let mut app = focused_detail_app(0);
        assert_eq!(colours(&app), (Color::Yellow, Color::Green));
        app.detail_view.focused = false;
        assert_eq!(colours(&app), (Color::Green, Color::Yellow));
    }

    #[test]
    fn render_delete_prompt_in_the_detail_panel() {
        let mut app = detail_app(three_failures(), false);
        app.pending_delete = Some(app.in_flight_jobs[0].id.clone());
        assert_ui_snapshot!(render_to_string(&app, 200, 40));
    }

    #[test]
    fn render_delete_prompt_in_the_detail_panel_below_the_table() {
        let mut app = detail_app(three_failures(), false);
        app.pending_delete = Some(app.in_flight_jobs[0].id.clone());
        assert_ui_snapshot!(render_to_string(&app, 160, 30));
    }

    #[test]
    fn the_delete_prompt_is_red_across_the_panel() {
        use ratatui::style::Color;

        let mut app = detail_app(three_failures(), false);
        app.pending_delete = Some(app.in_flight_jobs[0].id.clone());
        let mut terminal = Terminal::new(TestBackend::new(200, 40)).unwrap();
        let frame = terminal.draw(|f| render(&mut app, f)).unwrap();

        // The panel's last row, above the help bar, from end to end.
        let row = 40 - 2;
        assert!((130..200).all(|x| frame.buffer[(x, row)].bg == Color::Red));
        assert_eq!(frame.buffer[(129, row)].bg, Color::Reset, "not the table");
    }

    #[test]
    fn render_detail_panel_while_loading() {
        let mut app = sample_app(Tab::InFlight);
        app.show_detail = true;
        assert_ui_snapshot!(render_to_string(&app, 160, 30));
    }

    #[test]
    fn render_empty_ready_tab() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            server_tier: Some(Tier::Pro),
            subscription_limit: None,
            total_ready: 0,
            total_in_flight: 0,
            total_scheduled: 0,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            now_ms: 1_700_000_000_000,
            active_tab: Tab::Ready,
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        };
        assert_ui_snapshot!(render_to_string(&app, 120, 8));
    }

    #[test]
    fn render_disconnected_wide() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Disconnected,
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        };
        assert_ui_snapshot!(render_to_string(&app, 120, 8));
    }

    #[test]
    fn render_free_tier_ready_tab_with_cap_message() {
        let now_ms = 1_700_000_000_000u64;
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            server_tier: Some(Tier::Free),
            subscription_limit: Some(10),
            total_ready: 50,
            total_in_flight: 3,
            total_scheduled: 2,
            ready_jobs: vec![
                realistic_job(
                    "0195b70a3cc87a1f0890da08e8b3cc86",
                    "default",
                    "send_email",
                    0,
                    now_ms - 5_200,
                    0,
                    None,
                    None,
                ),
                realistic_job(
                    "0195b70a3dd47c2308a1fb09e9c4dd97",
                    "default",
                    "charge",
                    5,
                    now_ms - 62_000,
                    2,
                    None,
                    None,
                ),
            ],
            in_flight_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            now_ms,
            active_tab: Tab::Ready,
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        };
        assert_ui_snapshot!(render_to_string(&app, 80, 20));
    }

    #[test]
    fn render_free_tier_in_flight_tab_with_cap_message() {
        let now_ms = 1_700_000_000_000u64;
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            server_tier: Some(Tier::Free),
            subscription_limit: Some(10),
            total_ready: 5,
            total_in_flight: 2,
            total_scheduled: 0,
            ready_jobs: Vec::new(),
            in_flight_jobs: vec![
                realistic_job(
                    "0195b70a2bb46e1f07a0c908d7a2bb40",
                    "default",
                    "send_email",
                    0,
                    now_ms - 10_000,
                    1,
                    Some(now_ms - 3_100),
                    None,
                ),
                realistic_job(
                    "0195b70a1aa35d0e06b0b807c691aa30",
                    "billing",
                    "charge",
                    0,
                    now_ms - 200_000,
                    3,
                    Some(now_ms - 150_000),
                    Some(now_ms - 60_000),
                ),
            ],
            scheduled_jobs: Vec::new(),
            now_ms,
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
            modal_mode_flag: None,
            job_detail: None,
            detail_request: None,
            detail_view: Default::default(),
        };
        assert_ui_snapshot!(render_to_string(&app, 120, 12));
    }
}
