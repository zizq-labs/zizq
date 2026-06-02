// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
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

    // Content area — render only the active tab's table.
    let content_area = chunks[2];

    let subscription_limit = if matches!(app.status, ConnectionStatus::Connected) {
        app.subscription_limit
    } else {
        None
    };

    // Split content area into table + optional detail panel + optional cap message.
    // Panel height: min(DETAIL_PANEL_MAX, 30% of content area).
    // Suppressed if the table would get fewer than MIN_TABLE_ROWS body rows.
    let detail_height = DETAIL_PANEL_MAX.min((content_area.height * 30 / 100).max(1));
    let show_detail_panel =
        app.show_detail && content_area.height > detail_height + MIN_TABLE_ROWS + 1;

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

    if let Some(area) = detail_area {
        detail_panel::render(app, frame, area);
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

    help_bar::render(app, frame, chunks[3]);
}

/// Minimum width for the table content area. When the viewport is narrower,
/// the table renders at this width and the visible portion is determined by
/// the horizontal scroll offset.
pub(super) const MIN_TABLE_WIDTH: u16 = 120;

/// Maximum height of the detail panel (heading + all possible fields).
const DETAIL_PANEL_MAX: u16 = 13;

/// Minimum table body rows required before showing the detail panel.
const MIN_TABLE_ROWS: u16 = 3;

/// Foreground color for tab labels and table header text.
pub(super) const fn tab_fg() -> Color {
    Color::Black
}

/// Background color shared by the active tab and the table header row.
pub(super) const fn header_bg() -> Color {
    Color::Green
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::admin::AdminJob;
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

    #[test]
    fn render_delete_prompt_replaces_help_bar() {
        let mut app = new_app();
        app.status = ConnectionStatus::Connected;
        app.pending_delete = Some("j_abc123".into());
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

    #[test]
    fn render_detail_panel() {
        use crate::api::admin::{AdminBackoff, AdminRetention};

        let mut app = sample_app(Tab::InFlight);
        app.show_detail = true;

        // Give the first in-flight job full detail fields.
        app.in_flight_jobs[0].payload = Some(serde_json::json!({
            "to": "user@example.com",
            "subject": "Welcome!",
            "urgent": true,
        }));
        app.in_flight_jobs[0].retry_limit = Some(25);
        app.in_flight_jobs[0].backoff = Some(AdminBackoff {
            base_ms: 15000,
            exponent: 4.0,
            jitter_ms: 30000,
        });
        app.in_flight_jobs[0].retention = Some(AdminRetention {
            completed_ms: Some(604_800_000),
            dead_ms: Some(2_592_000_000),
        });
        app.in_flight_jobs[0].unique_key = Some("report:annual:2026".to_string());
        app.in_flight_jobs[0].unique_while = Some("active".to_string());

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
        };
        assert_ui_snapshot!(render_to_string(&app, 120, 12));
    }
}
