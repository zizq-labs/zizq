// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Ratatui rendering for the TUI dashboard.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::admin::AdminJobSummary;

use super::app::{App, ConnectionStatus, Tab};

/// Render the current application state to a terminal frame.
pub fn render(app: &App, frame: &mut Frame) {
    let chunks = Layout::vertical([
        Constraint::Length(3), // status bar
        Constraint::Length(1), // tab bar
        Constraint::Min(0),    // content table
        Constraint::Length(1), // help bar
    ])
    .split(frame.area());

    // Status bar.
    let (indicator, status_style) = match app.status {
        ConnectionStatus::Connected => (
            "Connected",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        ConnectionStatus::Connecting => (
            "Connecting...",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        ConnectionStatus::Disconnected => (
            "Disconnected",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    };

    let mut status_spans = vec![
        Span::raw(" "),
        Span::styled("\u{25cf} ", status_style),
        Span::styled(indicator, status_style),
    ];

    if let Some(ref version) = app.server_version {
        status_spans.push(Span::raw("  "));
        status_spans.push(Span::raw(format!("v{version}")));
    }

    if let Some(uptime_ms) = app.server_uptime_ms {
        status_spans.push(Span::raw("  "));
        status_spans.push(Span::raw(format!(
            "uptime: {}",
            format_duration_ms(uptime_ms)
        )));
    }

    let status_bar = Paragraph::new(Line::from(status_spans)).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Zizq {}", env!("CARGO_PKG_VERSION"))),
    );

    frame.render_widget(status_bar, chunks[0]);

    // Tab bar.
    let tab_labels = [(Tab::Ready, " Ready "), (Tab::InFlight, " In-Flight ")];

    let header_style = Style::default().fg(tab_fg()).bg(header_bg());
    let inactive_tab_style = Style::default().fg(tab_fg()).bg(Color::Blue);

    let mut tab_spans: Vec<Span> = Vec::new();
    for (i, (tab, label)) in tab_labels.iter().enumerate() {
        if i > 0 {
            tab_spans.push(Span::raw(" "));
        }
        let style = if *tab == app.active_tab {
            header_style
        } else {
            inactive_tab_style
        };
        tab_spans.push(Span::styled(*label, style));
    }

    frame.render_widget(Paragraph::new(Line::from(tab_spans)), chunks[1]);

    // Content area — render only the active tab's table.
    let table = match app.active_tab {
        Tab::Ready => job_table_ready(&app.ready_jobs, app.now_ms),
        Tab::InFlight => job_table_in_flight(&app.in_flight_jobs, app.now_ms),
    };

    frame.render_widget(table, chunks[2]);

    // Help bar.
    let key_style = Style::default().fg(Color::White);
    let label_style = Style::default().fg(Color::Black).bg(Color::White);
    let help = Paragraph::new(Line::from(vec![
        Span::styled(" h/l ", key_style),
        Span::styled("Switch tab", label_style),
        Span::styled("  q ", key_style),
        Span::styled("Quit", label_style),
    ]));
    frame.render_widget(help, chunks[3]);
}

/// Foreground color for tab labels and table header text.
const fn tab_fg() -> Color {
    Color::Black
}

/// Background color shared by the active tab and the table header row.
const fn header_bg() -> Color {
    Color::Green
}

/// Build a table widget for the Ready pane.
fn job_table_ready(jobs: &[AdminJobSummary], now_ms: u64) -> Table<'_> {
    let header = Row::new([
        Cell::from(Line::from("ID").alignment(Alignment::Right)),
        Cell::from(Line::from("PRIORITY").alignment(Alignment::Right)),
        Cell::from("QUEUE"),
        Cell::from("TYPE"),
        Cell::from("DELAY"),
        Cell::from(Line::from("ATTEMPTS").alignment(Alignment::Right)),
    ])
    .style(Style::default().fg(tab_fg()).bg(header_bg()))
    .bottom_margin(0);

    let rows: Vec<Row> = jobs
        .iter()
        .map(|job| {
            Row::new([
                Cell::from(Line::from(job.id.as_str()).alignment(Alignment::Right)),
                Cell::from(Line::from(job.priority.to_string()).alignment(Alignment::Right)),
                Cell::from(job.queue.as_str()),
                Cell::from(job.job_type.as_str()),
                Cell::from(format_elapsed(job.ready_at, now_ms)),
                Cell::from(Line::from(job.attempts.to_string()).alignment(Alignment::Right)),
            ])
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Fill(3),
            Constraint::Length(10),
            Constraint::Fill(2),
            Constraint::Fill(3),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(header)
}

/// Build a table widget for the In-Flight pane.
fn job_table_in_flight(jobs: &[AdminJobSummary], now_ms: u64) -> Table<'_> {
    let header = Row::new([
        Cell::from(Line::from("ID").alignment(Alignment::Right)),
        Cell::from(Line::from("PRIORITY").alignment(Alignment::Right)),
        Cell::from("QUEUE"),
        Cell::from("TYPE"),
        Cell::from("DURATION"),
        Cell::from(Line::from("ATTEMPTS").alignment(Alignment::Right)),
    ])
    .style(Style::default().fg(tab_fg()).bg(header_bg()))
    .bottom_margin(0);

    let rows: Vec<Row> = jobs
        .iter()
        .map(|job| {
            let dequeued = job.dequeued_at.unwrap_or(0);
            Row::new([
                Cell::from(Line::from(job.id.as_str()).alignment(Alignment::Right)),
                Cell::from(Line::from(job.priority.to_string()).alignment(Alignment::Right)),
                Cell::from(job.queue.as_str()),
                Cell::from(job.job_type.as_str()),
                Cell::from(format_elapsed(dequeued, now_ms)),
                Cell::from(Line::from(job.attempts.to_string()).alignment(Alignment::Right)),
            ])
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Fill(3),
            Constraint::Length(10),
            Constraint::Fill(2),
            Constraint::Fill(3),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(header)
}

/// Format elapsed time since a timestamp (e.g. "5.2s", "2.3m").
fn format_elapsed(timestamp_ms: u64, now_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "-".to_string();
    }
    let diff_ms = now_ms.saturating_sub(timestamp_ms);
    format_duration_ms(diff_ms)
}

/// Format a duration in milliseconds as a human-readable string.
fn format_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1_000.0)
    } else if ms < 3_600_000 {
        format!("{:.1}m", ms as f64 / 60_000.0)
    } else if ms < 86_400_000 {
        format!("{:.1}h", ms as f64 / 3_600_000.0)
    } else {
        format!("{:.1}d", ms as f64 / 86_400_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    /// Render the app state to a fixed-size terminal and return the
    /// buffer contents as a string suitable for snapshot comparison.
    fn render_to_string(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        let frame = terminal.draw(|f| render(app, f)).unwrap();
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

    fn new_app() -> App {
        App {
            status: ConnectionStatus::Connecting,
            server_version: None,
            server_uptime_ms: None,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            now_ms: 0,
            active_tab: Tab::Ready,
        }
    }

    fn sample_ready_job(
        queue: &str,
        job_type: &str,
        ready_at: u64,
        attempts: u32,
    ) -> AdminJobSummary {
        AdminJobSummary {
            id: format!("job-{queue}-{job_type}"),
            queue: queue.to_string(),
            job_type: job_type.to_string(),
            priority: 0,
            ready_at,
            attempts,
            dequeued_at: None,
            failed_at: None,
        }
    }

    fn sample_in_flight_job(
        queue: &str,
        job_type: &str,
        dequeued_at: u64,
        attempts: u32,
        failed_at: Option<u64>,
    ) -> AdminJobSummary {
        AdminJobSummary {
            id: format!("job-{queue}-{job_type}"),
            queue: queue.to_string(),
            job_type: job_type.to_string(),
            priority: 0,
            ready_at: dequeued_at.saturating_sub(1000),
            attempts,
            dequeued_at: Some(dequeued_at),
            failed_at,
        }
    }

    #[test]
    fn render_connecting() {
        let app = new_app();
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_connected_with_server_info() {
        let app = App {
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(65_000),
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            now_ms: 0,
            active_tab: Tab::Ready,
        };
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_disconnected() {
        let app = App {
            status: ConnectionStatus::Disconnected,
            server_version: None,
            server_uptime_ms: None,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            now_ms: 0,
            active_tab: Tab::Ready,
        };
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_ready_tab_with_jobs() {
        let now_ms = 1_700_000_000_000u64;
        let app = App {
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(120_000),
            ready_jobs: vec![
                sample_ready_job("emails", "send_email", now_ms - 5_200, 0),
                sample_ready_job("reports", "gen_report", now_ms - 62_000, 2),
            ],
            in_flight_jobs: vec![
                sample_in_flight_job("emails", "send_email", now_ms - 3_100, 1, None),
                sample_in_flight_job(
                    "billing",
                    "charge",
                    now_ms - 150_000,
                    3,
                    Some(now_ms - 60_000),
                ),
            ],
            now_ms,
            active_tab: Tab::Ready,
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 12));
    }

    #[test]
    fn render_in_flight_tab_with_jobs() {
        let now_ms = 1_700_000_000_000u64;
        let app = App {
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(120_000),
            ready_jobs: vec![
                sample_ready_job("emails", "send_email", now_ms - 5_200, 0),
                sample_ready_job("reports", "gen_report", now_ms - 62_000, 2),
            ],
            in_flight_jobs: vec![
                sample_in_flight_job("emails", "send_email", now_ms - 3_100, 1, None),
                sample_in_flight_job(
                    "billing",
                    "charge",
                    now_ms - 150_000,
                    3,
                    Some(now_ms - 60_000),
                ),
            ],
            now_ms,
            active_tab: Tab::InFlight,
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 12));
    }

    #[test]
    fn render_empty_ready_tab() {
        let app = App {
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            now_ms: 1_700_000_000_000,
            active_tab: Tab::Ready,
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 8));
    }

    #[test]
    fn render_disconnected_wide() {
        let app = App {
            status: ConnectionStatus::Disconnected,
            server_version: None,
            server_uptime_ms: None,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            now_ms: 0,
            active_tab: Tab::Ready,
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 8));
    }

    #[test]
    fn format_elapsed_examples() {
        let now = 10_000_000u64;
        assert_eq!(format_elapsed(now - 500, now), "500ms");
        assert_eq!(format_elapsed(now - 5_200, now), "5.2s");
        assert_eq!(format_elapsed(now - 138_000, now), "2.3m");
        assert_eq!(format_elapsed(now - 3_960_000, now), "1.1h");
        assert_eq!(format_elapsed(0, now), "-");
    }

    #[test]
    fn format_duration_ms_examples() {
        assert_eq!(format_duration_ms(0), "0ms");
        assert_eq!(format_duration_ms(500), "500ms");
        assert_eq!(format_duration_ms(999), "999ms");
        assert_eq!(format_duration_ms(1_000), "1.0s");
        assert_eq!(format_duration_ms(5_200), "5.2s");
        assert_eq!(format_duration_ms(60_000), "1.0m");
        assert_eq!(format_duration_ms(138_000), "2.3m");
        assert_eq!(format_duration_ms(3_600_000), "1.0h");
        assert_eq!(format_duration_ms(3_960_000), "1.1h");
        assert_eq!(format_duration_ms(86_400_000), "1.0d");
        assert_eq!(format_duration_ms(190_800_000), "2.2d");
    }
}
