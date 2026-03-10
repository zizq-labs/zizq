// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Ratatui rendering for the TUI dashboard.

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, Widget};

use crate::admin::AdminJobSummary;

use super::app::{App, ConnectionStatus, Tab};

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

    // Header area (5 lines, no border).
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

    let bold_gray = Style::default()
        .fg(Color::Gray)
        .add_modifier(Modifier::BOLD);
    let cyan = Style::default().fg(Color::Cyan);
    let connected = matches!(app.status, ConnectionStatus::Connected);

    let version_line = Line::from(vec![
        Span::raw("  "),
        Span::raw(format!("Zizq {}", env!("CARGO_PKG_VERSION"))),
    ]);

    let status_text_style = match app.status {
        ConnectionStatus::Connected => Style::default().fg(Color::LightGreen),
        ConnectionStatus::Connecting => Style::default().fg(Color::LightYellow),
        ConnectionStatus::Disconnected => Style::default().fg(Color::LightRed),
    };
    let status_line = Line::from(vec![
        Span::styled("\u{25cf} ", status_style),
        Span::styled(indicator, status_text_style),
        Span::raw(" "),
        Span::styled(&*app.host, cyan),
    ]);

    let server_info_line = if connected {
        let version = app.server_version.as_deref().unwrap_or("?");
        let uptime = app
            .server_uptime_ms
            .map(format_duration_ms)
            .unwrap_or_else(|| "?".to_string());
        Line::from(vec![
            Span::raw("  "),
            Span::styled("Server version: ", cyan),
            Span::styled(version.to_string(), bold_gray),
            Span::styled(", Uptime: ", cyan),
            Span::styled(uptime, bold_gray),
        ])
    } else {
        Line::default()
    };

    let totals_line = if connected {
        let n_in_flight = app.total_in_flight;
        let n_ready = app.total_ready;
        let n_scheduled = app.total_scheduled;
        Line::from(vec![
            Span::raw("  "),
            Span::styled("Queue: ", cyan),
            Span::styled(
                n_in_flight.to_string(),
                Style::default()
                    .fg(Color::LightYellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " in-flight, ",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                n_ready.to_string(),
                Style::default()
                    .fg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " ready, ",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                n_scheduled.to_string(),
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " scheduled",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            ),
        ])
    } else {
        Line::default()
    };

    let depth_lines = if connected {
        let ls = &app.list_states[app.active_tab.idx()];
        render_depth_bar(
            app.total_in_flight,
            app.total_ready,
            app.total_scheduled,
            chunks[0].width as usize,
            app.active_tab,
            ls.cursor,
        )
    } else {
        [Line::default(), Line::default(), Line::default()]
    };

    let header = Paragraph::new(vec![
        version_line,
        status_line,
        server_info_line,
        totals_line,
        depth_lines[0].clone(),
        depth_lines[1].clone(),
        depth_lines[2].clone(),
    ]);
    frame.render_widget(header, chunks[0]);

    // Tab bar.
    let tab_labels = [
        (Tab::InFlight, " In-Flight "),
        (Tab::Ready, " Ready "),
        (Tab::Scheduled, " Scheduled "),
    ];

    let header_style = Style::default().fg(tab_fg()).bg(header_bg());
    let inactive_tab_style = Style::default().fg(tab_fg()).bg(Color::Blue);

    let mut tab_spans: Vec<Span> = Vec::new();
    tab_spans.push(Span::raw("  "));
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
    // In-flight is always available. Ready and Scheduled require Pro.
    let content_area = chunks[2];

    // Store the viewport height (minus 1 for the header row) so
    // scroll logic can reference it. If it changed (terminal resize),
    // re-subscribe with an updated window size.
    let table_body_height = content_area.height.saturating_sub(1) as usize;
    if table_body_height != app.viewport_height {
        app.viewport_height = table_body_height;
        app.resubscribe_all();
    }

    let tab_gated = connected
        && app.active_tab != Tab::InFlight
        && app
            .server_tier
            .is_none_or(|t| t < crate::license::Tier::Pro);

    if tab_gated {
        // Render the table header row, then the license message centered
        // in the remaining space.
        let table = match app.active_tab {
            Tab::Ready => job_table_ready(&[], app.now_ms, None),
            Tab::InFlight => unreachable!(),
            Tab::Scheduled => job_table_scheduled(&[], app.now_ms, None),
        };

        let inner = Layout::vertical([
            Constraint::Length(1), // table header row
            Constraint::Min(0),    // license message
        ])
        .split(content_area);

        frame.render_widget(table, inner[0]);

        // Vertically center by padding top.
        let msg_height = inner[1].height;
        let pad_top = msg_height / 2;
        let centered = Layout::vertical([
            Constraint::Length(pad_top),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(inner[1]);

        let msg = Paragraph::new(Line::from(Span::styled(
            "Live queue detail requires a pro license",
            Style::default().fg(Color::DarkGray),
        )))
        .alignment(Alignment::Center);
        frame.render_widget(msg, centered[1]);
    } else {
        let ls = &app.list_states[app.active_tab.idx()];
        let jobs = match app.active_tab {
            Tab::Ready => &app.ready_jobs,
            Tab::InFlight => &app.in_flight_jobs,
            Tab::Scheduled => &app.scheduled_jobs,
        };

        // Extract the visible slice from the buffer.
        let visible_start = ls.scroll_pos.saturating_sub(ls.buffer_offset);
        let visible_end = (visible_start + table_body_height).min(jobs.len());
        let visible = if visible_start < jobs.len() {
            &jobs[visible_start..visible_end]
        } else {
            &[]
        };

        // Determine which row in the visible slice is the cursor.
        let cursor_in_view =
            if ls.cursor >= ls.scroll_pos && ls.cursor < ls.scroll_pos + table_body_height {
                Some(ls.cursor - ls.scroll_pos)
            } else {
                None
            };

        let table = match app.active_tab {
            Tab::Ready => job_table_ready(visible, app.now_ms, cursor_in_view),
            Tab::InFlight => job_table_in_flight(visible, app.now_ms, cursor_in_view),
            Tab::Scheduled => job_table_scheduled(visible, app.now_ms, cursor_in_view),
        };
        let tab_scroll = app.h_scroll[app.active_tab.idx()];
        render_scrollable(frame, table, content_area, tab_scroll);
    }

    // Help bar.
    let key_style = Style::default().fg(Color::White);
    let label_style = Style::default().fg(Color::Black).bg(Color::LightCyan);
    let help = Paragraph::new(Line::from(vec![
        Span::styled(" n/p ", key_style),
        Span::styled("Tabs", label_style),
        Span::styled("  \u{2190}\u{2191}\u{2192}\u{2193} ", key_style),
        Span::styled("Scroll", label_style),
        Span::styled("  q ", key_style),
        Span::styled("Quit", label_style),
    ]));
    frame.render_widget(help, chunks[3]);
}

/// Minimum width for the table content area. When the viewport is narrower,
/// the table renders at this width and the visible portion is determined by
/// the horizontal scroll offset.
const MIN_TABLE_WIDTH: u16 = 120;

/// Style for the cursor-highlighted row.
const CURSOR_STYLE: Style = Style::new().fg(Color::Black).bg(Color::LightCyan);

/// Render a widget into `area`, applying horizontal scrolling when the area
/// is narrower than [`MIN_TABLE_WIDTH`].
fn render_scrollable(frame: &mut Frame, widget: impl Widget, area: Rect, h_scroll: u16) {
    let virtual_width = area.width.max(MIN_TABLE_WIDTH);

    if virtual_width == area.width {
        // No scrolling needed — render directly.
        frame.render_widget(widget, area);
        return;
    }

    // Clamp scroll so we don't scroll past the end.
    let max_scroll = virtual_width - area.width;
    let scroll = h_scroll.min(max_scroll);

    // Render the widget into a temporary off-screen buffer.
    let virtual_area = Rect {
        x: 0,
        y: 0,
        width: virtual_width,
        height: area.height,
    };
    let mut vbuf = Buffer::empty(virtual_area);
    widget.render(virtual_area, &mut vbuf);

    // Copy the visible slice into the frame buffer.
    let fbuf = frame.buffer_mut();
    for y in 0..area.height {
        for x in 0..area.width {
            let src_x = x + scroll;
            let dst_x = area.x + x;
            let dst_y = area.y + y;
            if src_x < virtual_width && dst_x < fbuf.area.width && dst_y < fbuf.area.height {
                let cell = &vbuf[(src_x, y)];
                fbuf[(dst_x, dst_y)].set_symbol(cell.symbol());
                fbuf[(dst_x, dst_y)].set_style(cell.style());
            }
        }
    }
}

/// Render the depth bar with cursor markers above and below.
///
/// Returns `[marker_above, depth_bar, marker_below]`.
fn render_depth_bar(
    in_flight: usize,
    ready: usize,
    scheduled: usize,
    width: usize,
    active_tab: Tab,
    cursor: usize,
) -> [Line<'static>; 3] {
    let total = in_flight + ready + scheduled;
    let total_label = format!("{total} jobs");
    let bold_white = Style::default()
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);
    let cyan = Style::default().fg(Color::Cyan);

    // Cap the bar at 120 columns (or terminal width if narrower).
    let max_width = width.min(120);

    let dim = Style::default()
        .fg(Color::DarkGray)
        .add_modifier(Modifier::BOLD);

    // "  Depth[" = 8 chars, "{N} jobs]  " = total_label.len() + 3
    let prefix_len = 8;
    let overhead = prefix_len + total_label.len() + 3;
    let bar_width = max_width.saturating_sub(overhead);

    let (if_bars, ready_bars, sched_bars) = if total == 0 || bar_width == 0 {
        (0, 0, 0)
    } else if total <= bar_width {
        // One bar per job when total fits.
        (in_flight, ready, scheduled)
    } else {
        // Scale proportionally.
        let if_b = ((in_flight as f64 / total as f64) * bar_width as f64).round() as usize;
        let ready_b = ((ready as f64 / total as f64) * bar_width as f64).round() as usize;
        let sched_b = bar_width.saturating_sub(if_b + ready_b);
        (if_b, ready_b, sched_b)
    };

    // Pad so the label is right-aligned against "]".
    let pad = bar_width.saturating_sub(if_bars + ready_bars + sched_bars);

    let depth_line = Line::from(vec![
        Span::raw("  "),
        Span::styled("Depth", cyan),
        Span::styled("[", bold_white),
        Span::styled(
            "\u{2502}".repeat(if_bars),
            Style::default()
                .fg(Color::LightYellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "\u{2502}".repeat(ready_bars),
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "\u{2502}".repeat(sched_bars),
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" ".repeat(pad)),
        Span::styled(total_label, dim),
        Span::styled("]  ", bold_white),
    ]);

    // Compute cursor marker position within the bar.
    let (section_total, section_bars, section_start) = match active_tab {
        Tab::InFlight => (in_flight, if_bars, 0),
        Tab::Ready => (ready, ready_bars, if_bars),
        Tab::Scheduled => (scheduled, sched_bars, if_bars + ready_bars),
    };

    let marker_color = match active_tab {
        Tab::InFlight => Color::LightYellow,
        Tab::Ready => Color::Blue,
        Tab::Scheduled => Color::Magenta,
    };

    let marker_lines = if section_total == 0 {
        // No jobs in this section — no marker.
        [Line::default(), Line::default()]
    } else {
        let pos_in_section = if section_bars == 0 {
            // Section is invisible; place at boundary.
            0
        } else if total <= bar_width {
            // 1:1 mode — cursor maps directly.
            cursor.min(section_bars.saturating_sub(1))
        } else {
            // Ratio mode — proportional position.
            let frac = cursor as f64 / section_total as f64;
            (frac * section_bars as f64)
                .round()
                .min(section_bars.saturating_sub(1) as f64) as usize
        };

        let col = prefix_len + section_start + pos_in_section;
        let marker_style = Style::default().fg(marker_color);

        let make_marker = |ch: &'static str| -> Line<'static> {
            Line::from(vec![
                Span::raw(" ".repeat(col)),
                Span::styled(ch, marker_style),
            ])
        };

        [make_marker("\u{25bc}"), make_marker("\u{25b2}")]
    };

    [marker_lines[0].clone(), depth_line, marker_lines[1].clone()]
}

/// Foreground color for tab labels and table header text.
const fn tab_fg() -> Color {
    Color::Black
}

/// Background color shared by the active tab and the table header row.
const fn header_bg() -> Color {
    Color::Green
}

/// Style for ID and TYPE cells, based on attempt count.
fn attempt_fg(attempts: u32, highlighted: bool) -> Style {
    if highlighted {
        return CURSOR_STYLE;
    }
    match attempts {
        0 => Style::default(),
        1 => Style::default().fg(Color::Yellow),
        _ => Style::default().fg(Color::Red),
    }
}

/// Style for ATTEMPTS cells (always bold), colored by attempt count.
fn attempt_bold(attempts: u32, highlighted: bool) -> Style {
    if highlighted {
        return CURSOR_STYLE;
    }
    match attempts {
        0 => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
        1 => Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        _ => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

/// Bold style for PRIORITY cells.
fn priority_style(highlighted: bool) -> Style {
    if highlighted {
        return CURSOR_STYLE;
    }
    Style::new().fg(Color::White).add_modifier(Modifier::BOLD)
}

/// Build a table widget for the Ready pane.
fn job_table_ready(jobs: &[AdminJobSummary], now_ms: u64, cursor_row: Option<usize>) -> Table<'_> {
    let header = Row::new([
        Cell::from(Line::from("ID").alignment(Alignment::Right)),
        Cell::from(Line::from("PRIORITY").alignment(Alignment::Right)),
        Cell::from("QUEUE"),
        Cell::from(Line::from("DELAY").alignment(Alignment::Right)),
        Cell::from(Line::from("ATTEMPTS").alignment(Alignment::Right)),
        Cell::from("TYPE"),
    ])
    .style(Style::default().fg(tab_fg()).bg(header_bg()))
    .bottom_margin(0);

    let rows: Vec<Row> = jobs
        .iter()
        .enumerate()
        .map(|(i, job)| {
            let hl = cursor_row == Some(i);
            let fg = attempt_fg(job.attempts, hl);
            let att = attempt_bold(job.attempts, hl);
            let row = Row::new([
                Cell::from(
                    Line::from(Span::styled(job.id.as_str(), fg)).alignment(Alignment::Right),
                ),
                Cell::from(
                    Line::from(Span::styled(job.priority.to_string(), priority_style(hl)))
                        .alignment(Alignment::Right),
                ),
                Cell::from(job.queue.as_str()),
                Cell::from(
                    Line::from(format_elapsed(job.ready_at, now_ms)).alignment(Alignment::Right),
                ),
                Cell::from(
                    Line::from(Span::styled(job.attempts.to_string(), att))
                        .alignment(Alignment::Right),
                ),
                Cell::from(Span::styled(job.job_type.as_str(), fg)),
            ]);
            if hl { row.style(CURSOR_STYLE) } else { row }
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Length(27),
            Constraint::Length(10),
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Fill(3),
        ],
    )
    .header(header)
}

/// Build a table widget for the In-Flight pane.
fn job_table_in_flight(
    jobs: &[AdminJobSummary],
    now_ms: u64,
    cursor_row: Option<usize>,
) -> Table<'_> {
    let header = Row::new([
        Cell::from(Line::from("ID").alignment(Alignment::Right)),
        Cell::from(Line::from("PRIORITY").alignment(Alignment::Right)),
        Cell::from("QUEUE"),
        Cell::from(Line::from("DURATION").alignment(Alignment::Right)),
        Cell::from(Line::from("ATTEMPTS").alignment(Alignment::Right)),
        Cell::from("TYPE"),
    ])
    .style(Style::default().fg(tab_fg()).bg(header_bg()))
    .bottom_margin(0);

    let rows: Vec<Row> = jobs
        .iter()
        .enumerate()
        .map(|(i, job)| {
            let hl = cursor_row == Some(i);
            let dequeued = job.dequeued_at.unwrap_or(0);
            let fg = attempt_fg(job.attempts, hl);
            let att = attempt_bold(job.attempts, hl);
            let row = Row::new([
                Cell::from(
                    Line::from(Span::styled(job.id.as_str(), fg)).alignment(Alignment::Right),
                ),
                Cell::from(
                    Line::from(Span::styled(job.priority.to_string(), priority_style(hl)))
                        .alignment(Alignment::Right),
                ),
                Cell::from(job.queue.as_str()),
                Cell::from(
                    Line::from(format_elapsed(dequeued, now_ms)).alignment(Alignment::Right),
                ),
                Cell::from(
                    Line::from(Span::styled(job.attempts.to_string(), att))
                        .alignment(Alignment::Right),
                ),
                Cell::from(Span::styled(job.job_type.as_str(), fg)),
            ]);
            if hl { row.style(CURSOR_STYLE) } else { row }
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Length(27),
            Constraint::Length(10),
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Fill(3),
        ],
    )
    .header(header)
}

/// Build a table widget for the Scheduled pane.
fn job_table_scheduled(
    jobs: &[AdminJobSummary],
    now_ms: u64,
    cursor_row: Option<usize>,
) -> Table<'_> {
    let header = Row::new([
        Cell::from(Line::from("ID").alignment(Alignment::Right)),
        Cell::from(Line::from("PRIORITY").alignment(Alignment::Right)),
        Cell::from("QUEUE"),
        Cell::from(Line::from("DUE").alignment(Alignment::Right)),
        Cell::from(Line::from("ATTEMPTS").alignment(Alignment::Right)),
        Cell::from("TYPE"),
    ])
    .style(Style::default().fg(tab_fg()).bg(header_bg()))
    .bottom_margin(0);

    let rows: Vec<Row> = jobs
        .iter()
        .enumerate()
        .map(|(i, job)| {
            let hl = cursor_row == Some(i);
            let fg = attempt_fg(job.attempts, hl);
            let att = attempt_bold(job.attempts, hl);
            let row = Row::new([
                Cell::from(
                    Line::from(Span::styled(job.id.as_str(), fg)).alignment(Alignment::Right),
                ),
                Cell::from(
                    Line::from(Span::styled(job.priority.to_string(), priority_style(hl)))
                        .alignment(Alignment::Right),
                ),
                Cell::from(job.queue.as_str()),
                Cell::from(
                    Line::from(format_due(job.ready_at, now_ms)).alignment(Alignment::Right),
                ),
                Cell::from(
                    Line::from(Span::styled(job.attempts.to_string(), att))
                        .alignment(Alignment::Right),
                ),
                Cell::from(Span::styled(job.job_type.as_str(), fg)),
            ]);
            if hl { row.style(CURSOR_STYLE) } else { row }
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Length(27),
            Constraint::Length(10),
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Fill(3),
        ],
    )
    .header(header)
}

/// Format time until a scheduled job is due, or how overdue it is.
fn format_due(ready_at: u64, now_ms: u64) -> String {
    if ready_at == 0 {
        return "-".to_string();
    }
    let diff = ready_at.abs_diff(now_ms);
    if diff < 1_000 {
        return "< 1s".to_string();
    }
    if ready_at >= now_ms {
        format_duration_ms(diff)
    } else {
        let elapsed = format_duration_ms(diff);
        format!("{elapsed} ago")
    }
}

/// Format elapsed time since a timestamp (e.g. "5s", "2m18s").
fn format_elapsed(timestamp_ms: u64, now_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "-".to_string();
    }
    let diff_ms = now_ms.saturating_sub(timestamp_ms);
    if diff_ms < 1_000 {
        return "< 1s".to_string();
    }
    format_duration_ms(diff_ms)
}

/// Format a duration in milliseconds as a human-readable compound string.
fn format_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        let s = ms / 1_000;
        format!("{s}s")
    } else if ms < 3_600_000 {
        let m = ms / 60_000;
        let s = (ms % 60_000) / 1_000;
        if s > 0 {
            format!("{m}m{s}s")
        } else {
            format!("{m}m")
        }
    } else if ms < 86_400_000 {
        let h = ms / 3_600_000;
        let m = (ms % 3_600_000) / 60_000;
        if m > 0 {
            format!("{h}h{m}m")
        } else {
            format!("{h}h")
        }
    } else {
        let d = ms / 86_400_000;
        let h = (ms % 86_400_000) / 3_600_000;
        if h > 0 {
            format!("{d}d{h}h")
        } else {
            format!("{d}d")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn new_app() -> App {
        App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connecting,
            server_version: None,
            server_uptime_ms: None,
            server_tier: None,
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

    #[test]
    fn render_connecting() {
        let app = new_app();
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_connected_with_server_info() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(65_000),
            server_tier: Some(Tier::Pro),
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
        };
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_disconnected() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Disconnected,
            server_version: None,
            server_uptime_ms: None,
            server_tier: None,
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
        };
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
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
    ) -> AdminJobSummary {
        AdminJobSummary {
            id: id.to_string(),
            queue: queue.to_string(),
            job_type: job_type.to_string(),
            priority,
            ready_at,
            attempts,
            dequeued_at,
            failed_at,
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
            ws_tx: None,
        }
    }

    #[test]
    fn render_ready_tab_wide() {
        let app = sample_app(Tab::Ready);
        insta::assert_snapshot!(render_to_string(&app, 160, 12));
    }

    #[test]
    fn render_ready_tab_narrow() {
        let app = sample_app(Tab::Ready);
        insta::assert_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_in_flight_tab_wide() {
        let app = sample_app(Tab::InFlight);
        insta::assert_snapshot!(render_to_string(&app, 160, 12));
    }

    #[test]
    fn render_in_flight_tab_narrow() {
        let app = sample_app(Tab::InFlight);
        insta::assert_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_in_flight_tab_narrow_scrolled() {
        let mut app = sample_app(Tab::InFlight);
        app.h_scroll[Tab::InFlight.idx()] = 20;
        insta::assert_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_in_flight_tab_narrow_scrolled_max() {
        let mut app = sample_app(Tab::InFlight);
        app.h_scroll[Tab::InFlight.idx()] = 40;
        insta::assert_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_scheduled_tab_wide() {
        let app = sample_app(Tab::Scheduled);
        insta::assert_snapshot!(render_to_string(&app, 160, 12));
    }

    #[test]
    fn render_scheduled_tab_narrow() {
        let app = sample_app(Tab::Scheduled);
        insta::assert_snapshot!(render_to_string(&app, 80, 12));
    }

    #[test]
    fn render_empty_ready_tab() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            server_tier: Some(Tier::Pro),
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
            ws_tx: None,
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 8));
    }

    #[test]
    fn render_disconnected_wide() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Disconnected,
            server_version: None,
            server_uptime_ms: None,
            server_tier: None,
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
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 8));
    }

    #[test]
    fn render_free_tier_license_message() {
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            server_tier: Some(Tier::Free),
            total_ready: 5,
            total_in_flight: 3,
            total_scheduled: 2,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            now_ms: 1_700_000_000_000,
            active_tab: Tab::Ready,
            h_scroll: [0; 3],
            list_states: Default::default(),
            viewport_height: 0,
            ws_tx: None,
        };
        insta::assert_snapshot!(render_to_string(&app, 80, 20));
    }

    #[test]
    fn render_free_tier_in_flight_tab() {
        let now_ms = 1_700_000_000_000u64;
        let app = App {
            host: "127.0.0.1:8901".to_string(),
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(10_000),
            server_tier: Some(Tier::Free),
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
            ws_tx: None,
        };
        insta::assert_snapshot!(render_to_string(&app, 120, 12));
    }

    #[test]
    fn format_elapsed_examples() {
        let now = 10_000_000u64;
        assert_eq!(format_elapsed(now - 500, now), "< 1s");
        assert_eq!(format_elapsed(now - 5_200, now), "5s");
        assert_eq!(format_elapsed(now - 138_000, now), "2m18s");
        assert_eq!(format_elapsed(now - 3_960_000, now), "1h6m");
        assert_eq!(format_elapsed(0, now), "-");
    }

    #[test]
    fn format_duration_ms_examples() {
        assert_eq!(format_duration_ms(0), "0ms");
        assert_eq!(format_duration_ms(500), "500ms");
        assert_eq!(format_duration_ms(999), "999ms");
        assert_eq!(format_duration_ms(1_000), "1s");
        assert_eq!(format_duration_ms(5_200), "5s");
        assert_eq!(format_duration_ms(60_000), "1m");
        assert_eq!(format_duration_ms(138_000), "2m18s");
        assert_eq!(format_duration_ms(3_600_000), "1h");
        assert_eq!(format_duration_ms(3_960_000), "1h6m");
        assert_eq!(format_duration_ms(86_400_000), "1d");
        assert_eq!(format_duration_ms(190_800_000), "2d5h");
    }

    #[test]
    fn format_due_examples() {
        let now = 10_000_000u64;
        // Future: shows remaining time.
        assert_eq!(format_due(now + 232_000, now), "3m52s");
        assert_eq!(format_due(now + 5_000, now), "5s");
        // Overdue: shows elapsed with "ago" suffix.
        assert_eq!(format_due(now - 60_000, now), "1m ago");
        assert_eq!(format_due(now - 500, now), "< 1s");
        // Sub-second future rounds to "< 1s".
        assert_eq!(format_due(now + 500, now), "< 1s");
        // Zero timestamp.
        assert_eq!(format_due(0, now), "-");
    }
}
