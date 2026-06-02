// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Detail panel — the lower split that shows the metadata + payload of
//! the cursor row when `app.show_detail` is on.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::commands::top::app::{App, Tab};

use super::format::{format_duration_ms, format_timestamp, pad_to};
use super::tab_fg;

/// Width of the label column in detail panel metadata lines.
const DETAIL_LABEL_WIDTH: usize = 26;

/// Render the detail panel into `area`. If no row is selected the panel
/// degrades to a heading + "No job selected" placeholder so the user
/// still sees where the panel would be.
pub(super) fn render(app: &App, frame: &mut Frame, area: Rect) {
    let heading_style = Style::default().fg(tab_fg()).bg(Color::Yellow);

    let ls = &app.list_states[app.active_tab.idx()];
    let jobs = match app.active_tab {
        Tab::Ready => &app.ready_jobs,
        Tab::InFlight => &app.in_flight_jobs,
        Tab::Scheduled => &app.scheduled_jobs,
    };

    let buf_idx = ls.cursor.checked_sub(ls.buffer_offset);
    let job = buf_idx.and_then(|i| jobs.get(i));

    let Some(job) = job else {
        // Heading bar even when no job selected.
        let heading = Line::from(Span::styled(
            pad_to("  ATTRIBUTES", area.width),
            heading_style,
        ));
        frame.render_widget(
            Paragraph::new(vec![
                heading,
                Line::styled("  No job selected", Style::default().fg(Color::DarkGray)),
            ]),
            area,
        );
        return;
    };

    // Split into left (metadata) and right (payload).
    let halves =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(area);

    // Left column: heading + key-value metadata.
    let dim = Style::default().fg(Color::DarkGray);
    let val = Style::default().fg(Color::White);

    let left_heading = Line::from(Span::styled(
        pad_to("  ATTRIBUTES", halves[0].width),
        heading_style,
    ));

    let status = match app.active_tab {
        Tab::Ready => "ready",
        Tab::InFlight => "in_flight",
        Tab::Scheduled => "scheduled",
    };

    let attempts_value = match job.retry_limit {
        Some(limit) => format!("{}/{}", job.attempts, limit),
        None => job.attempts.to_string(),
    };

    let mut meta: Vec<Line> = vec![
        left_heading,
        detail_line("  ID", job.id.clone(), dim, val),
        detail_line("  Queue", job.queue.clone(), dim, val),
        detail_line("  Type", job.job_type.clone(), dim, val),
        detail_line("  Priority", job.priority.to_string(), dim, val),
        detail_line("  Status", status, dim, val),
        detail_line("  Ready At", format_timestamp(job.ready_at), dim, val),
    ];

    if let Some(at) = job.failed_at {
        meta.push(detail_line("  Failed At", format_timestamp(at), dim, val));
    }
    if let Some(at) = job.dequeued_at {
        meta.push(detail_line("  Dequeued At", format_timestamp(at), dim, val));
    }

    meta.push(detail_line(
        "  Attempts / Retry Limit",
        attempts_value,
        dim,
        val,
    ));

    if let Some(ref b) = job.backoff {
        meta.push(detail_line(
            "  Backoff",
            format!(
                "base={},exponent={},jitter={}",
                b.base_ms, b.exponent, b.jitter_ms
            ),
            dim,
            val,
        ));
    }

    if let Some(ref r) = job.retention {
        let parts: Vec<String> = [
            r.completed_ms
                .map(|ms| format!("completed={}", format_duration_ms(ms))),
            r.dead_ms
                .map(|ms| format!("dead={}", format_duration_ms(ms))),
        ]
        .into_iter()
        .flatten()
        .collect();
        if !parts.is_empty() {
            meta.push(detail_line(
                "  Retention Policy",
                parts.join(", "),
                dim,
                val,
            ));
        }
    }

    if let Some(ref key) = job.unique_key {
        let scope = job.unique_while.as_deref().unwrap_or("queued");
        meta.push(detail_line(
            "  Unique Key",
            format!("{scope} {key}"),
            dim,
            val,
        ));
    }

    frame.render_widget(Paragraph::new(meta), halves[0]);

    // Right column: heading + pretty-printed payload.
    let right_heading = Line::from(Span::styled(
        pad_to(" PAYLOAD", halves[1].width),
        heading_style,
    ));

    let mut payload_lines: Vec<Line> = vec![right_heading];
    match &job.payload {
        Some(payload) => {
            let pretty =
                serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
            payload_lines.extend(pretty.lines().map(|l| Line::raw(format!(" {l}"))));
        }
        None => {
            payload_lines.push(Line::styled(" (not loaded)", dim));
        }
    }

    frame.render_widget(Paragraph::new(payload_lines), halves[1]);
}

/// Build a single key-value line for the detail panel metadata column.
fn detail_line(
    label: &str,
    value: impl Into<String>,
    label_style: Style,
    value_style: Style,
) -> Line<'static> {
    let padded = format!("{:width$}", label, width = DETAIL_LABEL_WIDTH);
    Line::from(vec![
        Span::styled(padded, label_style),
        Span::styled(value.into(), value_style),
    ])
}
