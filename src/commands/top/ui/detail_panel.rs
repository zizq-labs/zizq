// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Detail panel — the lower split that shows the metadata, payload and
//! failures of the cursor row when `app.show_detail` is on.
//!
//! On a wide enough terminal all three are columns side by side. On a
//! narrower one there are two: the metadata, and the failures if the job
//! has any, or its payload if not, since failures are usually what's
//! being looked for.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::api::admin::{AdminJob, AdminJobError};
use crate::commands::top::app::{App, JobDetail, Tab};

use super::format::{format_duration_ms, format_elapsed, format_timestamp, pad_to};
use super::tab_fg;

/// Width of the label column in detail panel metadata lines.
const DETAIL_LABEL_WIDTH: usize = 26;

/// The narrowest panel that shows metadata, payload and failures side by
/// side. Narrower panels show two of them.
const THREE_COLUMN_WIDTH: u16 = 150;

/// Render the detail panel into `area`. If no row is selected the panel
/// degrades to a heading + "No job selected" placeholder so the user
/// still sees where the panel would be.
pub(super) fn render(app: &App, frame: &mut Frame, area: Rect) {
    let ls = &app.list_states[app.active_tab.idx()];
    let jobs = match app.active_tab {
        Tab::Ready => &app.ready_jobs,
        Tab::InFlight => &app.in_flight_jobs,
        Tab::Scheduled => &app.scheduled_jobs,
    };

    let buf_idx = ls.cursor.checked_sub(ls.buffer_offset);
    let row = buf_idx.and_then(|i| jobs.get(i));

    // The server's full answer for the row's job, once it has arrived.
    // Until then the row itself is shown, which has no payload.
    let detail = row.and_then(|row| app.job_detail.as_ref().filter(|d| d.id == row.id));
    let job = detail.and_then(|d| d.job.as_ref()).or(row);

    let Some(job) = job else {
        // Heading bar even when no job selected.
        let heading = Line::from(Span::styled(
            pad_to("  ATTRIBUTES", area.width),
            heading_style(),
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

    let has_failures = detail.is_some_and(|d| !d.errors.is_empty());
    let columns: &[(Column, u16)] = if area.width >= THREE_COLUMN_WIDTH {
        &[
            (Column::Metadata, 38),
            (Column::Payload, 22),
            (Column::Failures, 40),
        ]
    } else if has_failures {
        &[(Column::Metadata, 50), (Column::Failures, 50)]
    } else {
        &[(Column::Metadata, 50), (Column::Payload, 50)]
    };
    let areas =
        Layout::horizontal(columns.iter().map(|(_, pct)| Constraint::Percentage(*pct))).split(area);

    for ((column, _), area) in columns.iter().zip(areas.iter()) {
        let lines = match column {
            Column::Metadata => metadata_lines(app, job, detail, area.width),
            Column::Payload => payload_lines(job, detail, area.width),
            Column::Failures => failure_lines(detail, app.now_ms, area.width, area.height),
        };
        frame.render_widget(Paragraph::new(lines), *area);
    }
}

/// The columns the panel can show.
enum Column {
    Metadata,
    Payload,
    Failures,
}

fn heading_style() -> Style {
    Style::default().fg(tab_fg()).bg(Color::Yellow)
}

fn dim() -> Style {
    Style::default().fg(Color::DarkGray)
}

/// The job's attributes, one per line.
fn metadata_lines(
    app: &App,
    job: &AdminJob,
    detail: Option<&JobDetail>,
    width: u16,
) -> Vec<Line<'static>> {
    let dim = dim();
    let val = Style::default().fg(Color::White);

    let left_heading = Line::from(Span::styled(pad_to("  ATTRIBUTES", width), heading_style()));
    // The job may have moved on since the row was fetched.
    let status = detail
        .and_then(|d| d.status.as_deref())
        .unwrap_or(match app.active_tab {
            Tab::Ready => "ready",
            Tab::InFlight => "in_flight",
            Tab::Scheduled => "scheduled",
        });

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

    meta
}

/// The job's payload, pretty-printed.
fn payload_lines(job: &AdminJob, detail: Option<&JobDetail>, width: u16) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(Span::styled(
        pad_to(" PAYLOAD", width),
        heading_style(),
    ))];
    match &job.payload {
        Some(payload) => {
            let pretty =
                serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
            lines.extend(pretty.lines().map(|l| Line::raw(format!(" {l}"))));
        }
        None if detail.is_some() => {
            lines.push(Line::styled(" (this job no longer exists)", dim()));
        }
        None => {
            lines.push(Line::styled(" (loading…)", dim()));
        }
    }
    lines
}

/// The job's failures, newest first, one line each. The newest is the one
/// usually being investigated, so the rest of its message and its
/// backtrace follow its line: in whatever height the others leave, but at
/// least half of it, however many others there are.
fn failure_lines(
    detail: Option<&JobDetail>,
    now_ms: u64,
    width: u16,
    height: u16,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(Span::styled(
        pad_to(" FAILURES", width),
        heading_style(),
    ))];
    let Some(detail) = detail else {
        lines.push(Line::styled(" (loading…)", dim()));
        return lines;
    };
    let Some((newest, older)) = detail.errors.split_first() else {
        lines.push(Line::styled(" None", dim()));
        return lines;
    };

    let summary = |error: &AdminJobError, newest: bool| {
        let first_line = error.message.lines().next().unwrap_or("");
        let what = match &error.error_type {
            Some(error_type) => format!("{error_type}: {first_line}"),
            None => first_line.to_string(),
        };
        let what_style = if newest {
            Style::default().fg(Color::LightRed)
        } else {
            Style::default()
        };
        Line::from(vec![
            Span::styled(format!(" #{:<3}", error.attempt), dim()),
            Span::styled(
                format!("{:>6} ago  ", format_elapsed(error.failed_at, now_ms)),
                dim(),
            ),
            Span::styled(what, what_style),
        ])
    };

    // Height for the newest failure's message and backtrace: what's left
    // after a line for every other failure (and one for the note of more),
    // or half of what's below its own line, whichever is more.
    let rows = height.saturating_sub(1) as usize;
    let more = detail.more_errors;
    let below_newest = rows.saturating_sub(1);
    let others = older.len() + usize::from(more);
    let room = below_newest.saturating_sub(others).max(below_newest / 2);

    lines.push(summary(newest, true));
    let rest_of_message = newest
        .message
        .lines()
        .skip(1)
        .map(|l| Line::raw(format!("      {l}")));
    let backtrace = newest
        .backtrace
        .iter()
        .flat_map(|b| b.lines())
        .map(|l| Line::styled(format!("      {l}"), dim()));
    lines.extend(rest_of_message.chain(backtrace).take(room));

    // The rest get a line each while they fit. If they don't all fit,
    // the last line says so instead.
    let remaining = (rows + 1).saturating_sub(lines.len());
    let all_fit = !more && older.len() <= remaining;
    let older_fit = if all_fit {
        older.len()
    } else {
        remaining.saturating_sub(1)
    };
    lines.extend(older.iter().take(older_fit).map(|e| summary(e, false)));
    if !all_fit && remaining > 0 {
        lines.push(Line::styled(" … older failures not shown", dim()));
    }
    lines
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
