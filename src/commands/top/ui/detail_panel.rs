// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Detail panel — the lower split that shows the metadata, payload and
//! failures of the cursor row when `app.show_detail` is on.
//!
//! Beside the table, on a wide terminal, the three are stacked: metadata,
//! then the payload, then the failures in whatever height is left.
//!
//! Below the table, on a wide enough terminal, all three are columns side
//! by side. On a narrower one there are two: the metadata, and the
//! failures if the job has any, or its payload if not, since failures are
//! usually what's being looked for.
//!
//! With focus the panel scrolls, and shows everything: the whole payload,
//! and every loaded failure with its message and backtrace. Without focus
//! it is a compact summary that fits its space.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::api::admin::{AdminJob, AdminJobError};
use crate::commands::top::app::{App, JobDetail, Tab};

use super::format::{format_duration_ms, format_elapsed, format_timestamp, pad_to};
use super::{header_bg, tab_fg};

/// Width of the label column in detail panel metadata lines.
const DETAIL_LABEL_WIDTH: usize = 26;

/// The narrowest panel that shows metadata, payload and failures side by
/// side. Narrower panels show two of them.
const THREE_COLUMN_WIDTH: u16 = 150;

/// The width of the metadata column beside the other two: enough for its
/// labels and a full job ID, with the rest shared by payload and failures.
const METADATA_COLUMN_WIDTH: u16 = 62;

/// Ends a section cut short to fit, while the panel doesn't have focus.
fn more_hint() -> Line<'static> {
    Line::styled("  … Enter to see all", dim())
}

/// Where the panel is, which decides how its parts are arranged.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Placement {
    /// Below the table: the parts side by side.
    Below,
    /// Beside the table: the parts stacked.
    Beside,
}

/// Render the detail panel into `area`. If no row is selected the panel
/// degrades to a heading + "No job selected" placeholder so the user
/// still sees where the panel would be.
///
/// Returns, while it has focus, how far the panel is scrolled once clamped
/// to its content, and whether that's to its end.
pub(super) fn render(
    app: &App,
    frame: &mut Frame,
    area: Rect,
    placement: Placement,
) -> Option<(usize, bool)> {
    let focused = app.detail_view.focused;
    let heading = heading_style(focused);

    // A delete awaiting confirmation asks at the foot of the panel.
    let area = if app.pending_delete.is_some() {
        let [body, prompt] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);
        frame.render_widget(super::help_bar::delete_prompt(None), prompt);
        body
    } else {
        area
    };

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
        let heading = Line::from(Span::styled(pad_to("  ATTRIBUTES", area.width), heading));
        frame.render_widget(
            Paragraph::new(vec![
                heading,
                Line::styled("  No job selected", Style::default().fg(Color::DarkGray)),
            ]),
            area,
        );
        return focused.then_some((0, false));
    };
    let scroll = if focused { app.detail_view.scroll } else { 0 };

    // Beside the table the panel scrolls as one document.
    if placement == Placement::Beside {
        let lines = stacked_lines(app, job, detail, area, focused);
        let end = lines.len().saturating_sub(area.height as usize);
        let scroll = scroll.min(end);
        let lines: Vec<_> = lines.into_iter().skip(scroll).collect();
        frame.render_widget(Paragraph::new(lines), area);
        return focused.then_some((scroll, scroll == end));
    }

    let has_failures = detail.is_some_and(|d| !d.errors.is_empty());
    let columns: &[(Column, Constraint)] = if area.width >= THREE_COLUMN_WIDTH {
        &[
            (Column::Metadata, Constraint::Length(METADATA_COLUMN_WIDTH)),
            (Column::Payload, Constraint::Fill(1)),
            (Column::Failures, Constraint::Fill(1)),
        ]
    } else if has_failures {
        &[
            (Column::Metadata, Constraint::Percentage(50)),
            (Column::Failures, Constraint::Percentage(50)),
        ]
    } else {
        &[
            (Column::Metadata, Constraint::Percentage(50)),
            (Column::Payload, Constraint::Percentage(50)),
        ]
    };
    let areas = Layout::horizontal(columns.iter().map(|(_, size)| *size)).split(area);

    // Below the table the columns scroll together, under their headings.
    let failures_height = (!focused).then_some(area.height);
    let contents: Vec<Vec<Line>> = columns
        .iter()
        .zip(areas.iter())
        .map(|((column, _), area)| match column {
            Column::Metadata => metadata_lines(app, job, detail, area.width, heading),
            Column::Payload => payload_lines(job, detail, area.width, heading),
            Column::Failures => {
                failure_lines(detail, app.now_ms, area.width, failures_height, heading)
            }
        })
        .collect();
    let body_height = area.height.saturating_sub(1) as usize;
    let longest = contents
        .iter()
        .map(|lines| lines.len().saturating_sub(1))
        .max();
    let end = longest.unwrap_or(0).saturating_sub(body_height);
    let scroll = scroll.min(end);

    for (mut lines, area) in contents.into_iter().zip(areas.iter()) {
        if !focused && lines.len() > area.height as usize {
            lines.truncate((area.height as usize).saturating_sub(1));
            lines.push(more_hint());
        }
        let mut lines = lines.into_iter();
        let heading = lines.next();
        let lines: Vec<_> = heading.into_iter().chain(lines.skip(scroll)).collect();
        frame.render_widget(Paragraph::new(lines), *area);
    }
    focused.then_some((scroll, scroll == end))
}

/// Metadata, payload and failures one above another, for a panel beside
/// the table. When `full` everything is shown, to be scrolled through.
///
/// Otherwise, when the payload and failures don't both fit below the
/// metadata, the payload gets what the failures leave, but at least half,
/// and the failures fit themselves into the rest.
fn stacked_lines(
    app: &App,
    job: &AdminJob,
    detail: Option<&JobDetail>,
    area: Rect,
    full: bool,
) -> Vec<Line<'static>> {
    let heading = heading_style(full);
    let mut lines = metadata_lines(app, job, detail, area.width, heading);
    lines.push(Line::default());

    // Less the gap between payload and failures.
    let below = (area.height as usize).saturating_sub(lines.len() + 1);
    // The failures' compact form, with no limit on its height.
    let failures_wanted =
        failure_lines(detail, app.now_ms, area.width, Some(u16::MAX), heading).len();
    let payload_room = below.saturating_sub(failures_wanted).max(below / 2);
    let mut payload = payload_lines(job, detail, area.width, heading);
    if !full && payload.len() > payload_room {
        payload.truncate(payload_room.saturating_sub(1));
        payload.push(more_hint());
    }
    lines.extend(payload);
    lines.push(Line::default());

    let failures_height = (area.height as usize).saturating_sub(lines.len()) as u16;
    lines.extend(failure_lines(
        detail,
        app.now_ms,
        area.width,
        (!full).then_some(failures_height),
        heading,
    ));
    lines
}

/// The columns the panel can show.
enum Column {
    Metadata,
    Payload,
    Failures,
}

/// Section headings, coloured by whether the panel has focus.
fn heading_style(focused: bool) -> Style {
    Style::default().fg(tab_fg()).bg(header_bg(focused))
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
    heading: Style,
) -> Vec<Line<'static>> {
    let dim = dim();
    let val = Style::default().fg(Color::White);

    let left_heading = Line::from(Span::styled(pad_to("  ATTRIBUTES", width), heading));
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
fn payload_lines(
    job: &AdminJob,
    detail: Option<&JobDetail>,
    width: u16,
    heading: Style,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(Span::styled(
        pad_to("  PAYLOAD", width),
        heading,
    ))];
    match &job.payload {
        Some(payload) => {
            let pretty =
                serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
            lines.extend(pretty.lines().map(|l| Line::raw(format!("  {l}"))));
        }
        None if detail.is_some() => {
            lines.push(Line::styled("  (this job no longer exists)", dim()));
        }
        None => {
            lines.push(Line::styled("  (loading…)", dim()));
        }
    }
    lines
}

/// The job's failures, newest first.
///
/// Without a `height` every failure is shown in full, its message and
/// backtrace beneath its line.
///
/// Within a `height` they get one line each, except the newest, which is
/// the one usually being investigated: the rest of its message and its
/// backtrace follow its line, in whatever height the others leave, but at
/// least half of it, however many others there are.
fn failure_lines(
    detail: Option<&JobDetail>,
    now_ms: u64,
    width: u16,
    height: Option<u16>,
    heading: Style,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(Span::styled(
        pad_to("  FAILURES", width),
        heading,
    ))];
    let Some(detail) = detail else {
        lines.push(Line::styled("  (loading…)", dim()));
        return lines;
    };
    let (errors, more) = detail.failures();
    let Some((newest, older)) = errors.split_first() else {
        lines.push(Line::styled("  None", dim()));
        return lines;
    };

    // An expanded failure's summary names only its type, when it has one,
    // so its whole message follows beneath, where a long first line isn't
    // cut short by the type before it.
    let summary = |error: &AdminJobError, expanded: bool, newest: bool| {
        let first_line = error.message.lines().next().unwrap_or("");
        let what = match &error.error_type {
            Some(error_type) if expanded => error_type.clone(),
            Some(error_type) => format!("{error_type}: {first_line}"),
            None => first_line.to_string(),
        };
        let what_style = if newest {
            Style::default().fg(Color::LightRed)
        } else {
            Style::default()
        };
        Line::from(vec![
            Span::styled(format!("  #{:<3}", error.attempt), dim()),
            Span::styled(
                format!("{:>6} ago  ", format_elapsed(error.failed_at, now_ms)),
                dim(),
            ),
            Span::styled(what, what_style),
        ])
    };

    let Some(height) = height else {
        for (i, error) in errors.iter().enumerate() {
            lines.push(summary(error, true, i == 0));
            lines.extend(failure_body(error));
        }
        // Older failures load once the panel is scrolled to its end.
        if detail.loading_older() {
            lines.push(Line::styled("  Loading older failures…", dim()));
        } else if more {
            lines.push(Line::styled("  … scroll for older failures", dim()));
        }
        return lines;
    };

    // Height for the newest failure's message and backtrace: what's left
    // after a line for every other failure (and one for the note of more),
    // or half of what's below its own line, whichever is more.
    let rows = height.saturating_sub(1) as usize;
    let below_newest = rows.saturating_sub(1);
    let others = older.len() + usize::from(more);
    let room = below_newest.saturating_sub(others).max(below_newest / 2);

    lines.push(summary(newest, true, true));
    lines.extend(failure_body(newest).into_iter().take(room));

    // The rest get a line each while they fit. If they don't all fit,
    // the last line says so instead.
    let remaining = (rows + 1).saturating_sub(lines.len());
    let all_fit = !more && older.len() <= remaining;
    let older_fit = if all_fit {
        older.len()
    } else {
        remaining.saturating_sub(1)
    };
    lines.extend(
        older
            .iter()
            .take(older_fit)
            .map(|e| summary(e, false, false)),
    );
    if !all_fit && remaining > 0 {
        lines.push(Line::styled("  … older failures not shown", dim()));
    }
    lines
}

/// The lines beneath an expanded failure's summary: its message, except a
/// first line already on the summary, then its backtrace.
fn failure_body(error: &AdminJobError) -> Vec<Line<'static>> {
    let message_shown_above = usize::from(error.error_type.is_none());
    let message = error
        .message
        .lines()
        .skip(message_shown_above)
        .map(|l| Line::raw(format!("       {l}")));
    let backtrace = error
        .backtrace
        .iter()
        .flat_map(|b| b.lines())
        .map(|l| Line::styled(format!("       {l}"), dim()));
    message.chain(backtrace).collect()
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
