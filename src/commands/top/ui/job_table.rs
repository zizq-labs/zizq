// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job-table rendering for each tab (Ready / In-Flight / Scheduled),
//! plus the horizontal-scroll wrapper for narrow viewports and the
//! cell-styling helpers (attempt color, priority bold, cursor row).

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row, Table, Widget};

use crate::api::admin::AdminJob;
use crate::commands::top::app::{App, Tab};

use super::format::{format_due, format_elapsed};
use super::{MIN_TABLE_WIDTH, header_bg, tab_fg};

/// Render the table for the active tab into `area`. Slices the buffer
/// down to the visible window, picks the right table builder for the
/// tab, and wraps in the horizontal-scroll helper.
pub(super) fn render_active(app: &App, frame: &mut Frame, area: Rect, table_body_height: usize) {
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
    render_scrollable(frame, table, area, tab_scroll);
}

/// Style for the cursor-highlighted row.
pub(super) const CURSOR_STYLE: Style = Style::new().fg(Color::Black).bg(Color::LightCyan);

/// Render a widget into `area`, applying horizontal scrolling when the area
/// is narrower than [`MIN_TABLE_WIDTH`].
pub(super) fn render_scrollable(frame: &mut Frame, widget: impl Widget, area: Rect, h_scroll: u16) {
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
    Style::new().add_modifier(Modifier::BOLD)
}

/// Build a table widget for the Ready pane.
pub(super) fn job_table_ready<'a>(
    jobs: &'a [AdminJob],
    now_ms: u64,
    cursor_row: Option<usize>,
) -> Table<'a> {
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
pub(super) fn job_table_in_flight<'a>(
    jobs: &'a [AdminJob],
    now_ms: u64,
    cursor_row: Option<usize>,
) -> Table<'a> {
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
pub(super) fn job_table_scheduled<'a>(
    jobs: &'a [AdminJob],
    now_ms: u64,
    cursor_row: Option<usize>,
) -> Table<'a> {
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
