// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Bottom help bar — keyboard shortcut hints, the delete confirmation
//! prompt (when active), the `/` search prompt (with Type and Queue
//! fields), and the "Resume to scroll further" hint at the edges of a
//! paused/frozen buffer.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::commands::top::app::{App, SearchField, SearchPrompt, Tab};

pub(super) fn render(app: &App, frame: &mut Frame, area: Rect) {
    let key_style = Style::default();
    let label_style = Style::default().fg(Color::Black).bg(Color::LightCyan);

    // While the `/` search prompt is open the help bar becomes an
    // input line with two labeled fields — Type and Queue. Tab switches
    // between them.
    if let Some(prompt) = &app.search_input {
        render_search_prompt(prompt, frame, area);
        return;
    }

    // When a delete confirmation is pending, the help bar becomes a modal
    // prompt — replaces the shortcuts entirely so the user is never in
    // doubt about what `y` and `n` will do.
    if let Some(id) = &app.pending_delete {
        // Full red bar, white text. Keys (y / n) are bold; the job id
        // is bold; everything else is regular white on red.
        let base = Style::default().fg(Color::White).bg(Color::Red);
        let strong = base.add_modifier(Modifier::BOLD);
        let prompt = Paragraph::new(Line::from(vec![
            Span::styled(" Delete job ", base),
            Span::styled(id.as_str(), strong),
            Span::styled("? ", base),
            Span::styled("y", strong),
            Span::styled(" Yes  ", base),
            Span::styled("n", strong),
            Span::styled(" No ", base),
        ]));
        frame.render_widget(prompt, area);
        return;
    }

    // While paused the `i` slot strikes through (rather than vanishing
    // entirely) so it reads as "disabled" without depending on a
    // light/dark terminal theme. We only mark the glyphs themselves with
    // CROSSED_OUT so the strike-through line doesn't spill through the
    // padding spaces and touch the neighbouring labels.
    let pause_label = if app.paused { "Resume" } else { "Pause" };
    let mut help_spans: Vec<Span> = vec![
        Span::raw(" "),
        Span::styled("Tab", key_style),
        Span::styled("Change Tab", label_style),
        Span::raw(" "),
    ];
    if app.paused {
        let crossed = Modifier::CROSSED_OUT;
        help_spans.push(Span::styled("i", key_style.add_modifier(crossed)));
        help_spans.push(Span::styled("Info", label_style.add_modifier(crossed)));
    } else {
        help_spans.push(Span::styled("i", key_style));
        help_spans.push(Span::styled("Info", label_style));
    }
    help_spans.extend([
        Span::styled(" p", key_style),
        Span::styled(pause_label, label_style),
        Span::styled(" /", key_style),
        Span::styled("Find", label_style),
        Span::styled(" D", key_style),
        Span::styled("Delete", label_style),
        Span::styled(" q", key_style),
        Span::styled("Quit", label_style),
    ]);

    // When paused and the cursor is at the edge of the frozen buffer,
    // tell the user the only way to scroll further is to resume.
    if app.paused {
        let ls = &app.list_states[app.active_tab.idx()];
        let buffer_size = match app.active_tab {
            Tab::Ready => app.ready_jobs.len(),
            Tab::InFlight => app.in_flight_jobs.len(),
            Tab::Scheduled => app.scheduled_jobs.len(),
        };
        if buffer_size > 0 {
            let buffer_end = ls.buffer_offset + buffer_size - 1;
            let at_top = ls.cursor == ls.buffer_offset;
            let at_bottom = ls.cursor == buffer_end;
            if at_top || at_bottom {
                let arrow = if at_bottom { "\u{2193}" } else { "\u{2191}" };
                help_spans.push(Span::raw("   "));
                help_spans.push(Span::styled(
                    format!("{arrow} Resume to scroll further"),
                    Style::default().fg(Color::LightYellow),
                ));
            }
        }
    }

    let help = Paragraph::new(Line::from(help_spans));
    frame.render_widget(help, area);
}

/// Minimum visual width reserved for each search field's value area.
/// The field grows past this if the user types more than fits — but
/// never shrinks below, so the two fields don't visually reflow as
/// characters are added or removed.
const SEARCH_FIELD_MIN_WIDTH: u16 = 20;

/// Render the two-field search prompt (Type / Queue). The inactive
/// field is dimmed; the active field gets the terminal cursor placed
/// on it via `frame.set_cursor_position` (which yields a real blinking
/// block cursor rather than injecting a `_` character that shifts the
/// text as the user edits). Each field reserves at least
/// `SEARCH_FIELD_MIN_WIDTH` columns of value area, padded with
/// spaces, so the fields don't jiggle as the user types.
fn render_search_prompt(prompt: &SearchPrompt, frame: &mut Frame, area: Rect) {
    let banner_style = Style::default().fg(Color::Black).bg(Color::LightYellow);
    let label_style = Style::default().fg(Color::LightYellow);
    let active_value_style = Style::default().add_modifier(Modifier::BOLD);
    let inactive_value_style = Style::default().fg(Color::DarkGray);

    let mut spans: Vec<Span> = Vec::new();
    let mut x: u16 = area.x;
    let mut cursor_x: Option<u16> = None;

    // Helper: push a span and advance the running x by its width.
    let push = |spans: &mut Vec<Span>, x: &mut u16, span: Span<'static>| {
        *x = x.saturating_add(span.width() as u16);
        spans.push(span);
    };

    push(&mut spans, &mut x, Span::styled(" Search ", banner_style));
    push(&mut spans, &mut x, Span::raw(" "));

    for (field, label) in [
        (SearchField::Type, "Type: "),
        (SearchField::Queue, "Queue: "),
    ] {
        let is_active = prompt.active == field;
        let input = match field {
            SearchField::Type => &prompt.type_input,
            SearchField::Queue => &prompt.queue_input,
        };
        let value = input.value();

        push(&mut spans, &mut x, Span::styled(label, label_style));

        let value_style = if is_active {
            active_value_style
        } else {
            inactive_value_style
        };
        let value_width = if is_active {
            value.chars().count() as u16
        } else if value.is_empty() {
            1 // rendered "…"
        } else {
            value.chars().count() as u16
        };

        if is_active {
            // Record where the terminal cursor should land — one visual
            // column per character before the cursor.
            cursor_x = Some(x.saturating_add(input.visual_cursor() as u16));
            push(
                &mut spans,
                &mut x,
                Span::styled(value.to_string(), value_style),
            );
        } else if value.is_empty() {
            push(&mut spans, &mut x, Span::styled("…", value_style));
        } else {
            push(
                &mut spans,
                &mut x,
                Span::styled(value.to_string(), value_style),
            );
        }

        // Pad the field to the minimum width so short values don't
        // cause the next field to reflow. If the value already exceeds
        // the minimum, the field just grows — no truncation.
        let pad = SEARCH_FIELD_MIN_WIDTH.saturating_sub(value_width);
        if pad > 0 {
            push(&mut spans, &mut x, Span::raw(" ".repeat(pad as usize)));
        }

        push(&mut spans, &mut x, Span::raw("  "));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
    if let Some(cx) = cursor_x {
        frame.set_cursor_position((cx, area.y));
    }
}
