// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Bottom help bar — keyboard shortcut hints, the delete confirmation
//! prompt (when active), and the "Resume to scroll further" hint at the
//! edges of a paused/frozen buffer.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::commands::top::app::{App, Tab};

pub(super) fn render(app: &App, frame: &mut Frame, area: Rect) {
    let key_style = Style::default();
    let label_style = Style::default().fg(Color::Black).bg(Color::LightCyan);

    // When a delete confirmation is pending, the help bar becomes a modal
    // prompt — replaces the shortcuts entirely so the user is never in
    // doubt about what `y` and `n` will do.
    if let Some(id) = &app.pending_delete {
        let prompt = Paragraph::new(Line::from(vec![
            Span::styled(
                " Delete job ",
                Style::default().fg(Color::Black).bg(Color::LightRed),
            ),
            Span::styled(
                id.as_str(),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::LightRed)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("?  ", Style::default().fg(Color::Black).bg(Color::LightRed)),
            Span::styled(" y ", key_style),
            Span::styled("Yes", label_style),
            Span::styled("  n ", key_style),
            Span::styled("No", label_style),
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
        Span::styled(" Tab ", key_style),
        Span::styled("Change Tab", label_style),
        Span::raw("  "),
    ];
    if app.paused {
        let crossed = Modifier::CROSSED_OUT;
        help_spans.push(Span::styled("i", key_style.add_modifier(crossed)));
        help_spans.push(Span::raw(" "));
        help_spans.push(Span::styled("Info", label_style.add_modifier(crossed)));
    } else {
        help_spans.push(Span::styled("i", key_style));
        help_spans.push(Span::raw(" "));
        help_spans.push(Span::styled("Info", label_style));
    }
    help_spans.extend([
        Span::styled("  p ", key_style),
        Span::styled(pause_label, label_style),
        Span::styled("  D ", key_style),
        Span::styled("Delete", label_style),
        Span::styled("  q ", key_style),
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
