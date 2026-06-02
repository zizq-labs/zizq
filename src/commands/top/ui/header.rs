// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Top-of-screen header — version, connection indicator, server info,
//! totals line, and the embedded depth bar with cursor markers.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::commands::top::app::{App, ConnectionStatus};

use super::depth_bar;
use super::format::format_duration_ms;

pub(super) fn render(app: &App, frame: &mut Frame, area: Rect) {
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

    let bold_dark_gray = Style::default()
        .fg(Color::DarkGray)
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
    let mut status_spans = vec![
        Span::styled("\u{25cf} ", status_style),
        Span::styled(indicator, status_text_style),
        Span::raw(" "),
        Span::styled(&*app.host, cyan),
    ];
    if app.paused {
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            "PAUSED",
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightYellow)
                .add_modifier(Modifier::BOLD),
        ));
    }
    let status_line = Line::from(status_spans);

    let server_info_line = if connected {
        let version = app.server_version.as_deref().unwrap_or("?");
        let uptime = app
            .server_uptime_ms
            .map(format_duration_ms)
            .unwrap_or_else(|| "?".to_string());
        Line::from(vec![
            Span::raw("  "),
            Span::styled("Server version: ", cyan),
            Span::styled(version.to_string(), bold_dark_gray),
            Span::styled(", Uptime: ", cyan),
            Span::styled(uptime, bold_dark_gray),
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
        depth_bar::render(
            app.total_in_flight,
            app.total_ready,
            app.total_scheduled,
            area.width as usize,
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
    frame.render_widget(header, area);
}
