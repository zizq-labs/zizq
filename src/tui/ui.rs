// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Ratatui rendering for the TUI dashboard.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use super::app::{App, ConnectionStatus};

/// Render the current application state to a terminal frame.
pub fn render(app: &App, frame: &mut Frame) {
    let chunks = Layout::vertical([Constraint::Length(3), Constraint::Min(0)]).split(frame.area());

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
        status_spans.push(Span::raw(format!("uptime: {}s", uptime_ms / 1000)));
    }

    let status_bar = Paragraph::new(Line::from(status_spans)).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Zanxio {}", env!("CARGO_PKG_VERSION"))),
    );

    frame.render_widget(status_bar, chunks[0]);

    // Content area.
    let content = Paragraph::new("Press 'q' to quit")
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL));

    frame.render_widget(content, chunks[1]);
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

    #[test]
    fn render_connecting() {
        let app = App::new();
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_connected_with_server_info() {
        let app = App {
            status: ConnectionStatus::Connected,
            server_version: Some("1.0.0".to_string()),
            server_uptime_ms: Some(65_000),
        };
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }

    #[test]
    fn render_disconnected() {
        let app = App {
            status: ConnectionStatus::Disconnected,
            server_version: None,
            server_uptime_ms: None,
        };
        insta::assert_snapshot!(render_to_string(&app, 60, 10));
    }
}
