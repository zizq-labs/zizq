// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Tab bar — the row of `[In-Flight] [Ready] [Scheduled]` labels above
//! the content area. The active tab uses the table-header background
//! so it visually connects to the table below.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::commands::top::app::{App, Tab};

use super::{header_bg, tab_fg};

pub(super) fn render(app: &App, frame: &mut Frame, area: Rect) {
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

    frame.render_widget(Paragraph::new(Line::from(tab_spans)), area);
}
