// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Depth bar visualization — the `Depth[│││             3 jobs]` widget
//! in the header that shows in-flight / ready / scheduled proportions
//! plus a triangle marker indicating where the cursor sits within the
//! active tab's slice.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use crate::commands::top::app::Tab;

/// Render the depth bar with cursor markers above and below.
///
/// Returns `[marker_above, depth_bar, marker_below]`.
pub(super) fn render(
    in_flight: usize,
    ready: usize,
    scheduled: usize,
    width: usize,
    active_tab: Tab,
    cursor: usize,
) -> [Line<'static>; 3] {
    let total = in_flight + ready + scheduled;
    let total_label = format!("{total} jobs");
    let bold_default = Style::default().add_modifier(Modifier::BOLD);
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
        Span::styled("[", bold_default),
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
        Span::styled("]  ", bold_default),
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
