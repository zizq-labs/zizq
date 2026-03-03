// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Terminal UI dashboard for Zanxio.
//!
//! Connects to the admin API via WebSocket and renders a live
//! dashboard using ratatui. Requires a Pro license.

pub mod app;
pub mod events;
pub mod ui;

use std::io;
use std::time::Duration;

use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use self::app::App;
use self::events::Event;

/// Arguments for the `tui` subcommand.
#[derive(Parser)]
pub struct Args {
    /// Admin API base URL to connect to.
    #[arg(
        long,
        default_value = "http://127.0.0.1:8901",
        env = "ZANXIO_ADMIN_URL"
    )]
    url: String,
}

/// Run the TUI dashboard.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Set up terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (tx, mut rx) = mpsc::channel::<Event>(64);

    // Spawn terminal input reader (blocking, runs in a thread).
    events::read_terminal_events(tx.clone());

    // Spawn WebSocket connection manager (async).
    events::manage_ws_connection(tx, args.url);

    let mut app = App::new();

    // Main event loop.
    loop {
        terminal.draw(|f| ui::render(&app, f))?;

        // Wait for the next event with a timeout for periodic redraws.
        match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
            Ok(Some(event)) => {
                if app.handle_event(event) {
                    break;
                }
            }
            Ok(None) => break, // Channel closed.
            Err(_) => {}       // Timeout — just redraw.
        }
    }

    // Restore terminal.
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
