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

    /// UI refresh rate in milliseconds.
    #[arg(long, short = 'r', value_name = "MILLISECONDS", default_value = "500")]
    refresh_rate: u64,
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

    // Main event loop — process events as they arrive, redraw on tick.
    let mut tick = tokio::time::interval(Duration::from_millis(args.refresh_rate));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut should_quit = false;

    loop {
        // Process events until the next tick fires.
        loop {
            tokio::select! {
                biased;
                _ = tick.tick() => break,
                event = rx.recv() => {
                    match event {
                        Some(event) => {
                            if app.handle_event(event) {
                                should_quit = true;
                                break;
                            }
                        }
                        None => { should_quit = true; break; }
                    }
                }
            }
        }

        // Drain any remaining buffered events.
        while !should_quit {
            match rx.try_recv() {
                Ok(event) => {
                    if app.handle_event(event) {
                        should_quit = true;
                    }
                }
                Err(_) => break,
            }
        }

        if should_quit {
            break;
        }

        app.now_ms = crate::time::now_millis();
        terminal.draw(|f| ui::render(&app, f))?;
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
