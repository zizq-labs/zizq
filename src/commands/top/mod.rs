// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Terminal UI dashboard for Zizq.
//!
//! Parses CLI arguments, connects to the admin API via WebSocket, and renders
//! a live view of queue activity. Display limits apply without a pro license.
//!
//! ```text
//! Usage: zizq top [OPTIONS]
//! ```

pub mod app;
pub mod events;
pub mod ui;

use std::io;
use std::time::Duration;

use clap::Parser;
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use self::app::App;
use self::events::Event;

use super::AdminClientArgs;

/// Arguments for the `tui` subcommand.
#[derive(Parser)]
pub struct Args {
    #[command(flatten)]
    admin: AdminClientArgs,

    /// UI refresh rate in milliseconds.
    #[arg(long, short = 'r', value_name = "MILLISECONDS", default_value = "500")]
    refresh_rate: u64,
}

/// Run the TUI dashboard.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    args.admin.validate()?;
    let http_client = args.admin.build_http_client()?;

    // Set up terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (tx, mut rx) = mpsc::channel::<Event>(64);

    // Channel for outbound WS messages from App to the WS task.
    let (ws_out_tx, ws_out_rx) = mpsc::channel::<String>(64);

    // Spawn terminal input reader (blocking, runs in a thread).
    events::read_terminal_events(tx.clone());

    let host = reqwest::Url::parse(&args.admin.url)
        .ok()
        .and_then(|u| {
            let host = u.host_str()?.to_string();
            match u.port() {
                Some(port) => Some(format!("{host}:{port}")),
                None => Some(host),
            }
        })
        .unwrap_or_else(|| args.admin.url.clone());

    // Spawn WebSocket connection manager (async, bidirectional).
    events::manage_ws_connection(tx, ws_out_rx, args.admin.url, http_client);

    let mut app = App::new(host);
    app.set_ws_tx(ws_out_tx);

    // Main event loop — process events as they arrive, redraw on tick.
    let mut tick = tokio::time::interval(Duration::from_millis(args.refresh_rate));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut should_quit = false;

    // Short delay for batching rapid scroll events before rendering.
    let scroll_debounce = Duration::from_millis(16);

    loop {
        // Process events until the next tick fires, or a user input
        // event arrives (user input triggers an immediate render).
        // Scroll events set a short debounce instead of rendering
        // immediately, so holding an arrow key batches updates.
        let mut scroll_dirty = false;

        loop {
            tokio::select! {
                biased;
                _ = tick.tick() => break,
                _ = tokio::time::sleep(scroll_debounce), if scroll_dirty => break,
                event = rx.recv() => {
                    match event {
                        Some(event) => {
                            let immediate = event.is_user_input();
                            let scroll = event.is_scroll();
                            if app.handle_event(event) {
                                should_quit = true;
                                break;
                            }
                            if immediate {
                                break;
                            }
                            if scroll {
                                scroll_dirty = true;
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
        terminal.draw(|f| ui::render(&mut app, f))?;
    }

    // Restore terminal.
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
