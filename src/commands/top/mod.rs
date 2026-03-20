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

/// Arguments for the `tui` subcommand.
#[derive(Parser)]
pub struct Args {
    /// Admin API base URL to connect to.
    #[arg(long, default_value = "http://127.0.0.1:8901", env = "ZIZQ_ADMIN_URL")]
    url: String,

    /// UI refresh rate in milliseconds.
    #[arg(long, short = 'r', value_name = "MILLISECONDS", default_value = "500")]
    refresh_rate: u64,

    /// Path to a PEM-encoded CA certificate for verifying the admin server.
    #[arg(long, value_name = "PATH", env = "ZIZQ_ADMIN_TLS_CA")]
    ca_cert: Option<String>,

    /// Path to a PEM-encoded client certificate for mTLS with the admin server.
    /// Must be used together with --client-key.
    #[arg(long, value_name = "PATH", env = "ZIZQ_ADMIN_TLS_CLIENT_CERT")]
    client_cert: Option<String>,

    /// Path to a PEM-encoded client private key for mTLS with the admin server.
    /// Must be used together with --client-cert.
    #[arg(long, value_name = "PATH", env = "ZIZQ_ADMIN_TLS_CLIENT_KEY")]
    client_key: Option<String>,
}

/// Run the TUI dashboard.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Validate TLS argument combinations.
    match (&args.client_cert, &args.client_key) {
        (Some(_), None) | (None, Some(_)) => {
            return Err("--client-cert and --client-key must be provided together".into());
        }
        _ => {}
    }

    // Build the HTTP client with optional TLS configuration.
    let http_client = build_http_client(&args)?;

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

    let host = reqwest::Url::parse(&args.url)
        .ok()
        .and_then(|u| {
            let host = u.host_str()?.to_string();
            match u.port() {
                Some(port) => Some(format!("{host}:{port}")),
                None => Some(host),
            }
        })
        .unwrap_or_else(|| args.url.clone());

    // Spawn WebSocket connection manager (async, bidirectional).
    events::manage_ws_connection(tx, ws_out_rx, args.url, http_client);

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

/// Build a reqwest HTTP client with optional TLS configuration.
fn build_http_client(args: &Args) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let mut builder = reqwest::Client::builder();

    if let Some(ref ca_path) = args.ca_cert {
        let pem =
            std::fs::read(ca_path).map_err(|e| format!("failed to read CA cert {ca_path}: {e}"))?;
        let cert = reqwest::tls::Certificate::from_pem(&pem)
            .map_err(|e| format!("failed to parse CA cert {ca_path}: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }

    if let (Some(cert_path), Some(key_path)) = (&args.client_cert, &args.client_key) {
        let cert_pem = std::fs::read(cert_path)
            .map_err(|e| format!("failed to read client cert {cert_path}: {e}"))?;
        let key_pem = std::fs::read(key_path)
            .map_err(|e| format!("failed to read client key {key_path}: {e}"))?;
        let mut identity_pem = cert_pem;
        identity_pem.extend_from_slice(&key_pem);
        let identity = reqwest::tls::Identity::from_pem(&identity_pem)
            .map_err(|e| format!("failed to build client identity: {e}"))?;
        builder = builder.identity(identity);
    }

    builder.build().map_err(|e| e.into())
}
