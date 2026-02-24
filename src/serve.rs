// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zanxio CLI `serve` command entry point.
//!
//! Parses CLI arguments, initializes the database, and starts the HTTP server.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::http::{
    self, AppState, BackoffConfig, DEFAULT_BACKOFF_BASE_MS, DEFAULT_BACKOFF_EXPONENT,
    DEFAULT_BACKOFF_JITTER_MS, DEFAULT_GLOBAL_WORKING_LIMIT, DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_RETRY_LIMIT,
};
use crate::store::Store;

/// Location of the internal database within the root directory.
const DATABASE_DIR: &str = "data";

/// Arguments for the `serve` subcommand.
#[derive(Parser)]
pub struct Args {
    /// Root directory for all server data and configuration.
    #[arg(long, default_value = "./zanxio-root", env = "ZANXIO_ROOT_DIR")]
    root_dir: String,

    /// Address to bind the HTTP server to.
    #[arg(long, default_value = "127.0.0.1", env = "ZANXIO_HOST")]
    host: String,

    /// Port to listen for HTTP connections on.
    #[arg(long, default_value_t = 7890, env = "ZANXIO_PORT")]
    port: u16,

    /// Seconds between heartbeat frames on idle take connections.
    #[arg(long, default_value_t = DEFAULT_HEARTBEAT_SECONDS, env = "ZANXIO_HEARTBEAT_INTERVAL")]
    heartbeat_interval: u64,

    /// Maximum number of jobs in the working set across all connections.
    /// 0 means no limit.
    #[arg(long, default_value_t = DEFAULT_GLOBAL_WORKING_LIMIT, env = "ZANXIO_GLOBAL_WORKING_LIMIT")]
    global_working_limit: u64,

    /// Default maximum retries before a failed job is killed.
    /// Jobs can override this at enqueue time with a per-job retry_limit.
    #[arg(long, default_value_t = DEFAULT_RETRY_LIMIT, env = "ZANXIO_DEFAULT_RETRY_LIMIT")]
    default_retry_limit: u32,

    /// Default backoff exponent (power curve steepness).
    #[arg(long, default_value_t = DEFAULT_BACKOFF_EXPONENT, env = "ZANXIO_DEFAULT_BACKOFF_EXPONENT")]
    default_backoff_exponent: f32,

    /// Default backoff base delay in milliseconds.
    #[arg(long, default_value_t = DEFAULT_BACKOFF_BASE_MS, env = "ZANXIO_DEFAULT_BACKOFF_BASE_MS")]
    default_backoff_base_ms: f32,

    /// Default backoff jitter in milliseconds (max random ms per attempt multiplier).
    #[arg(long, default_value_t = DEFAULT_BACKOFF_JITTER_MS, env = "ZANXIO_DEFAULT_BACKOFF_JITTER_MS")]
    default_backoff_jitter_ms: f32,
}

/// Initializes the database and starts the HTTP server.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Make sure the root dir exists.
    let root = std::path::Path::new(&args.root_dir);
    std::fs::create_dir_all(root)?;

    // Storage engine tuning. These env vars are intentionally not exposed
    // as CLI flags — they're advanced knobs for tuning LSM compaction
    // behaviour and unlikely to need changing in normal operation.
    let mut storage_config = crate::store::StorageConfig::default();
    if let Ok(v) = std::env::var("ZANXIO_DATA_TABLE_SIZE") {
        storage_config.data_table_size = v.parse()?;
    }
    if let Ok(v) = std::env::var("ZANXIO_INDEX_TABLE_SIZE") {
        storage_config.index_table_size = v.parse()?;
    }
    if let Ok(v) = std::env::var("ZANXIO_JOURNAL_SIZE") {
        storage_config.journal_size = v.parse()?;
    }
    if let Ok(v) = std::env::var("ZANXIO_L0_THRESHOLD") {
        storage_config.l0_threshold = v.parse()?;
    }

    // Init/open the store (within the root dir).
    let store = Store::open(root.join(DATABASE_DIR), storage_config)?;
    tracing::info!(root_dir = %root.display(), "store opened");

    // Recover any jobs left in Working state from a previous crash.
    let recovered = store.recover_working_jobs().await?;
    if recovered > 0 {
        tracing::info!(count = recovered, "recovered orphaned working jobs");
    }

    // Shutdown signal for long-lived take tasks.
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // Initialize shared state accessible to all request handlers.
    let state = Arc::new(AppState {
        store,
        heartbeat_interval: Duration::from_secs(args.heartbeat_interval),
        global_working_limit: args.global_working_limit,
        global_in_flight: AtomicU64::new(0),
        shutdown: shutdown_rx,
        default_retry_limit: args.default_retry_limit,
        default_backoff: BackoffConfig {
            exponent: args.default_backoff_exponent,
            base_ms: args.default_backoff_base_ms,
            jitter_ms: args.default_backoff_jitter_ms,
        },
        clock: Arc::new(crate::time::now_millis),
    });

    // Start the background scheduler that promotes scheduled jobs to Ready
    // once their ready_at timestamp arrives.
    let scheduler_shutdown = state.shutdown.clone();
    let scheduler_batch_size = std::env::var("ZANXIO_SCHEDULER_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(crate::scheduler::DEFAULT_BATCH_SIZE);
    tokio::spawn(crate::scheduler::run(
        state.store.clone(),
        crate::time::now_millis,
        scheduler_batch_size,
        scheduler_shutdown,
    ));

    // Set up the TCP socket for incoming connections.
    let addr: std::net::SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening");

    eprintln!("Zanxio {}", env!("CARGO_PKG_VERSION"));
    eprintln!("Accepting connections on {addr}");

    // Start the server with graceful shutdown. Signal the watch channel
    // first so spawned take tasks break out of their loops, allowing
    // their connections to close.
    axum::serve(listener, http::app(state))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = shutdown_tx.send(());
        })
        .await?;

    eprintln!("Server stopped.");
    Ok(())
}

/// Async function that returns once a signal is received.
///
/// Axum handles waiting for this signal before shutting down.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(windows)]
    {
        let mut ctrl_close =
            tokio::signal::windows::ctrl_close().expect("failed to register ctrl_close handler");
        let mut ctrl_shutdown = tokio::signal::windows::ctrl_shutdown()
            .expect("failed to register ctrl_shutdown handler");

        tokio::select! {
            _ = ctrl_c => {}
            _ = ctrl_close.recv() => {}
            _ = ctrl_shutdown.recv() => {}
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        ctrl_c.await.ok();
    }
}
