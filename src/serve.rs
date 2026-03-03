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

use crate::http::{self, AppState, DEFAULT_GLOBAL_WORKING_LIMIT};
use crate::license::License;
use crate::logging;
use crate::store::{self, Store};

/// Location of the internal database within the root directory.
const DATABASE_DIR: &str = "data";

/// Parse a commit mode string into a `CommitMode` enum variant.
fn parse_commit_mode(s: &str) -> Result<store::CommitMode, String> {
    match s {
        "buffered" => Ok(store::CommitMode::Buffered),
        "fsync" => Ok(store::CommitMode::Fsync),
        _ => Err(format!(
            "invalid commit mode '{s}', expected 'buffered' or 'fsync'"
        )),
    }
}

/// Parse a duration string into milliseconds.
///
/// Accepts either a bare number (interpreted as milliseconds) or a
/// human-readable duration string via `humantime` (e.g. "5s", "1.5s", "7d").
fn parse_duration_ms(s: &str) -> Result<u64, String> {
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    humantime::parse_duration(s)
        .map(|d| d.as_millis() as u64)
        .map_err(|e| e.to_string())
}

/// Arguments for the `serve` subcommand.
#[derive(Parser)]
pub struct Args {
    /// Log output format.
    #[arg(long, default_value = "pretty", value_name = "FORMAT")]
    log_format: logging::LogFormat,

    /// Log level for zanxio.
    #[arg(
        long,
        default_value = "info",
        value_name = "LEVEL",
        env = "ZANXIO_LOG_LEVEL"
    )]
    log_level: logging::LogLevel,

    /// Root directory for all server data and configuration.
    #[arg(
        long,
        default_value = "./zanxio-root",
        value_name = "PATH",
        env = "ZANXIO_ROOT_DIR"
    )]
    root_dir: String,

    /// Address to bind the HTTP server to.
    #[arg(long, default_value = "127.0.0.1", env = "ZANXIO_HOST")]
    host: String,

    /// Port to listen for HTTP connections on.
    #[arg(long, default_value_t = 7890, env = "ZANXIO_PORT")]
    port: u16,

    /// Interval between heartbeat frames on idle take connections (e.g. 3s, 500ms).
    #[arg(long = "heartbeat-interval", default_value = "3s", value_name = "DURATION", value_parser = parse_duration_ms, env = "ZANXIO_HEARTBEAT_INTERVAL")]
    heartbeat_interval_ms: u64,

    /// Maximum number of jobs in the working set across all connections.
    /// 0 means no limit.
    #[arg(long, default_value_t = DEFAULT_GLOBAL_WORKING_LIMIT, value_name = "NUMBER", env = "ZANXIO_GLOBAL_WORKING_LIMIT")]
    global_working_limit: u64,

    /// Default maximum retries before a failed job is killed.
    /// Jobs can override this at enqueue time with a per-job retry_limit.
    #[arg(long, default_value_t = store::DEFAULT_RETRY_LIMIT, value_name = "NUMBER", env = "ZANXIO_DEFAULT_RETRY_LIMIT")]
    default_retry_limit: u32,

    /// Default backoff exponent (power curve steepness).
    #[arg(long, default_value_t = store::DEFAULT_BACKOFF_EXPONENT, value_name = "NUMBER", env = "ZANXIO_DEFAULT_BACKOFF_EXPONENT")]
    default_backoff_exponent: f32,

    /// Default backoff base delay (e.g. 15s, 500ms).
    #[arg(long = "default-backoff-base", default_value = "15s", value_name = "DURATION", value_parser = parse_duration_ms, env = "ZANXIO_DEFAULT_BACKOFF_BASE")]
    default_backoff_base_ms: u64,

    /// Default backoff jitter — max random delay per attempt multiplier (e.g. 30s, 500ms).
    #[arg(long = "default-backoff-jitter", default_value = "30s", value_name = "DURATION", value_parser = parse_duration_ms, env = "ZANXIO_DEFAULT_BACKOFF_JITTER")]
    default_backoff_jitter_ms: u64,

    /// Default retention period for completed jobs (e.g. 0, 1h, 7d).
    /// 0 means completed jobs are purged immediately.
    #[arg(long = "default-completed-job-retention", default_value = "0", value_name = "DURATION", value_parser = parse_duration_ms, env = "ZANXIO_DEFAULT_COMPLETED_JOB_RETENTION")]
    default_completed_job_retention_ms: u64,

    /// Default retention period for dead jobs (e.g. 7d, 24h).
    /// 0 means dead jobs are purged immediately.
    #[arg(long = "default-dead-job-retention", default_value = "7d", value_name = "DURATION", value_parser = parse_duration_ms, env = "ZANXIO_DEFAULT_DEAD_JOB_RETENTION")]
    default_dead_job_retention_ms: u64,

    /// Interval between reaper scans (e.g. 30s, 1m).
    #[arg(long = "reaper-check-interval", default_value = "30s", value_name = "DURATION", value_parser = parse_duration_ms, env = "ZANXIO_REAPER_CHECK_INTERVAL")]
    reaper_check_interval_ms: u64,

    /// Default commit durability mode for most operations.
    /// "buffered" flushes to the OS page cache (durability is guaranteed
    /// even if the server process crashes after commit).
    /// "fsync" is the same as "buffered" but also fsyncs the WAL to disk
    /// (durability is guaranteed even in the event of power failure after
    /// commit).
    /// Expect to pay a significant throughput penalty for fsync. You may not
    /// need this guarantee. If you only care about at-last-once execution of
    /// jobs, see --enqueue-commit-mode.
    #[arg(long, default_value = "buffered", value_name = "MODE", value_parser = parse_commit_mode, env = "ZANXIO_DEFAULT_COMMIT_MODE")]
    default_commit_mode: store::CommitMode,

    /// Commit durability mode for enqueue operations only.
    /// Overrides --default-commit-mode for enqueues. When unset, enqueue
    /// inherits the default commit mode.
    /// Use "--default-commit-mode buffered --enqueue-commit-mode fsync"
    /// to get fsync durability for enqueues while keeping dequeue and
    /// other operations fast. This basically provides at-least-once guarantees
    /// while accepting jobs may be dequeued more than once in the case of a
    /// system failure, such as sudden loss of power.
    #[arg(long, value_name = "MODE", value_parser = parse_commit_mode, env = "ZANXIO_ENQUEUE_COMMIT_MODE")]
    enqueue_commit_mode: Option<store::CommitMode>,

    /// Address to bind the admin API server to.
    #[arg(long, default_value = "127.0.0.1", env = "ZANXIO_ADMIN_HOST")]
    admin_host: String,

    /// Port to listen for admin API connections on.
    #[arg(long, default_value_t = 8901, env = "ZANXIO_ADMIN_PORT")]
    admin_port: u16,

    /// Disable the admin API listener.
    #[arg(long, default_value_t = false, env = "ZANXIO_NO_ADMIN")]
    no_admin: bool,
}

/// Initializes the database and starts the HTTP server.
pub async fn run(args: Args, license: License) -> Result<(), Box<dyn std::error::Error>> {
    logging::init(&args.log_format, &args.log_level);
    log_license(&license);

    // Make sure the root dir exists.
    let root = std::path::Path::new(&args.root_dir);
    std::fs::create_dir_all(root)?;

    // Init/open the store (within the root dir).
    let mut storage_config = crate::store::StorageConfig::from_env()?;
    storage_config.default_completed_retention_ms = args.default_completed_job_retention_ms;
    storage_config.default_dead_retention_ms = args.default_dead_job_retention_ms;
    storage_config.default_retry_limit = args.default_retry_limit;
    storage_config.default_backoff = store::BackoffConfig {
        exponent: args.default_backoff_exponent,
        base_ms: args.default_backoff_base_ms as u32,
        jitter_ms: args.default_backoff_jitter_ms as u32,
    };
    storage_config.default_commit_mode = args.default_commit_mode;
    storage_config.enqueue_commit_mode = args.enqueue_commit_mode;
    let store = Store::open(root.join(DATABASE_DIR), storage_config)?;
    tracing::info!(root_dir = %root.display(), "store opened");

    // Recover orphaned working jobs and rebuild the in-memory indexes.
    // Must complete before accepting requests.
    let (recovered, ready_indexed, scheduled_indexed) = store.recover().await?;
    if recovered > 0 {
        tracing::info!(count = recovered, "recovered orphaned working jobs");
    }
    tracing::info!(count = ready_indexed, "ready index rebuilt");
    tracing::info!(count = scheduled_indexed, "scheduled index rebuilt");

    // Shutdown signal for long-lived take tasks.
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // Admin event broadcast channel. The receiver is dropped — subscribers
    // get their own via `.subscribe()`.
    let (admin_events_tx, _) = tokio::sync::broadcast::channel(64);

    // Initialize shared state accessible to all request handlers.
    let state = Arc::new(AppState {
        license,
        store,
        heartbeat_interval_ms: Duration::from_millis(args.heartbeat_interval_ms),
        global_working_limit: args.global_working_limit,
        global_in_flight: AtomicU64::new(0),
        shutdown: shutdown_rx,
        clock: Arc::new(crate::time::now_millis),
        admin_events: admin_events_tx,
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

    // Start the background reaper that purges expired completed/dead jobs.
    let reaper_shutdown = state.shutdown.clone();
    tokio::spawn(crate::reaper::run(
        state.store.clone(),
        crate::time::now_millis,
        crate::reaper::DEFAULT_BATCH_SIZE,
        Duration::from_millis(args.reaper_check_interval_ms),
        reaper_shutdown,
    ));

    // Start the admin API listener (unless disabled).
    if !args.no_admin {
        // Start the admin event producer.
        let admin_events = state.admin_events.clone();
        let admin_shutdown = state.shutdown.clone();
        let start_time = std::time::Instant::now();

        tokio::spawn(async move {
            let mut shutdown = admin_shutdown;

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {
                        let _ = admin_events.send(crate::admin::AdminEvent::Heartbeat {
                            version: env!("CARGO_PKG_VERSION").to_string(),
                            uptime_ms: start_time.elapsed().as_millis() as u64,
                        });
                    }
                    _ = shutdown.changed() => break,
                }
            }
        });

        let admin_addr: std::net::SocketAddr =
            format!("{}:{}", args.admin_host, args.admin_port).parse()?;
        let admin_listener = TcpListener::bind(admin_addr).await?;
        tracing::info!(addr = %admin_addr, "admin API listening");

        eprintln!("Accepting admin connections on {admin_addr}");

        let admin_state = state.clone();
        let admin_shutdown = state.shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = axum::serve(admin_listener, crate::admin::app(admin_state))
                .with_graceful_shutdown(async move {
                    let mut rx = admin_shutdown;
                    let _ = rx.changed().await;
                })
                .await
            {
                tracing::error!(error = %e, "admin API listener failed");
            }
        });
    }

    // Set up the TCP socket for incoming connections.
    let addr: std::net::SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening");

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
/// Log license status after the tracing subscriber has been initialized.
fn log_license(license: &License) {
    match license {
        License::Licensed {
            licensee_name,
            tier,
            expires_at,
            ..
        } => {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let remaining = humantime::format_duration(std::time::Duration::from_secs(
                expires_at.saturating_sub(now_secs),
            ));

            tracing::info!(
                licensee = %licensee_name,
                %tier,
                expires_at,
                %remaining,
                "license validated"
            );
        }
        License::Free => {
            tracing::info!("no license key provided, running in free tier");
        }
    }
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
