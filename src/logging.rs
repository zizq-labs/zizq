// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Logging initialization for Zizq.
//!
//! Configures the tracing subscriber based on the log format and level.
//! The ZIZQ_LOG_FILTER environment variable can be used to override the
//! log level with a full EnvFilter directive string.
//!
//! Supports writing to stdout (default) or to rotated log files on disk
//! via `--log-to-disk` / `--log-dir`.

use std::path::Path;

use clap::ValueEnum;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

/// Valid log levels for the --log-level flag.
///
/// This information is used to configure the EnvFilter for tracing.
#[derive(Clone, ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

/// Valid log output formats.
///
/// This information is used to configure the output format when initialising
/// tracing.
#[derive(Clone, ValueEnum)]
pub enum LogFormat {
    Pretty,
    Compact,
    Json,
}

/// Time-based log rotation frequency.
#[derive(Clone, ValueEnum)]
pub enum LogRotation {
    Daily,
    Hourly,
    Never,
}

/// Configuration bundle for logging initialization.
pub struct LogConfig<'a> {
    pub format: Option<&'a LogFormat>,
    pub level: &'a LogLevel,
    pub log_dir: Option<&'a Path>,
    pub rotation: &'a LogRotation,
    pub max_size: u64,
    pub max_files: usize,
}

/// Initializes the tracing crate with settings from the environment and the
/// command line.
///
/// When `log_dir` is set, logs are written to rotated files on disk via a
/// non-blocking writer. The returned `WorkerGuard` must be held for the
/// lifetime of the process to ensure buffered logs are flushed on shutdown.
pub fn init(config: LogConfig) -> Option<WorkerGuard> {
    // ZIZQ_LOG_FILTER overrides everything for power users.
    let filter = match std::env::var("ZIZQ_LOG_FILTER") {
        Ok(raw) => match EnvFilter::try_new(&raw) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("invalid ZIZQ_LOG_FILTER: {e}, falling back to defaults");
                EnvFilter::new(format!("warn,zizq={}", config.level.as_str()))
            }
        },
        Err(_) => EnvFilter::new(format!("warn,zizq={}", config.level.as_str())),
    };

    // Resolve writer: rolling file (non-blocking) or stdout.
    let guard = if let Some(log_dir) = config.log_dir {
        let mut condition = rolling_file::RollingConditionBasic::new();
        condition = match config.rotation {
            LogRotation::Daily => condition.daily(),
            LogRotation::Hourly => condition.hourly(),
            LogRotation::Never => condition,
        };
        condition = condition.max_size(config.max_size);

        let file_path = log_dir.join("zizq.log");
        let file_appender =
            rolling_file::RollingFileAppender::new(file_path, condition, config.max_files)
                .expect("failed to create log file appender");

        Some(tracing_appender::non_blocking(file_appender))
    } else {
        None
    };

    // ANSI only when writing to a real terminal.
    let is_tty = config.log_dir.is_none() && std::io::IsTerminal::is_terminal(&std::io::stderr());

    // Default format: pretty for interactive TTY, compact otherwise.
    let default_format = if is_tty {
        LogFormat::Pretty
    } else {
        LogFormat::Compact
    };
    let format = config.format.unwrap_or(&default_format);

    // Build the subscriber. The writer diverges on file vs stdout but
    // format + filter + ansi logic is shared.
    macro_rules! init_subscriber {
        ($base:expr) => {
            match format {
                LogFormat::Json => $base.with_ansi(false).json().init(),
                LogFormat::Compact => $base.with_ansi(false).compact().init(),
                LogFormat::Pretty => $base.with_ansi(is_tty).init(),
            }
        };
    }

    if let Some((non_blocking, worker_guard)) = guard {
        init_subscriber!(
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(non_blocking)
        );
        Some(worker_guard)
    } else {
        init_subscriber!(tracing_subscriber::fmt().with_env_filter(filter));
        None
    }
}
