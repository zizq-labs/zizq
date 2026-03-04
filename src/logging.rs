// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Logging initialization for Zizq.
//!
//! Configures the tracing subscriber based on the log format and level.
//! The ZIZQ_LOG_FILTER environment variable can be used to override the
//! log level with a full EnvFilter directive string.

use clap::ValueEnum;
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

/// Initializes the tracing crate with settings from the environment and the
/// command line.
pub fn init(format: &LogFormat, level: &LogLevel) {
    // ZIZQ_LOG_FILTER overrides everything for power users.
    let filter = match std::env::var("ZIZQ_LOG_FILTER") {
        Ok(raw) => match EnvFilter::try_new(&raw) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("invalid ZIZQ_LOG_FILTER: {e}, falling back to defaults");
                EnvFilter::new(format!("warn,zizq={}", level.as_str()))
            }
        },
        Err(_) => EnvFilter::new(format!("warn,zizq={}", level.as_str())),
    };

    match format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .init();
        }
        LogFormat::Compact => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .compact()
                .with_ansi(false)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}
