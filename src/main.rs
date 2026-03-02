// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zanxio CLI entry point.
//!
//! Parses CLI arguments and dispatches to the appropriate subcommand.
//!
//! ```text
//! Usage: zanxio [OPTIONS] [COMMAND]
//!
//! Commands:
//!   serve    Start the server (default)
//!
//! Options:
//!      --log-format <FORMAT>
//!      --log-level <LEVEL>
//!  -k, --license-key <KEY>
//!  -h, --help
//!  -V, --version
//! ```
//!
//! The following environment variables are also supported:
//!
//! ZANXIO_ROOT_DIR:             Same as --root-dir, specifies where Zanxio stores data
//! ZANXIO_HOST:                 Same as --host, specifies the address to bind to
//! ZANXIO_PORT:                 Same as --port, specifies the port to listen on
//! ZANXIO_LOG_LEVEL:            Same as --log-level, specifies how verbose logs are
//! ZANXIO_LOG_FILTER:           EnvFilter string for the tracing crate (overrides
//!                              --log-level). This is useful when debugging
//!                              dependencies.
//! ZANXIO_LICENSE_KEY:          Same as --license-key, Ed25519-signed JWT for paid
//!                              features. Prefix with @ to read from a file.
//! ZANXIO_SCHEDULER_BATCH_SIZE: Max scheduled jobs to promote per iteration
//!                              (default: 200).

use clap::{Parser, Subcommand};

use zanxio::{license::License, logging, serve};

/// Struct used to handle command line arguments.
#[derive(Parser)]
#[command(name = "zanxio", version, about = "A self-contained job queue server")]
struct Cli {
    /// Log output format.
    #[arg(long, default_value = "pretty", value_name = "FORMAT", global = true)]
    log_format: logging::LogFormat,

    /// Log level for zanxio.
    #[arg(
        long,
        default_value = "info",
        value_name = "LEVEL",
        env = "ZANXIO_LOG_LEVEL",
        global = true
    )]
    log_level: logging::LogLevel,

    /// License key string or filename for access to paid features.
    /// The format is an Ed25519 signed JWT.
    /// Prefixing with @ reads from a file (e.g. @/etc/zanxio/license.jwt).
    #[arg(
        long = "license-key",
        short = 'k',
        value_name = "KEY",
        env = "ZANXIO_LICENSE_KEY",
        global = true
    )]
    license_key: Option<String>,

    /// The subcommand that is to be arg parsed and executed.
    #[command(subcommand)]
    command: Option<Command>,
}

/// Enum to handle the valid set of subcommands and their arguments.
#[derive(Subcommand)]
enum Command {
    /// Start the server.
    Serve(serve::Args),
}

/// Resolve a license key value, reading from a file if `@`-prefixed.
fn resolve_license_key(value: &str) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(path) = value.strip_prefix('@') {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read license key from {path}: {e}"))?;
        Ok(contents.trim().to_string())
    } else {
        Ok(value.to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    logging::init(&cli.log_format, &cli.log_level);

    // Load the software license if present so it can be provided to each
    // subcommand for feature gating.
    let license = match cli.license_key {
        Some(ref value) => {
            let token = resolve_license_key(value)?;

            let license =
                License::from_token(&token).map_err(|e| format!("invalid license key: {e}"))?;

            match &license {
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
                License::Free => unreachable!(),
            }
            license
        }
        None => {
            tracing::info!("no license key provided, running in free tier");
            License::Free
        }
    };

    match cli.command {
        Some(Command::Serve(args)) => serve::run(args, license).await,
        None => serve::run(serve::Args::parse_from(std::env::args()), license).await,
    }
}
