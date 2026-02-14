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
//!      --log-format <LOG_FORMAT>
//!      --log-level <LOG_LEVEL>
//!  -h, --help
//!  -V, --version
//! ```
//!
//! The following environment variables are also supported:
//!
//! ZANXIO_ROOT_DIR:      Same as --root-dir, specifies where Zanxio stores data
//! ZANXIO_HOST:          Same as --host, specifies the address to bind to
//! ZANXIO_PORT:          Same as --port, specifies the port to listen on
//! ZANXIO_LOG_LEVEL:     Same as --log-level, specifies how verbose logs are
//! ZANXIO_LOG_FILTER:    EnvFilter string for the tracing crate (overrides
//!                       --log-level). This is useful when debugging
//!                       dependencies.

use clap::{Parser, Subcommand};

mod http;
mod logging;
mod serve;
mod store;

/// Struct used to handle command line arguments.
#[derive(Parser)]
#[command(name = "zanxio", version, about = "A self-contained job queue server")]
struct Cli {
    /// Log output format.
    #[arg(long, default_value = "pretty", global = true)]
    log_format: logging::LogFormat,

    /// Log level for zanxio.
    #[arg(long, default_value = "info", env = "ZANXIO_LOG_LEVEL", global = true)]
    log_level: logging::LogLevel,

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    logging::init(&cli.log_format, &cli.log_level);

    match cli.command {
        Some(Command::Serve(args)) => serve::run(args).await,
        None => serve::run(serve::Args::parse_from(std::env::args())).await,
    }
}
