// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zizq CLI entry point.
//!
//! Parses CLI arguments and dispatches to the appropriate subcommand.
//!
//! ```text
//! Usage: zizq [OPTIONS] [COMMAND]
//!
//! Commands:
//!   serve    Start the server (default)
//!   tui      Launch the terminal UI dashboard (requires Pro license)
//!
//! Options:
//!  -k, --license-key <KEY>
//!  -h, --help
//!  -V, --version
//! ```
//!
//! The following environment variables are also supported:
//!
//! ZIZQ_ROOT_DIR:             Same as --root-dir, specifies where Zizq stores data
//! ZIZQ_HOST:                 Same as --host, specifies the address to bind to
//! ZIZQ_PORT:                 Same as --port, specifies the port to listen on
//! ZIZQ_LOG_LEVEL:            Same as --log-level, specifies how verbose logs are
//! ZIZQ_LOG_FILTER:           EnvFilter string for the tracing crate (overrides
//!                              --log-level). This is useful when debugging
//!                              dependencies.
//! ZIZQ_LICENSE_KEY:          Same as --license-key, Ed25519-signed JWT for paid
//!                              features. Prefix with @ to read from a file.
//! ZIZQ_SCHEDULER_BATCH_SIZE: Max scheduled jobs to promote per iteration
//!                              (default: 200).

use clap::{CommandFactory, Parser, Subcommand};

use zizq::{
    license::{Feature, License},
    serve, tui,
};

/// Struct used to handle command line arguments.
#[derive(Parser)]
#[command(name = "zizq", version, about = "A self-contained job queue server")]
struct Cli {
    /// License key string or filename for access to paid features.
    /// The format is an Ed25519 signed JWT.
    /// Prefixing with @ reads from a file (e.g. @/etc/zizq/license.jwt).
    #[arg(
        long = "license-key",
        short = 'k',
        value_name = "KEY",
        env = "ZIZQ_LICENSE_KEY",
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

    /// Launch the terminal UI dashboard (requires Pro license).
    Tui(tui::Args),
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

    // Load the software license if present so it can be provided to each
    // subcommand for feature gating.
    let license = match cli.license_key {
        Some(ref value) => {
            let token = resolve_license_key(value)?;
            License::from_token(&token).map_err(|e| format!("invalid license key: {e}"))?
        }
        None => License::Free,
    };

    eprintln!("Zizq {}", env!("CARGO_PKG_VERSION"));
    eprintln!();

    match cli.command {
        Some(Command::Tui(args)) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if let Err(msg) = license.require(now, Feature::Tui) {
                // Capitalize the first letter of the error message.
                let msg = {
                    let mut chars = msg.chars();
                    match chars.next() {
                        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
                        None => msg,
                    }
                };

                eprintln!("{msg}. See --license-key.");
                eprintln!();

                // Build the full command tree so global args are
                // propagated into the subcommand help output.
                let mut cmd = Cli::command();
                cmd.build();
                cmd.find_subcommand("tui")
                    .unwrap()
                    .clone()
                    .name("zizq tui")
                    .print_help()
                    .ok();
                std::process::exit(2);
            }

            tui::run(args).await
        }
        Some(Command::Serve(args)) => serve::run(args, license).await,
        None => serve::run(serve::Args::parse_from(std::env::args()), license).await,
    }
}
