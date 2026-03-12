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
//!   top      Launch the interactive terminal UI
//!
//! Options:
//!  -k, --license-key <KEY>
//!  -h, --help
//!  -V, --version
//! ```

use clap::{Parser, Subcommand};

use zizq::{license::License, serve, top};

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

    /// Launch the interactive terminal UI.
    Top(top::Args),
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

    match cli.command {
        Some(Command::Top(args)) => top::run(args).await,
        Some(Command::Serve(args)) => serve::run(args, license).await,
        None => serve::run(serve::Args::parse_from(std::env::args()), license).await,
    }
}
