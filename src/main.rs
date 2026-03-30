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
//!      --root-dir <PATH>
//!  -k, --license-key <KEY>
//!  -h, --help
//!  -V, --version
//! ```

use clap::{Parser, Subcommand};

use zizq::commands::{serve, tls, top};
use zizq::license::License;

/// Struct used to handle command line arguments.
#[derive(Parser)]
#[command(name = "zizq", version, about = "A self-contained job queue server")]
struct Cli {
    /// Root directory for all server data and configuration.
    #[arg(
        long,
        default_value = "./zizq-root",
        value_name = "PATH",
        env = "ZIZQ_ROOT_DIR",
        global = true
    )]
    root_dir: String,

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

    /// Generate TLS certificates for use with `zizq serve'.
    Tls(tls::Args),
}

/// Resolve a license key value, reading from a file if `@`-prefixed.
///
/// Returns `(token, file_path)` where `file_path` is `Some` when the
/// token was read from a file (for license hot-reload).
fn resolve_license_key(
    value: &str,
) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    if let Some(path) = value.strip_prefix('@') {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read license key from {path}: {e}"))?;
        Ok((contents.trim().to_string(), Some(path.to_string())))
    } else {
        Ok((value.to_string(), None))
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Load the software license if present so it can be provided to each
    // subcommand for feature gating.
    let (license, license_key_path) = match cli.license_key {
        Some(ref value) => {
            let (token, path) = resolve_license_key(value)?;
            let lic =
                License::from_token(&token).map_err(|e| format!("invalid license key: {e}"))?;
            (lic, path)
        }
        None => (License::Free, None),
    };

    match cli.command {
        Some(Command::Top(args)) => top::run(args).await,
        Some(Command::Serve(args)) => {
            serve::run(args, &cli.root_dir, license, license_key_path).await
        }
        Some(Command::Tls(args)) => tls::run(args, &cli.root_dir).await,
        None => {
            serve::run(
                serve::Args::parse_from(std::env::args()),
                &cli.root_dir,
                license,
                license_key_path,
            )
            .await
        }
    }
}
