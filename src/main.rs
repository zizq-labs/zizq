// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zanxio server entry point.
//!
//! Parses CLI arguments and initializes the server.
//!
//! ```text
//! Usage: zanxio [OPTIONS]
//!
//! Options:
//!      --root-dir <ROOT_DIR>
//!      --log-format <LOG_FORMAT>
//!      --log-level <LOG_LEVEL>
//!  -h, --help
//!  -V, --version
//! ```
//!
//! The following environment variables are also supported:
//!
//! ZANXIO_ROOT_DIR:      Same as --root-dir, specifies where Zanxio stores data
//! ZANXIO_LOG_LEVEL:     Same as --log-level, specifies how verbose logs are
//! ZANXIO_LOG_FILTER:    EnvFilter string for the tracing crate (overrides
//!                       --log-level). This is useful when debugging
//!                       dependencies.

use clap::Parser;

mod logging;

/// Default location of the root directory if not otherwise specified.
const DEFAULT_ROOT_DIR: &str = "./zanxio-root";

/// Location of the internal database within the root directory.
const DATABASE_DIR: &str = "data";

/// Struct used to handle command line arguments.
#[derive(Parser)]
#[command(name = "zanxio", version, about = "A self-contained job queue server")]
struct Args {
    /// Root directory for all server data and configuration.
    #[arg(long, default_value = DEFAULT_ROOT_DIR, env = "ZANXIO_ROOT_DIR")]
    root_dir: String,

    /// Log output format.
    #[arg(long, default_value = "pretty")]
    log_format: logging::LogFormat,

    /// Log level for zanxio.
    #[arg(long, default_value = "info", env = "ZANXIO_LOG_LEVEL")]
    log_level: logging::LogLevel,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    logging::init(&args.log_format, &args.log_level);

    let root = std::path::Path::new(&args.root_dir);
    std::fs::create_dir_all(root)?;

    let _db = fjall::SingleWriterTxDatabase::builder(root.join(DATABASE_DIR)).open()?;

    tracing::info!(root_dir = %root.display(), "database opened");

    Ok(())
}
