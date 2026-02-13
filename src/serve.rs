// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zanxio CLI `serve` command entry point.
//!
//! Initializes the database and starts the HTTP server.

use clap::Parser;

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
}

/// Initializes the database and starts the HTTP server.
pub fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(&args.root_dir);
    std::fs::create_dir_all(root)?;

    let _db = fjall::SingleWriterTxDatabase::builder(root.join(DATABASE_DIR)).open()?;

    tracing::info!(root_dir = %root.display(), "database opened");
    tracing::info!(
        host = %args.host,
        port = args.port,
        "server will start here"
    );

    Ok(())
}
