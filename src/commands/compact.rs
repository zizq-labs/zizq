// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! `zizq compact` subcommand.
//!
//! Connects to the admin API and triggers a full LSM compaction.

use clap::Parser;

use super::AdminClientArgs;

/// Arguments for the `compact` subcommand.
#[derive(Parser)]
pub struct Args {
    #[command(flatten)]
    admin: AdminClientArgs,
}

/// Run the compact command.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    args.admin.validate()?;

    // No read/overall timeout — compaction on a large keyspace can take a
    // while and the server doesn't return until it finishes.
    let client = args.admin.build_http_client()?;
    let endpoint_url = format!("{}/compact", args.admin.url.trim_end_matches('/'));

    eprintln!("Requesting compaction from {}...", args.admin.url);

    let response = client.post(&endpoint_url).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("compact request failed: HTTP {status}: {body}").into());
    }

    eprintln!("Compaction complete.");

    Ok(())
}
