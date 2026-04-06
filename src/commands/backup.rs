// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! `zizq backup` subcommand.
//!
//! Connects to the admin API and streams a `.tar.gz` backup to a local file.

use clap::Parser;
use futures_util::StreamExt;

use super::AdminClientArgs;

/// Arguments for the `backup` subcommand.
#[derive(Parser)]
pub struct Args {
    #[command(flatten)]
    admin: AdminClientArgs,

    /// Output path for the backup archive (.tar.gz).
    #[arg(value_name = "FILENAME")]
    output: String,
}

/// Run the backup command.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    args.admin.validate()?;

    // Only a connection timeout — no read/overall timeout since backups of
    // large databases can take a while to snapshot and stream.
    let client = args.admin.build_http_client()?;
    let endpoint_url = format!("{}/backup", args.admin.url.trim_end_matches('/'));

    eprintln!("Requesting backup from {}...", args.admin.url);

    let response = client.post(&endpoint_url).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("backup request failed: HTTP {status}: {body}").into());
    }

    let output_path = std::path::Path::new(&args.output);

    // Create parent directories if needed.
    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut file = tokio::fs::File::create(&output_path).await?;
    let mut stream = response.bytes_stream();
    let mut bytes_written: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        bytes_written += chunk.len() as u64;
    }

    tokio::io::AsyncWriteExt::flush(&mut file).await?;

    eprintln!(
        "Backup saved to {} ({})",
        args.output,
        bytesize::ByteSize(bytes_written)
    );

    Ok(())
}
