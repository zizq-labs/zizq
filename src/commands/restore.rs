// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! `zizq restore` subcommand.
//!
//! Extracts a `.tar.gz` backup archive to the configured root directory.
//! Does not require a running server.

use std::fs;
use std::path::Path;

use clap::Parser;
use flate2::read::GzDecoder;

/// Arguments for the `restore` subcommand.
#[derive(Parser)]
pub struct Args {
    /// Path to the backup archive to restore.
    path: String,
}

/// Run the restore command.
pub async fn run(args: Args, root_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(root_dir);

    // Refuse to overwrite a non-empty directory.
    if root.exists() {
        let has_entries = root
            .read_dir()
            .map_err(|e| format!("failed to read {}: {e}", root.display()))?
            .next()
            .is_some();

        if has_entries {
            return Err(format!(
                "target directory {} is not empty — remove it first or use --root-dir to specify a different location",
                root.display()
            ).into());
        }
    }

    let path = Path::new(&args.path);
    if !path.exists() {
        return Err(format!("backup file not found: {}", path.display()).into());
    }

    eprintln!("Restoring from {} to {}...", path.display(), root.display());

    let file = fs::File::open(path)?;
    let gz = GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);

    // The archive contains {PREFIX}/data/... — we strip the leading prefix
    // and extract into root_dir so the result is {root_dir}/data/.
    let prefix = format!("{}/", crate::BACKUP_ARCHIVE_PREFIX);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;

        let stripped = match path.strip_prefix(&prefix) {
            Ok(p) => p.to_path_buf(),
            Err(_) => {
                // Skip entries that don't have the expected prefix.
                continue;
            }
        };

        let dest = root.join(&stripped);

        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }

        if entry.header().entry_type().is_dir() {
            fs::create_dir_all(&dest)?;
        } else {
            let mut file = fs::File::create(&dest)?;
            std::io::copy(&mut entry, &mut file)?;
        }
    }

    eprintln!("Restore complete.");
    Ok(())
}
