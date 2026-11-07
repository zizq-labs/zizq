// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Compresses the admin web UI for embedding in the binary.
//!
//! By convention, this file is run automatically by Cargo as a pre-build
//! script.
//!
//! Every file in `web/` is gzipped into `OUT_DIR`, and `web_assets.rs` is
//! generated there to list them for `src/api/admin/web.rs`. The binary
//! then carries only the compressed bytes, and the server sends them as
//! they are.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::write::GzEncoder;

fn main() {
    let web_dir = Path::new("web");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by cargo"));

    // Output prefixed with "cargo::" is read by Cargo and used as a build
    // instruction. In this case, Cargo won't waste time re-running this
    // script if the contents of web/ have not changed.
    println!("cargo::rerun-if-changed=web");

    let mut paths: Vec<PathBuf> = fs::read_dir(web_dir)
        .expect("web/ is readable")
        .map(|entry| entry.expect("web/ entry is readable").path())
        .collect();
    paths.sort();

    let mut table = String::from("static ASSETS: &[Asset] = &[\n");

    for path in paths {
        let name = path.file_name().unwrap().to_str().expect("UTF-8 file name");

        // Hidden files are never assets, but OS and editor clutter such as
        // `.DS_Store` and swap files end up here.
        if name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            panic!("web/{name}: subdirectories are not supported");
        }

        let content_type = match path.extension().and_then(|e| e.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("js") => "text/javascript; charset=utf-8",
            _ => panic!("web/{name}: no content type is known for this file"),
        };
        let route = match name {
            "index.html" => "/".to_string(),
            _ => format!("/{name}"),
        };

        let raw = fs::read(&path).expect("asset is readable");

        // The gzip header's timestamp is left at zero, so the same input
        // always produces the same bytes.
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(&raw).unwrap();
        let gzipped = encoder.finish().unwrap();

        let gz_path = out_dir.join(format!("{name}.gz"));
        fs::write(&gz_path, &gzipped).expect("OUT_DIR is writable");

        // Weak, because the same tag covers both the gzipped and the
        // decompressed form of the asset.
        let etag = format!("W/\"{:016x}\"", fnv1a(&raw));

        writeln!(
            table,
            "    Asset {{ path: {route:?}, content_type: {content_type:?}, \
             etag: {etag:?}, gzipped: include_bytes!({gz_path:?}) }},"
        )
        .unwrap();
    }

    table.push_str("];\n");
    fs::write(out_dir.join("web_assets.rs"), table).expect("OUT_DIR is writable");
}

/// 64-bit FNV-1a, which is stable across Rust versions, unlike the
/// standard library's hashers.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}
