// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zizq job queue library crate.
//!
//! Re-exports the core modules so they can be used by benchmarks and
//! integration tests without duplicating the module tree.

/// Top-level directory name inside backup archives.
pub const BACKUP_ARCHIVE_PREFIX: &str = "zizq-root";

pub mod api;
pub mod commands;
pub mod filter;
pub mod license;
pub mod logging;
pub mod state;
pub mod store;
pub mod time;

/// Install ring as the default rustls crypto provider. Production code
/// does this in `main()`, but unit tests don't run `main` so any test
/// that constructs a `reqwest::Client` (via `rustls-no-provider`) must
/// call this first. Safe to call repeatedly — `Once` guards it.
#[cfg(test)]
pub fn ensure_test_crypto() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("default crypto provider already installed");
    });
}
