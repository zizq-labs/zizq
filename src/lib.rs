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
