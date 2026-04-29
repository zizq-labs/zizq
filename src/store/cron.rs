// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Cron scheduling types and in-memory schedule index.
//!
//! Cron groups are named collections of entries, each with a cron expression
//! and a job template. The scheduler enqueues jobs when entries become due.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Metadata for a cron group, stored at `C{group}\0` in the data keyspace.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CronGroup {
    /// Whether the group is paused (scheduler skips all entries).
    #[serde(rename = "z")]
    #[serde(default)]
    pub paused: bool,

    /// When the group was last paused (ms since epoch).
    #[serde(rename = "p")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<u64>,

    /// When the group was last resumed (ms since epoch).
    #[serde(rename = "r")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_at: Option<u64>,
}

/// A single cron entry, stored at `C{group}\0{entry_name}` in the data
/// keyspace. Contains the cron expression, enqueue options, scheduling state,
/// and pause state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronEntry {
    /// Entry name (unique within the group).
    #[serde(rename = "N")]
    pub name: String,

    /// Cron expression (e.g. `*/15 * * * *`).
    #[serde(rename = "E")]
    pub expression: String,

    /// Whether this entry is paused.
    #[serde(rename = "z")]
    #[serde(default)]
    pub paused: bool,

    /// When this entry was last paused (ms since epoch).
    #[serde(rename = "p")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<u64>,

    /// When this entry was last resumed (ms since epoch).
    #[serde(rename = "r")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_at: Option<u64>,

    /// Job template — the fields used to enqueue a job when this entry fires.
    #[serde(rename = "J")]
    pub job: super::EnqueueOptions,

    /// Next scheduled enqueue time (ms since epoch).
    #[serde(rename = "n")]
    pub next_enqueue_at: u64,

    /// Last time a job was enqueued from this entry (ms since epoch).
    #[serde(rename = "l")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_enqueue_at: Option<u64>,
}

/// In-memory schedule index, ordered by `next_enqueue_at`.
///
/// Used by the cron scheduler to efficiently find due entries and determine
/// how long to sleep. Rebuilt from disk on startup.
///
/// The number of cron entries is expected to be small (hundreds at most),
/// so a simple `BTreeMap` behind a `Mutex` is sufficient.
pub(super) struct CronScheduleIndex {
    /// Maps `next_enqueue_at` → list of `(group, entry_name)`.
    ///
    /// Multiple entries can share the same timestamp, so the value is a Vec.
    inner: Mutex<BTreeMap<u64, Vec<(String, String)>>>,
}

impl CronScheduleIndex {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
        }
    }

    /// Insert an entry into the index.
    pub fn insert(&self, next_enqueue_at: u64, group: String, entry_name: String) {
        let mut map = self.inner.lock().unwrap();
        map.entry(next_enqueue_at)
            .or_default()
            .push((group, entry_name));
    }

    /// Remove an entry from the index.
    pub fn remove(&self, next_enqueue_at: u64, group: &str, entry_name: &str) {
        let mut map = self.inner.lock().unwrap();
        if let Some(entries) = map.get_mut(&next_enqueue_at) {
            entries.retain(|(g, e)| g != group || e != entry_name);
            if entries.is_empty() {
                map.remove(&next_enqueue_at);
            }
        }
    }

    /// Peek at the earliest due timestamp, if any.
    pub fn next_due_at(&self) -> Option<u64> {
        let map = self.inner.lock().unwrap();
        map.keys().next().copied()
    }

    /// Remove and return all entries where `next_enqueue_at <= now`.
    pub fn take_due(&self, now: u64) -> Vec<(u64, String, String)> {
        let mut map = self.inner.lock().unwrap();
        let mut due = Vec::new();

        // Collect keys up to `now`.
        let due_keys: Vec<u64> = map.range(..=now).map(|(k, _)| *k).collect();
        for key in due_keys {
            if let Some(entries) = map.remove(&key) {
                for (group, entry_name) in entries {
                    due.push((key, group, entry_name));
                }
            }
        }

        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_insert_and_next_due_at() {
        let idx = CronScheduleIndex::new();
        assert_eq!(idx.next_due_at(), None);

        idx.insert(1000, "g1".into(), "e1".into());
        assert_eq!(idx.next_due_at(), Some(1000));

        idx.insert(500, "g1".into(), "e2".into());
        assert_eq!(idx.next_due_at(), Some(500));
    }

    #[test]
    fn index_remove() {
        let idx = CronScheduleIndex::new();
        idx.insert(1000, "g1".into(), "e1".into());
        idx.insert(1000, "g1".into(), "e2".into());
        idx.insert(2000, "g1".into(), "e3".into());

        idx.remove(1000, "g1", "e1");
        assert_eq!(idx.next_due_at(), Some(1000)); // e2 still at 1000

        idx.remove(1000, "g1", "e2");
        assert_eq!(idx.next_due_at(), Some(2000)); // 1000 bucket gone

        idx.remove(2000, "g1", "e3");
        assert_eq!(idx.next_due_at(), None);
    }

    #[test]
    fn index_remove_nonexistent_is_noop() {
        let idx = CronScheduleIndex::new();
        idx.insert(1000, "g1".into(), "e1".into());
        idx.remove(1000, "g1", "e_missing");
        idx.remove(9999, "g1", "e1");
        assert_eq!(idx.next_due_at(), Some(1000));
    }

    #[test]
    fn index_take_due() {
        let idx = CronScheduleIndex::new();
        idx.insert(100, "g1".into(), "e1".into());
        idx.insert(200, "g1".into(), "e2".into());
        idx.insert(300, "g1".into(), "e3".into());
        idx.insert(400, "g2".into(), "e4".into());

        let due = idx.take_due(250);
        assert_eq!(due.len(), 2);
        assert_eq!(due[0], (100, "g1".into(), "e1".into()));
        assert_eq!(due[1], (200, "g1".into(), "e2".into()));

        // Those entries are removed from the index.
        assert_eq!(idx.next_due_at(), Some(300));
    }

    #[test]
    fn index_take_due_none_ready() {
        let idx = CronScheduleIndex::new();
        idx.insert(1000, "g1".into(), "e1".into());

        let due = idx.take_due(500);
        assert!(due.is_empty());
        assert_eq!(idx.next_due_at(), Some(1000));
    }

    #[test]
    fn index_take_due_empty() {
        let idx = CronScheduleIndex::new();
        let due = idx.take_due(1000);
        assert!(due.is_empty());
    }

    #[test]
    fn index_multiple_entries_same_timestamp() {
        let idx = CronScheduleIndex::new();
        idx.insert(1000, "g1".into(), "e1".into());
        idx.insert(1000, "g1".into(), "e2".into());
        idx.insert(1000, "g2".into(), "e3".into());

        let due = idx.take_due(1000);
        assert_eq!(due.len(), 3);
        assert!(due.iter().all(|(ts, _, _)| *ts == 1000));
        assert_eq!(idx.next_due_at(), None);
    }
}
