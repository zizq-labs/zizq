// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

use crossbeam_skiplist::SkipSet;
use scru128::Id;

use super::index_id::index_id;

/// In-memory chronological index of in-flight jobs.
///
/// Keeps a `SkipSet<(u64, Id)>` ordered by `(dequeued_at, job_id)`.
/// The tuple's derived `Ord` gives dequeue-time ordering (oldest first),
/// then FIFO within the same timestamp (scru128 IDs are time-ordered).
///
/// Lock-free — uses `crossbeam-skiplist` internally. Producers
/// (`take_next_n_jobs`) call `insert`; consumers (complete batcher,
/// `record_failure`, `requeue`, `delete_job`/`delete_jobs`, `purge_job`)
/// call `remove`. The admin event stream reads via `iter` / `len`.
///
/// Starts empty on every process start: `recover_in_flight` migrates all
/// orphaned InFlight jobs back to Ready before traffic is accepted, so
/// there is never anything to rebuild.
///
/// All access goes through the public methods. Direct access to `entries`
/// is prevented by module-level privacy.
pub(super) struct InFlightIndex {
    entries: SkipSet<(u64, Id)>,
}

impl InFlightIndex {
    pub(super) fn new() -> Self {
        Self {
            entries: SkipSet::new(),
        }
    }

    /// Total number of in-flight jobs across all workers.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate in dequeue-time order. Yields `(dequeued_at, job_id)` pairs.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, String)> + '_ {
        self.entries.iter().map(|entry| {
            let (dequeued_at, job_id) = entry.value();
            (*dequeued_at, job_id.to_string())
        })
    }

    pub(super) fn insert(&self, dequeued_at: u64, job_id: String) {
        if let Some(id) = index_id(&job_id) {
            self.entries.insert((dequeued_at, id));
        }
    }

    pub(super) fn remove(&self, dequeued_at: u64, job_id: &str) {
        if let Some(id) = index_id(job_id) {
            self.entries.remove(&(dequeued_at, id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::job_id;
    use super::*;

    #[test]
    fn empty_index() {
        let idx = InFlightIndex::new();
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.iter().count(), 0);
    }

    #[test]
    fn insert_and_len() {
        let idx = InFlightIndex::new();
        idx.insert(1000, job_id("j1"));
        idx.insert(2000, job_id("j2"));
        idx.insert(3000, job_id("j3"));
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove() {
        let idx = InFlightIndex::new();
        idx.insert(1000, job_id("j1"));
        idx.insert(2000, job_id("j2"));
        idx.remove(1000, &job_id("j1"));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let idx = InFlightIndex::new();
        idx.insert(1000, job_id("j1"));
        idx.remove(9999, &job_id("nonexistent"));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn iter_returns_dequeue_time_order() {
        let idx = InFlightIndex::new();
        idx.insert(3000, job_id("j3"));
        idx.insert(1000, job_id("j1"));
        idx.insert(2000, job_id("j2"));

        let items: Vec<(u64, String)> = idx.iter().collect();
        assert_eq!(
            items,
            vec![
                (1000, job_id("j1")),
                (2000, job_id("j2")),
                (3000, job_id("j3")),
            ]
        );
    }

    #[test]
    fn iter_breaks_ties_by_job_id() {
        let idx = InFlightIndex::new();
        idx.insert(1000, job_id("b"));
        idx.insert(1000, job_id("a"));
        idx.insert(1000, job_id("c"));

        let items: Vec<(u64, String)> = idx.iter().collect();
        assert_eq!(
            items,
            vec![
                (1000, job_id("a")),
                (1000, job_id("b")),
                (1000, job_id("c")),
            ]
        );
    }
}
