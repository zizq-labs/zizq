// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

use crossbeam_skiplist::SkipSet;

/// In-memory chronological index of in-flight jobs.
///
/// Keeps a `SkipSet<(u64, String)>` ordered by `(dequeued_at, job_id)`.
/// The tuple's derived `Ord` gives dequeue-time ordering (oldest first),
/// then FIFO within the same timestamp (scru128 IDs are lexicographically
/// time-ordered).
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
    entries: SkipSet<(u64, String)>,
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
            (*dequeued_at, job_id.clone())
        })
    }

    pub(super) fn insert(&self, dequeued_at: u64, job_id: String) {
        self.entries.insert((dequeued_at, job_id));
    }

    pub(super) fn remove(&self, dequeued_at: u64, job_id: &str) {
        self.entries.remove(&(dequeued_at, job_id.to_string()));
    }
}

#[cfg(test)]
mod tests {
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
        idx.insert(1000, "j1".into());
        idx.insert(2000, "j2".into());
        idx.insert(3000, "j3".into());
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove() {
        let idx = InFlightIndex::new();
        idx.insert(1000, "j1".into());
        idx.insert(2000, "j2".into());
        idx.remove(1000, "j1");
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let idx = InFlightIndex::new();
        idx.insert(1000, "j1".into());
        idx.remove(9999, "nonexistent");
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn iter_returns_dequeue_time_order() {
        let idx = InFlightIndex::new();
        idx.insert(3000, "j3".into());
        idx.insert(1000, "j1".into());
        idx.insert(2000, "j2".into());

        let items: Vec<(u64, String)> = idx.iter().collect();
        assert_eq!(
            items,
            vec![
                (1000, "j1".into()),
                (2000, "j2".into()),
                (3000, "j3".into()),
            ]
        );
    }

    #[test]
    fn iter_breaks_ties_by_job_id() {
        let idx = InFlightIndex::new();
        idx.insert(1000, "b".into());
        idx.insert(1000, "a".into());
        idx.insert(1000, "c".into());

        let items: Vec<(u64, String)> = idx.iter().collect();
        assert_eq!(
            items,
            vec![(1000, "a".into()), (1000, "b".into()), (1000, "c".into()),]
        );
    }
}
