// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

use crossbeam_skiplist::SkipSet;

/// In-memory chronological index of scheduled jobs.
///
/// Keeps a `SkipSet<(u64, String)>` ordered by `(ready_at, job_id)`.
/// The tuple's derived `Ord` gives chronological ordering (ready_at ASC),
/// then FIFO within the same timestamp (scru128 IDs are lexicographically
/// time-ordered).
///
/// Lock-free — uses `crossbeam-skiplist` internally. The scheduler is the
/// sole consumer of `next_due`/`remove`, so there's no concurrent claim
/// contention; producers (`enqueue`, `record_failure`) only call `insert`.
///
/// All access goes through the public methods. Direct access to `entries`
/// is prevented by module-level privacy.
pub(super) struct ScheduledIndex {
    entries: SkipSet<(u64, String)>,
}

impl ScheduledIndex {
    pub(super) fn new() -> Self {
        Self {
            entries: SkipSet::new(),
        }
    }

    /// Total number of scheduled jobs.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate in chronological order. Yields `(ready_at, job_id)` pairs.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, String)> + '_ {
        self.entries.iter().map(|entry| {
            let (ready_at, job_id) = entry.value();
            (*ready_at, job_id.clone())
        })
    }

    pub(super) fn insert(&self, ready_at: u64, job_id: String) {
        self.entries.insert((ready_at, job_id));
    }

    pub(super) fn remove(&self, ready_at: u64, job_id: &str) {
        self.entries.remove(&(ready_at, job_id.to_string()));
    }

    /// Collect entries where `ready_at <= now`, up to `limit`.
    ///
    /// Returns `(due_entries, next_ready_at)`:
    /// - `due_entries`: Vec of `(ready_at, job_id)` tuples that are due now.
    /// - `next_ready_at`: The `ready_at` of the first future entry, if any.
    ///   `None` if the batch limit was hit (caller should loop immediately)
    ///   or if there are no more scheduled jobs at all.
    pub(super) fn next_due(&self, now: u64, limit: usize) -> (Vec<(u64, String)>, Option<u64>) {
        let mut due = Vec::new();
        for entry in self.entries.iter() {
            let (ready_at, job_id) = entry.value();
            if *ready_at > now {
                // This entry (and everything after) is in the future.
                return (due, Some(*ready_at));
            }
            if due.len() >= limit {
                // Hit the batch cap but there are more due entries.
                // Return without a next_ready_at so the caller loops immediately.
                return (due, None);
            }
            due.push((*ready_at, job_id.clone()));
        }
        // Exhausted the index — no more scheduled jobs at all.
        (due, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_index() {
        let idx = ScheduledIndex::new();
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.iter().count(), 0);
        let (due, next) = idx.next_due(1000, 100);
        assert!(due.is_empty());
        assert_eq!(next, None);
    }

    #[test]
    fn insert_and_len() {
        let idx = ScheduledIndex::new();
        idx.insert(1000, "j1".into());
        idx.insert(2000, "j2".into());
        idx.insert(3000, "j3".into());
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove() {
        let idx = ScheduledIndex::new();
        idx.insert(1000, "j1".into());
        idx.insert(2000, "j2".into());
        idx.remove(1000, "j1");
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let idx = ScheduledIndex::new();
        idx.insert(1000, "j1".into());
        idx.remove(9999, "nonexistent");
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn iter_returns_chronological_order() {
        let idx = ScheduledIndex::new();
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
    fn next_due_returns_only_due_entries() {
        let idx = ScheduledIndex::new();
        idx.insert(100, "j1".into());
        idx.insert(200, "j2".into());
        idx.insert(500, "j3".into());
        idx.insert(1000, "j4".into());

        let (due, next) = idx.next_due(500, 100);
        assert_eq!(due.len(), 3);
        assert_eq!(due[0], (100, "j1".into()));
        assert_eq!(due[1], (200, "j2".into()));
        assert_eq!(due[2], (500, "j3".into()));
        assert_eq!(next, Some(1000)); // j4 is the next future entry
    }

    #[test]
    fn next_due_returns_none_when_all_due() {
        let idx = ScheduledIndex::new();
        idx.insert(100, "j1".into());
        idx.insert(200, "j2".into());

        let (due, next) = idx.next_due(999, 100);
        assert_eq!(due.len(), 2);
        assert_eq!(next, None); // nothing left in the future
    }

    #[test]
    fn next_due_respects_limit() {
        let idx = ScheduledIndex::new();
        idx.insert(100, "j1".into());
        idx.insert(200, "j2".into());
        idx.insert(300, "j3".into());

        let (due, next) = idx.next_due(999, 2);
        assert_eq!(due.len(), 2);
        assert_eq!(due[0], (100, "j1".into()));
        assert_eq!(due[1], (200, "j2".into()));
        // next is None because we hit the batch cap (caller should loop).
        assert_eq!(next, None);
    }

    #[test]
    fn next_due_returns_none_when_all_future() {
        let idx = ScheduledIndex::new();
        idx.insert(500, "j1".into());
        idx.insert(1000, "j2".into());

        let (due, next) = idx.next_due(100, 100);
        assert!(due.is_empty());
        assert_eq!(next, Some(500));
    }

    #[test]
    fn remove_then_reinsert_same_timestamp() {
        let idx = ScheduledIndex::new();
        idx.insert(1000, "j1".into());
        idx.remove(1000, "j1");
        idx.insert(1000, "j2".into());

        assert_eq!(idx.len(), 1);
        let (due, _) = idx.next_due(1000, 100);
        assert_eq!(due, vec![(1000, "j2".into())]);
    }
}
