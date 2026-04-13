// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

use std::collections::HashSet;

use crossbeam_skiplist::{SkipMap, SkipSet};
use dashmap::DashMap;

/// In-memory priority index for ready jobs (lock-free).
///
/// The ready index is inherently ephemeral — jobs live in it for milliseconds
/// under normal throughput — which is worst-case for LSM trees (high tombstone
/// churn causes scan latency spikes tied to compaction cycles). Skip lists give
/// O(log n) lookups without a global mutex, so enqueue (insert) and take
/// (remove) can operate on the index concurrently. The authoritative source of
/// data remains in the database; this index is rebuilt on startup via
/// `Store::rebuild_ready_index` and then kept in sync at runtime.
///
/// Keys are `(priority, job_id)`. `Ord` on `(u16, String)` gives priority ASC,
/// then job_id ASC — exactly the ordering needed (FIFO within same priority,
/// since scru128 IDs are lexicographically time-ordered).
///
/// All mutations go through the public methods (`insert`, `remove`, `claim`)
/// which maintain consistency between the global and per-queue structures.
/// Direct access to `global` and `by_queue` is prevented by module-level
/// privacy — callers in `store/mod.rs` cannot bypass the public API.
pub(super) struct ReadyIndex {
    /// Global prioritised job index.
    ///
    /// All claims to take a job use this index as the authoritative source of
    /// truth. If removal from this set fails (CAS operation under the hood)
    /// then a new candidate must be selected.
    ///
    /// Key: `(priority, job_id)`, Value: `queue_name`.
    /// The value stores the queue name so that after `remove()` we know which
    /// per-queue set to clean up.
    global: SkipMap<(u16, String), String>,

    /// Per-queue indexes — for queue-filtered candidate selection.
    ///
    /// `DashMap` provides concurrent access to the `queue -> SkipSet` mapping
    /// without a global lock.
    ///
    /// Entries are only removed from this set if they were successfully
    /// claimed in the global set. No entries should exist in the global set
    /// that do not exist in the per-queue set (insert order is per-queue first
    /// then global).
    by_queue: DashMap<String, SkipSet<(u16, String)>>,
}

impl ReadyIndex {
    pub(super) fn new() -> Self {
        Self {
            global: SkipMap::new(),
            by_queue: DashMap::new(),
        }
    }

    /// Total number of ready jobs across all queues.
    pub(super) fn len(&self) -> usize {
        self.global.len()
    }

    /// Remove a job from both the global and per-queue indexes.
    pub(super) fn remove(&self, queue: &str, priority: u16, job_id: &str) {
        self.global.remove(&(priority, job_id.to_string()));
        if let Some(set) = self.by_queue.get(queue) {
            set.remove(&(priority, job_id.to_string()));
        }
    }

    /// Insert a job into both the per-queue and global indexes.
    ///
    /// Inserts into `by_queue` first, then `global`. This ordering
    /// guarantees that any entry visible in `global` is already in
    /// `by_queue`, so the unfiltered `remove()` path's by_queue cleanup always
    /// finds the entry. The filtered path may briefly peek a by_queue entry
    /// whose global counterpart isn't inserted yet — `global.get()` returns
    /// `None` and it retries harmlessly.
    pub(super) fn insert(&self, queue: &str, priority: u16, job_id: String) {
        let entry = (priority, job_id);
        self.by_queue
            .entry(queue.to_string())
            .or_insert_with(SkipSet::new)
            .insert(entry.clone());
        self.global.insert(entry, queue.to_string());
    }

    /// Iterate over the global ready index in priority order.
    ///
    /// Yields `(priority, job_id)` pairs. Callers should use `.skip()`
    /// and `.take()` for pagination.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u16, String)> + '_ {
        self.global.iter().map(|entry| {
            let (priority, job_id) = entry.key();
            (*priority, job_id.clone())
        })
    }

    /// Atomically claim the highest-priority (lowest number), oldest ready job.
    ///
    /// Returns `(priority, job_id, queue)` if a job was claimed, or `None` if
    /// the index is empty (or empty for the requested queues).
    ///
    /// Both paths use the same CAS pattern: find a candidate entry, call
    /// `Entry::remove()` (only one thread wins the CAS), and retry on failure.
    ///
    /// - Unfiltered (queues is empty): peeks `global.front()` directly.
    /// - Queue-filtered: peeks each target queue's SkipSet front, picks
    ///   the minimum, then looks up the global entry via `get()`.
    ///
    /// Important: `SkipMap::remove(&key)` is *not* safe as a claim
    /// mechanism — it returns `Some(entry)` to ALL concurrent callers that
    /// find the node, regardless of who actually marked it. Only
    /// `Entry::remove()` (which returns `bool` from the CAS) is safe.
    pub(super) fn claim(&self, queues: &HashSet<String>) -> Option<(u16, String, String)> {
        loop {
            // Find the best candidate. Unfiltered peeks global directly;
            // filtered peeks per-queue sets and resolves via global.get().
            // front() and get() both skip marked (already-claimed) nodes.
            let candidate = if queues.is_empty() {
                // If we're just taking a job from any queue, it's just
                // whatever is at the front of the global queue.
                let entry = self.global.front()?; // ?: or return None
                let key = entry.key().clone();
                let queue = entry.value().clone();
                Some((entry, key, queue))
            } else {
                // If we're taking a job from a specific set of queues, we need
                // to find the first job on each of those queues and then
                // select the highest priority/earliest job ID.
                let (priority, job_id, queue) = queues
                    .iter()
                    .filter_map(|q| {
                        let set = self.by_queue.get(q)?;
                        let front = set.front()?;
                        let (priority, job_id) = front.value().clone();
                        Some((priority, job_id, q.clone()))
                    })
                    .min()?; // ?: or return None

                // Look up the global entry for the CAS. Returns None if:
                // (a) not yet in global (insert race — by_queue is populated
                //     first, global second), or
                // (b) already claimed and marked by another thread.
                // In either case, don't touch by_queue — the insert will
                // complete shortly (a), or the winner will clean up (b).
                let key = (priority, job_id);
                self.global.get(&key).map(|entry| (entry, key, queue))
            };

            // Claim via Entry::remove() — CAS, only one thread wins.
            if let Some((entry, (priority, job_id), queue)) = candidate {
                if entry.remove() {
                    // Won the claim — clean up the per-queue set.
                    if let Some(set) = self.by_queue.get(&queue) {
                        set.remove(&(priority, job_id.clone()));
                    }
                    return Some((priority, job_id, queue));
                }
            } // else loop again
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_index() {
        let idx = ReadyIndex::new();
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.claim(&HashSet::new()), None);
        assert_eq!(idx.iter().count(), 0);
    }

    #[test]
    fn insert_and_len() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());
        idx.insert("emails", 20, "j2".into());
        idx.insert("webhooks", 10, "j3".into());
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove_cleans_both_global_and_per_queue() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());
        idx.insert("emails", 20, "j2".into());

        idx.remove("emails", 10, "j1");
        assert_eq!(idx.len(), 1);

        // Filtered claim should only see j2, not a ghost of j1.
        let queues: HashSet<String> = ["emails".into()].into();
        let claimed = idx.claim(&queues);
        assert_eq!(claimed, Some((20, "j2".into(), "emails".into())));
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());
        idx.remove("emails", 99, "nonexistent");
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn claim_unfiltered_returns_highest_priority() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 20, "j1".into());
        idx.insert("webhooks", 5, "j2".into());
        idx.insert("emails", 10, "j3".into());

        let claimed = idx.claim(&HashSet::new()).unwrap();
        assert_eq!(claimed, (5, "j2".into(), "webhooks".into()));
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn claim_unfiltered_fifo_within_same_priority() {
        let idx = ReadyIndex::new();
        // IDs are lexicographically ordered: a < b < c.
        idx.insert("q", 10, "a".into());
        idx.insert("q", 10, "c".into());
        idx.insert("q", 10, "b".into());

        let (_, id1, _) = idx.claim(&HashSet::new()).unwrap();
        let (_, id2, _) = idx.claim(&HashSet::new()).unwrap();
        let (_, id3, _) = idx.claim(&HashSet::new()).unwrap();
        assert_eq!(id1, "a");
        assert_eq!(id2, "b");
        assert_eq!(id3, "c");
        assert_eq!(idx.claim(&HashSet::new()), None);
    }

    #[test]
    fn claim_filtered_returns_only_from_requested_queues() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());
        idx.insert("webhooks", 5, "j2".into());
        idx.insert("reports", 1, "j3".into());

        let queues: HashSet<String> = ["emails".into(), "webhooks".into()].into();
        let (priority, id, queue) = idx.claim(&queues).unwrap();
        // webhooks has priority 5, emails has 10 — webhooks wins.
        assert_eq!(priority, 5);
        assert_eq!(id, "j2");
        assert_eq!(queue, "webhooks");
        // reports (priority 1) should NOT have been returned.
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn claim_filtered_returns_none_for_empty_queue() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());

        let queues: HashSet<String> = ["webhooks".into()].into();
        assert_eq!(idx.claim(&queues), None);
        // j1 should still be there.
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn claim_filtered_picks_best_across_queues() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 20, "j1".into());
        idx.insert("webhooks", 10, "j2".into());

        let queues: HashSet<String> = ["emails".into(), "webhooks".into()].into();
        let (_, id, _) = idx.claim(&queues).unwrap();
        assert_eq!(id, "j2"); // lower priority number wins
    }

    #[test]
    fn claim_removes_from_both_global_and_per_queue() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());

        let queues: HashSet<String> = ["emails".into()].into();
        idx.claim(&queues).unwrap();

        // Both global (len) and per-queue (filtered claim) should be empty.
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.claim(&queues), None);
    }

    #[test]
    fn remove_then_reinsert_same_queue_claim_works() {
        // Regression: removing a job and re-inserting to the same queue
        // must allow filtered claim to find the new job. Previously,
        // remove only cleaned the global index, leaving orphaned
        // per-queue entries that caused filtered claim to spin.
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, "j1".into());
        idx.remove("emails", 10, "j1");

        idx.insert("emails", 20, "j2".into());

        let queues: HashSet<String> = ["emails".into()].into();
        let claimed = idx.claim(&queues);
        assert_eq!(claimed, Some((20, "j2".into(), "emails".into())));
    }

    #[test]
    fn iter_returns_priority_order() {
        let idx = ReadyIndex::new();
        idx.insert("q", 30, "j3".into());
        idx.insert("q", 10, "j1".into());
        idx.insert("q", 20, "j2".into());

        let items: Vec<(u16, String)> = idx.iter().collect();
        assert_eq!(
            items,
            vec![(10, "j1".into()), (20, "j2".into()), (30, "j3".into()),]
        );
    }

    #[test]
    fn iter_empty() {
        let idx = ReadyIndex::new();
        assert_eq!(idx.iter().collect::<Vec<_>>(), Vec::<(u16, String)>::new());
    }
}
