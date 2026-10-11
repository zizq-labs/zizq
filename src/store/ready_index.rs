// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

use std::collections::HashSet;
use std::sync::Arc;

use crossbeam_skiplist::{SkipMap, SkipSet};
use dashmap::DashMap;
use scru128::Id;

use super::index_id::index_id;

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
/// Keys are `(priority, job_id)`. `Ord` on `(u16, Id)` gives priority ASC,
/// then job_id ASC — exactly the ordering needed (FIFO within same priority,
/// since scru128 IDs are time-ordered, as integers and as strings alike).
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
    /// per-queue set to clean up. This is the queue's interned name from
    /// `by_queue`, shared by every job on the queue, so it costs no extra
    /// allocation.
    global: SkipMap<(u16, Id), Arc<str>>,

    /// Per-queue indexes — for queue-filtered candidate selection.
    ///
    /// `DashMap` provides concurrent access to the `queue -> SkipSet` mapping
    /// without a global lock.
    ///
    /// Entries are only removed from this set if they were successfully
    /// claimed in the global set. No entries should exist in the global set
    /// that do not exist in the per-queue set (insert order is per-queue first
    /// then global).
    ///
    /// The keys are the interned queue names. A queue is removed once its
    /// set is empty, which frees its name when no job holds it any more, so
    /// queues created on the fly don't accumulate for the life of the
    /// process.
    by_queue: DashMap<Arc<str>, SkipSet<(u16, Id)>>,
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
        let Some(id) = index_id(job_id) else {
            return;
        };
        self.global.remove(&(priority, id));
        self.remove_from_queue(queue, &(priority, id));
    }

    /// Remove an entry from its queue's set, and the queue itself once it
    /// has no jobs left.
    fn remove_from_queue(&self, queue: &str, entry: &(u16, Id)) {
        let now_empty = match self.by_queue.get(queue) {
            Some(set) => {
                set.remove(entry);
                set.is_empty()
            }
            None => return,
        };

        // The read guard above is released before this takes the shard's
        // write lock, which would otherwise deadlock. Emptiness is checked
        // again under the write lock, because an insert may have refilled
        // the set in between. Inserts add to a set only while holding a
        // guard on it, so none can land in a set being removed.
        if now_empty {
            self.by_queue.remove_if(queue, |_, set| set.is_empty());
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
        let Some(id) = index_id(&job_id) else {
            return;
        };
        let entry = (priority, id);

        // An existing queue is found under a read lock, and its interned
        // name reused. Only a new queue takes the write lock and allocates.
        let queue = match self.by_queue.get(queue) {
            Some(set) => {
                set.insert(entry);
                set.key().clone()
            }
            None => {
                let set = self
                    .by_queue
                    .entry(Arc::from(queue))
                    .or_insert_with(SkipSet::new);
                set.insert(entry);
                set.key().clone()
            }
        };
        self.global.insert(entry, queue);
    }

    /// Iterate over the global ready index in priority order.
    ///
    /// Yields `(priority, job_id)` pairs. Callers should use `.skip()`
    /// and `.take()` for pagination.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u16, String)> + '_ {
        self.global.iter().map(|entry| {
            let (priority, job_id) = entry.key();
            (*priority, job_id.to_string())
        })
    }

    /// Whether an entry is present.
    pub(super) fn contains(&self, priority: u16, job_id: &str) -> bool {
        index_id(job_id).is_some_and(|id| self.global.contains_key(&(priority, id)))
    }

    /// Iterate every entry, yielding `(priority, job_id, queue)`.
    ///
    /// The queue-bearing counterpart of [`iter`](Self::iter), for
    /// callers that need to address entries in another index.
    pub(super) fn iter_with_queue(&self) -> impl Iterator<Item = (u16, String, String)> + '_ {
        self.global.iter().map(|entry| {
            let (priority, job_id) = entry.key();
            (*priority, job_id.to_string(), entry.value().to_string())
        })
    }

    /// Drop every entry.
    pub(super) fn clear(&self) {
        self.global.clear();
        self.by_queue.clear();
    }

    /// The job this index would offer next, without taking it.
    ///
    /// Same selection as [`claim`](Self::claim) but non-destructive, for
    /// callers that must decide whether they can accept a job before
    /// committing to removing it — a budgeted job cannot be taken until
    /// its budgets have been debited, and that can fail.
    pub(super) fn peek(&self, queues: &HashSet<String>) -> Option<(u16, String, String)> {
        if queues.is_empty() {
            let entry = self.global.front()?;
            let (priority, job_id) = entry.key();
            return Some((*priority, job_id.to_string(), entry.value().to_string()));
        }

        queues
            .iter()
            .filter_map(|queue| {
                let set = self.by_queue.get(queue.as_str())?;
                let entry = set.front()?;
                let (priority, job_id) = entry.value();
                Some((*priority, *job_id, queue.clone()))
            })
            .min()
            .map(|(priority, job_id, queue)| (priority, job_id.to_string(), queue))
    }

    /// The job each queue would offer next, one per queue.
    ///
    /// [`peek`](Self::peek) answers for a *worker*, who names the queues
    /// it serves and wants the single best job across them. This answers
    /// for a caller with no worker in mind — a waker deciding what to
    /// announce — which is a different question: every queue is somebody
    /// else's best job, and collapsing them to one loses all but one.
    ///
    /// Bounded by the number of queues with work rather than by how many
    /// jobs are waiting, since each queue is peeked rather than walked.
    pub(super) fn peek_each_queue(&self) -> Vec<(u16, String, String)> {
        self.by_queue
            .iter()
            .filter_map(|entry| {
                let front = entry.value().front()?;
                let (priority, job_id) = front.value();
                Some((*priority, job_id.to_string(), entry.key().to_string()))
            })
            .collect()
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
                let key = *entry.key();
                let queue = entry.value().clone();
                Some((entry, key, queue))
            } else {
                // If we're taking a job from a specific set of queues, we need
                // to find the first job on each of those queues and then
                // select the highest priority/earliest job ID.
                let (priority, job_id, queue) = queues
                    .iter()
                    .filter_map(|q| {
                        let set = self.by_queue.get(q.as_str())?;
                        let front = set.front()?;
                        let (priority, job_id) = *front.value();
                        Some((priority, job_id, set.key().clone()))
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
                    self.remove_from_queue(&queue, &(priority, job_id));
                    return Some((priority, job_id.to_string(), queue.to_string()));
                }
            } // else loop again
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::job_id;
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
        idx.insert("emails", 10, job_id("j1"));
        idx.insert("emails", 20, job_id("j2"));
        idx.insert("webhooks", 10, job_id("j3"));
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove_cleans_both_global_and_per_queue() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.insert("emails", 20, job_id("j2"));

        idx.remove("emails", 10, &job_id("j1"));
        assert_eq!(idx.len(), 1);

        // Filtered claim should only see j2, not a ghost of j1.
        let queues: HashSet<String> = ["emails".into()].into();
        let claimed = idx.claim(&queues);
        assert_eq!(claimed, Some((20, job_id("j2"), "emails".into())));
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.remove("emails", 99, &job_id("nonexistent"));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn claim_unfiltered_returns_highest_priority() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 20, job_id("j1"));
        idx.insert("webhooks", 5, job_id("j2"));
        idx.insert("emails", 10, job_id("j3"));

        let claimed = idx.claim(&HashSet::new()).unwrap();
        assert_eq!(claimed, (5, job_id("j2"), "webhooks".into()));
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn claim_unfiltered_fifo_within_same_priority() {
        let idx = ReadyIndex::new();
        // IDs are lexicographically ordered: a < b < c.
        idx.insert("q", 10, job_id("a"));
        idx.insert("q", 10, job_id("c"));
        idx.insert("q", 10, job_id("b"));

        let (_, id1, _) = idx.claim(&HashSet::new()).unwrap();
        let (_, id2, _) = idx.claim(&HashSet::new()).unwrap();
        let (_, id3, _) = idx.claim(&HashSet::new()).unwrap();
        assert_eq!(id1, job_id("a"));
        assert_eq!(id2, job_id("b"));
        assert_eq!(id3, job_id("c"));
        assert_eq!(idx.claim(&HashSet::new()), None);
    }

    #[test]
    fn claim_filtered_returns_only_from_requested_queues() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.insert("webhooks", 5, job_id("j2"));
        idx.insert("reports", 1, job_id("j3"));

        let queues: HashSet<String> = ["emails".into(), "webhooks".into()].into();
        let (priority, id, queue) = idx.claim(&queues).unwrap();
        // webhooks has priority 5, emails has 10 — webhooks wins.
        assert_eq!(priority, 5);
        assert_eq!(id, job_id("j2"));
        assert_eq!(queue, "webhooks");
        // reports (priority 1) should NOT have been returned.
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn claim_filtered_returns_none_for_empty_queue() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));

        let queues: HashSet<String> = ["webhooks".into()].into();
        assert_eq!(idx.claim(&queues), None);
        // j1 should still be there.
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn claim_filtered_picks_best_across_queues() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 20, job_id("j1"));
        idx.insert("webhooks", 10, job_id("j2"));

        let queues: HashSet<String> = ["emails".into(), "webhooks".into()].into();
        let (_, id, _) = idx.claim(&queues).unwrap();
        assert_eq!(id, job_id("j2")); // lower priority number wins
    }

    #[test]
    fn claim_removes_from_both_global_and_per_queue() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));

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
        idx.insert("emails", 10, job_id("j1"));
        idx.remove("emails", 10, &job_id("j1"));

        idx.insert("emails", 20, job_id("j2"));

        let queues: HashSet<String> = ["emails".into()].into();
        let claimed = idx.claim(&queues);
        assert_eq!(claimed, Some((20, job_id("j2"), "emails".into())));
    }

    #[test]
    fn iter_returns_priority_order() {
        let idx = ReadyIndex::new();
        idx.insert("q", 30, job_id("j3"));
        idx.insert("q", 10, job_id("j1"));
        idx.insert("q", 20, job_id("j2"));

        let items: Vec<(u16, String)> = idx.iter().collect();
        assert_eq!(
            items,
            vec![(10, job_id("j1")), (20, job_id("j2")), (30, job_id("j3")),]
        );
    }

    #[test]
    fn iter_empty() {
        let idx = ReadyIndex::new();
        assert_eq!(idx.iter().collect::<Vec<_>>(), Vec::<(u16, String)>::new());
    }

    #[test]
    fn jobs_on_a_queue_share_one_interned_name() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.insert("emails", 20, job_id("j2"));

        let names: Vec<Arc<str>> = idx.global.iter().map(|e| e.value().clone()).collect();
        assert!(Arc::ptr_eq(&names[0], &names[1]));
    }

    #[test]
    fn a_queue_is_dropped_once_its_last_job_is_removed() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.insert("emails", 20, job_id("j2"));

        idx.remove("emails", 10, &job_id("j1"));
        assert_eq!(idx.by_queue.len(), 1);

        idx.remove("emails", 20, &job_id("j2"));
        assert_eq!(idx.by_queue.len(), 0);
    }

    #[test]
    fn a_queue_is_dropped_once_its_last_job_is_claimed() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.insert("webhooks", 10, job_id("j2"));

        idx.claim(&HashSet::new()).unwrap();
        idx.claim(&["webhooks".into()].into()).unwrap();
        assert_eq!(idx.by_queue.len(), 0);
    }

    #[test]
    fn a_dropped_queue_is_recreated_by_its_next_job() {
        let idx = ReadyIndex::new();
        idx.insert("emails", 10, job_id("j1"));
        idx.claim(&HashSet::new()).unwrap();

        idx.insert("emails", 20, job_id("j2"));
        let queues: HashSet<String> = ["emails".into()].into();
        assert_eq!(
            idx.claim(&queues),
            Some((20, job_id("j2"), "emails".into()))
        );
    }

    /// Inserts and claims racing on one queue, which empties and is
    /// dropped over and over. An insert that landed in a set being
    /// dropped would lose its job from the queue-filtered view.
    #[test]
    fn no_job_is_lost_while_a_queue_is_dropped_and_recreated() {
        const PRODUCERS: u32 = 4;
        const PER_PRODUCER: u32 = 2_000;

        let idx = ReadyIndex::new();
        let queues: HashSet<String> = ["q".into()].into();
        let claimed = std::sync::atomic::AtomicU32::new(0);
        let total = PRODUCERS * PER_PRODUCER;

        std::thread::scope(|scope| {
            for p in 0..PRODUCERS {
                let idx = &idx;
                scope.spawn(move || {
                    for i in 0..PER_PRODUCER {
                        let id = scru128::Id::from_u128(u128::from(p * PER_PRODUCER + i + 1));
                        idx.insert("q", 0, id.to_string());
                    }
                });
            }
            for _ in 0..4 {
                let (idx, queues, claimed) = (&idx, &queues, &claimed);
                scope.spawn(move || {
                    while claimed.load(std::sync::atomic::Ordering::Relaxed) < total {
                        if idx.claim(queues).is_some() {
                            claimed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        assert_eq!(claimed.into_inner(), total);
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.by_queue.len(), 0);
    }
}
