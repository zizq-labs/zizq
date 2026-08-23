// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Startup recovery: re-promote orphaned in-flight jobs to Ready, then
//! rebuild the in-memory indexes (ready, scheduled, cron) from disk.
//!
//! Must complete before the server accepts API requests — the ready
//! index gate (`Store::index_ready`) guards `take_next_n_jobs` so workers
//! see an empty queue until the rebuild finishes and emits
//! `StoreEvent::IndexRebuilt`.

use std::ops::Bound;
use std::sync::atomic::Ordering;

use fjall::{Readable, Slice};
use tokio::task;

use super::dispatch::Placement;
use super::keys::{IndexKind, make_job_key, make_status_key, make_unique_key};
use super::store::{Store, StoreEvent};
use super::types::{Job, JobStatus, StoreError, UniqueWhile};
use crate::time::now_millis;

impl Store {
    /// Recover state after startup.
    ///
    /// Move orphaned in-flight jobs back to Ready.
    ///
    /// Must complete before accepting API requests to avoid races
    /// with concurrent job completions. The job scan and status
    /// transitions run synchronously; only the durable commit is
    /// awaited.
    pub async fn recover_in_flight(&self) -> Result<usize, StoreError> {
        let store = self.clone();
        task::spawn_blocking(move || store.recover_in_flight_jobs()).await?
    }

    /// Rebuild all in-memory indexes asynchronously.
    ///
    /// Sets `index_ready` to `false` while rebuilding and emits
    /// `IndexRebuilt` when complete. Workers wait on `index_ready`
    /// before taking jobs.
    pub async fn rebuild_indexes(&self) -> Result<(usize, usize, usize), StoreError> {
        self.index_ready.store(false, Ordering::Release);

        // Sequenced before the rebuilds rather than joined with them:
        // `rebuild_dispatch` parks budgeted jobs into their groups, and
        // those groups have to exist by the time it does.
        let loaded = self.load_budgets().await?;
        if loaded > 0 {
            tracing::info!(count = loaded, "budget policies loaded");
        }

        let (ready, scheduled, cron) = tokio::try_join!(
            self.rebuild_dispatch(),
            self.rebuild_scheduled_index(),
            self.rebuild_cron_index(),
        )?;

        self.index_ready.store(true, Ordering::Release);
        let _ = self.event_tx.send(StoreEvent::IndexRebuilt);
        Ok((ready, scheduled, cron))
    }

    /// Load every stored budget policy into the live registry.
    ///
    /// Must run before the ready index is rebuilt. A budgeted job is
    /// dispatched from its budget's group rather than from the ready
    /// index, and `Budgets::park` skips a group that does not exist —
    /// deliberately, since conjuring one would invent an allocation to
    /// go with it. Loading after the rebuild would therefore drop every
    /// budgeted job out of dispatch until something happened to
    /// re-insert it.
    ///
    /// Allocations come back full, and for concurrency budgets that is
    /// simply accurate: `recover_in_flight_jobs` has already returned
    /// every in-flight job to Ready, so nothing holds a slot. Time-based
    /// budgets also come back full, which does mean a restart forgives
    /// whatever the previous process had spent — an acceptable trade-off.
    /// Any other implementation would require heavy disk access to
    /// persist budget state (token usage etc).
    async fn load_budgets(&self) -> Result<usize, StoreError> {
        let budgets = self.list_budgets().await?;
        let now = now_millis();

        for (key, budget) in &budgets {
            self.budgets.sync(key, budget, now);
        }

        Ok(budgets.len())
    }

    /// Move orphaned in-flight jobs back to Ready in the LSM indexes.
    ///
    /// Runs synchronously so it completes before any API requests are
    /// accepted. Does not touch the in-memory ready index — that's
    /// handled by `rebuild_dispatch`, which runs immediately after.
    fn recover_in_flight_jobs(&self) -> Result<usize, StoreError> {
        let ks = &self.ks;

        // Scan the status index for all InFlight jobs.
        // InFlight = 2, so the prefix range is [S, 0, 2, 0]..[S, 0, 3, 0].
        let start: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::InFlight as u8, 0];
        let end: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::InFlight as u8 + 1, 0];
        let range = (Bound::Included(start), Bound::Excluded(end));

        // Collect IDs via a read snapshot first — the write tx type
        // does not support range scans.
        let snapshot = ks.db.read_tx();
        let in_flight_ids: Vec<String> = snapshot
            .range::<Vec<u8>, _>(&ks.index, range)
            .map(|entry| {
                let (key, _) = entry.into_inner()?;
                // Key layout: S\0{status_u8}\0{job_id} — skip the 4-byte prefix.
                String::from_utf8(key[4..].to_vec())
                    .map_err(|e| StoreError::Corruption(format!("job ID is not valid UTF-8: {e}")))
            })
            .collect::<Result<_, _>>()?;
        drop(snapshot);

        if in_flight_ids.is_empty() {
            return Ok(0);
        }

        let count = in_flight_ids.len();
        let mut tx = ks.write_tx();

        for id in &in_flight_ids {
            let job_key = make_job_key(id);
            let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                StoreError::Corruption(format!("in-flight job missing from data keyspace: {id:?}"))
            })?;
            let mut job: Job = rmp_serde::from_slice(&job_bytes)?;

            let old_status_key = make_status_key(JobStatus::InFlight, id);
            let new_status_key = make_status_key(JobStatus::Ready, id);
            job.status = JobStatus::Ready.into();
            let updated_bytes = rmp_serde::to_vec_named(&job)?;

            tx.insert(&ks.data, &job_key, &updated_bytes);
            tx.remove(&ks.index, &old_status_key);
            tx.insert(&ks.index, &new_status_key, b"");

            // If unique_while == Queued, restore the unique index when
            // a job recovers from InFlight back to Ready. Only insert if no
            // other job has claimed the key while this one was in-flight.
            if let Some(ref uc) = job.unique {
                if uc.unique_while() == UniqueWhile::Queued {
                    let id_bytes: Slice = id.as_bytes().into();
                    tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                        Some(existing) => Some(existing.clone()),
                        None => Some(id_bytes.clone()),
                    })?;
                }
            }
        }

        ks.commit(tx, ks.default_commit_mode)?;
        Ok(count)
    }

    /// Populate the in-memory ready index from the `jobs_by_status` index.
    ///
    /// Scans for all Ready jobs, reads their metadata to get queue and
    /// priority, and inserts each entry into the skip list. No mutex needed —
    /// each `insert()` is lock-free, and recovery runs before any consumers.
    ///
    /// Also counts each job against its budgets. That accounting is
    /// memory-only, so it has to be rebuilt from the same scan rather
    /// than surviving the restart; folding it in here avoids a third
    /// pass over the job records purely to re-derive it.
    async fn rebuild_dispatch(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let dispatch = self.dispatch.clone();
        let budgets = self.budgets.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Ready jobs.
            // Ready = 1, so the prefix range is [S, 0, 1, 0]..[S, 0, 2, 0].
            let start: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::Ready as u8, 0];
            let end: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::Ready as u8 + 1, 0];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let snapshot = ks.db.read_tx();
            let mut count = 0;

            for entry in snapshot.range::<Vec<u8>, _>(&ks.index, range) {
                let (key, _) = entry.into_inner()?;
                // Key layout: S\0{status_u8}\0{job_id} — skip the 4-byte prefix.
                let job_id = String::from_utf8(key[4..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;

                let job_key = make_job_key(&job_id);
                let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "ready job missing from data keyspace: {job_id:?}"
                    ))
                })?;
                let job: Job = rmp_serde::from_slice(&job_bytes)?;

                budgets.track(&job.budgets);
                dispatch.insert(Placement::of(&job));
                count += 1;
            }

            Ok(count)
        })
        .await?
    }

    /// Populate the in-memory scheduled index from the `jobs_by_status` index.
    ///
    /// Scans for all Scheduled jobs, reads their metadata to get `ready_at`,
    /// and inserts each entry into the SkipSet.
    ///
    /// Counts each job against its budgets too. A scheduled job is not
    /// queued for dispatch and holds no tokens, but it is unfinished
    /// work already committed to the budget — shrinking an allocation
    /// below what one costs would strand it the moment it came due.
    async fn rebuild_scheduled_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();
        let budgets = self.budgets.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan the status index for all Scheduled jobs.
            // Scheduled = 0, so the prefix range is [S, 0, 0, 0]..[S, 0, 1, 0].
            let start: Vec<u8> = vec![IndexKind::Status as u8, 0, JobStatus::Scheduled as u8, 0];
            let end: Vec<u8> = vec![
                IndexKind::Status as u8,
                0,
                JobStatus::Scheduled as u8 + 1,
                0,
            ];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let snapshot = ks.db.read_tx();
            let mut count = 0;

            for entry in snapshot.range::<Vec<u8>, _>(&ks.index, range) {
                let (key, _) = entry.into_inner()?;
                // Key layout: S\0{status_u8}\0{job_id} — skip the 4-byte prefix.
                let job_id = String::from_utf8(key[4..].to_vec()).map_err(|e| {
                    StoreError::Corruption(format!("job ID is not valid UTF-8: {e}"))
                })?;

                let job_key = make_job_key(&job_id);
                let job_bytes = ks.data.get(&job_key)?.ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "scheduled job missing from data keyspace: {job_id:?}"
                    ))
                })?;
                let job: Job = rmp_serde::from_slice(&job_bytes)?;

                budgets.track(&job.budgets);
                scheduled_index.insert(job.ready_at, job_id);
                count += 1;
            }

            Ok(count)
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::SystemTime;

    use super::super::budget::{BudgetBinding, BudgetStrategy};
    use super::super::options::EnqueueOptions;
    use super::super::test_support::test_store;
    use super::super::types::JobStatus;
    use crate::time::now_millis;

    #[tokio::test]
    async fn recover_moves_in_flight_to_ready() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        // Take the job so it becomes InFlight.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, job.id);

        // Nothing left to take.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );

        // Recover should move it back to Ready and rebuild the index.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 1);
        assert_eq!(indexed, 1);

        // The job should be takeable again.
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, job.id);
    }

    #[tokio::test]
    async fn recover_returns_zero_when_none_in_flight() {
        let store = test_store();

        // Enqueue a job but don't take it — it stays Ready.
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 0);
        // The already-ready job should be indexed.
        assert_eq!(indexed, 1);
    }

    #[tokio::test]
    async fn recover_preserves_priority() {
        let store = test_store();

        // Enqueue two jobs at different priorities.
        let low = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();

        // Take both so they become InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Recover both.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 2);
        assert_eq!(indexed, 2);

        // They should come back in priority order (high first, then low).
        let first = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        let second = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, high.id);
        assert_eq!(second.id, low.id);
    }

    #[tokio::test]
    async fn recover_ignores_other_statuses() {
        let store = test_store();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // A Ready job.
        let ready = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("ready")),
            )
            .await
            .unwrap()
            .into_job();

        // A Scheduled job (far in the future).
        let scheduled = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("scheduled"))
                    .ready_at(now + 600_000),
            )
            .await
            .unwrap()
            .into_job();

        // Recover should find no in-flight jobs, but index the ready one.
        let recovered = store.recover_in_flight().await.unwrap();
        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(recovered, 0);
        assert_eq!(indexed, 1);

        // The ready job is still takeable.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, ready.id);

        // The scheduled job is still in the scheduled index.
        let fetched = store
            .get_job(now_millis(), &scheduled.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.status, JobStatus::Scheduled as u8);
    }

    /// A budgeted job is dispatched from its budget's group rather than
    /// from the ready index, so the rebuild has to put it back there.
    /// Parking into a group that has not been loaded yet is a no-op,
    /// which would leave the job queued on disk but invisible to any
    /// worker.
    ///
    /// `budgets.clear()` stands in for a process restart: the registry
    /// is memory-only, so emptying it leaves exactly what a new process
    /// starts with — populated keyspaces and no live budget state.
    #[tokio::test]
    async fn a_budgeted_job_is_dispatchable_after_a_rebuild() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("emails", 10, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("a"))
                    .budget(BudgetBinding::new("emails")),
            )
            .await
            .unwrap()
            .into_job();

        store.budgets.clear();

        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(indexed, 1);

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, job.id);
    }

    /// The allocation has to come back with the group. Groups restored
    /// without their policy would make every budgeted job dispatchable
    /// at once, which looks like success right up until the throttle is
    /// the thing being relied on.
    #[tokio::test]
    async fn a_reloaded_budget_still_throttles() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("one-at-a-time", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();

        for payload in ["a", "b"] {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(payload))
                        .budget(BudgetBinding::new("one-at-a-time")),
                )
                .await
                .unwrap();
        }

        store.budgets.clear();

        let (indexed, _scheduled, _cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(indexed, 2);

        // The allocation of one survives the reload.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    /// The cost accounting is memory-only, so a restart has to re-derive
    /// it or every budget comes back looking unreferenced — and the
    /// guards would then happily delete one out from under a full queue.
    ///
    /// Covers both scans: a ready job and a scheduled one. The scheduled
    /// job is the easier one to miss, since it is not queued for
    /// dispatch and holds no tokens, but shrinking below its cost would
    /// strand it the moment it came due.
    #[tokio::test]
    async fn a_rebuild_restores_the_cost_accounting() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("ready"))
                    .budget(BudgetBinding::new("stripe").cost(3)),
            )
            .await
            .unwrap();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("later"))
                    .budget(BudgetBinding::new("stripe").cost(8))
                    .ready_at(now + 600_000),
            )
            .await
            .unwrap();

        assert_eq!(store.budgets.tracked("stripe"), 2);

        store.budgets.clear();
        assert_eq!(store.budgets.tracked("stripe"), 0);

        store.rebuild_indexes().await.unwrap();

        assert_eq!(store.budgets.tracked("stripe"), 2);
        assert_eq!(store.budgets.max_cost("stripe"), Some(8));
    }

    /// A job that was mid-flight when the process died holds no token in
    /// the new one: recovery returns it to Ready, so the concurrency it
    /// was occupying is genuinely free. A budget of one that failed to
    /// notice would deadlock on its own orphan.
    #[tokio::test]
    async fn an_orphaned_budgeted_job_does_not_hold_its_own_slot() {
        let store = test_store();
        let now = now_millis();

        store
            .create_budget("solo", 1, BudgetStrategy::WhileInFlight, now)
            .await
            .unwrap();

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!("a"))
                    .budget(BudgetBinding::new("solo")),
            )
            .await
            .unwrap()
            .into_job();

        // Take it, leaving it in flight and holding the only slot.
        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.id, job.id);

        store.budgets.clear();

        assert_eq!(store.recover_in_flight().await.unwrap(), 1);
        store.rebuild_indexes().await.unwrap();

        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retaken.id, job.id);
    }
}
