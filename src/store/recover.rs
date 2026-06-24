// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
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

use super::keys::{IndexKind, make_job_key, make_status_key, make_unique_key};
use super::store::{Store, StoreEvent};
use super::types::{Job, JobStatus, StoreError, UniqueWhile};

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

        let (ready, scheduled, cron) = tokio::try_join!(
            self.rebuild_ready_index(),
            self.rebuild_scheduled_index(),
            self.rebuild_cron_index(),
        )?;

        self.index_ready.store(true, Ordering::Release);
        let _ = self.event_tx.send(StoreEvent::IndexRebuilt);
        Ok((ready, scheduled, cron))
    }

    /// Move orphaned in-flight jobs back to Ready in the LSM indexes.
    ///
    /// Runs synchronously so it completes before any API requests are
    /// accepted. Does not touch the in-memory ready index — that's
    /// handled by `rebuild_ready_index`, which runs immediately after.
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
    async fn rebuild_ready_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();

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

                ready_index.insert(&job.queue, job.priority, job_id);
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
    async fn rebuild_scheduled_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();

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
}
