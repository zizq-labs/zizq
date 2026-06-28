// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job dequeue: claim ready jobs from the priority index and mark them
//! in-flight.
//!
//! The singular `take_next_job` is a thin wrapper over the bulk
//! `take_next_n_jobs`. Both lift candidates out of the lock-free ready
//! index *before* opening a transaction (so concurrent takers can't
//! race on the same entry), pre-read each candidate's stored job
//! record to filter out deletions and stale ready-index entries, then
//! batch-write the Ready→InFlight transition in one fjall commit.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fjall::Slice;
use tokio::task;

use super::keys::{make_job_key, make_payload_key, make_status_key, make_unique_key};
use super::store::{Store, StoreEvent};
use super::types::{Job, JobStatus, StoreError, UniqueWhile};

impl Store {
    /// Take the next job from the priority index.
    ///
    /// Atomically claims the highest-priority (lowest number), oldest job
    /// from the lock-free ready index and marks it as in-flight. Returns `None`
    /// if no ready jobs match.
    ///
    /// When `queues` is non-empty, only jobs in the specified queues are
    /// considered. When empty, all ready jobs are candidates.
    ///
    /// The `claim()` call removes the entry from the skip-list index before
    /// we even start the fjall transaction — this prevents duplicates without
    /// needing a mutex around peek+remove. If the fjall commit fails, we
    /// re-insert into the index to restore consistency.
    ///
    /// Subscribers are not notified.
    pub async fn take_next_job(
        &self,
        now: u64,
        queues: &HashSet<String>,
    ) -> Result<Option<Job>, StoreError> {
        Ok(self
            .take_next_n_jobs(now, queues, 1)
            .await?
            .into_iter()
            .next())
    }

    /// Take up to `n` ready jobs from the queue in a single batch.
    ///
    /// Claims up to `n` candidates from the ready index, validates them,
    /// writes all status transitions in one transaction, and hydrates
    /// payloads. Returns a `Vec<Job>` that may be shorter than `n` if
    /// the queue had fewer ready jobs.
    pub async fn take_next_n_jobs(
        &self,
        now: u64,
        queues: &HashSet<String>,
        n: usize,
    ) -> Result<Vec<Job>, StoreError> {
        if n == 0 || !self.index_ready.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let ks = self.ks.clone();
        let ready_index = self.ready_index.clone();
        let event_tx = self.event_tx.clone();
        let queues = queues.clone();

        let spawn_start = std::time::Instant::now();
        let jobs = task::spawn_blocking(move || -> Result<_, StoreError> {
            let blocking_start = std::time::Instant::now();
            let spawn_wait = blocking_start.duration_since(spawn_start);

            // PreRead is declared outside the loop since struct definitions
            // aren't allowed inside loop bodies in all editions.
            struct PreRead {
                job: Job,
                pre_bytes: Slice,
                job_key: Vec<u8>,
                old_status_key: Vec<u8>,
                new_status_key: Vec<u8>,
                updated_slice: Slice,
                job_id: String,
            }

            // Retry loop: on CAS mismatch we re-insert into the ready
            // index and re-claim, same pattern as take_next_job.
            loop {
                // 1. Claim phase — collect up to `n` candidates from the ready index.
                let mut claimed: Vec<(u16, String, String)> = Vec::with_capacity(n);
                while claimed.len() < n {
                    match ready_index.claim(&queues) {
                        Some(entry) => claimed.push(entry),
                        None => break,
                    }
                }

                if claimed.is_empty() {
                    tracing::trace!(
                        spawn_wait_ms = spawn_wait.as_secs_f64() * 1000.0,
                        "take_next_n_jobs: empty"
                    );
                    return Ok(Vec::new());
                }

                // 2. Pre-read phase — read each claimed job from the data keyspace.
                //    Filter out any that are deleted or no longer Ready.
                let mut valid: Vec<PreRead> = Vec::with_capacity(claimed.len());

                for (_priority, job_id, _queue) in &claimed {
                    let job_key = make_job_key(job_id);
                    let pre_bytes = match ks.data.get(&job_key)? {
                        Some(bytes) => bytes,
                        None => continue, // Deleted.
                    };

                    let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;
                    if job.status != JobStatus::Ready as u8 {
                        continue; // No longer ready.
                    }

                    let old_status_key = make_status_key(JobStatus::Ready, &job.id);
                    let new_status_key = make_status_key(JobStatus::InFlight, &job.id);

                    job.status = JobStatus::InFlight.into();
                    job.dequeued_at = Some(now);

                    let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                    valid.push(PreRead {
                        job,
                        pre_bytes,
                        job_key,
                        old_status_key,
                        new_status_key,
                        updated_slice,
                        job_id: job_id.clone(),
                    });
                }

                if valid.is_empty() {
                    tracing::trace!(
                        spawn_wait_ms = spawn_wait.as_secs_f64() * 1000.0,
                        claimed = claimed.len(),
                        "take_next_n_jobs: all claimed candidates invalid"
                    );
                    return Ok(Vec::new());
                }

                // 3. Write phase — one transaction for all valid pre-reads.
                //    If any CAS fails, abort the entire transaction since
                //    fetch_update already wrote into the tx buffer.
                let mut tx = ks.write_tx();
                let mut cas_conflict_detected = false;

                for pre in &valid {
                    let prev = tx.fetch_update(&ks.data, &pre.job_key, |_| {
                        Some(pre.updated_slice.clone())
                    })?;

                    if prev.as_deref() != Some(&*pre.pre_bytes) {
                        cas_conflict_detected = true;
                        break;
                    }

                    tx.remove(&ks.index, &pre.old_status_key);
                    tx.insert(&ks.index, &pre.new_status_key, b"");

                    // If unique_while == Queued, remove the unique index when
                    // the job transitions out of the queue (Ready -> InFlight),
                    // but only if it still belongs to this job.
                    if let Some(ref uc) = pre.job.unique {
                        if uc.unique_while() == UniqueWhile::Queued {
                            let job_id = pre.job_id.as_bytes();
                            tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                                Some(v) if v.as_ref() == job_id => None,
                                other => other.cloned(),
                            })?;
                        }
                    }
                }

                if cas_conflict_detected {
                    // CAS mismatch — abort the whole batch, re-insert
                    // valid entries into the ready index, and retry.
                    tracing::trace!(
                        valid = valid.len(),
                        "take_next_n_jobs: CAS conflict, retrying"
                    );
                    drop(tx);
                    for pre in &valid {
                        ready_index.insert(&pre.job.queue, pre.job.priority, pre.job_id.clone());
                    }
                    continue; // Back to the top of the retry loop
                }

                let write_tx_elapsed = blocking_start.elapsed();

                if let Err(e) = ks.commit(tx, ks.default_commit_mode) {
                    // 4. Rollback — re-insert valid entries into the
                    //    ready index and notify workers.
                    let _tx = ks.write_tx();
                    for pre in &valid {
                        ready_index.insert(&pre.job.queue, pre.job.priority, pre.job_id.clone());

                        let _ = event_tx.send(StoreEvent::JobCreated {
                            id: pre.job_id.clone(),
                            queue: pre.job.queue.clone(),
                            token: Arc::new(AtomicBool::new(false)),
                        });
                    }
                    return Err(e);
                }

                // 5. Hydrate phase — read payloads for all committed jobs.
                let hydrate_start = std::time::Instant::now();
                for pre in &mut valid {
                    let payload_key = make_payload_key(&pre.job_id);
                    if let Some(payload_bytes) = ks.data.get(&payload_key)? {
                        pre.job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
                    }
                }
                let hydrate_elapsed = hydrate_start.elapsed();

                let count = valid.len();
                let jobs: Vec<Job> = valid.into_iter().map(|pre| pre.job).collect();

                tracing::trace!(
                    spawn_wait_ms = spawn_wait.as_secs_f64() * 1000.0,
                    write_tx_ms = write_tx_elapsed.as_secs_f64() * 1000.0,
                    hydrate_ms = hydrate_elapsed.as_secs_f64() * 1000.0,
                    total_ms = blocking_start.elapsed().as_secs_f64() * 1000.0,
                    count,
                    "take_next_n_jobs"
                );

                return Ok(jobs);
            }
        })
        .await??;

        // Insert into the in-flight index and broadcast events.
        for job in &jobs {
            self.in_flight_index
                .insert(job.dequeued_at.unwrap_or(now), job.id.clone());
            let _ = self
                .event_tx
                .send(StoreEvent::JobInFlight { id: job.id.clone() });
        }

        Ok(jobs)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::SystemTime;

    use super::super::options::{BulkDeleteOptions, EnqueueOptions};
    use super::super::store::StoreEvent;
    use super::super::test_support::test_store;
    use super::super::types::JobStatus;
    use crate::time::now_millis;

    #[tokio::test]
    async fn take_next_job_returns_none_when_empty() {
        let store = test_store();
        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        assert!(job.is_none());
    }

    #[tokio::test]
    async fn take_next_job_returns_highest_priority() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("mid")).priority(5),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.priority, 1);
        assert_eq!(job.status, u8::from(JobStatus::InFlight));
        assert_eq!(job.payload, Some(serde_json::json!("high")));
    }

    #[tokio::test]
    async fn take_next_job_fifo_within_same_priority() {
        let store = test_store();
        let first = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, first.id);
    }

    #[tokio::test]
    async fn take_next_job_removes_from_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

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

        assert_ne!(first.id, second.id);
        assert_eq!(first.payload, Some(serde_json::json!("a")));
        assert_eq!(second.payload, Some(serde_json::json!("b")));

        // Queue should now be empty.
        assert!(
            store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_never_returns_duplicates_under_contention() {
        let store = Arc::new(test_store());
        let num_jobs = 50;

        for i in 0..num_jobs {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        // Spawn many concurrent workers all racing to take jobs.
        let mut handles = Vec::new();
        for _ in 0..20 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(job) = store
                    .take_next_job(now_millis(), &HashSet::new())
                    .await
                    .unwrap()
                {
                    taken.push(job.id);
                }
                taken
            }));
        }

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.await.unwrap());
        }

        // Every job should have been taken exactly once.
        assert_eq!(all_ids.len(), num_jobs);
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), num_jobs);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn take_next_job_filtered_never_returns_duplicates_under_contention() {
        let store = Arc::new(test_store());
        let num_jobs = 50;
        let queue = "contention";

        for i in 0..num_jobs {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", queue, serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let queues: HashSet<String> = [queue.to_string()].into_iter().collect();

        // Spawn many concurrent workers all racing to take jobs
        // using the queue-filtered path.
        let mut handles = Vec::new();
        for _ in 0..20 {
            let store = store.clone();
            let queues = queues.clone();
            handles.push(tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(job) = store.take_next_job(now_millis(), &queues).await.unwrap() {
                    taken.push(job.id.clone());
                    // Also verify the ack works (job is in InFlight status).
                    assert!(
                        store.mark_completed(now_millis(), &job.id).await.unwrap(),
                        "mark_completed returned false for job {}",
                        job.id,
                    );
                }
                taken
            }));
        }

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.await.unwrap());
        }

        // Every job should have been taken and acked exactly once.
        assert_eq!(all_ids.len(), num_jobs);
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), num_jobs);
    }

    // --- take_next_job queue filter tests ---

    #[tokio::test]
    async fn take_next_job_filters_by_single_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["reports".into()]);
        let job = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, b.id);
        assert_eq!(job.queue, "reports");

        // No more reports jobs.
        assert!(
            store
                .take_next_job(now_millis(), &queues)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_filters_by_multiple_queues() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into(), "webhooks".into()]);
        let first = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, a.id);

        let second = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.id, c.id);

        assert!(
            store
                .take_next_job(now_millis(), &queues)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_respects_priority() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into()]);
        let job = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, high.id);
        assert_eq!(job.priority, 1);
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_returns_none_for_empty_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["reports".into()]);
        assert!(
            store
                .take_next_job(now_millis(), &queues)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn take_next_job_queue_filter_picks_best_across_queues() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into(), "webhooks".into()]);
        let job = store
            .take_next_job(now_millis(), &queues)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, high.id);
        assert_eq!(job.queue, "webhooks");
        assert_eq!(job.priority, 1);
    }

    #[tokio::test]
    async fn take_next_job_sets_dequeued_at() {
        let store = test_store();
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let dequeued_at = job.dequeued_at.expect("dequeued_at should be set");
        assert!(
            dequeued_at >= before && dequeued_at <= after,
            "dequeued_at ({dequeued_at}) should be between {before} and {after}"
        );
    }

    #[tokio::test]
    async fn take_next_job_dequeued_at_persists() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        let stored = store
            .get_job(now_millis(), &taken.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.dequeued_at, taken.dequeued_at);
    }

    // --- take_next_n_jobs tests ---

    #[tokio::test]
    async fn take_next_n_jobs_returns_up_to_n() {
        let store = test_store();
        for i in 0..5 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 3)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn take_next_n_jobs_returns_fewer_when_not_enough() {
        let store = test_store();
        for i in 0..2 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 5)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn take_next_n_jobs_empty_store() {
        let store = test_store();
        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 5)
            .await
            .unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn take_next_n_jobs_respects_priority() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("low")).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("high")).priority(1),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("mid")).priority(5),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 3)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].priority, 1);
        assert_eq!(jobs[1].priority, 5);
        assert_eq!(jobs[2].priority, 10);
    }

    #[tokio::test]
    async fn take_next_n_jobs_fifo_within_priority() {
        let store = test_store();
        let first = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let second = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 2)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, first.id);
        assert_eq!(jobs[1].id, second.id);
    }

    #[tokio::test]
    async fn take_next_n_jobs_filters_by_queue() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "alpha", serde_json::json!("a1")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "beta", serde_json::json!("b1")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "alpha", serde_json::json!("a2")),
            )
            .await
            .unwrap()
            .into_job();

        let queues: HashSet<String> = ["alpha".to_string()].into_iter().collect();
        let jobs = store
            .take_next_n_jobs(now_millis(), &queues, 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|j| j.queue == "alpha"));
    }

    #[tokio::test]
    async fn take_next_n_jobs_marks_in_flight() {
        let store = test_store();
        for i in 0..3 {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "default", serde_json::json!(i)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 3)
            .await
            .unwrap();
        for job in &jobs {
            assert_eq!(job.status, u8::from(JobStatus::InFlight));
            assert!(job.dequeued_at.is_some());
        }
    }

    #[tokio::test]
    async fn take_next_n_jobs_hydrates_payload() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"key": "value"})),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 1)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].payload, Some(serde_json::json!({"key": "value"})));
    }

    #[tokio::test]
    async fn take_next_n_jobs_broadcasts_events() {
        let store = test_store();
        let mut rx = store.subscribe();

        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        // Drain the enqueue events.
        while let Ok(StoreEvent::JobCreated { .. }) = rx.try_recv() {}

        let jobs = store
            .take_next_n_jobs(now_millis(), &HashSet::new(), 2)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);

        // We should receive exactly 2 JobInFlight events.
        let mut in_flight_ids = Vec::new();
        for _ in 0..2 {
            match rx.try_recv() {
                Ok(StoreEvent::JobInFlight { id }) => in_flight_ids.push(id),
                other => panic!("expected JobInFlight, got {:?}", other),
            }
        }
        assert_eq!(in_flight_ids.len(), 2);
        assert!(in_flight_ids.contains(&jobs[0].id));
        assert!(in_flight_ids.contains(&jobs[1].id));
    }

    /// Regression test: enqueue to queue X, delete all, enqueue to queue X
    /// again, then take with a queue filter. The filtered take must deliver
    /// the new jobs. Previously, `delete_jobs` only removed entries from the
    /// global ready index but not the per-queue index, leaving orphaned
    /// entries that caused the filtered take to spin in an infinite loop.
    #[tokio::test]
    async fn take_filtered_works_after_delete_and_reenqueue() {
        let store = test_store();
        let now = now_millis();
        let queues: HashSet<String> = ["myqueue".to_string()].into();

        // 1. Enqueue a job to the target queue.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("first")),
            )
            .await
            .unwrap();

        // 2. Delete all jobs (unscoped).
        let deleted = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(deleted, 1);

        // 3. Enqueue a new job to the same queue.
        let enqueued = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("second")),
            )
            .await
            .unwrap()
            .into_job();

        // 4. Filtered take must return the new job.
        let taken = store.take_next_job(now, &queues).await.unwrap();
        assert!(taken.is_some(), "filtered take should return the new job");
        assert_eq!(taken.unwrap().id, enqueued.id);
    }

    /// Same as above but with a queue-scoped delete instead of unscoped.
    #[tokio::test]
    async fn take_filtered_works_after_scoped_delete_and_reenqueue() {
        let store = test_store();
        let now = now_millis();
        let queues: HashSet<String> = ["myqueue".to_string()].into();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("first")),
            )
            .await
            .unwrap();

        let mut delete_opts = BulkDeleteOptions::new();
        delete_opts.filter.queues = ["myqueue".to_string()].into();
        let deleted = store.delete_jobs(delete_opts).await.unwrap();
        assert_eq!(deleted, 1);

        let enqueued = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "myqueue", serde_json::json!("second")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store.take_next_job(now, &queues).await.unwrap();
        assert!(taken.is_some(), "filtered take should return the new job");
        assert_eq!(taken.unwrap().id, enqueued.id);
    }
}
