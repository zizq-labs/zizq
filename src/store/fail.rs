// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Job failure handling: retry vs kill, backoff, error record persistence.

use fjall::Slice;
use tokio::task;

use super::delete::{apply_job_deletion, prepare_job_deletion};
use super::keys::{make_error_key, make_job_key, make_purge_key, make_status_key, make_unique_key};
use super::options::FailureOptions;
use super::store::{Store, StoreEvent};
use super::types::{BackoffConfig, ErrorRecord, Job, JobStatus, StoreError, UniqueWhile};

impl Store {
    /// Record a job failure and either schedule a retry or kill the job
    /// depending on the backoff policy.
    ///
    /// Returns `Some(job)` with metadata-only fields if the job was found in
    /// the InFlight state. The returned job's status will be either `Scheduled`
    /// in the retry scenario or `Dead` if the retry limit has been reached.
    /// Returns `None` if the job wasn't found or wasn't in the InFlight state.
    ///
    /// Dead jobs are transitioned to Dead status with a `purge_at` timestamp
    /// so the reaper can hard-delete them after the retention period.
    pub async fn record_failure(
        &self,
        now: u64,
        id: &str,
        opts: FailureOptions,
    ) -> Result<Option<Job>, StoreError> {
        let ks = self.ks.clone();
        let scheduled_index = self.scheduled_index.clone();
        let ready_index = self.ready_index.clone();
        let default_dead_retention_ms = self.default_dead_retention_ms;
        let default_retry_limit = self.default_retry_limit;
        let default_backoff = self.default_backoff.clone();

        let id = id.to_string();
        let id_for_event = id.clone();

        let result = task::spawn_blocking(move || -> Result<_, StoreError> {
            // Extract fields from opts before the retry loop so they can
            // be cloned on each iteration.
            let kill = opts.kill;
            let retry_at = opts.retry_at;
            let error_message = opts.message;
            let error_type_opt = opts.error_type;
            let error_backtrace = opts.backtrace;

            // Retry loop: pre-read and pre-compute updates outside the tx,
            // then compare-and-write inside. Retries only if a concurrent job
            // metadata update modified the job between the pre-read and lock
            // acquisition.
            let job_key = make_job_key(&id);
            let job = loop {
                // ---- outside tx ----
                let pre_bytes = match ks.data.get(&job_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                };

                let mut job: Job = rmp_serde::from_slice(&pre_bytes)?;

                if job.status != JobStatus::InFlight as u8 {
                    return Ok(None);
                }

                job.attempts += 1;
                job.failed_at = Some(now);

                let error_key = make_error_key(&id, job.attempts);

                let error_record = ErrorRecord {
                    message: error_message.clone(),
                    error_type: error_type_opt.clone(),
                    backtrace: error_backtrace.clone(),
                    dequeued_at: job.dequeued_at.unwrap_or(0),
                    failed_at: now,
                    attempt: job.attempts,
                };

                let error_bytes = rmp_serde::to_vec_named(&error_record)?;

                let retry_limit = job.retry_limit.unwrap_or(default_retry_limit);

                if kill {
                    job.status = JobStatus::Dead.into();
                } else if let Some(ra) = retry_at {
                    job.status = JobStatus::Scheduled.into();
                    job.ready_at = ra;
                } else if job.attempts > retry_limit {
                    job.status = JobStatus::Dead.into();
                } else {
                    let backoff = job.backoff.as_ref().unwrap_or(&default_backoff);
                    let delay_ms = compute_backoff(job.attempts, backoff);
                    job.status = JobStatus::Scheduled.into();
                    job.ready_at = now + delay_ms;
                };

                let old_status_key = make_status_key(JobStatus::InFlight, &id);

                if job.status == JobStatus::Scheduled as u8 {
                    let new_status_key = make_status_key(JobStatus::Scheduled, &id);
                    let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                    // ---- inside tx (writes only, plus one compare) ----
                    let mut tx = ks.write_tx();

                    let prev =
                        tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                    if prev.as_deref() != Some(&*pre_bytes) {
                        drop(tx);
                        continue;
                    }

                    tx.insert(&ks.data, &error_key, &error_bytes);
                    tx.remove(&ks.index, &old_status_key);
                    tx.insert(&ks.index, &new_status_key, b"");

                    // If unique_while == Queued, restore the unique index now
                    // that the job is re-entering the queue. Only insert if no
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

                    ks.commit(tx, ks.default_commit_mode)?;

                    scheduled_index.insert(job.ready_at, id.clone());

                    break job;
                }

                // Dead: check retention to decide sync delete vs deferred.
                let retention_ms = job
                    .retention
                    .as_ref()
                    .and_then(|r| r.dead_ms)
                    .unwrap_or(default_dead_retention_ms);

                if retention_ms == 0 {
                    // Zero retention: prepare deletion keys outside tx.
                    let del = prepare_job_deletion(&job, JobStatus::InFlight, &ks);

                    // ---- inside tx ----
                    let mut tx = ks.write_tx();
                    let prev = tx.take(&ks.data, &job_key)?;
                    if prev.as_deref() != Some(&*pre_bytes) {
                        drop(tx);
                        continue;
                    }
                    apply_job_deletion(&mut tx, &del, &ks);
                    job.status = JobStatus::Dead.into();
                    ks.commit(tx, ks.default_commit_mode)?;

                    ready_index.remove(&job.queue, job.priority, &id);

                    break job;
                }

                // Non-zero retention: defer deletion to the reaper.
                let purge_at = now + retention_ms;
                let new_status_key = make_status_key(JobStatus::Dead, &id);
                let purge_key = make_purge_key(purge_at, &id);

                job.purge_at = Some(purge_at);
                let updated_slice: Slice = rmp_serde::to_vec_named(&job)?.into();

                // ---- inside tx ----
                let mut tx = ks.write_tx();

                let prev = tx.fetch_update(&ks.data, &job_key, |_| Some(updated_slice.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue;
                }

                tx.insert(&ks.data, &error_key, &error_bytes);
                tx.remove(&ks.index, &old_status_key);
                tx.insert(&ks.index, &new_status_key, b"");
                tx.insert(&ks.index, &purge_key, b"");

                // Remove unique index for Queued or Active scope on death,
                // but only if it still belongs to this job.
                if let Some(ref uc) = job.unique {
                    let scope = uc.unique_while();
                    if scope == UniqueWhile::Queued || scope == UniqueWhile::Active {
                        let job_id = id.as_bytes();
                        tx.fetch_update(&ks.index, &make_unique_key(&uc.key), |v| match v {
                            Some(v) if v.as_ref() == job_id => None,
                            other => other.cloned(),
                        })?;
                    }
                }

                ks.commit(tx, ks.default_commit_mode)?;

                ready_index.remove(&job.queue, job.priority, &id);

                break job;
            };

            Ok(Some(job))
        })
        .await??;

        // Emit events outside spawn_blocking due to &self.event_tx.
        if let Some(ref job) = result {
            self.in_flight_index
                .remove(job.dequeued_at.unwrap_or(0), &id_for_event);

            // Always emit JobFailed first so workers prune their
            // in-flight set before any downstream events arrive.
            //
            // The attempts count must match what the worker stored when
            // it took the job — that's the pre-increment value, since
            // record_failure bumps attempts by 1 inside the closure.
            let _ = self.event_tx.send(StoreEvent::JobFailed {
                id: id_for_event,
                attempts: job.attempts - 1,
            });

            if job.status == JobStatus::Scheduled as u8 {
                // Rescheduled for retry — tell the scheduler to wake up
                // at the new ready_at.
                let _ = self.event_tx.send(StoreEvent::JobScheduled {
                    id: job.id.clone(),
                    ready_at: job.ready_at,
                });
            }
        }

        Ok(result)
    }
}

/// Compute the backoff delay in milliseconds for a given attempt count.
///
/// Formula: `delay_ms = attempts^exponent + base_ms + rand(0..jitter_ms) * (attempts + 1)`
///
/// The jitter component scales linearly with the attempt count so that
/// later retries spread further apart, reducing collision likelihood when
/// many jobs fail at similar times.
fn compute_backoff(attempts: u32, backoff: &BackoffConfig) -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let base_delay = (attempts as f32).powf(backoff.exponent) + backoff.base_ms as f32;

    // Cheap random value using the same approach as rand_id() in http.rs.
    let rand_frac = (RandomState::new().build_hasher().finish() as f64) / (u64::MAX as f64); // 0.0..1.0

    let jitter = rand_frac * backoff.jitter_ms as f64 * (attempts as f64 + 1.0);

    (base_delay as f64 + jitter) as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::keys::{error_keys, make_error_key, make_payload_key};
    use super::super::options::{EnqueueOptions, ListJobsOptions};
    use super::super::store::StoreEvent;
    use super::super::test_support::{
        enqueue_and_take, test_failure_opts, test_store, test_store_with_retry_limit,
    };
    use super::super::types::{BackoffConfig, ErrorRecord, JobStatus};
    use super::compute_backoff;
    use crate::time::now_millis;

    // --- compute_backoff tests ---

    #[test]
    fn compute_backoff_zero_jitter_is_deterministic() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0,
        };

        // attempts=1: 1^2 + 100 = 101
        assert_eq!(compute_backoff(1, &backoff), 101);
        // attempts=2: 2^2 + 100 = 104
        assert_eq!(compute_backoff(2, &backoff), 104);
        // attempts=3: 3^2 + 100 = 109
        assert_eq!(compute_backoff(3, &backoff), 109);
    }

    #[test]
    fn compute_backoff_with_jitter_stays_in_range() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 50,
        };

        // Run many times to exercise randomness.
        for attempts in 1..=10 {
            let delay = compute_backoff(attempts, &backoff);
            let base = (attempts as f64).powf(2.0) + 100.0;
            let max_jitter = 50.0 * (attempts as f64 + 1.0);
            assert!(
                delay >= base as u64,
                "delay {delay} below base {base} for attempt {attempts}"
            );
            assert!(
                delay <= (base + max_jitter) as u64,
                "delay {delay} above max {} for attempt {attempts}",
                base + max_jitter
            );
        }
    }

    #[test]
    fn compute_backoff_zero_attempts() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0,
        };

        // attempts=0: 0^2 + 100 = 100
        assert_eq!(compute_backoff(0, &backoff), 100);
    }

    // --- record_failure tests ---

    #[tokio::test]
    async fn record_failure_retries_under_limit() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.attempts, 1);
        assert!(result.failed_at.is_some());
        // ready_at should be in the future (backoff from the store's default config).
        assert!(result.ready_at > 0);
    }

    #[tokio::test]
    async fn record_failure_kills_when_retries_exhausted() {
        // retry_limit=0 means the first failure kills.
        let store = test_store_with_retry_limit(0);
        let job = enqueue_and_take(&store).await;

        let opts = test_failure_opts();

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);
        assert_eq!(result.attempts, 1);

        // Dead job is still visible during its retention period.
        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn record_failure_kills_when_kill_flag_set() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.kill = true;

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);

        // Dead job is still visible during its retention period.
        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn record_failure_kill_overrides_remaining_retries() {
        // retry_limit=100, but kill=true should still kill.
        let store = test_store_with_retry_limit(100);
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.kill = true;

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Dead as u8);
    }

    #[tokio::test]
    async fn record_failure_retry_at_overrides_backoff() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.retry_at = Some(999_999);

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, 999_999);
    }

    #[tokio::test]
    async fn record_failure_retry_at_overrides_exhausted_limit() {
        // retry_limit=0, but retry_at should force a retry anyway.
        let store = test_store_with_retry_limit(0);
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.retry_at = Some(123_456);

        let result = store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.status, JobStatus::Scheduled as u8);
        assert_eq!(result.ready_at, 123_456);
    }

    #[tokio::test]
    async fn record_failure_returns_none_for_unknown_id() {
        let store = test_store();
        let result = store
            .record_failure(now_millis(), "nonexistent", test_failure_opts())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn record_failure_returns_none_for_ready_job() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        // Don't take it — job is still Ready.
        let job = store
            .list_jobs(ListJobsOptions::new())
            .await
            .unwrap()
            .jobs
            .pop()
            .unwrap();

        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn record_failure_increments_attempts_across_retries() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // First failure: attempts goes from 0 -> 1.
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        // The job is now Scheduled. Promote it and take it again.
        let scheduled = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Second failure: attempts goes from 1 -> 2.
        let result = store
            .record_failure(now_millis(), &retaken.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.attempts, 2);
    }

    #[tokio::test]
    async fn record_failure_writes_error_record() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut opts = test_failure_opts();
        opts.message = "connection timeout".into();
        opts.error_type = Some("TimeoutError".into());
        opts.backtrace = Some("at line 42".into());

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Read the error record from the data keyspace.
        let key = make_error_key(&job.id, 1);
        let raw = store.ks.data.inner().get(&key).unwrap().unwrap();
        let err: ErrorRecord = rmp_serde::from_slice(&raw).unwrap();

        assert_eq!(err.message, "connection timeout");
        assert_eq!(err.error_type.as_deref(), Some("TimeoutError"));
        assert_eq!(err.backtrace.as_deref(), Some("at line 42"));
        assert!(err.failed_at > 0);
    }

    #[tokio::test]
    async fn record_failure_dead_retains_error_records_for_reaper() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Kill immediately — error records are now retained until the
        // reaper purges the job.
        let mut opts = test_failure_opts();
        opts.kill = true;

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Error records should still exist (cleanup deferred to reaper).
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(!keys.is_empty());

        // purge_job should clean them up.
        store.purge_job(&job.id).await.unwrap();
        let keys: Vec<_> = error_keys(&store.ks, &job.id).collect();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn record_failure_dead_retains_payload_for_reaper() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Verify payload exists before kill.
        let payload_key = make_payload_key(&job.id);
        assert!(store.ks.data.inner().get(&payload_key).unwrap().is_some());

        let mut opts = test_failure_opts();
        opts.kill = true;

        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap()
            .unwrap();

        // Payload should still exist (cleanup deferred to reaper).
        assert!(store.ks.data.inner().get(&payload_key).unwrap().is_some());

        // purge_job should clean it up.
        store.purge_job(&job.id).await.unwrap();
        assert!(store.ks.data.inner().get(&payload_key).unwrap().is_none());
    }

    #[tokio::test]
    async fn record_failure_retry_broadcasts_job_failed_then_scheduled() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut rx = store.subscribe();
        store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap();

        // First event: JobFailed so workers prune their in-flight set.
        match rx.recv().await.unwrap() {
            StoreEvent::JobFailed { id, attempts } => {
                assert_eq!(id, job.id);
                // attempts=0: the pre-increment value that matches what
                // the worker stored when it took the job.
                assert_eq!(attempts, 0);
            }
            other => panic!("expected JobFailed, got {other:?}"),
        }

        // Second event: JobScheduled so the scheduler wakes up.
        match rx.recv().await.unwrap() {
            StoreEvent::JobScheduled { ready_at, .. } => assert!(ready_at > 0),
            other => panic!("expected JobScheduled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_failure_dead_broadcasts_job_failed() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let mut rx = store.subscribe();
        let mut opts = test_failure_opts();
        opts.kill = true;
        store
            .record_failure(now_millis(), &job.id, opts)
            .await
            .unwrap();

        // Even for killed jobs, the event is JobFailed — workers don't
        // need to distinguish retry from kill; they just prune in-flight.
        match rx.recv().await.unwrap() {
            StoreEvent::JobFailed { id, attempts } => {
                assert_eq!(id, job.id);
                assert_eq!(attempts, 0);
            }
            other => panic!("expected JobFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_failure_does_not_hydrate_payload() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();

        // Payload should be None (not hydrated), not the stored value.
        assert_eq!(result.payload, None);
    }

    #[tokio::test]
    async fn record_failure_uses_job_retry_limit_over_default() {
        let store = test_store();

        // Enqueue with per-job retry_limit=1.
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")).retry_limit(1),
            )
            .await
            .unwrap()
            .into_job();
        let job = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // First failure (attempts 0->1): should retry (1 <= 1).
        let result = store
            .record_failure(now_millis(), &job.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, JobStatus::Scheduled as u8);

        // Promote and take again.
        let scheduled = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Second failure (attempts 1->2): should die (2 > 1).
        let result = store
            .record_failure(now_millis(), &retaken.id, test_failure_opts())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, JobStatus::Dead as u8);
    }
}
