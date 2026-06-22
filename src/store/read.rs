// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Read-only operations on the store: single-job lookup, paginated job
//! and error listings, queue enumeration, and direct scans of the
//! in-memory ready/scheduled indexes.
//!
//! The scan plumbing (`JobStream`, `IdStream`, `PayloadFilteredIter`,
//! `build_id_stream`) still lives in `store.rs` for now since
//! `delete::delete_jobs` and `patch::patch_jobs` also use it; it can
//! move alongside these read methods in a subsequent pass.

use std::ops::Bound;
use std::sync::atomic::Ordering;

use fjall::Readable;
use tokio::task;

use super::options::{ListErrorsOptions, ListJobsOptions};
use super::results::{ListErrorsPage, ListJobsPage};
use super::store::{
    IndexKind, JobStream, PayloadFilteredIter, RecordKind, Store, build_id_stream, make_error_key,
    make_job_key, make_payload_key,
};
use super::types::{ErrorRecord, Job, ScanDirection, StoreError};

impl Store {
    /// Look up a job by ID.
    ///
    /// Returns `None` if the job does not exist, or if its `purge_at`
    /// timestamp has passed (logically invisible before the reaper
    /// physically deletes it).
    pub async fn get_job(&self, now: u64, id: &str) -> Result<Option<Job>, StoreError> {
        let ks = self.ks.clone();
        let id = id.to_string();

        task::spawn_blocking(move || {
            let job_key = make_job_key(&id);
            let Some(bytes) = ks.data.get(&job_key)? else {
                return Ok(None);
            };
            let mut job: Job = rmp_serde::from_slice(&bytes)?;

            // Filter out expired jobs (logically invisible).
            if job.purge_at.is_some_and(|p| p <= now) {
                return Ok(None);
            }

            // Hydrate the payload from the data keyspace.
            let payload_key = make_payload_key(&id);
            if let Some(payload_bytes) = ks.data.get(&payload_key)? {
                job.payload = Some(rmp_serde::from_slice(&payload_bytes)?);
            }

            Ok(Some(job))
        })
        .await?
    }

    /// List jobs with cursor-based pagination.
    ///
    /// Returns a page of jobs along with options for fetching the next and
    /// previous pages. Internally over-selects by one to determine whether
    /// more results exist in the scan direction.
    pub async fn list_jobs(&self, opts: ListJobsOptions) -> Result<ListJobsPage, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();
            let fetch = opts.limit.saturating_add(1);

            let id_stream_opt = build_id_stream(
                &snapshot,
                &ks,
                &opts.ids,
                &opts.statuses,
                &opts.queues,
                &opts.types,
                &opts.from,
                opts.direction,
            );

            let job_stream = match id_stream_opt {
                Some((ids, source, _has_id_filter)) => {
                    JobStream::by_id(ids, &snapshot, &ks.data, opts.now, true, source)
                }
                None => {
                    JobStream::full_scan(&snapshot, &ks, &opts.from, opts.direction, opts.now, true)
                }
            };

            let mut rows: Vec<Job> = if let Some(filter) = opts.filter.clone() {
                PayloadFilteredIter {
                    inner: job_stream,
                    filter,
                }
                .take(fetch)
                .collect::<Result<Vec<_>, _>>()?
            } else {
                job_stream.take(fetch).collect::<Result<Vec<_>, _>>()?
            };

            // If we got more rows than the requested limit, we know there's a
            // next page, and we can just discard that extra row now.
            let has_more = rows.len() > opts.limit;
            if has_more {
                rows.truncate(opts.limit);
            }

            // Build next/prev options, preserving all filters.
            let make_opts = |cursor: &str, direction: ScanDirection| {
                ListJobsOptions::new()
                    .from(cursor.to_string())
                    .direction(direction)
                    .limit(opts.limit)
                    .ids(opts.ids.clone())
                    .statuses(opts.statuses.clone())
                    .queues(opts.queues.clone())
                    .types(opts.types.clone())
                    .now(opts.now)
                    .filter(opts.filter.clone())
            };

            // Next page: exists if over-select found an extra row.
            let next = if has_more {
                let last_id = &rows.last().unwrap().id;
                Some(make_opts(last_id, opts.direction))
            } else {
                None
            };

            // Previous page: exists if a cursor was provided (we came from
            // somewhere).
            let prev = if opts.from.is_some() && !rows.is_empty() {
                let first_id = &rows.first().unwrap().id;
                Some(make_opts(first_id, opts.direction.reverse()))
            } else {
                None
            };

            Ok(ListJobsPage {
                jobs: rows,
                next,
                prev,
            })
        })
        .await?
    }

    /// Count jobs matching the given filters.
    ///
    /// Uses the same index scan and filter machinery as `list_jobs` but
    /// only counts matching IDs instead of hydrating full job records.
    /// Payload hydration is skipped unless a payload filter is specified.
    pub async fn count_jobs(&self, opts: ListJobsOptions) -> Result<usize, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            let snapshot = ks.db.read_tx();

            let id_stream_opt = build_id_stream(
                &snapshot,
                &ks,
                &opts.ids,
                &opts.statuses,
                &opts.queues,
                &opts.types,
                &None,              // no cursor for count
                ScanDirection::Asc, // direction doesn't matter for count
            );

            let needs_payload = opts.filter.is_some();

            let mut job_stream = match id_stream_opt {
                Some((ids, source, _has_id_filter)) => {
                    JobStream::by_id(ids, &snapshot, &ks.data, opts.now, needs_payload, source)
                }
                None => JobStream::full_scan(
                    &snapshot,
                    &ks,
                    &None,
                    ScanDirection::Asc,
                    opts.now,
                    needs_payload,
                ),
            };

            let count = if let Some(filter) = opts.filter.clone() {
                PayloadFilteredIter {
                    inner: job_stream,
                    filter,
                }
                .try_fold(0, |acc, r| r.map(|_| acc + 1))?
            } else {
                job_stream.try_fold(0, |acc, r| r.map(|_| acc + 1))?
            };

            Ok(count)
        })
        .await?
    }

    /// Return all distinct queue names present in the store.
    ///
    /// Uses a seek-skip algorithm on the on-disk queue index to enumerate
    /// queue names in O(distinct queues), not O(total jobs). Each step is
    /// a single LSM tree seek via `range()`.
    pub async fn list_queues(&self) -> Result<Vec<String>, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();

            let end: Vec<u8> = vec![IndexKind::Queue as u8 + 1, 0];
            let mut start: Vec<u8> = vec![IndexKind::Queue as u8, 0];
            let mut queues = Vec::new();

            loop {
                let mut range = snapshot.range::<Vec<u8>, _>(
                    ks.index.as_ref(),
                    (Bound::Included(start.clone()), Bound::Excluded(end.clone())),
                );

                let entry = match range.next() {
                    Some(entry) => entry,
                    None => break,
                };

                let (key, _) = entry.into_inner()?;

                // Key layout: Q\0{queue_name}\0{job_id} — extract queue_name.
                let name_start = 2; // skip Q\0
                let name_end = key[name_start..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|p| name_start + p)
                    .unwrap_or(key.len());
                let queue_name = std::str::from_utf8(&key[name_start..name_end])
                    .map_err(|e| {
                        StoreError::Corruption(format!("queue name is not valid UTF-8: {e}"))
                    })?
                    .to_string();
                queues.push(queue_name.clone());

                // Advance past all entries for this queue: Q\0{queue_name}\x01
                start.truncate(2);
                start.extend_from_slice(queue_name.as_bytes());
                start.push(1); // one byte past \0 separator
            }

            Ok(queues)
        })
        .await?
    }

    /// List ready jobs in priority order from the in-memory index.
    ///
    /// Iterates the `ReadyIndex` SkipMap and hydrates job metadata from
    /// the `jobs` keyspace. Skips jobs whose metadata is missing (claimed
    /// between index scan and disk read — next snapshot corrects). Does
    /// NOT hydrate payloads.
    pub async fn list_ready_jobs(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let ready_index = self.ready_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let mut jobs = Vec::with_capacity(limit);
            for (_priority, job_id) in ready_index.iter().skip(offset) {
                if jobs.len() >= limit {
                    break;
                }
                let job_key = make_job_key(&job_id);
                if let Some(bytes) = ks.data.get(&job_key)? {
                    let job: Job = rmp_serde::from_slice(&bytes)?;
                    jobs.push(job);
                }
            }
            Ok(jobs)
        })
        .await?
    }

    /// Scan the in-memory ReadyIndex and return up to `limit` priority keys.
    ///
    /// Returns `Vec<(priority, job_id)>` in priority order — zero disk I/O.
    /// Used by the admin event handler to diff capped ready windows.
    pub async fn scan_ready_ids(&self, offset: usize, limit: usize) -> Vec<(u16, String)> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Vec::new();
        }

        let ready_index = self.ready_index.clone();

        task::spawn_blocking(move || ready_index.iter().skip(offset).take(limit).collect())
            .await
            .unwrap_or_default()
    }

    /// List scheduled jobs in chronological order from the in-memory index.
    ///
    /// Iterates the `ScheduledIndex` SkipSet and hydrates job metadata from
    /// the `jobs` keyspace. Skips jobs whose metadata is missing. Does NOT
    /// hydrate payloads.
    pub async fn list_scheduled_jobs(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let scheduled_index = self.scheduled_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let mut jobs = Vec::with_capacity(limit);
            for (_ready_at, job_id) in scheduled_index.iter().skip(offset) {
                if jobs.len() >= limit {
                    break;
                }
                let job_key = make_job_key(&job_id);
                if let Some(bytes) = ks.data.get(&job_key)? {
                    let job: Job = rmp_serde::from_slice(&bytes)?;
                    jobs.push(job);
                }
            }
            Ok(jobs)
        })
        .await?
    }

    /// Scan the in-memory ScheduledIndex and return up to `limit` keys.
    ///
    /// Returns `Vec<(ready_at, job_id)>` in chronological order — zero disk I/O.
    /// Used by the admin event handler to diff capped scheduled windows.
    pub async fn scan_scheduled_ids(&self, offset: usize, limit: usize) -> Vec<(u64, String)> {
        if !self.index_ready.load(Ordering::Acquire) {
            return Vec::new();
        }

        let scheduled_index = self.scheduled_index.clone();

        task::spawn_blocking(move || scheduled_index.iter().skip(offset).take(limit).collect())
            .await
            .unwrap_or_default()
    }

    /// Get a single error record by job ID and attempt number.
    ///
    /// Returns `None` if the job or attempt doesn't exist.
    pub async fn get_error(
        &self,
        job_id: &str,
        attempt: u32,
    ) -> Result<Option<ErrorRecord>, StoreError> {
        let ks = self.ks.clone();
        let job_id = job_id.to_string();

        task::spawn_blocking(move || {
            let key = make_error_key(&job_id, attempt);
            match ks.data.get(&key)? {
                Some(bytes) => {
                    let mut record: ErrorRecord = rmp_serde::from_slice(&bytes)?;
                    record.attempt = attempt;
                    Ok(Some(record))
                }
                None => Ok(None),
            }
        })
        .await
        .unwrap()
    }

    /// List error records for a job with cursor-based pagination.
    ///
    /// Returns a page of `ErrorRecord`s plus `next` / `prev` cursors for
    /// follow-up requests. The cursor is the attempt number (u32), which is
    /// the natural sort key within a job's error records.
    pub async fn list_errors(&self, opts: ListErrorsOptions) -> Result<ListErrorsPage, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            // We take 1 more record than requested so that we know if there's
            // a next page or not.
            let fetch = opts.limit.saturating_add(1);

            // Build the prefix for this job: `E\0{job_id}\0`
            let mut prefix = Vec::with_capacity(opts.job_id.len() + 3);
            prefix.push(RecordKind::Error as u8);
            prefix.push(0);
            prefix.extend_from_slice(opts.job_id.as_bytes());
            prefix.push(0);

            // End sentinel: `E\0{job_id}\x01` — one byte past the null separator,
            // so the range covers all `E\0{job_id}\0{...}` keys.
            let mut end_sentinel = Vec::with_capacity(opts.job_id.len() + 3);
            end_sentinel.push(RecordKind::Error as u8);
            end_sentinel.push(0);
            end_sentinel.extend_from_slice(opts.job_id.as_bytes());
            end_sentinel.push(1);

            let start = match opts.from {
                Some(cursor) => Bound::Excluded(make_error_key(&opts.job_id, cursor)),
                None => Bound::Included(prefix),
            };
            let end = Bound::Excluded(end_sentinel);

            let mut rows = Vec::with_capacity(fetch.min(1024));

            match opts.direction {
                ScanDirection::Asc => {
                    for entry in ks
                        .data
                        .inner()
                        .range::<Vec<u8>, _>((start, end))
                        .take(fetch)
                    {
                        let (key, value) = entry.into_inner()?;
                        let mut record: ErrorRecord = rmp_serde::from_slice(&value)?;
                        // Extract the attempt number from the last 4 bytes of the key.
                        let attempt_bytes: [u8; 4] =
                            key[key.len() - 4..].try_into().map_err(|_| {
                                StoreError::Corruption(
                                    "error key must end with 4-byte attempt".into(),
                                )
                            })?;
                        record.attempt = u32::from_be_bytes(attempt_bytes);
                        rows.push(record);
                    }
                }
                ScanDirection::Desc => {
                    for entry in ks
                        .data
                        .inner()
                        .range::<Vec<u8>, _>((start, end))
                        .rev()
                        .take(fetch)
                    {
                        let (key, value) = entry.into_inner()?;
                        let mut record: ErrorRecord = rmp_serde::from_slice(&value)?;
                        let attempt_bytes: [u8; 4] =
                            key[key.len() - 4..].try_into().map_err(|_| {
                                StoreError::Corruption(
                                    "error key must end with 4-byte attempt".into(),
                                )
                            })?;
                        record.attempt = u32::from_be_bytes(attempt_bytes);
                        rows.push(record);
                    }
                }
            }

            // If we got more rows than the requested limit, there's a next page.
            let has_more = rows.len() > opts.limit;
            if has_more {
                rows.truncate(opts.limit);
            }

            // Build next/prev options, preserving direction and limit.
            let make_opts = |cursor: u32, direction: ScanDirection| {
                ListErrorsOptions::new(&opts.job_id)
                    .from(cursor)
                    .direction(direction)
                    .limit(opts.limit)
            };

            // Next page: exists if over-select found an extra row.
            let next = if has_more {
                let last_attempt = rows.last().unwrap().attempt;
                Some(make_opts(last_attempt, opts.direction))
            } else {
                None
            };

            // Previous page: exists if a cursor was provided (we came from
            // somewhere).
            let prev = if opts.from.is_some() && !rows.is_empty() {
                let first_attempt = rows.first().unwrap().attempt;
                Some(make_opts(first_attempt, opts.direction.reverse()))
            } else {
                None
            };

            Ok(ListErrorsPage {
                errors: rows,
                next,
                prev,
            })
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::filter::PayloadFilter;

    use super::super::options::{EnqueueOptions, ListErrorsOptions, ListJobsOptions};
    use super::super::store::Store;
    use super::super::test_support::{enqueue_and_take, test_failure_opts, test_store};
    use super::super::types::{Job, JobStatus, ScanDirection};
    use crate::time::now_millis;

    #[tokio::test]
    async fn get_job_returns_enqueued_job() {
        let store = test_store();
        let enqueued = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("hello")),
            )
            .await
            .unwrap()
            .into_job();

        let job = store
            .get_job(now_millis(), &enqueued.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, enqueued.id);
        assert_eq!(job.queue, "default");
        assert_eq!(job.status, u8::from(JobStatus::Ready));
        assert_eq!(job.payload, Some(serde_json::json!("hello")));
    }

    #[tokio::test]
    async fn get_job_returns_none_for_unknown_id() {
        let store = test_store();
        assert!(
            store
                .get_job(now_millis(), "nonexistent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_job_returns_none_after_completion() {
        let store = test_store();
        let enqueued = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .mark_completed(now_millis(), &enqueued.id)
            .await
            .unwrap();

        assert!(
            store
                .get_job(now_millis(), &enqueued.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_asc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_returns_all_in_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().direction(ScanDirection::Desc))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, c.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_respects_limit() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
    }

    #[tokio::test]
    async fn list_jobs_cursor_is_exclusive_asc() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().from(&a.id))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_cursor_is_exclusive_desc() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .from(&c.id)
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_empty_store() {
        let store = test_store();
        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert!(page.jobs.is_empty());
        assert!(page.next.is_none());
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_jobs_cursor_past_end() {
        let store = test_store();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().from(&c.id))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_includes_in_flight_jobs() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::InFlight));
        assert_eq!(page.jobs[1].status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn list_jobs_excludes_completed_jobs() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().now(now_millis()))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_next_present_when_more_results() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);

        let next = page.next.unwrap();
        assert_eq!(next.from.as_deref(), Some(b.id.as_str()));
        assert_eq!(next.direction, ScanDirection::Asc);
        assert_eq!(next.limit, 2);
    }

    #[tokio::test]
    async fn list_jobs_next_absent_on_last_page() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_prev_absent_on_first_page() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store.list_jobs(ListJobsOptions::new()).await.unwrap();
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_jobs_prev_present_when_cursor_provided() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().from(&a.id).limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);

        let prev = page.prev.unwrap();
        assert_eq!(prev.from.as_deref(), Some(b.id.as_str()));
        assert_eq!(prev.direction, ScanDirection::Desc);
        assert_eq!(prev.limit, 2);
    }

    #[tokio::test]
    async fn list_jobs_next_page_returns_remaining() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // First page.
        let page1 = store
            .list_jobs(ListJobsOptions::new().limit(2))
            .await
            .unwrap();
        assert_eq!(page1.jobs.len(), 2);

        // Follow next to get second page.
        let page2 = store.list_jobs(page1.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    // --- list_jobs status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_ready_status() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        // Take the first job so it becomes InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Ready])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn list_jobs_filters_by_in_flight_status() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[0].status, u8::from(JobStatus::InFlight));
    }

    #[tokio::test]
    async fn list_jobs_status_filter_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_status_filter_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        // Follow next page — should preserve the status filter.
        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_status_filter_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_status_filter_reflects_requeue() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Now in-flight.
        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        // Requeue — should move back to ready.
        store.requeue(&a.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::Ready])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_status_filter_reflects_completion() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(ListJobsOptions::new().statuses(HashSet::from([JobStatus::InFlight])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    // --- list_jobs queue filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_single_queue() {
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
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().queues(HashSet::from(["emails".into()])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_queues() {
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

        let page = store
            .list_jobs(
                ListJobsOptions::new().queues(HashSet::from(["emails".into(), "webhooks".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_merges_in_order() {
        let store = test_store();
        let a = store
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
                EnqueueOptions::new("test", "webhooks", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();
        let d = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("d")),
            )
            .await
            .unwrap()
            .into_job();

        // Ascending: interleaved by job ID (time order).
        let page = store
            .list_jobs(
                ListJobsOptions::new().queues(HashSet::from(["emails".into(), "webhooks".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 4);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
        assert_eq!(page.jobs[3].id, d.id);

        // Descending: reverse order.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into(), "webhooks".into()]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 4);
        assert_eq!(page.jobs[0].id, d.id);
        assert_eq!(page.jobs[1].id, c.id);
        assert_eq!(page.jobs[2].id, b.id);
        assert_eq!(page.jobs[3].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().queues(HashSet::from(["reports".into()])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_with_pagination() {
        let store = test_store();
        let a = store
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
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let queues = HashSet::from(["emails".into(), "reports".into()]);

        let page = store
            .list_jobs(ListJobsOptions::new().queues(queues).limit(2))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        // Follow next page — should preserve the queue filter.
        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_reflects_completion() {
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
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .now(now_millis()),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    // --- list_jobs multi-status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_statuses() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a and b so they become InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Filter by Ready + InFlight — should get all 3.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_multi_status_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    // --- list_jobs combined queue + status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_queue_and_status() {
        let store = test_store();
        let a = store
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
        let _c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a so it becomes InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Only ready jobs in the emails queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, _c.id);

        // InFlight jobs in emails queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);

        // Ready jobs in reports queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["reports".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_multiple_queues_and_statuses() {
        let store = test_store();
        let a = store
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
                EnqueueOptions::new("test", "webhooks", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();
        let d = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("d")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a and b so they become InFlight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready + InFlight in emails + webhooks (excludes reports).
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into(), "webhooks".into()]))
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 3);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert_eq!(page.jobs[2].id, d.id);

        // Verify reports/c is excluded.
        assert!(page.jobs.iter().all(|j| j.id != c.id));
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_desc_order() {
        let store = test_store();
        let a = store
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
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_with_pagination() {
        let store = test_store();
        let a = store
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
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        // Follow next — should preserve both queue and status filters.
        let next = page.next.unwrap();
        assert!(!next.queues.is_empty());
        assert!(!next.statuses.is_empty());

        let page2 = store.list_jobs(next).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_reflects_requeue() {
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
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        // InFlight in emails.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);

        // Requeue — should move back to ready.
        store.requeue(&a.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert!(page.jobs.is_empty());

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_reflects_completion() {
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
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);

        // InFlight should be empty after completion.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .queues(HashSet::from(["emails".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_multi_status_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap(); // a -> in-flight

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .statuses(HashSet::from([JobStatus::Ready, JobStatus::InFlight]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    // --- list_jobs type filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_single_type() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().types(HashSet::from(["send_email".into()])))
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_types() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_sms", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into(), "send_sms".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, c.id);
    }

    #[tokio::test]
    async fn list_jobs_type_filter_empty_when_none_match() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(ListJobsOptions::new().types(HashSet::from(["no_such_type".into()])))
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_jobs_type_filter_reflects_completion() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let taken = store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        store.mark_completed(now_millis(), &taken.id).await.unwrap();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .now(now_millis()),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, b.id);
    }

    #[tokio::test]
    async fn list_jobs_type_filter_with_pagination() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .limit(2),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, a.id);
        assert_eq!(page.jobs[1].id, b.id);
        assert!(page.next.is_some());

        let page2 = store.list_jobs(page.next.unwrap()).await.unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].id, c.id);
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_jobs_type_filter_desc_order() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .direction(ScanDirection::Desc),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 2);
        assert_eq!(page.jobs[0].id, b.id);
        assert_eq!(page.jobs[1].id, a.id);
    }

    // --- list_jobs combined type + queue + status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_queue() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "low", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .queues(HashSet::from(["high".into()])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_status() {
        let store = test_store();
        let a = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a so it becomes in-flight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // InFlight send_email jobs only.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .statuses(HashSet::from([JobStatus::InFlight])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, a.id);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_queue_and_status() {
        let store = test_store();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("b")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("send_email", "low", serde_json::json!("c")),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("d")),
            )
            .await
            .unwrap()
            .into_job();

        // Take a so it becomes in-flight.
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready send_email in high queue.
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .types(HashSet::from(["send_email".into()]))
                    .queues(HashSet::from(["high".into()]))
                    .statuses(HashSet::from([JobStatus::Ready])),
            )
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        // b is the only ready send_email in the high queue (a is in-flight).
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!("b")));
    }

    // --- list_errors tests ---

    /// Helper: fail a in-flight job, promote+retake it, and return the retaken job.
    ///
    /// This cycles one failure through the store so that `list_errors` has
    /// something to read. The `error_msg` lets callers tag each failure for
    /// assertion clarity.
    async fn fail_and_retake(store: &Store, job_id: &str, error_msg: &str) -> Job {
        let mut opts = test_failure_opts();
        opts.message = error_msg.into();
        store
            .record_failure(now_millis(), job_id, opts)
            .await
            .unwrap()
            .unwrap();

        // Promote from Scheduled -> Ready and take again.
        let scheduled = store.get_job(now_millis(), job_id).await.unwrap().unwrap();
        store.promote_scheduled(&scheduled).await.unwrap();
        store
            .take_next_job(now_millis(), &HashSet::new())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn list_errors_returns_empty_for_job_without_errors() {
        let store = test_store();
        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("payload")),
            )
            .await
            .unwrap()
            .into_job();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id))
            .await
            .unwrap();

        assert!(page.errors.is_empty());
        assert!(page.next.is_none());
        assert!(page.prev.is_none());
    }

    #[tokio::test]
    async fn list_errors_returns_errors_in_asc_order() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Fail twice to produce two error records (attempts 1, 2).
        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let mut opts = test_failure_opts();
        opts.message = "error two".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 2);
        assert_eq!(page.errors[0].message, "error one");
        assert_eq!(page.errors[1].message, "error two");
    }

    #[tokio::test]
    async fn list_errors_returns_errors_in_desc_order() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let mut opts = test_failure_opts();
        opts.message = "error two".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id).direction(ScanDirection::Desc))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 2);
        // Desc: newest first.
        assert_eq!(page.errors[0].message, "error two");
        assert_eq!(page.errors[1].message, "error one");
    }

    #[tokio::test]
    async fn list_errors_paginates_with_limit() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        // Fail three times.
        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let retaken = fail_and_retake(&store, &retaken.id, "error two").await;
        let mut opts = test_failure_opts();
        opts.message = "error three".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        // Page 1: limit=2, should see errors 1 and 2 with a next cursor.
        let page = store
            .list_errors(ListErrorsOptions::new(&job.id).limit(2))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 2);
        assert_eq!(page.errors[0].message, "error one");
        assert_eq!(page.errors[1].message, "error two");
        assert!(page.next.is_some());

        // Page 2: follow the next cursor.
        let page2 = store.list_errors(page.next.unwrap()).await.unwrap();

        assert_eq!(page2.errors.len(), 1);
        assert_eq!(page2.errors[0].message, "error three");
        assert!(page2.next.is_none());
    }

    #[tokio::test]
    async fn list_errors_cursor_is_exclusive() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let retaken = fail_and_retake(&store, &job.id, "error one").await;
        let mut opts = test_failure_opts();
        opts.message = "error two".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        // Start after attempt 1 — should only see attempt 2.
        let page = store
            .list_errors(ListErrorsOptions::new(&job.id).from(1))
            .await
            .unwrap();

        assert_eq!(page.errors.len(), 1);
        assert_eq!(page.errors[0].attempt, 2);
        assert_eq!(page.errors[0].message, "error two");
    }

    #[tokio::test]
    async fn list_errors_includes_attempt_number() {
        let store = test_store();
        let job = enqueue_and_take(&store).await;

        let retaken = fail_and_retake(&store, &job.id, "first").await;
        let mut opts = test_failure_opts();
        opts.message = "second".into();
        store
            .record_failure(now_millis(), &retaken.id, opts)
            .await
            .unwrap();

        let page = store
            .list_errors(ListErrorsOptions::new(&job.id))
            .await
            .unwrap();

        // Attempts are 1-based: first failure = 1, second = 2.
        assert_eq!(page.errors[0].attempt, 1);
        assert_eq!(page.errors[1].attempt, 2);
    }

    #[tokio::test]
    async fn list_ready_jobs_returns_priority_order() {
        let store = test_store();
        let now = now_millis();

        // Enqueue jobs at different priorities.
        let low = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let high = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(0),
            )
            .await
            .unwrap()
            .into_job();
        let mid = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)).priority(5),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store.list_ready_jobs(0, 10).await.unwrap();
        let ids: Vec<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
        assert_eq!(ids, vec![high.id, mid.id, low.id]);
    }

    #[tokio::test]
    async fn list_ready_jobs_respects_limit() {
        let store = test_store();
        let now = now_millis();

        for _ in 0..5 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(null)),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store.list_ready_jobs(0, 3).await.unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn list_ready_jobs_empty_store() {
        let store = test_store();
        let jobs = store.list_ready_jobs(0, 10).await.unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn list_ready_jobs_excludes_in_flight() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        // Take one job — it moves to InFlight.
        store.take_next_job(now, &HashSet::new()).await.unwrap();

        let jobs = store.list_ready_jobs(0, 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
    }

    // --- list_scheduled_jobs tests ---

    #[tokio::test]
    async fn list_scheduled_jobs_returns_chronological_order() {
        let store = test_store();
        let now = now_millis();

        // Enqueue jobs at different ready_at times.
        let late = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 30_000),
            )
            .await
            .unwrap()
            .into_job();
        let early = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 10_000),
            )
            .await
            .unwrap()
            .into_job();
        let mid = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 20_000),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        let ids: Vec<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
        assert_eq!(ids, vec![early.id, mid.id, late.id]);
    }

    #[tokio::test]
    async fn list_scheduled_jobs_respects_limit() {
        let store = test_store();
        let now = now_millis();

        for i in 0..5 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "default", serde_json::json!(null))
                        .ready_at(now + (i + 1) * 10_000),
                )
                .await
                .unwrap()
                .into_job();
        }

        let jobs = store.list_scheduled_jobs(0, 3).await.unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn list_scheduled_jobs_empty_store() {
        let store = test_store();
        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn list_scheduled_jobs_excludes_ready() {
        let store = test_store();
        let now = now_millis();

        // One ready job.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        // One scheduled job.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        let jobs = store.list_scheduled_jobs(0, 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
    }

    // --- count_jobs tests ---

    #[tokio::test]
    async fn count_jobs_empty_store() {
        let store = test_store();
        let count = store.count_jobs(ListJobsOptions::new()).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn count_jobs_returns_total() {
        let store = test_store();
        let now = now_millis();
        for i in 0..7 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(i)))
                .await
                .unwrap();
        }
        let count = store
            .count_jobs(ListJobsOptions::new().now(now))
            .await
            .unwrap();
        assert_eq!(count, 7);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_status() {
        let store = test_store();
        let now = now_millis();

        // Enqueue 3 ready jobs.
        for i in 0..3 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(i)))
                .await
                .unwrap();
        }

        // Take one (moves to in_flight).
        store
            .take_next_job(now, &std::collections::HashSet::new())
            .await
            .unwrap();

        let ready_count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .statuses([JobStatus::Ready].into()),
            )
            .await
            .unwrap();
        assert_eq!(ready_count, 2);

        let in_flight_count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .statuses([JobStatus::InFlight].into()),
            )
            .await
            .unwrap();
        assert_eq!(in_flight_count, 1);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_queue() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "emails", serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "emails", serde_json::json!(2)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "reports", serde_json::json!(3)),
            )
            .await
            .unwrap();

        let count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .queues(["emails".to_string()].into()),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_type() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "q", serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "q", serde_json::json!(2)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("generate_report", "q", serde_json::json!(3)),
            )
            .await
            .unwrap();

        let count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .types(["send_email".to_string()].into()),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn count_jobs_with_payload_filter() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"score": 10})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"score": 50})),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("t", "q", serde_json::json!({"score": 90})),
            )
            .await
            .unwrap();

        let filter = crate::filter::PayloadFilter::compile(".score > 20").unwrap();
        let count = store
            .count_jobs(ListJobsOptions::new().now(now).filter(Arc::new(filter)))
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn count_jobs_combined_filters() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "emails", serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "emails", serde_json::json!(2)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("send_email", "reports", serde_json::json!(3)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("generate_report", "emails", serde_json::json!(4)),
            )
            .await
            .unwrap();

        let count = store
            .count_jobs(
                ListJobsOptions::new()
                    .now(now)
                    .queues(["emails".to_string()].into())
                    .types(["send_email".to_string()].into()),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    // --- list_jobs with ID filter ---

    #[tokio::test]
    async fn list_jobs_id_filter_returns_matching_jobs() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(now, EnqueueOptions::new("b", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let _j3 = store
            .enqueue(now, EnqueueOptions::new("c", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [j1.id.clone(), j2.id.clone()].into();
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 2);
        let returned_ids: HashSet<String> = page.jobs.iter().map(|j| j.id.clone()).collect();
        assert!(returned_ids.contains(&j1.id));
        assert!(returned_ids.contains(&j2.id));
    }

    #[tokio::test]
    async fn list_jobs_id_filter_skips_nonexistent_ids() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [j1.id.clone(), "0000000000000000000000000".into()].into();
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j1.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_combined_with_status() {
        let store = test_store();
        store.rebuild_indexes().await.unwrap();
        let now = now_millis();
        let j1 = store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(now, EnqueueOptions::new("b", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        // Take j1 so it becomes InFlight.
        store.take_next_job(now, &HashSet::new()).await.unwrap();

        // Filter for both IDs but only Ready status — should only return j2.
        let ids: HashSet<String> = [j1.id.clone(), j2.id.clone()].into();
        let page = store
            .list_jobs(
                ListJobsOptions::new()
                    .ids(ids)
                    .statuses([JobStatus::Ready].into())
                    .now(now),
            )
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j2.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_combined_with_payload_filter() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(
                now,
                EnqueueOptions::new("a", "q", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();
        let j2 = store
            .enqueue(
                now,
                EnqueueOptions::new("b", "q", serde_json::json!({"x": 2})),
            )
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [j1.id.clone(), j2.id.clone()].into();
        let filter = Arc::new(PayloadFilter::compile(".x == 1").unwrap());
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).filter(filter).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j1.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_skips_nonexistent_with_payload_filter() {
        let store = test_store();
        let now = now_millis();
        let j1 = store
            .enqueue(
                now,
                EnqueueOptions::new("a", "q", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();

        let ids: HashSet<String> = [
            j1.id.clone(),
            "0000000000000000000000000".into(), // nonexistent but valid format
        ]
        .into();
        let filter = Arc::new(PayloadFilter::compile(".").unwrap());
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).filter(filter).now(now))
            .await
            .unwrap();

        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, j1.id);
    }

    #[tokio::test]
    async fn list_jobs_id_filter_empty_when_none_match() {
        let store = test_store();
        let ids: HashSet<String> = ["0000000000000000000000000".into()].into();
        let page = store
            .list_jobs(ListJobsOptions::new().ids(ids).now(now_millis()))
            .await
            .unwrap();

        assert!(page.jobs.is_empty());
    }

    #[tokio::test]
    async fn list_queues_returns_distinct_names() {
        let store = test_store();
        let now = now_millis();

        // Enqueue jobs on 3 different queues.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "alpha", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "beta", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "gamma", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let queues = store.list_queues().await.unwrap();
        assert_eq!(queues, vec!["alpha", "beta", "gamma"]);

        // Enqueue more jobs on existing queues — count should not change.
        store
            .enqueue(
                now,
                EnqueueOptions::new("other", "alpha", serde_json::json!(null)),
            )
            .await
            .unwrap();
        store
            .enqueue(
                now,
                EnqueueOptions::new("other", "beta", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let queues = store.list_queues().await.unwrap();
        assert_eq!(queues, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn list_queues_empty_store() {
        let store = test_store();
        let queues = store.list_queues().await.unwrap();
        assert!(queues.is_empty());
    }
}
