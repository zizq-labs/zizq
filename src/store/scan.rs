// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Iterator scaffolding shared by `read`, `delete`, and `patch`.
//!
//! Three layers compose:
//!
//! - `IdStream` — a lazy stream of job ID bytes, fed by either a
//!   `MergeSource` over secondary index scans or a sorted user-supplied
//!   ID set. Multiple streams are combined via k-way merge
//!   (`merge_sources`) and intersection (`intersect_streams`).
//! - `JobStream` — hydrates job records from the data keyspace, either
//!   `ById` (drains an `IdStream`) or `FullScan` (range-scans J-tagged
//!   keys directly). Filters out expired jobs when `now` is supplied.
//! - `PayloadFilteredIter` — wraps a `JobStream` and applies a jq
//!   payload filter, skipping non-matching jobs. Requires the
//!   underlying stream to hydrate payloads.
//!
//! `build_id_stream` is the entry point: it composes one stream per
//! active filter group (queues / statuses / types / ids), then
//! intersects them. Returns `None` if no filters apply, signalling the
//! caller to use `JobStream::full_scan` instead.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::ops::{Bound, RangeInclusive};
use std::sync::Arc;

use fjall::{Readable, SingleWriterTxKeyspace};

use crate::filter::PayloadFilter;

use super::keys::{
    IndexKind, RecordKind, make_job_key, make_payload_key, make_queue_key, make_status_key,
    make_type_key,
};
use super::options::JobFilter;
use super::store::Keyspaces;
use super::types::{Job, JobStatus, ScanDirection, StoreError};

// --- K-way merge helpers ---

/// A source of job IDs for k-way merging. Returns `Some(Ok(id))` for the
/// next ID, `Some(Err(e))` on error, or `None` when exhausted.
type MergeSource<'a> = Box<dyn FnMut() -> Option<Result<Vec<u8>, StoreError>> + 'a>;

/// Wrap a fjall range iterator into a `MergeSource`.
///
/// Each entry's key has a `prefix_len`-byte prefix followed by the job ID.
/// This macro exists because forward and reverse iterators are different
/// types in Rust, so a generic function cannot accept both without boxing
/// the iterator itself. The macro monomorphizes over the concrete iterator
/// type while producing the same `MergeSource` closure.
macro_rules! range_source {
    ($iter:expr, $prefix_len:expr) => {{
        let mut iter = $iter;
        let prefix_len = $prefix_len;
        Box::new(move || {
            iter.next().map(|e| {
                let (key, _) = e.into_inner()?;
                Ok(key[prefix_len..].to_vec())
            })
        }) as MergeSource<'_>
    }};
}

/// A lazy, sorted stream of job IDs.
///
/// Wraps a `MergeSource` closure and implements `Iterator` so we get
/// standard combinators like `.take(n)` and `.collect()` for free.
pub(super) struct IdStream<'a>(MergeSource<'a>);

impl<'a> Iterator for IdStream<'a> {
    type Item = Result<Vec<u8>, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.0)()
    }
}

/// Lazily merge multiple sorted `MergeSource`s into a single `IdStream`
/// using a binary heap (k-way merge).
///
/// For ascending order, uses a min-heap (`Reverse`). For descending, a
/// max-heap. Each source must already yield IDs in the matching order.
/// Errors encountered during seeding or iteration are deferred to the
/// next call to `next()`.
fn merge_sources<'a>(mut sources: Vec<MergeSource<'a>>, direction: ScanDirection) -> IdStream<'a> {
    // Deferred error: if seeding the heap hits an error, we store it here
    // and surface it on the first call to `next()`.
    let mut deferred_err: Option<StoreError> = None;

    match direction {
        ScanDirection::Asc => {
            let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(result) = src() {
                    match result {
                        Ok(id) => heap.push(Reverse((id, i))),
                        Err(e) => {
                            deferred_err = Some(e);
                            break;
                        }
                    }
                }
            }

            IdStream(Box::new(move || {
                if let Some(e) = deferred_err.take() {
                    return Some(Err(e));
                }
                let Reverse((id, i)) = heap.pop()?;
                if let Some(result) = sources[i]() {
                    match result {
                        Ok(next) => heap.push(Reverse((next, i))),
                        Err(e) => deferred_err = Some(e),
                    }
                }
                Some(Ok(id))
            }))
        }
        ScanDirection::Desc => {
            let mut heap: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::new();
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(result) = src() {
                    match result {
                        Ok(id) => heap.push((id, i)),
                        Err(e) => {
                            deferred_err = Some(e);
                            break;
                        }
                    }
                }
            }

            IdStream(Box::new(move || {
                if let Some(e) = deferred_err.take() {
                    return Some(Err(e));
                }
                let (id, i) = heap.pop()?;
                if let Some(result) = sources[i]() {
                    match result {
                        Ok(next) => heap.push((next, i)),
                        Err(e) => deferred_err = Some(e),
                    }
                }
                Some(Ok(id))
            }))
        }
    }
}

/// Lazily intersect two sorted `IdStream`s, yielding only IDs present in
/// both.
///
/// Both streams must be sorted in the same `direction`. The intersection
/// preserves that ordering. Internally buffers one item from each stream
/// and advances whichever is "behind" (smaller in asc, larger in desc).
fn intersect_streams<'a>(
    mut a: IdStream<'a>,
    mut b: IdStream<'a>,
    direction: ScanDirection,
) -> IdStream<'a> {
    // Buffered next values from each stream.
    let mut buf_a: Option<Vec<u8>> = None;
    let mut buf_b: Option<Vec<u8>> = None;
    // Whether we've primed the buffers yet.
    let mut primed = false;

    IdStream(Box::new(move || {
        // Prime on first call so construction is cheap.
        if !primed {
            primed = true;
            match a.next() {
                Some(Ok(v)) => buf_a = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
            match b.next() {
                Some(Ok(v)) => buf_b = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }

        loop {
            let (va, vb) = match (buf_a.as_ref(), buf_b.as_ref()) {
                (Some(va), Some(vb)) => (va, vb),
                _ => return None, // One or both streams exhausted.
            };

            match va.cmp(vb) {
                std::cmp::Ordering::Equal => {
                    // Match! Yield this ID and advance both streams.
                    let matched = buf_a.take().unwrap();
                    buf_a = match a.next() {
                        Some(Ok(v)) => Some(v),
                        Some(Err(e)) => return Some(Err(e)),
                        None => None,
                    };
                    buf_b = match b.next() {
                        Some(Ok(v)) => Some(v),
                        Some(Err(e)) => return Some(Err(e)),
                        None => None,
                    };
                    return Some(Ok(matched));
                }
                std::cmp::Ordering::Less => {
                    // a < b: in asc mode a is behind, advance a.
                    //        in desc mode a is ahead, advance b.
                    if direction == ScanDirection::Asc {
                        buf_a = match a.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    } else {
                        buf_b = match b.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    }
                }
                std::cmp::Ordering::Greater => {
                    // a > b: in asc mode b is behind, advance b.
                    //        in desc mode b is ahead, advance a.
                    if direction == ScanDirection::Asc {
                        buf_b = match b.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    } else {
                        buf_a = match a.next() {
                            Some(Ok(v)) => Some(v),
                            Some(Err(e)) => return Some(Err(e)),
                            None => None,
                        };
                    }
                }
            }
        }
    }))
}

// --- Index scan source builders ---
//
// Build MergeSource iterators from the secondary indexes. Used by both
// list_jobs and delete_jobs.

/// Build queue-index MergeSources for the given queues.
fn queue_scan_sources<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    queues: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Vec<MergeSource<'a>> {
    queues
        .iter()
        .map(|queue_name| {
            let prefix_len = queue_name.len() + 3;
            let range_start = Bound::Included(make_queue_key(queue_name, ""));
            let mut range_end_key = Vec::with_capacity(queue_name.len() + 3);
            range_end_key.push(IndexKind::Queue as u8);
            range_end_key.push(0);
            range_end_key.extend_from_slice(queue_name.as_bytes());
            range_end_key.push(1);
            let range_end = Bound::Excluded(range_end_key);

            match (direction, from) {
                (ScanDirection::Asc, Some(cursor)) => range_source!(
                    snapshot.range::<Vec<u8>, _>(
                        ks.index.as_ref(),
                        (
                            Bound::Excluded(make_queue_key(queue_name, cursor)),
                            range_end
                        ),
                    ),
                    prefix_len
                ),
                (ScanDirection::Asc, None) => range_source!(
                    snapshot.range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end),),
                    prefix_len
                ),
                (ScanDirection::Desc, Some(cursor)) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.index.as_ref(),
                            (
                                range_start,
                                Bound::Excluded(make_queue_key(queue_name, cursor))
                            ),
                        )
                        .rev(),
                    prefix_len
                ),
                (ScanDirection::Desc, None) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end))
                        .rev(),
                    prefix_len
                ),
            }
        })
        .collect()
}

/// Build status-index MergeSources for the given statuses.
fn status_scan_sources<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    statuses: &HashSet<JobStatus>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Vec<MergeSource<'a>> {
    statuses
        .iter()
        .map(|status| {
            let prefix = *status as u8;
            let range_start = Bound::Included(vec![IndexKind::Status as u8, 0, prefix, 0]);
            let range_end = Bound::Excluded(vec![IndexKind::Status as u8, 0, prefix + 1, 0]);

            match (direction, from) {
                (ScanDirection::Asc, Some(cursor)) => range_source!(
                    snapshot.range::<Vec<u8>, _>(
                        ks.index.as_ref(),
                        (Bound::Excluded(make_status_key(*status, cursor)), range_end),
                    ),
                    4
                ),
                (ScanDirection::Asc, None) => range_source!(
                    snapshot.range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end),),
                    4
                ),
                (ScanDirection::Desc, Some(cursor)) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.index.as_ref(),
                            (
                                range_start,
                                Bound::Excluded(make_status_key(*status, cursor))
                            ),
                        )
                        .rev(),
                    4
                ),
                (ScanDirection::Desc, None) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end))
                        .rev(),
                    4
                ),
            }
        })
        .collect()
}

/// Build type-index MergeSources for the given types.
fn type_scan_sources<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    types: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> Vec<MergeSource<'a>> {
    types
        .iter()
        .map(|type_name| {
            let prefix_len = type_name.len() + 3;
            let range_start = Bound::Included(make_type_key(type_name, ""));
            let mut range_end_key = Vec::with_capacity(type_name.len() + 3);
            range_end_key.push(IndexKind::Type as u8);
            range_end_key.push(0);
            range_end_key.extend_from_slice(type_name.as_bytes());
            range_end_key.push(1);
            let range_end = Bound::Excluded(range_end_key);

            match (direction, from) {
                (ScanDirection::Asc, Some(cursor)) => range_source!(
                    snapshot.range::<Vec<u8>, _>(
                        ks.index.as_ref(),
                        (Bound::Excluded(make_type_key(type_name, cursor)), range_end),
                    ),
                    prefix_len
                ),
                (ScanDirection::Asc, None) => range_source!(
                    snapshot.range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end),),
                    prefix_len
                ),
                (ScanDirection::Desc, Some(cursor)) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.index.as_ref(),
                            (
                                range_start,
                                Bound::Excluded(make_type_key(type_name, cursor))
                            ),
                        )
                        .rev(),
                    prefix_len
                ),
                (ScanDirection::Desc, None) => range_source!(
                    snapshot
                        .range::<Vec<u8>, _>(ks.index.as_ref(), (range_start, range_end))
                        .rev(),
                    prefix_len
                ),
            }
        })
        .collect()
}

/// Build an IdStream from a set of user-provided job IDs, sorted and
/// filtered by the pagination cursor.
fn id_stream(
    ids: &HashSet<String>,
    from: &Option<String>,
    direction: ScanDirection,
) -> IdStream<'static> {
    let mut sorted: Vec<Vec<u8>> = ids
        .iter()
        .filter(|id| match (from, direction) {
            (Some(cursor), ScanDirection::Asc) => id.as_str() > cursor.as_str(),
            (Some(cursor), ScanDirection::Desc) => id.as_str() < cursor.as_str(),
            (None, _) => true,
        })
        .map(|id| id.as_bytes().to_vec())
        .collect();

    match direction {
        ScanDirection::Asc => sorted.sort(),
        ScanDirection::Desc => sorted.sort_by(|a, b| b.cmp(a)),
    }

    let mut iter = sorted.into_iter();
    IdStream(Box::new(move || iter.next().map(Ok)))
}

/// A lazy stream of `Job`s, either from an `IdStream` (filtered path) or
/// from a direct range scan of the data keyspace (unfiltered path).
///
/// When `now` is `Some`, jobs past their `purge_at` are skipped (expired).
/// When `needs_payload` is true, the payload is hydrated from the data
/// keyspace. Payload filtering is NOT applied here — use
/// `PayloadFilteredIter` to wrap this stream when a filter is present.
pub(super) enum JobStream<'a, R: Readable> {
    /// Reads jobs by looking up IDs from an index scan.
    ById {
        ids: IdStream<'a>,
        reader: &'a R,
        data_ks: &'a SingleWriterTxKeyspace,
        now: Option<u64>,
        needs_payload: bool,
        source: String,
    },
    /// Reads jobs directly from a range scan of J-tagged keys.
    FullScan {
        /// Boxed to unify the Asc/Desc iterator types.
        entries: Box<dyn Iterator<Item = fjall::Guard> + 'a>,
        data_ks: &'a SingleWriterTxKeyspace,
        reader: &'a R,
        now: Option<u64>,
        needs_payload: bool,
    },
}

impl<'a, R: Readable> JobStream<'a, R> {
    /// Construct a `FullScan` variant for the given direction and cursor.
    pub(super) fn full_scan(
        snapshot: &'a R,
        ks: &'a Keyspaces,
        from: &Option<String>,
        direction: ScanDirection,
        now: Option<u64>,
        needs_payload: bool,
    ) -> Self {
        let job_prefix_start: Vec<u8> = vec![RecordKind::Job as u8, 0];
        let job_prefix_end: Vec<u8> = vec![RecordKind::Job as u8, 1];

        let entries: Box<dyn Iterator<Item = fjall::Guard> + 'a> = match direction {
            ScanDirection::Asc => {
                let start = match from {
                    Some(cursor) => Bound::Excluded(make_job_key(cursor)),
                    None => Bound::Included(job_prefix_start),
                };
                Box::new(snapshot.range::<Vec<u8>, _>(
                    ks.data.as_ref(),
                    (start, Bound::Excluded(job_prefix_end)),
                ))
            }
            ScanDirection::Desc => {
                let end = match from {
                    Some(cursor) => Bound::Excluded(make_job_key(cursor)),
                    None => Bound::Excluded(job_prefix_end),
                };
                Box::new(
                    snapshot
                        .range::<Vec<u8>, _>(
                            ks.data.as_ref(),
                            (Bound::Included(job_prefix_start), end),
                        )
                        .rev(),
                )
            }
        };

        Self::FullScan {
            entries,
            data_ks: &ks.data,
            reader: snapshot,
            now,
            needs_payload,
        }
    }

    /// Construct a `ById` variant from a pre-built `IdStream`.
    pub(super) fn by_id(
        ids: IdStream<'a>,
        reader: &'a R,
        data_ks: &'a SingleWriterTxKeyspace,
        now: Option<u64>,
        needs_payload: bool,
        source: String,
    ) -> Self {
        Self::ById {
            ids,
            reader,
            data_ks,
            now,
            needs_payload,
            source,
        }
    }
}

impl<'a, R: Readable> Iterator for JobStream<'a, R> {
    type Item = Result<Job, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::ById {
                ids,
                reader,
                data_ks,
                now,
                needs_payload,
                source,
            } => loop {
                let id = match ids.next()? {
                    Ok(id) => id,
                    Err(e) => return Some(Err(e)),
                };

                let id_str = match std::str::from_utf8(&id) {
                    Ok(s) => s,
                    Err(e) => {
                        return Some(Err(StoreError::Corruption(format!(
                            "job ID is not valid UTF-8: {e}"
                        ))));
                    }
                };

                let job_key = make_job_key(id_str);
                let bytes = match reader.get(*data_ks, &job_key) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => {
                        // Job was deleted between index scan and data read.
                        tracing::trace!(
                            id = id_str,
                            source = &**source,
                            "job in index but missing from data keyspace, skipping"
                        );
                        continue;
                    }
                    Err(e) => return Some(Err(e.into())),
                };

                let mut job: Job = match rmp_serde::from_slice(&bytes) {
                    Ok(j) => j,
                    Err(e) => return Some(Err(e.into())),
                };

                if let Some(now) = *now {
                    if job.purge_at.is_some_and(|p| p <= now) {
                        continue;
                    }
                }

                if *needs_payload {
                    let payload_key = make_payload_key(&job.id);
                    match reader.get(*data_ks, &payload_key) {
                        Ok(Some(pb)) => match rmp_serde::from_slice(&pb) {
                            Ok(v) => job.payload = Some(v),
                            Err(e) => return Some(Err(e.into())),
                        },
                        Ok(None) => {}
                        Err(e) => return Some(Err(e.into())),
                    }
                }

                return Some(Ok(job));
            },

            Self::FullScan {
                entries,
                data_ks,
                reader,
                now,
                needs_payload,
            } => loop {
                let entry = entries.next()?;

                let (key, value) = match entry.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => return Some(Err(e.into())),
                };

                let mut job: Job = match rmp_serde::from_slice(&value) {
                    Ok(j) => j,
                    Err(e) => return Some(Err(e.into())),
                };

                if let Some(now) = *now {
                    if job.purge_at.is_some_and(|p| p <= now) {
                        continue;
                    }
                }

                if *needs_payload {
                    // Swap J tag for P tag to look up payload.
                    let mut payload_key = key.to_vec();
                    payload_key[0] = RecordKind::Payload as u8;
                    match reader.get(*data_ks, &payload_key) {
                        Ok(Some(pb)) => match rmp_serde::from_slice(&pb) {
                            Ok(v) => job.payload = Some(v),
                            Err(e) => return Some(Err(e.into())),
                        },
                        Ok(None) => {}
                        Err(e) => return Some(Err(e.into())),
                    }
                }

                return Some(Ok(job));
            },
        }
    }
}

/// Wraps a job iterator and skips jobs whose numeric fields fall outside
/// the supplied inclusive range(s).
///
/// Both bounds are checked off the already-hydrated `Job` record, so no
/// extra disk reads are required. Cheap integer comparisons — chain this
/// before `PayloadFilteredIter` so the payload filter only runs for rows
/// that already passed the range check.
pub(super) struct RangeFilteredIter<I> {
    pub(super) inner: I,
    pub(super) priority: Option<RangeInclusive<u16>>,
    pub(super) ready_at: Option<RangeInclusive<u64>>,
    pub(super) attempts: Option<RangeInclusive<u32>>,
}

impl<I: Iterator<Item = Result<Job, StoreError>>> Iterator for RangeFilteredIter<I> {
    type Item = Result<Job, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let job = match self.inner.next()? {
                Ok(j) => j,
                Err(e) => return Some(Err(e)),
            };

            if let Some(range) = &self.priority {
                if !range.contains(&job.priority) {
                    continue;
                }
            }
            if let Some(range) = &self.ready_at {
                if !range.contains(&job.ready_at) {
                    continue;
                }
            }
            if let Some(range) = &self.attempts {
                if !range.contains(&job.attempts) {
                    continue;
                }
            }

            return Some(Ok(job));
        }
    }
}

/// Wraps a job iterator and applies a jq payload filter, skipping
/// non-matching jobs. The inner iterator must yield jobs with payloads
/// already hydrated (set `needs_payload: true` on the `JobStream`).
pub(super) struct PayloadFilteredIter<I> {
    pub(super) inner: I,
    pub(super) filter: Arc<PayloadFilter>,
}

impl<I: Iterator<Item = Result<Job, StoreError>>> Iterator for PayloadFilteredIter<I> {
    type Item = Result<Job, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let job = match self.inner.next()? {
                Ok(j) => j,
                Err(e) => return Some(Err(e)),
            };

            let payload = job.payload.as_ref().unwrap_or(&serde_json::Value::Null);
            if self.filter.matches(payload) {
                return Some(Ok(job));
            }
        }
    }
}

/// Build an `IdStream` from a `JobFilter`'s index-backed fields.
///
/// Returns `None` when no index-backed filter is active (caller should use
/// a full-scan `JobStream` instead). Returns `Some((stream, source,
/// has_ids))` when at least one filter is active.
///
/// Range and payload fields are NOT consulted here — they are applied as
/// post-hydration filters by [`apply_filters`].
pub(super) fn build_id_stream<'a>(
    snapshot: &'a impl Readable,
    ks: &'a Keyspaces,
    filter: &JobFilter,
    from: &Option<String>,
    direction: ScanDirection,
) -> Option<(IdStream<'a>, String, bool)> {
    let mut filters: Vec<(&str, IdStream<'a>)> = Vec::new();

    if !filter.queues.is_empty() {
        filters.push((
            "jobs_by_queue",
            merge_sources(
                queue_scan_sources(snapshot, ks, &filter.queues, from, direction),
                direction,
            ),
        ));
    }

    if !filter.statuses.is_empty() {
        filters.push((
            "jobs_by_status",
            merge_sources(
                status_scan_sources(snapshot, ks, &filter.statuses, from, direction),
                direction,
            ),
        ));
    }

    if !filter.types.is_empty() {
        filters.push((
            "jobs_by_type",
            merge_sources(
                type_scan_sources(snapshot, ks, &filter.types, from, direction),
                direction,
            ),
        ));
    }

    let has_id_filter = !filter.ids.is_empty();

    if has_id_filter {
        filters.push(("jobs_by_id", id_stream(&filter.ids, from, direction)));
    }

    if filters.is_empty() {
        return None;
    }

    let source_desc = if filters.len() == 1 {
        filters[0].0.to_string()
    } else {
        let names: Vec<&str> = filters.iter().map(|(n, _)| *n).collect();
        format!("({})", names.join(" & "))
    };

    let mut iter = filters.into_iter().map(|(_, s)| s);
    let first = iter.next().unwrap();
    let combined = iter.fold(first, |acc, s| intersect_streams(acc, s, direction));

    Some((combined, source_desc, has_id_filter))
}

/// Wrap a `JobStream` with the post-hydration filters from a `JobFilter`
/// (range, then payload) and return as a boxed iterator.
///
/// Range checks are O(1) integer comparisons, so they run first to
/// short-circuit the more expensive jq payload evaluation. Range checks
/// don't need a hydrated payload — only the payload filter does.
pub(super) fn apply_filters<'a, R: Readable + 'a>(
    inner: JobStream<'a, R>,
    filter: &JobFilter,
) -> Box<dyn Iterator<Item = Result<Job, StoreError>> + 'a> {
    let stream: Box<dyn Iterator<Item = Result<Job, StoreError>> + 'a> =
        if filter.priority.is_some() || filter.ready_at.is_some() || filter.attempts.is_some() {
            Box::new(RangeFilteredIter {
                inner,
                priority: filter.priority.clone(),
                ready_at: filter.ready_at.clone(),
                attempts: filter.attempts.clone(),
            })
        } else {
            Box::new(inner)
        };

    if let Some(payload_filter) = &filter.payload_filter {
        Box::new(PayloadFilteredIter {
            inner: stream,
            filter: Arc::clone(payload_filter),
        })
    } else {
        stream
    }
}

/// Does `filter` need each candidate job's payload hydrated?
///
/// Range filters operate on top-level `Job` fields, so they don't require
/// payload hydration. Only a jq payload filter does.
pub(super) fn filter_needs_payload(filter: &JobFilter) -> bool {
    filter.payload_filter.is_some()
}
