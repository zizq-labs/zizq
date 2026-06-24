// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! On-disk key encoding for the data and index keyspaces.
//!
//! Each key starts with a one-byte tag (`RecordKind` for the data
//! keyspace, `IndexKind` for the index keyspace) so that all record and
//! index kinds coexist in their respective keyspace without collision.
//! The builder functions assemble the appropriate tag, separators, and
//! payload for each kind.

use super::store::Keyspaces;
use super::types::JobStatus;

/// One-byte tag prefix for records in the `data` keyspace.
///
/// Each record kind occupies a disjoint prefix range so that jobs, payloads,
/// and errors coexist in the same fjall keyspace without collision.
#[repr(u8)]
pub(super) enum RecordKind {
    Cron = b'C',
    Error = b'E',
    Job = b'J',
    Payload = b'P',
}

/// One-byte tag prefix for entries in the `index` keyspace.
///
/// Each index kind occupies a disjoint prefix range so that all secondary
/// indexes coexist in the same fjall keyspace without collision.
#[repr(u8)]
pub(super) enum IndexKind {
    PurgeAt = b'A',
    Queue = b'Q',
    Status = b'S',
    Type = b'T',
    Unique = b'U',
}

/// Build a job metadata key: `J\0{job_id}`.
pub(super) fn make_job_key(job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(RecordKind::Job as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a payload key: `P\0{job_id}`.
pub(super) fn make_payload_key(job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + job_id.len());
    key.push(RecordKind::Payload as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a status index key: `S\0{status_byte}\0{job_id}`.
pub(super) fn make_status_key(status: JobStatus, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + job_id.len());
    key.push(IndexKind::Status as u8);
    key.push(0);
    key.push(status as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a queue membership index key: `Q\0{queue_name}\0{job_id}`.
pub(super) fn make_queue_key(queue_name: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(queue_name.len() + 3 + job_id.len());
    key.push(IndexKind::Queue as u8);
    key.push(0);
    key.extend_from_slice(queue_name.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a type membership index key: `T\0{job_type}\0{job_id}`.
pub(super) fn make_type_key(job_type: &str, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(job_type.len() + 3 + job_id.len());
    key.push(IndexKind::Type as u8);
    key.push(0);
    key.extend_from_slice(job_type.as_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a purge-at index key: `A\0{purge_at_be_u64}\0{job_id}`.
///
/// Big-endian encoding gives chronological ordering so the reaper can
/// range-scan from the start to find expired entries.
pub(super) fn make_purge_key(purge_at: u64, job_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + 3 + job_id.len());
    key.push(IndexKind::PurgeAt as u8);
    key.push(0);
    key.extend_from_slice(&purge_at.to_be_bytes());
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key
}

/// Build a unique deduplication index key: `U\0{unique_key}`.
pub(super) fn make_unique_key(unique_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + unique_key.len());
    key.push(IndexKind::Unique as u8);
    key.push(0);
    key.extend_from_slice(unique_key.as_bytes());
    key
}

/// Build an error record key: `E\0{job_id}\0{attempt_be_u32}`.
///
/// The null separator lets us prefix-scan all errors for a given job,
/// and the big-endian attempt number gives chronological ordering.
pub(super) fn make_error_key(job_id: &str, attempt: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(job_id.len() + 3 + 4);
    key.push(RecordKind::Error as u8);
    key.push(0);
    key.extend_from_slice(job_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&attempt.to_be_bytes());
    key
}

/// Iterate all error record keys for a job via prefix scan.
///
/// The returned iterator yields owned key bytes that the caller can pass
/// to `tx.remove`. The fjall `Iter` is an owned value (no borrow on the
/// keyspace), so it's safe to interleave iteration with mutable tx ops.
pub(super) fn error_keys(ks: &Keyspaces, job_id: &str) -> impl Iterator<Item = Vec<u8>> {
    let mut prefix = Vec::with_capacity(job_id.len() + 3);
    prefix.push(RecordKind::Error as u8);
    prefix.push(0);
    prefix.extend_from_slice(job_id.as_bytes());
    prefix.push(0);

    ks.data
        .inner()
        .prefix(&prefix)
        .filter_map(|guard| guard.key().ok().map(|k| k.to_vec()))
}
