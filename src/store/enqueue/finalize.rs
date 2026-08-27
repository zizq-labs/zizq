// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Post-commit phase of enqueue: update the in-memory ready/scheduled
//! index and broadcast the corresponding `JobDispatchable` / `JobScheduled`
//! event. Called by the auto-batcher after a successful commit, and by
//! `cron::promote_cron_entry` for the cron-driven enqueue path.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::broadcast;

use super::super::dispatch::{Dispatch, Placement};
use super::super::results::EnqueueResult;
use super::super::scheduled_index::ScheduledIndex;
use super::super::store::StoreEvent;
use super::super::types::JobStatus;

/// Update in-memory indexes for a successfully enqueued job.
///
/// Call after the transaction has been committed. Updates the ready or
/// scheduled index depending on the job's status.
pub(in crate::store) fn finalize_enqueue(
    result: &EnqueueResult,
    dispatch: &Dispatch,
    scheduled_index: &ScheduledIndex,
    event_tx: &broadcast::Sender<StoreEvent>,
) {
    if let EnqueueResult::Created(job) = result {
        match JobStatus::try_from(job.status) {
            Ok(JobStatus::Ready) => {
                dispatch.insert(Placement::of(job));
                let _ = event_tx.send(StoreEvent::JobDispatchable {
                    id: job.id.clone(),
                    queue: job.queue.clone(),
                    token: Arc::new(AtomicBool::new(false)),
                });
            }
            Ok(JobStatus::Scheduled) => {
                scheduled_index.insert(job.ready_at, job.id.clone());
                let _ = event_tx.send(StoreEvent::JobScheduled {
                    id: job.id.clone(),
                    ready_at: job.ready_at,
                });
            }
            _ => {
                tracing::warn!(
                    job_id = %job.id,
                    status = job.status,
                    "enqueue produced unexpected status"
                );
            }
        }
    }
}
