// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! The writer: one thread applying enqueues, takes and completions.
//!
//! fjall allows one write transaction at a time. Instead of each kind of
//! write competing for that lock from its own thread, the writer gathers
//! every enqueue, take and completion waiting at once and applies them
//! in one transaction with one commit. Takes and enqueues are served
//! from the same queue, so a burst of bulk enqueues can't push takes to
//! the back of a line for the lock.
//!
//! Each pass:
//!
//! 1. Blocks for the first op, then gathers whatever else is already
//!    waiting, within per-kind limits (see [`Limits`]). Ops that don't
//!    fit are carried to the next pass, ahead of anything newer. No
//!    time-based window: a lone op is never delayed waiting for more.
//! 2. Applies enqueues, then takes, then completions, in one
//!    transaction. Takes and completions arrive pre-read by their
//!    callers, and check every record they expect before writing any,
//!    so a conflict never leaves partial writes in a transaction other
//!    ops share. Checks read through the transaction, so they see writes
//!    made earlier in the pass.
//! 3. Commits once, and hands the pass to the finisher thread without
//!    waiting for the commit to be durable, then starts the next pass.
//!
//! The finisher takes passes in commit order. Once a pass is durable it
//! finishes each kind (in-memory indexes, events) and answers each op,
//! so jobs only become takeable, and callers only hear back, once their
//! writes are durable. Waiting for durability on the writer thread would
//! leave the write lock idle for the whole wait.
//!
//! A failure applying one kind (a corrupt record, say) drops the
//! transaction, fails that kind's ops, and carries the rest to the next
//! pass. A take or completion that conflicts is answered as such, for
//! its caller to pre-read again and resubmit. A failed commit, or one
//! that never becomes durable, fails every op in the pass.
//!
//! Callers submit from `spawn_blocking`: the channel is bounded, so a
//! submit blocks when the writer is behind.

pub(in crate::store) mod job;
pub(in crate::store) mod take;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender};

use tokio::sync::{broadcast, oneshot};

use super::budget::Budgets;
use super::complete::{CompleteGroup, CompleteOp};
use super::dispatch::Dispatch;
use super::enqueue::{EnqueueGroup, EnqueueOp, PreparedEnqueue};
use super::in_flight_index::InFlightIndex;
use super::results::EnqueueResult;
use super::scheduled_index::ScheduledIndex;
use super::store::{Durable, Keyspaces, StoreEvent};
use super::types::{CommitMode, StoreError};
use job::JobGroup;
pub(in crate::store) use job::{JobOp, JobOutcome, JobReply, JobWrite};
use take::{TakeGroup, TakeOp, TakeOutcome, TakeWrite};

/// Most takes in one pass. Each take carries at most a worker's
/// prefetch, so in practice this is bounded by how many take streams
/// are open.
const TAKE_BATCH_SIZE: usize = 1000;

/// Most single-job writes (fails, requeues, ...) in one pass.
const JOB_BATCH_SIZE: usize = 1000;

/// A write waiting for the writer.
pub(in crate::store) enum WriteOp {
    Enqueue(EnqueueOp),
    Complete(CompleteOp),
    Take(TakeOp),
    /// Boxed: a single-job write holds a whole job record, and inline it
    /// would make every op in the channel that size.
    Job(Box<JobOp>),
}

/// Settings for [`Writer::start`].
pub(in crate::store) struct WriterConfig {
    pub enqueue_batch_size: usize,
    pub write_batch_max_jobs: usize,
    pub complete_batch_size: usize,
}

/// What the writer thread needs to apply and finish writes.
pub(in crate::store) struct WriterContext {
    pub ks: Arc<Keyspaces>,
    pub dispatch: Arc<Dispatch>,
    pub budgets: Arc<Budgets>,
    pub scheduled_index: Arc<ScheduledIndex>,
    pub in_flight_index: Arc<InFlightIndex>,
    pub event_tx: broadcast::Sender<StoreEvent>,
}

/// Handle for submitting writes. The thread exits once every handle is
/// dropped and the ops already queued are done.
pub(in crate::store) struct Writer {
    tx: SyncSender<WriteOp>,
}

impl Writer {
    pub(in crate::store) fn start(cx: WriterContext, config: WriterConfig) -> Self {
        let limits = Limits {
            enqueue_ops: config.enqueue_batch_size.max(1),
            max_jobs: config.write_batch_max_jobs,
            complete_ops: config.complete_batch_size.max(1),
            take_ops: TAKE_BATCH_SIZE,
            job_ops: JOB_BATCH_SIZE,
        };
        let (tx, rx) = std::sync::mpsc::sync_channel::<WriteOp>(limits.total_ops());
        let (finish_tx, finish_rx) = std::sync::mpsc::sync_channel::<Pass>(FINISH_QUEUE);
        let cx = Arc::new(cx);

        std::thread::Builder::new()
            .name("writer".into())
            .spawn({
                let cx = cx.clone();
                move || {
                    let mut carry = VecDeque::new();
                    let mut skipped_enqueues = false;
                    while let Some(batch) = gather(&mut carry, &mut skipped_enqueues, &rx, &limits)
                    {
                        if let Some(pass) = run_pass(&cx, batch, &mut carry) {
                            // Blocks while too many passes await durability.
                            let _ = finish_tx.send(pass);
                        }
                    }
                }
            })
            .expect("failed to spawn writer thread");

        // Ends once the writer thread drops its sender and every pass sent
        // is finished, so nothing is left unanswered at shutdown.
        std::thread::Builder::new()
            .name("writer-finish".into())
            .spawn(move || finish_passes(&cx, finish_rx))
            .expect("failed to spawn writer-finish thread");

        Self { tx }
    }

    /// Submit an enqueue (one job, or a bulk). The receiver resolves with
    /// the op's results once its pass has committed, or with the error
    /// that failed it.
    pub(in crate::store) fn submit_enqueue(
        &self,
        prepared: Vec<PreparedEnqueue>,
    ) -> oneshot::Receiver<Result<Vec<EnqueueResult>, StoreError>> {
        let (reply, rx) = oneshot::channel();
        self.send(WriteOp::Enqueue(EnqueueOp { prepared, reply }));
        rx
    }

    /// Submit a completion of one or more jobs.
    pub(in crate::store) fn submit_complete(&self, op: CompleteOp) {
        self.send(WriteOp::Complete(op));
    }

    /// Submit a take's prepared writes.
    pub(in crate::store) fn submit_take(
        &self,
        writes: Vec<TakeWrite>,
    ) -> oneshot::Receiver<Result<TakeOutcome, StoreError>> {
        let (reply, rx) = oneshot::channel();
        self.send(WriteOp::Take(TakeOp { writes, reply }));
        rx
    }

    /// Submit a single-job write. Its reply goes back through the op.
    pub(in crate::store) fn submit_job(&self, op: JobOp) {
        self.send(WriteOp::Job(Box::new(op)));
    }

    /// Blocks while the channel is full. A send error means the thread is
    /// gone: the dropped reply closes the caller's receiver, which it
    /// reports as an internal error.
    fn send(&self, op: WriteOp) {
        let _ = self.tx.send(op);
    }
}

/// Most committed passes waiting to become durable. Bounds how far the
/// writer can run ahead of durability.
const FINISH_QUEUE: usize = 16;

/// Apply one pass's ops in one transaction and commit it, without
/// waiting for it to be durable. Returns the pass for the finisher, or
/// `None` if it already answered every op (a group failed to apply).
fn run_pass(
    cx: &WriterContext,
    batch: Vec<WriteOp>,
    carry: &mut VecDeque<WriteOp>,
) -> Option<Pass> {
    let ks = &cx.ks;

    let (mut enqueues, mut completes, mut takes, mut jobs) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for op in batch {
        match op {
            WriteOp::Enqueue(op) => enqueues.push(op),
            WriteOp::Complete(op) => completes.push(op),
            WriteOp::Take(op) => takes.push(op),
            WriteOp::Job(op) => jobs.push(*op),
        }
    }

    let mut pass = Pass {
        enqueue: (!enqueues.is_empty()).then(|| EnqueueGroup::new(enqueues)),
        take: (!takes.is_empty()).then(|| TakeGroup::new(takes)),
        complete: (!completes.is_empty()).then(|| CompleteGroup::new(completes)),
        job: (!jobs.is_empty()).then(|| JobGroup::new(jobs)),
        durable: None,
    };

    let mut tx = ks.write_tx();

    if let Some(group) = pass.enqueue.as_mut()
        && let Err(e) = group.apply(&mut tx, ks)
    {
        drop(tx);
        tracing::error!(error = %e, "writer: applying enqueues failed");
        pass.enqueue.take().unwrap().fail(&e.to_string());
        requeue(carry, None, pass.complete, pass.take, pass.job);
        return None;
    }

    if let Some(group) = pass.take.as_mut()
        && let Err(e) = group.apply(&mut tx, ks)
    {
        drop(tx);
        tracing::error!(error = %e, "writer: applying takes failed");
        pass.take.take().unwrap().fail(&e.to_string());
        requeue(carry, pass.enqueue, pass.complete, None, pass.job);
        return None;
    }

    if let Some(group) = pass.complete.as_mut()
        && let Err(e) = group.apply(&mut tx, ks)
    {
        drop(tx);
        tracing::error!(error = %e, "writer: applying completions failed");
        pass.complete.take().unwrap().fail(&e.to_string());
        requeue(carry, pass.enqueue, None, pass.take, pass.job);
        return None;
    }

    if let Some(group) = pass.job.as_mut()
        && let Err(e) = group.apply(&mut tx, ks)
    {
        // The group failed only the op that hit the error.
        drop(tx);
        tracing::error!(error = %e, "writer: applying a job write failed");
        requeue(carry, pass.enqueue, pass.complete, pass.take, pass.job);
        return None;
    }

    let enqueue_writes = pass.enqueue.as_ref().map_or(0, EnqueueGroup::writes);
    let writes = enqueue_writes
        + pass.take.as_ref().map_or(0, TakeGroup::writes)
        + pass.complete.as_ref().map_or(0, CompleteGroup::writes)
        + pass.job.as_ref().map_or(0, JobGroup::writes);

    if writes == 0 {
        // Still finished by the finisher, so its answers don't overtake
        // those of earlier passes still waiting to be durable.
        drop(tx);
        return Some(pass);
    }

    if let Some(group) = &pass.enqueue {
        group.stage(&cx.budgets);
    }

    let mode = if enqueue_writes > 0 {
        stricter(ks.enqueue_commit_mode, ks.default_commit_mode)
    } else {
        ks.default_commit_mode
    };

    match ks.commit_deferred(tx, mode) {
        Ok(durable) => {
            pass.durable = Some(durable);
            Some(pass)
        }
        Err(e) => {
            tracing::error!(error = %e, writes, "writer: commit failed");
            pass.fail(cx, &e.to_string());
            None
        }
    }
}

/// The finisher thread: finish each pass in the order the writer
/// committed them, until the writer is gone.
fn finish_passes(cx: &WriterContext, passes: Receiver<Pass>) {
    while let Ok(pass) = passes.recv() {
        pass.finish(cx);
    }
}

/// A committed pass, on its way to being durable.
struct Pass {
    enqueue: Option<EnqueueGroup>,
    take: Option<TakeGroup>,
    complete: Option<CompleteGroup>,
    job: Option<JobGroup>,
    /// `None` if the pass wrote nothing.
    durable: Option<Durable>,
}

impl Pass {
    /// Once durable, update the in-memory indexes, announce, and answer.
    /// Passes are finished in commit order, on the finisher thread, so
    /// jobs only become takeable, and callers are only answered, once
    /// their writes are durable.
    fn finish(mut self, cx: &WriterContext) {
        if let Some(durable) = self.durable.take()
            && let Err(e) = durable.wait()
        {
            tracing::error!(error = %e, "writer: persisting a pass failed");
            self.fail(cx, &e.to_string());
            return;
        }

        if let Some(group) = self.enqueue {
            group.finish(&cx.dispatch, &cx.scheduled_index, &cx.event_tx);
        }
        if let Some(group) = self.take {
            group.finish();
        }
        if let Some(group) = self.complete {
            group.finish(&cx.dispatch, &cx.in_flight_index, &cx.event_tx);
        }
        if let Some(group) = self.job {
            group.finish(cx);
        }
    }

    /// Fail every op, after a commit that didn't happen or didn't become
    /// durable. Nothing may go on referencing the pass's new jobs, so the
    /// budgets staged for them are undone.
    fn fail(self, cx: &WriterContext, msg: &str) {
        if let Some(group) = self.enqueue {
            group.unstage(&cx.budgets);
            group.fail(msg);
        }
        if let Some(group) = self.take {
            group.fail(msg);
        }
        if let Some(group) = self.complete {
            group.fail(msg);
        }
        if let Some(group) = self.job {
            group.fail(msg);
        }
    }
}

/// Put groups' ops back at the front of the carry, in their original
/// kinds' order, for the next pass.
fn requeue(
    carry: &mut VecDeque<WriteOp>,
    enqueue: Option<EnqueueGroup>,
    complete: Option<CompleteGroup>,
    take: Option<TakeGroup>,
    job: Option<JobGroup>,
) {
    let ops: Vec<WriteOp> = enqueue
        .into_iter()
        .flat_map(EnqueueGroup::into_ops)
        .map(WriteOp::Enqueue)
        .chain(
            take.into_iter()
                .flat_map(TakeGroup::into_ops)
                .map(WriteOp::Take),
        )
        .chain(
            complete
                .into_iter()
                .flat_map(CompleteGroup::into_ops)
                .map(WriteOp::Complete),
        )
        .chain(
            job.into_iter()
                .flat_map(JobGroup::into_ops)
                .map(|op| WriteOp::Job(Box::new(op))),
        )
        .collect();
    for op in ops.into_iter().rev() {
        carry.push_front(op);
    }
}

fn stricter(a: CommitMode, b: CommitMode) -> CommitMode {
    if a == CommitMode::Fsync || b == CommitMode::Fsync {
        CommitMode::Fsync
    } else {
        CommitMode::Buffered
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Enqueue,
    Complete,
    Take,
    Job,
}

/// What gathering needs to know about an op.
trait Pending {
    fn kind(&self) -> Kind;
    fn jobs(&self) -> usize;
}

impl Pending for WriteOp {
    fn kind(&self) -> Kind {
        match self {
            WriteOp::Enqueue(_) => Kind::Enqueue,
            WriteOp::Complete(_) => Kind::Complete,
            WriteOp::Take(_) => Kind::Take,
            WriteOp::Job(_) => Kind::Job,
        }
    }

    fn jobs(&self) -> usize {
        match self {
            WriteOp::Enqueue(op) => op.prepared.len(),
            WriteOp::Complete(op) => op.ids.len(),
            WriteOp::Take(op) => op.writes.len(),
            WriteOp::Job(_) => 1,
        }
    }
}

/// How much one pass may take of each kind.
struct Limits {
    /// Most enqueue requests (`--enqueue-batch-size`).
    enqueue_ops: usize,
    /// Most jobs in a pass (`--write-batch-max-jobs`), 0 for no limit.
    /// Takes and completions are admitted first and count towards it;
    /// enqueues fill whatever room is left. Keeps a burst of enqueues
    /// from crowding out the draining of the queue.
    ///
    /// When draining alone fills a pass, enqueues are left out of it,
    /// but never from two passes in a row, so workers get passes free of
    /// a large enqueue and producers still progress. Otherwise at least
    /// one enqueue request is admitted, whole: a request is never split.
    max_jobs: usize,
    /// Most completion requests (`--complete-batch-size`).
    complete_ops: usize,
    /// Most takes.
    take_ops: usize,
    /// Most single-job writes. Admitted with takes and completions:
    /// nacks and requeues are draining too.
    job_ops: usize,
}

impl Limits {
    /// Also the channel capacity, so backpressure starts where one pass
    /// could no longer absorb everything waiting.
    fn total_ops(&self) -> usize {
        self.enqueue_ops + self.complete_ops + self.take_ops + self.job_ops
    }
}

#[derive(Default)]
struct Counts {
    enqueue_ops: usize,
    complete_ops: usize,
    take_ops: usize,
    job_ops: usize,
    jobs: usize,
}

impl Counts {
    fn fits(&self, op: &impl Pending, limits: &Limits) -> bool {
        match op.kind() {
            Kind::Enqueue => {
                self.enqueue_ops < limits.enqueue_ops
                    && (limits.max_jobs == 0
                        || self.enqueue_ops == 0
                        || self.jobs + op.jobs() <= limits.max_jobs)
            }
            Kind::Complete => self.complete_ops < limits.complete_ops,
            Kind::Take => self.take_ops < limits.take_ops,
            Kind::Job => self.job_ops < limits.job_ops,
        }
    }

    fn add(&mut self, op: &impl Pending) {
        match op.kind() {
            Kind::Enqueue => self.enqueue_ops += 1,
            Kind::Complete => self.complete_ops += 1,
            Kind::Take => self.take_ops += 1,
            Kind::Job => self.job_ops += 1,
        }
        self.jobs += op.jobs();
    }
}

/// Gather the next pass from the carried ops, then whatever is already
/// waiting on the channel, blocking only for the first op.
///
/// Takes and completions are admitted first, within their limits, then
/// enqueues into the room they leave, or none at all if draining fills
/// the pass and the previous pass didn't already leave them out (see
/// [`Limits::max_jobs`]). `skipped_enqueues` carries that between passes.
/// The pass keeps the ops' arrival order, and ops not admitted are kept,
/// in order, as the new carry. Returns `None` once the channel is closed
/// and nothing is carried.
fn gather<T: Pending>(
    carry: &mut VecDeque<T>,
    skipped_enqueues: &mut bool,
    rx: &Receiver<T>,
    limits: &Limits,
) -> Option<Vec<T>> {
    let first = match carry.pop_front() {
        Some(op) => op,
        None => rx.recv().ok()?,
    };

    let mut candidates = vec![first];
    candidates.extend(carry.drain(..));
    let mut received = 0;
    while received < limits.total_ops() {
        match rx.try_recv() {
            Ok(op) => {
                received += 1;
                candidates.push(op);
            }
            Err(_) => break,
        }
    }

    let mut counts = Counts::default();
    let mut admitted = vec![false; candidates.len()];
    for draining in [true, false] {
        if !draining {
            let full = limits.max_jobs > 0 && counts.jobs >= limits.max_jobs;
            let waiting = candidates.iter().any(|op| op.kind() == Kind::Enqueue);
            *skipped_enqueues = full && waiting && !*skipped_enqueues;
            if *skipped_enqueues {
                break;
            }
        }
        for (i, op) in candidates.iter().enumerate() {
            if (op.kind() != Kind::Enqueue) == draining && counts.fits(op, limits) {
                counts.add(op);
                admitted[i] = true;
            }
        }
    }

    let mut batch = Vec::with_capacity(candidates.len());
    for (op, admitted) in candidates.into_iter().zip(admitted) {
        if admitted {
            batch.push(op);
        } else {
            carry.push_back(op);
        }
    }
    Some(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Op(Kind, usize);

    impl Pending for Op {
        fn kind(&self) -> Kind {
            self.0
        }
        fn jobs(&self) -> usize {
            self.1
        }
    }

    use Kind::{Complete as C, Enqueue as E, Job as J, Take as T};

    fn limits(max_jobs: usize) -> Limits {
        Limits {
            enqueue_ops: 1000,
            max_jobs,
            complete_ops: 2,
            take_ops: 1000,
            job_ops: 2,
        }
    }

    fn queued(ops: Vec<Op>) -> Receiver<Op> {
        let (tx, rx) = sync_channel(ops.len().max(1));
        for op in ops {
            tx.send(op).unwrap();
        }
        rx
    }

    #[test]
    fn gathers_every_kind_into_one_pass() {
        let rx = queued(vec![Op(T, 10), Op(C, 5), Op(E, 50)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(T, 10), Op(C, 5), Op(E, 50)]);
        assert!(carry.is_empty());
    }

    #[test]
    fn enqueues_past_the_job_limit_are_carried() {
        let rx = queued(vec![Op(E, 150), Op(T, 10), Op(E, 100)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(E, 150), Op(T, 10)]);
        assert_eq!(carry, VecDeque::from([Op(E, 100)]));
    }

    /// Draining comes first: takes and completions fill the pass before
    /// enqueues, and below the limit one enqueue request is always
    /// admitted.
    #[test]
    fn takes_and_completions_are_admitted_before_enqueues() {
        let rx = queued(vec![Op(E, 100), Op(E, 50), Op(T, 150), Op(C, 40)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(E, 100), Op(T, 150), Op(C, 40)]);
        assert_eq!(carry, VecDeque::from([Op(E, 50)]));
    }

    /// Fails and requeues are draining work, admitted with takes and
    /// completions ahead of enqueues, each counting as one job, within
    /// their own request limit.
    #[test]
    fn single_job_writes_are_admitted_with_draining() {
        // The two single-job writes admitted bring draining to exactly the
        // limit, so the enqueue sits this pass out.
        let rx = queued(vec![Op(E, 100), Op(J, 1), Op(T, 198), Op(J, 1), Op(J, 1)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(J, 1), Op(T, 198), Op(J, 1)]);
        assert_eq!(carry, VecDeque::from([Op(E, 100), Op(J, 1)]));
    }

    /// When draining alone fills a pass, enqueues sit that pass out, but
    /// never two in a row.
    #[test]
    fn enqueues_skip_a_pass_draining_fills_but_never_two() {
        let mut skipped = false;
        let mut carry = VecDeque::new();

        let rx = queued(vec![Op(E, 100), Op(T, 150), Op(C, 60)]);
        let batch = gather(&mut carry, &mut skipped, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(T, 150), Op(C, 60)]);
        assert_eq!(carry, VecDeque::from([Op(E, 100)]));
        assert!(skipped);

        let rx = queued(vec![Op(T, 150), Op(C, 60)]);
        let batch = gather(&mut carry, &mut skipped, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(E, 100), Op(T, 150), Op(C, 60)]);
        assert!(carry.is_empty());
        assert!(!skipped);
    }

    /// A pass that drains without any enqueue waiting doesn't use up the
    /// skip: the next waiting enqueue can still sit one pass out.
    #[test]
    fn a_pass_without_enqueues_waiting_is_not_a_skip() {
        let mut skipped = false;
        let mut carry = VecDeque::new();

        let rx = queued(vec![Op(T, 250)]);
        gather(&mut carry, &mut skipped, &rx, &limits(200)).unwrap();
        assert!(!skipped);

        let rx = queued(vec![Op(T, 250), Op(E, 10)]);
        let batch = gather(&mut carry, &mut skipped, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(T, 250)]);
        assert!(skipped);
    }

    #[test]
    fn enqueues_fill_the_room_takes_leave() {
        let rx = queued(vec![Op(E, 50), Op(E, 50), Op(E, 50), Op(T, 100)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(E, 50), Op(E, 50), Op(T, 100)]);
        assert_eq!(carry, VecDeque::from([Op(E, 50)]));
    }

    #[test]
    fn the_first_enqueue_is_taken_whole_however_large() {
        let rx = queued(vec![Op(E, 10)]);
        let mut carry = VecDeque::from([Op(E, 1000)]);
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(E, 1000)]);
        assert_eq!(carry, VecDeque::from([Op(E, 10)]));
    }

    #[test]
    fn zero_means_no_enqueue_job_limit() {
        let rx = queued(vec![Op(E, 1000), Op(E, 1000)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(0)).unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn request_limits_apply_per_kind() {
        let rx = queued(vec![Op(C, 1), Op(C, 1), Op(C, 1), Op(T, 1)]);
        let mut carry = VecDeque::new();
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(C, 1), Op(C, 1), Op(T, 1)]);
        assert_eq!(carry, VecDeque::from([Op(C, 1)]));
    }

    #[test]
    fn carried_ops_come_before_newer_ones() {
        let rx = queued(vec![Op(T, 1)]);
        let mut carry = VecDeque::from([Op(C, 1), Op(E, 5)]);
        let batch = gather(&mut carry, &mut false, &rx, &limits(200)).unwrap();
        assert_eq!(batch, vec![Op(C, 1), Op(E, 5), Op(T, 1)]);
    }

    #[test]
    fn ends_once_closed_and_nothing_is_carried() {
        let (tx, rx) = sync_channel::<Op>(1);
        drop(tx);
        let mut carry = VecDeque::from([Op(T, 1)]);
        assert_eq!(
            gather(&mut carry, &mut false, &rx, &limits(200)),
            Some(vec![Op(T, 1)])
        );
        assert_eq!(gather(&mut carry, &mut false, &rx, &limits(200)), None);
    }

    #[test]
    fn a_pass_is_fsynced_if_anything_in_it_needs_fsync() {
        use CommitMode::{Buffered, Fsync};
        assert_eq!(stricter(Buffered, Buffered), Buffered);
        assert_eq!(stricter(Fsync, Buffered), Fsync);
        assert_eq!(stricter(Buffered, Fsync), Fsync);
    }

    mod passes {
        use std::collections::HashSet;

        use fjall::Slice;

        use super::super::*;
        use std::time::Duration;

        use crate::store::keys::{make_job_key, make_status_key, make_unique_key};
        use crate::store::options::EnqueueOptions;
        use crate::store::store::Durable;
        use crate::store::store::Store;
        use crate::store::test_support::test_store;
        use crate::store::types::JobStatus;
        use crate::time::now_millis;

        fn context(store: &Store) -> WriterContext {
            WriterContext {
                ks: store.ks.clone(),
                dispatch: store.dispatch.clone(),
                budgets: store.budgets.clone(),
                scheduled_index: store.scheduled_index.clone(),
                in_flight_index: store.in_flight_index.clone(),
                event_tx: store.event_tx.clone(),
            }
        }

        /// Run and finish one pass on a blocking thread, as the writer and
        /// its finisher do: waiting for durability blocks.
        async fn pass(store: &Store, batch: Vec<WriteOp>) -> VecDeque<WriteOp> {
            let cx = context(store);
            tokio::task::spawn_blocking(move || {
                let mut carry = VecDeque::new();
                if let Some(pass) = run_pass(&cx, batch, &mut carry) {
                    pass.finish(&cx);
                }
                carry
            })
            .await
            .unwrap()
        }

        fn complete_op(
            store: &Store,
            id: &str,
        ) -> (
            CompleteOp,
            oneshot::Receiver<Result<crate::store::complete::CompleteOutcome, StoreError>>,
        ) {
            let (reply, rx) = oneshot::channel();
            let op =
                CompleteOp::prepare(&[id.to_string()], now_millis(), &store.ks, 0, reply).unwrap();
            (op, rx)
        }

        fn completed(
            outcome: Result<crate::store::complete::CompleteOutcome, StoreError>,
        ) -> Vec<String> {
            match outcome.unwrap() {
                crate::store::complete::CompleteOutcome::Done(result) => result.completed,
                crate::store::complete::CompleteOutcome::Conflict => {
                    panic!("expected the completion to be done, not to conflict")
                }
            }
        }

        async fn enqueue(store: &Store) -> String {
            store
                .enqueue(
                    now_millis(),
                    EnqueueOptions::new("test", "q", serde_json::json!({})),
                )
                .await
                .unwrap()
                .into_job()
                .id
        }

        fn record(store: &Store, id: &str) -> Slice {
            store.ks.data.get(make_job_key(id)).unwrap().unwrap()
        }

        /// A take write marking `id` in flight, as pre-read now.
        fn take_write(store: &Store, id: &str) -> TakeWrite {
            TakeWrite {
                job_key: make_job_key(id),
                pre_bytes: record(store, id),
                updated: Slice::from(b"in flight".as_slice()),
                old_status_key: make_status_key(JobStatus::Ready, id),
                new_status_key: make_status_key(JobStatus::InFlight, id),
                release_keys: Vec::new(),
                job_id: id.to_string(),
            }
        }

        #[tokio::test]
        async fn one_pass_applies_every_kind() {
            let store = test_store();
            enqueue(&store).await;
            let in_flight = store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .unwrap()
                .id;
            let ready = enqueue(&store).await;

            let prepared = crate::store::enqueue::prepare_enqueue(
                EnqueueOptions::new("test", "q", serde_json::json!({})),
                now_millis(),
            )
            .unwrap();
            let (enqueue_reply, enqueue_rx) = oneshot::channel();
            let (take_reply, take_rx) = oneshot::channel();
            let (complete, complete_rx) = complete_op(&store, &in_flight);

            let carry = pass(
                &store,
                vec![
                    WriteOp::Enqueue(EnqueueOp {
                        prepared: vec![prepared],
                        reply: enqueue_reply,
                    }),
                    WriteOp::Take(TakeOp {
                        writes: vec![take_write(&store, &ready)],
                        reply: take_reply,
                    }),
                    WriteOp::Complete(complete),
                ],
            )
            .await;

            assert!(carry.is_empty());
            assert!(matches!(
                enqueue_rx.await.unwrap().unwrap().as_slice(),
                [EnqueueResult::Created(_)]
            ));
            assert_eq!(take_rx.await.unwrap().unwrap(), TakeOutcome::Committed);
            assert_eq!(
                completed(complete_rx.await.unwrap()),
                vec![in_flight.clone()]
            );
            assert_eq!(&*record(&store, &ready), b"in flight");
            assert!(
                store
                    .ks
                    .data
                    .get(make_job_key(&in_flight))
                    .unwrap()
                    .is_none()
            );
        }

        /// A take with a job changed since its pre-read writes none of its
        /// jobs, and the other take in the pass still commits.
        #[tokio::test]
        async fn a_conflicting_take_writes_nothing_and_others_commit() {
            let store = test_store();
            let a = enqueue(&store).await;
            let b = enqueue(&store).await;
            let c = enqueue(&store).await;

            let (first_reply, first_rx) = oneshot::channel();
            let (second_reply, second_rx) = oneshot::channel();
            let first = TakeOp {
                writes: vec![take_write(&store, &a)],
                reply: first_reply,
            };
            let second = TakeOp {
                writes: vec![take_write(&store, &b), take_write(&store, &c)],
                reply: second_reply,
            };
            let b_before = record(&store, &b);

            // `c` changes after the second take pre-read it.
            let ks = store.ks.clone();
            let key = make_job_key(&c);
            tokio::task::spawn_blocking(move || {
                let mut tx = ks.write_tx();
                tx.insert(&ks.data, key, b"changed");
                ks.commit(tx, ks.default_commit_mode).unwrap();
            })
            .await
            .unwrap();

            pass(&store, vec![WriteOp::Take(first), WriteOp::Take(second)]).await;

            assert_eq!(first_rx.await.unwrap().unwrap(), TakeOutcome::Committed);
            assert_eq!(second_rx.await.unwrap().unwrap(), TakeOutcome::Conflict);
            assert_eq!(&*record(&store, &a), b"in flight");
            assert_eq!(record(&store, &b), b_before);
            assert_eq!(&*record(&store, &c), b"changed");
        }

        /// Many concurrent takes, gathered into shared passes, take every
        /// job exactly once.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_takes_take_every_job_exactly_once() {
            const JOBS: usize = 2_000;
            let store = std::sync::Arc::new(test_store());
            for _ in 0..JOBS {
                enqueue(&store).await;
            }

            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..16 {
                let store = store.clone();
                tasks.spawn(async move {
                    let mut taken = Vec::new();
                    loop {
                        let jobs = store
                            .take_next_n_jobs(now_millis(), &HashSet::new(), 25)
                            .await
                            .unwrap();
                        if jobs.is_empty() {
                            return taken;
                        }
                        taken.extend(jobs.into_iter().map(|job| job.id));
                    }
                });
            }

            let mut all = Vec::new();
            while let Some(taken) = tasks.join_next().await {
                all.extend(taken.unwrap());
            }
            let unique: HashSet<_> = all.iter().collect();
            assert_eq!(all.len(), JOBS);
            assert_eq!(unique.len(), JOBS);
            assert_eq!(store.in_flight_count(), JOBS);
        }

        /// One kind failing to apply fails that kind's ops, and carries
        /// the rest, unanswered and in order, to the next pass, which
        /// completes them. Nothing of theirs is committed meanwhile.
        #[tokio::test]
        async fn a_failing_kind_fails_alone_and_the_rest_are_carried() {
            let store = test_store();
            enqueue(&store).await;
            let in_flight = store
                .take_next_job(now_millis(), &HashSet::new())
                .await
                .unwrap()
                .unwrap()
                .id;
            let ready = enqueue(&store).await;

            // A unique-index entry that isn't valid UTF-8 makes applying
            // an enqueue with that key fail with a corruption error.
            let ks = store.ks.clone();
            tokio::task::spawn_blocking(move || {
                let mut tx = ks.write_tx();
                tx.insert(&ks.index, make_unique_key("corrupt"), vec![0xff, 0xfe]);
                ks.commit(tx, ks.default_commit_mode).unwrap();
            })
            .await
            .unwrap();

            let corrupt = crate::store::enqueue::prepare_enqueue(
                EnqueueOptions::new("test", "q", serde_json::json!({})).unique_key("corrupt"),
                now_millis(),
            )
            .unwrap();
            let ready_before = record(&store, &ready);

            let (enqueue_reply, enqueue_rx) = oneshot::channel();
            let (take_reply, mut take_rx) = oneshot::channel();
            let (complete, mut complete_rx) = complete_op(&store, &in_flight);

            let carry = pass(
                &store,
                vec![
                    WriteOp::Enqueue(EnqueueOp {
                        prepared: vec![corrupt],
                        reply: enqueue_reply,
                    }),
                    WriteOp::Take(TakeOp {
                        writes: vec![take_write(&store, &ready)],
                        reply: take_reply,
                    }),
                    WriteOp::Complete(complete),
                ],
            )
            .await;

            assert!(enqueue_rx.await.unwrap().is_err());
            assert!(take_rx.try_recv().is_err());
            assert!(complete_rx.try_recv().is_err());
            assert!(matches!(
                carry
                    .iter()
                    .map(Pending::kind)
                    .collect::<Vec<_>>()
                    .as_slice(),
                [Kind::Take, Kind::Complete]
            ));
            assert_eq!(record(&store, &ready), ready_before);
            assert!(
                store
                    .ks
                    .data
                    .get(make_job_key(&in_flight))
                    .unwrap()
                    .is_some()
            );

            let carry = pass(&store, carry.into_iter().collect()).await;

            assert!(carry.is_empty());
            assert_eq!(take_rx.await.unwrap().unwrap(), TakeOutcome::Committed);
            assert_eq!(completed(complete_rx.await.unwrap()), vec![in_flight]);
        }

        /// Dropping the last handle lets the thread finish everything
        /// queued and carried before it exits, so no caller is left
        /// waiting.
        #[tokio::test]
        async fn shutdown_finishes_queued_and_carried_ops() {
            let store = test_store();
            let writer = Writer::start(
                context(&store),
                WriterConfig {
                    enqueue_batch_size: 1000,
                    // One job per pass, so most enqueues are carried.
                    write_batch_max_jobs: 1,
                    complete_batch_size: 1000,
                },
            );

            let replies: Vec<_> = (0..5)
                .map(|_| {
                    let prepared = crate::store::enqueue::prepare_enqueue(
                        EnqueueOptions::new("test", "q", serde_json::json!({})),
                        now_millis(),
                    )
                    .unwrap();
                    writer.submit_enqueue(vec![prepared])
                })
                .collect();
            drop(writer);

            for reply in replies {
                let results = reply.await.expect("op dropped without a reply").unwrap();
                assert!(matches!(results.as_slice(), [EnqueueResult::Created(_)]));
            }
        }

        /// A pass with only completions, finished once `durable` is.
        fn completions(op: CompleteOp, durable: Option<Durable>) -> Pass {
            Pass {
                enqueue: None,
                take: None,
                complete: Some(CompleteGroup::new(vec![op])),
                job: None,
                durable,
            }
        }

        /// Passes are answered in commit order. A later pass, even one
        /// that wrote nothing and has nothing to wait for, isn't answered
        /// until every earlier pass is durable.
        #[tokio::test]
        async fn passes_are_answered_in_commit_order() {
            let store = test_store();
            let (first, mut first_rx) = complete_op(&store, "first");
            let (second, mut second_rx) = complete_op(&store, "second");
            let (persisted, durable) = Durable::pending();

            let (finish_tx, finish_rx) = std::sync::mpsc::sync_channel(FINISH_QUEUE);
            let cx = context(&store);
            let finisher = std::thread::spawn(move || finish_passes(&cx, finish_rx));

            finish_tx.send(completions(first, Some(durable))).unwrap();
            finish_tx.send(completions(second, None)).unwrap();

            let waited = tokio::time::timeout(Duration::from_millis(100), &mut second_rx).await;
            assert!(
                waited.is_err(),
                "answered before the pass before it was durable"
            );
            assert!(first_rx.try_recv().is_err());

            persisted.send(Ok(())).unwrap();
            assert_eq!(completed(first_rx.await.unwrap()), Vec::<String>::new());
            assert_eq!(completed(second_rx.await.unwrap()), Vec::<String>::new());

            drop(finish_tx);
            finisher.join().unwrap();
        }
    }
}
