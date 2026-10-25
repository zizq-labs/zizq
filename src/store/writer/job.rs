// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Writes applied one request at a time, within a writer pass.
//!
//! Enqueues, takes and completions are batched across requests, each
//! kind in its own way. Everything else is applied request by request,
//! and shares one kind of write, [`JobOp`], which lists them all.
//!
//! Most change one job, in the same shape: the caller pre-reads the job
//! and works out its new state, and the writer checks the job is
//! unchanged before writing it. An op whose job changed since its
//! pre-read writes nothing and is answered [`JobOutcome::Conflict`], for
//! its caller to pre-read again and resubmit. Checks read through the
//! pass's transaction, so they see writes made earlier in the same pass.
//!
//! Bulk patches and deletes change every job a filter matches, all or
//! nothing. They are *exclusive*: each runs in a pass of its own, so the
//! scan for matching jobs sees exactly the state its writes go into.

use super::super::budget::{BudgetOp, JobBudgetOp, JobsBudgetOp};
use super::super::cron::CronOp;
use super::super::delete::{BulkDeleteOp, PurgeOp, ReapOp};
use super::super::fail::FailOp;
use super::super::patch::{BulkPatchOp, PatchOp};
use super::super::requeue::RequeueOp;
use super::super::scheduled::PromoteOp;
use super::super::store::Keyspaces;
use super::super::types::StoreError;
use super::WriterContext;

/// What became of a write.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::store) enum JobOutcome<T> {
    Done(T),

    /// The job changed since it was pre-read. Nothing was written: pre-read
    /// again and resubmit.
    Conflict,
}

pub(in crate::store) type JobReply<T> =
    tokio::sync::oneshot::Sender<Result<JobOutcome<T>, StoreError>>;

/// What applying a write did to the pass's transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::store) enum Applied {
    Wrote,

    /// Nothing to write, as when a filter matches no jobs. Still finished.
    Nothing,

    /// The job changed since it was pre-read. Nothing was written.
    Conflict,
}

/// A write applied on its own, prepared by its caller.
pub(in crate::store) trait JobWrite {
    /// Check the job is still as pre-read, then write it into the pass's
    /// transaction.
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError>;

    /// Whether the op needs a pass of its own.
    fn exclusive(&self) -> bool {
        false
    }

    /// How many jobs the op may write, counted against the pass's job
    /// limit. An exclusive op runs alone, so its count doesn't matter.
    fn jobs(&self) -> usize {
        1
    }

    /// Once durable (or at once, if nothing was written): update the
    /// in-memory indexes, announce, and answer.
    fn finish(self, cx: &WriterContext);

    /// Answer that the job changed, and nothing was written.
    fn conflict(self);

    /// The pass's writes were thrown away after this op applied: undo
    /// whatever `apply` changed outside the transaction. The op may be
    /// applied again in a later pass.
    fn discard(&mut self) {}

    fn fail(self, msg: &str);
}

/// Declare [`JobOp`], with one variant per kind of write, and forward
/// [`JobWrite`] to each.
macro_rules! job_ops {
    ($($variant:ident($op:ty)),* $(,)?) => {
        /// Every write the writer applies one request at a time.
        pub(in crate::store) enum JobOp {
            $($variant($op)),*
        }

        $(
            impl From<$op> for JobOp {
                fn from(op: $op) -> Self {
                    JobOp::$variant(op)
                }
            }
        )*

        impl JobWrite for JobOp {
            fn apply(
                &mut self,
                tx: &mut fjall::SingleWriterWriteTx<'_>,
                ks: &Keyspaces,
            ) -> Result<Applied, StoreError> {
                match self {
                    $(JobOp::$variant(op) => op.apply(tx, ks)),*
                }
            }

            fn exclusive(&self) -> bool {
                match self {
                    $(JobOp::$variant(op) => op.exclusive()),*
                }
            }

            fn jobs(&self) -> usize {
                match self {
                    $(JobOp::$variant(op) => op.jobs()),*
                }
            }

            fn finish(self, cx: &WriterContext) {
                match self {
                    $(JobOp::$variant(op) => op.finish(cx)),*
                }
            }

            fn discard(&mut self) {
                match self {
                    $(JobOp::$variant(op) => op.discard()),*
                }
            }

            fn conflict(self) {
                match self {
                    $(JobOp::$variant(op) => op.conflict()),*
                }
            }

            fn fail(self, msg: &str) {
                match self {
                    $(JobOp::$variant(op) => op.fail(msg)),*
                }
            }
        }
    };
}

job_ops! {
    Fail(FailOp),
    Requeue(RequeueOp),
    Patch(PatchOp),
    Purge(PurgeOp),
    BulkPatch(BulkPatchOp),
    BulkDelete(BulkDeleteOp),
    Promote(PromoteOp),
    Reap(ReapOp),
    Budget(BudgetOp),
    JobBudget(JobBudgetOp),
    JobsBudget(JobsBudgetOp),
    Cron(CronOp),
}

/// The one-at-a-time writes in one pass.
pub(super) struct JobGroup {
    ops: Vec<JobOp>,
    /// Per op, once applied.
    applied: Vec<Applied>,
}

impl JobGroup {
    pub(super) fn new(ops: Vec<JobOp>) -> Self {
        Self {
            ops,
            applied: Vec::new(),
        }
    }

    /// Check and write every op into the pass's transaction. An error is
    /// the op's own: that op is failed with it and taken out of the
    /// group, leaving the rest to be carried to the next pass.
    pub(super) fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<(), StoreError> {
        self.applied.clear();
        for i in 0..self.ops.len() {
            match self.ops[i].apply(tx, ks) {
                Ok(applied) => self.applied.push(applied),
                Err(e) => {
                    // The transaction goes, so the ops before this one
                    // are carried, and must not leave anything behind.
                    self.discard();
                    self.ops.remove(i).fail(&e.to_string());
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// How many ops in this pass wrote anything.
    pub(super) fn writes(&self) -> usize {
        self.applied
            .iter()
            .filter(|a| **a == Applied::Wrote)
            .count()
    }

    pub(super) fn finish(self, cx: &WriterContext) {
        for (op, applied) in self.ops.into_iter().zip(self.applied) {
            match applied {
                Applied::Wrote | Applied::Nothing => op.finish(cx),
                Applied::Conflict => op.conflict(),
            }
        }
    }

    /// Fail every op, after a commit that didn't happen or didn't become
    /// durable.
    pub(super) fn fail(mut self, msg: &str) {
        self.discard();
        for op in self.ops {
            op.fail(msg);
        }
    }

    /// Undo what the ops that wrote did outside the transaction.
    fn discard(&mut self) {
        for (op, applied) in self.ops.iter_mut().zip(self.applied.drain(..)) {
            if applied == Applied::Wrote {
                op.discard();
            }
        }
    }

    pub(super) fn into_ops(self) -> Vec<JobOp> {
        self.ops
    }
}
