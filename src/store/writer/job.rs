// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Single-job writes within a writer pass.
//!
//! Enqueues, takes and completions are batched across requests, each
//! kind in its own way. Everything else changes one job per request, in
//! the same shape: the caller pre-reads the job and works out its new
//! state, and the writer checks the job is unchanged before writing it.
//! Those ops share one kind of write, [`JobOp`], which lists them all.
//!
//! An op whose job changed since its pre-read writes nothing and is
//! answered [`JobOutcome::Conflict`], for its caller to pre-read again
//! and resubmit. Checks read through the pass's transaction, so they see
//! writes made earlier in the same pass.

use super::super::fail::FailOp;
use super::super::requeue::RequeueOp;
use super::super::store::Keyspaces;
use super::super::types::StoreError;
use super::WriterContext;

/// What became of a single-job write.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::store) enum JobOutcome<T> {
    Done(T),

    /// The job changed since it was pre-read. Nothing was written: pre-read
    /// again and resubmit.
    Conflict,
}

pub(in crate::store) type JobReply<T> =
    tokio::sync::oneshot::Sender<Result<JobOutcome<T>, StoreError>>;

/// A write of one job, prepared by its caller.
pub(in crate::store) trait JobWrite {
    /// Check the job is still as pre-read, then write it into the pass's
    /// transaction. `Ok(false)` if it changed: nothing was written.
    fn apply(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<bool, StoreError>;

    /// Once durable: update the in-memory indexes, announce, and answer.
    fn finish(self, cx: &WriterContext);

    /// Answer that the job changed, and nothing was written.
    fn conflict(self);

    fn fail(self, msg: &str);
}

/// Declare [`JobOp`], with one variant per single-job write, and forward
/// [`JobWrite`] to each.
macro_rules! job_ops {
    ($($variant:ident($op:ty)),* $(,)?) => {
        /// Every single-job write the writer applies.
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
                &self,
                tx: &mut fjall::SingleWriterWriteTx<'_>,
                ks: &Keyspaces,
            ) -> Result<bool, StoreError> {
                match self {
                    $(JobOp::$variant(op) => op.apply(tx, ks)),*
                }
            }

            fn finish(self, cx: &WriterContext) {
                match self {
                    $(JobOp::$variant(op) => op.finish(cx)),*
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
}

/// The single-job writes in one pass.
pub(super) struct JobGroup {
    ops: Vec<JobOp>,
    /// Per op, once applied: whether it wrote, or conflicted.
    written: Vec<bool>,
}

impl JobGroup {
    pub(super) fn new(ops: Vec<JobOp>) -> Self {
        Self {
            ops,
            written: Vec::new(),
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
        self.written.clear();
        for i in 0..self.ops.len() {
            match self.ops[i].apply(tx, ks) {
                Ok(written) => self.written.push(written),
                Err(e) => {
                    self.written.clear();
                    self.ops.remove(i).fail(&e.to_string());
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// How many jobs this pass writes for single-job ops.
    pub(super) fn writes(&self) -> usize {
        self.written.iter().filter(|w| **w).count()
    }

    pub(super) fn finish(self, cx: &WriterContext) {
        for (op, written) in self.ops.into_iter().zip(self.written) {
            if written {
                op.finish(cx);
            } else {
                op.conflict();
            }
        }
    }

    pub(super) fn fail(self, msg: &str) {
        for op in self.ops {
            op.fail(msg);
        }
    }

    pub(super) fn into_ops(self) -> Vec<JobOp> {
        self.ops
    }
}
