// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Cron group/entry CRUD, scheduler integration, and recovery rebuild.
//!
//! Lives in a submodule of `cron/` so that helpers (key builders,
//! `merge_cron_entry`, `cron_next_after`) stay package-private to the
//! cron subsystem.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use fjall::{Readable, Slice};
use tokio::task;

use super::super::budget::{
    Budget, BudgetPlan, Budgets, plan_budgets, stage_budgets, sync_created_budgets,
    unstage_budgets, write_created_budgets,
};
use super::super::enqueue::{
    PreparedEnqueue, apply_enqueue, finalize_enqueue, plan_op_budgets, prepare_enqueue,
    validate_batch_config,
};
use super::super::keys::RecordKind;
use super::super::options::{CronEntryOptions, PatchCronGroupOptions, ReplaceCronGroupOptions};
use super::super::results::EnqueueResult;
use super::super::store::{Keyspaces, Store, StoreEvent};
use super::super::types::{Job, StoreError};
use super::super::writer::{Applied, JobOutcome, JobReply, JobWrite, WriterContext};
use super::{CronEntry, CronGroup, CronScheduleIndex};

impl Store {
    /// Replace all entries in a cron group.
    ///
    /// Performs a smart merge: entries with unchanged expressions preserve
    /// their scheduling state (`next_enqueue_at`, `last_enqueue_at`).
    /// Entries with changed expressions or new entries get `next_enqueue_at`
    /// computed from `now`. Entries absent from the input are deleted.
    ///
    /// The group metadata record is created if it doesn't exist.
    pub async fn replace_cron_group(
        &self,
        group: &str,
        opts: ReplaceCronGroupOptions,
        now: u64,
    ) -> Result<(CronGroup, Vec<CronEntry>), StoreError> {
        // Validated before it reaches the writer. The group timezone is
        // checked even when no entry inherits it, or it would be stored
        // unchecked and only fail later. It comes from the request, so
        // each entry's effective timezone resolves without a read.
        validate_timezone(opts.timezone.as_deref())?;
        for input in &opts.entries {
            let tz = input.timezone.as_deref().or(opts.timezone.as_deref());
            cron_next_after(&input.expression, now, tz)?;
            if let Some(ref cfg) = input.job.batch {
                validate_batch_config(cfg, &input.job.payload)?;
            }
        }

        let change = CronChange::ReplaceGroup {
            group: group.to_string(),
            opts,
        };
        match self.write_cron(change, now).await? {
            CronWritten::Group(group, entries) => Ok((group, entries)),
            _ => Err(StoreError::Internal("cron replace wrote no group".into())),
        }
    }

    /// List all cron group names.
    ///
    /// Uses a prefix-stepping range scan over `C` tag keys, extracting
    /// distinct group names. Same O(n) approach as `list_queues`.
    pub async fn list_cron_groups(&self) -> Result<Vec<String>, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();

            let end: Vec<u8> = vec![RecordKind::Cron as u8 + 1];
            let mut start: Vec<u8> = vec![RecordKind::Cron as u8];
            let mut groups = Vec::new();

            loop {
                let mut range = snapshot.range::<Vec<u8>, _>(
                    ks.data.as_ref(),
                    (Bound::Included(start.clone()), Bound::Excluded(end.clone())),
                );

                let entry = match range.next() {
                    Some(entry) => entry,
                    None => break,
                };

                let (key, _) = entry.into_inner()?;

                // Key layout: C{group_name}\0... — extract group_name.
                let name_start = 1; // skip C tag
                let name_end = key[name_start..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|p| name_start + p)
                    .unwrap_or(key.len());
                let group_name = std::str::from_utf8(&key[name_start..name_end])
                    .map_err(|e| {
                        StoreError::Corruption(format!("cron group name is not valid UTF-8: {e}"))
                    })?
                    .to_string();
                groups.push(group_name.clone());

                // Advance past all keys for this group: C{group_name}\x01
                start.truncate(1); // keep C tag
                start.extend_from_slice(group_name.as_bytes());
                start.push(1); // one byte past \0 separator
            }

            Ok(groups)
        })
        .await?
    }

    /// Load a cron group and all its entries.
    ///
    /// Returns `None` if the group does not exist.
    pub async fn get_cron_group(
        &self,
        group: &str,
    ) -> Result<Option<(CronGroup, Vec<CronEntry>)>, StoreError> {
        let ks = self.ks.clone();
        let group = group.to_string();

        task::spawn_blocking(
            move || -> Result<Option<(CronGroup, Vec<CronEntry>)>, StoreError> {
                let group_key = make_cron_group_key(&group);
                let prefix = make_cron_group_prefix(&group);

                let snapshot = ks.db.read_tx();

                let group_meta: CronGroup = match snapshot.get(&ks.data, &group_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => return Ok(None),
                };

                let mut range_end = prefix.clone();
                *range_end.last_mut().unwrap() = 1; // \0 → \1

                let entries: Vec<CronEntry> = snapshot
                    .range::<Vec<u8>, _>(
                        ks.data.as_ref(),
                        (Bound::Included(prefix.clone()), Bound::Excluded(range_end)),
                    )
                    .skip(1) // skip group metadata key
                    .map(|guard| {
                        let (_, value) = guard.into_inner()?;
                        let entry: CronEntry = rmp_serde::from_slice(&value)?;
                        Ok(entry)
                    })
                    .collect::<Result<_, StoreError>>()?;

                Ok(Some((group_meta, entries)))
            },
        )
        .await?
    }

    /// Add a single entry to a cron group.
    ///
    /// Creates the group if it does not exist. Returns `Err` with
    /// `StoreError::Conflict` if an entry with the same name already
    /// exists in the group.
    pub async fn add_cron_entry(
        &self,
        group: &str,
        opts: CronEntryOptions,
        now: u64,
    ) -> Result<CronEntry, StoreError> {
        // The expression can't be checked yet: resolving the entry's
        // effective timezone needs the group, read in the writer's pass.
        if let Some(ref cfg) = opts.job.batch {
            validate_batch_config(cfg, &opts.job.payload)?;
        }
        let change = CronChange::AddEntry {
            group: group.to_string(),
            opts,
        };
        match self.write_cron(change, now).await? {
            CronWritten::Entry(entry) => Ok(entry),
            _ => Err(StoreError::Internal("cron add wrote no entry".into())),
        }
    }

    /// Create or replace a single entry in a cron group.
    ///
    /// Creates the group if it does not exist. If the entry already exists,
    /// applies the same smart merge as `replace_cron_group`: preserves
    /// scheduling state when the expression is unchanged, preserves pause
    /// state when omitted.
    pub async fn put_cron_entry(
        &self,
        group: &str,
        opts: CronEntryOptions,
        now: u64,
    ) -> Result<CronEntry, StoreError> {
        if let Some(ref cfg) = opts.job.batch {
            validate_batch_config(cfg, &opts.job.payload)?;
        }
        let change = CronChange::PutEntry {
            group: group.to_string(),
            opts,
        };
        match self.write_cron(change, now).await? {
            CronWritten::Entry(entry) => Ok(entry),
            _ => Err(StoreError::Internal("cron put wrote no entry".into())),
        }
    }

    /// Rewrite a stored entry's expression, bypassing validation.
    ///
    /// Reproduces an entry written by an earlier version whose
    /// expression this version no longer parses. That is the only way a
    /// stored entry can fail on promotion, so it is the only way to
    /// exercise the scheduler's skip-and-continue path.
    #[cfg(test)]
    pub(crate) async fn set_cron_expression_unvalidated(
        &self,
        group: &str,
        entry_name: &str,
        expression: &str,
    ) -> Result<(), StoreError> {
        let change = CronChange::SetExpressionUnvalidated {
            group: group.to_string(),
            name: entry_name.to_string(),
            expression: expression.to_string(),
        };
        self.write_cron(change, crate::time::now_millis())
            .await
            .map(|_| ())
    }

    /// Update a single cron entry's pause state.
    ///
    /// Returns the updated entry, or `None` if the group or entry does
    /// not exist.
    pub async fn patch_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
        paused: bool,
        now: u64,
    ) -> Result<Option<CronEntry>, StoreError> {
        let change = CronChange::PatchEntry {
            group: group.to_string(),
            name: entry_name.to_string(),
            paused,
        };
        match self.write_cron(change, now).await? {
            CronWritten::MaybeEntry(entry) => Ok(entry),
            _ => Err(StoreError::Internal("cron patch wrote no entry".into())),
        }
    }

    /// Delete a single entry from a cron group.
    ///
    /// Returns `true` if the entry existed and was deleted, `false` if
    /// the group or entry does not exist.
    pub async fn delete_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
    ) -> Result<bool, StoreError> {
        let change = CronChange::DeleteEntry {
            group: group.to_string(),
            name: entry_name.to_string(),
        };
        let now = crate::time::now_millis();
        Ok(matches!(
            self.write_cron(change, now).await?,
            CronWritten::Deleted(n) if n > 0
        ))
    }

    /// Update group-level metadata (pause state and default timezone).
    ///
    /// Follows JSON Merge Patch semantics — absent fields are left alone.
    /// Changing the timezone recomputes `next_enqueue_at` for every entry
    /// in the group that does not name a timezone of its own; entries that
    /// do are unaffected.
    ///
    /// Returns the updated group, or `None` if the group does not exist.
    pub async fn patch_cron_group(
        &self,
        group: &str,
        opts: PatchCronGroupOptions,
        now: u64,
    ) -> Result<Option<CronGroup>, StoreError> {
        if let Some(ref tz) = opts.timezone {
            validate_timezone(tz.as_deref())?;
        }
        let change = CronChange::PatchGroup {
            group: group.to_string(),
            opts,
        };
        match self.write_cron(change, now).await? {
            CronWritten::MaybeGroup(group) => Ok(group),
            _ => Err(StoreError::Internal(
                "cron group patch wrote no group".into(),
            )),
        }
    }

    /// Delete every cron group and entry in a single transaction.
    ///
    /// Returns the number of groups deleted. Emits a single
    /// `CronScheduleChanged` event regardless of how many groups were
    /// removed.
    pub async fn delete_cron_groups(&self) -> Result<usize, StoreError> {
        let now = crate::time::now_millis();
        let count = match self.write_cron(CronChange::DeleteAllGroups, now).await? {
            CronWritten::Deleted(count) => count,
            _ => 0,
        };

        // Reclaim tombstone space when the wipe was large enough.
        let threshold = self.config.auto_compact_threshold;
        if threshold > 0 && count as u64 >= threshold {
            self.compact_all().await?;
        }

        Ok(count)
    }

    /// Delete a cron group and all its entries.
    ///
    /// Returns `true` if the group existed and was deleted, `false` if it
    /// did not exist.
    pub async fn delete_cron_group(&self, group: &str) -> Result<bool, StoreError> {
        let change = CronChange::DeleteGroup {
            group: group.to_string(),
        };
        let now = crate::time::now_millis();
        Ok(matches!(
            self.write_cron(change, now).await?,
            CronWritten::Deleted(n) if n > 0
        ))
    }

    /// Have the writer apply a change to the cron records, and mirror it
    /// into the schedule index once durable.
    async fn write_cron(&self, change: CronChange, now: u64) -> Result<CronWritten, StoreError> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = CronOp {
            change,
            now,
            live: self.budgets.clone(),
            cron_index: self.cron_index.clone(),
            decided: None,
            reply,
        };
        let writer = self.writer.clone();
        task::spawn_blocking(move || writer.submit_job(op.into())).await?;

        match rx
            .await
            .map_err(|_| StoreError::Internal("writer channel closed".into()))??
        {
            JobOutcome::Done(written) => Ok(written),
            JobOutcome::Conflict => Err(StoreError::Internal(
                "cron write reported a conflict".into(),
            )),
        }
    }

    /// Load a single cron entry from the store.
    pub async fn get_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
    ) -> Result<Option<CronEntry>, StoreError> {
        let ks = self.ks.clone();
        let key = make_cron_entry_key(group, entry_name);

        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            match ks.data.get(&key)? {
                Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
        .await?
    }

    /// Peek at the earliest due timestamp in the cron schedule index.
    pub fn cron_next_due_at(&self) -> Option<u64> {
        self.cron_index.next_due_at()
    }

    /// Return all cron entries where `next_enqueue_at <= now`.
    ///
    /// Read-only — does not modify the in-memory index. The caller
    /// (cron scheduler) processes each entry via `promote_cron_entry`.
    pub fn next_due_cron_entries(&self, now: u64) -> Vec<(u64, String, String)> {
        self.cron_index.due_entries(now)
    }

    /// Atomically advance a due cron entry's schedule and enqueue its job.
    ///
    /// Pre-reads the entry and group, then has the writer check the entry
    /// is unchanged before advancing it. Retries if the entry was modified
    /// concurrently. Returns early without writing if the entry is paused,
    /// not actually due, or no longer exists.
    pub async fn promote_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
        now: u64,
    ) -> Result<Option<CronEntry>, StoreError> {
        let ks = self.ks.clone();
        let writer = self.writer.clone();
        let live = self.budgets.clone();
        let cron_index = self.cron_index.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        // Prepared on a blocking thread, so the writer only checks and
        // writes, and the reply is waited for there too: the cron
        // scheduler holds one blocking thread while its promotion is
        // written. A changed entry comes back as a conflict: pre-read
        // again.
        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            loop {
                let (reply, rx) = tokio::sync::oneshot::channel();
                let Some(op) = CronPromoteOp::prepare(
                    &ks,
                    &group,
                    &entry_name,
                    now,
                    live.clone(),
                    cron_index.clone(),
                    reply,
                )?
                else {
                    return Ok(None);
                };
                writer.submit_job(Box::new(op).into());

                match rx
                    .blocking_recv()
                    .map_err(|_| StoreError::Internal("writer channel closed".into()))??
                {
                    JobOutcome::Done(entry) => return Ok(Some(entry)),
                    JobOutcome::Conflict => continue,
                }
            }
        })
        .await?
    }

    /// Scan all cron entries on disk and reinsert each `next_enqueue_at`
    /// into the in-memory cron schedule index.
    ///
    /// Skips group metadata keys and entries with `next_enqueue_at = None`
    /// (one-shot expressions whose only occurrence has already fired).
    pub(in crate::store) async fn rebuild_cron_index(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();

        task::spawn_blocking(move || -> Result<usize, StoreError> {
            // Scan all keys with the cron tag prefix: [C .. D)
            let start = vec![RecordKind::Cron as u8];
            let end = vec![RecordKind::Cron as u8 + 1];
            let range = (Bound::Included(start), Bound::Excluded(end));

            let data: &fjall::Keyspace = ks.data.as_ref();
            let mut count = 0;

            for guard in data.range::<Vec<u8>, _>(range) {
                let (key, value) = guard.into_inner()?;

                // Group metadata keys end with \0 (no entry name after it).
                // Entry keys have content after the \0.
                if let Some(pos) = key.iter().position(|&b| b == 0) {
                    if pos + 1 >= key.len() {
                        // Group metadata — skip.
                        continue;
                    }
                }

                let entry: CronEntry = rmp_serde::from_slice(&value)?;
                if let Some(next) = entry.next_enqueue_at {
                    // Extract group name from key: C{group}\0{entry_name}
                    let group_end = key.iter().position(|&b| b == 0).unwrap();
                    let group = String::from_utf8(key[1..group_end].to_vec()).map_err(|e| {
                        StoreError::Corruption(format!("cron group name is not valid UTF-8: {e}"))
                    })?;

                    cron_index.insert(next, group, entry.name.clone());
                    count += 1;
                }
            }

            Ok(count)
        })
        .await?
    }
}

/// How a set of cron entries depends on one budget.
///
/// A cron entry is a *standing* claim on a budget, unlike a job, which
/// eventually drains. That is why deleting or shrinking a budget has to
/// consult the schedule and not just the queue.
pub(in crate::store) struct CronBudgetUsage {
    /// Largest cost any referencing template draws from it.
    pub(in crate::store) max_cost: u32,

    /// How many entries reference it.
    pub(in crate::store) entries: usize,

    /// One of them, as `group/entry`, so an error can name something
    /// concrete for the operator to go and look at.
    pub(in crate::store) example: String,
}

/// Find how the installed schedule depends on a budget, or `None` if
/// no cron entry references it.
///
/// Scans the cron keyspace, which is small by design — the number of
/// entries is expected to be in the hundreds — and is only consulted
/// when a budget is deleted or its allocation changed, both
/// operator-scale events rather than per-job ones.
///
/// `reader` should be the open write transaction, so the answer cannot
/// shift between the check and the write it guards.
pub(in crate::store) fn cron_budget_usage(
    reader: &impl Readable,
    ks: &Keyspaces,
    budget_key: &str,
) -> Result<Option<CronBudgetUsage>, StoreError> {
    let start = vec![RecordKind::Cron as u8];
    let end = vec![RecordKind::Cron as u8 + 1];

    let mut usage: Option<CronBudgetUsage> = None;

    for guard in
        reader.range::<Vec<u8>, _>(&ks.data, (Bound::Included(start), Bound::Excluded(end)))
    {
        let (key, value) = guard.into_inner()?;

        // Group metadata keys end with \0 and carry no entry name.
        let Some(sep) = key.iter().position(|&b| b == 0) else {
            continue;
        };
        if sep + 1 >= key.len() {
            continue;
        }

        let entry: CronEntry = rmp_serde::from_slice(&value)?;

        let Some(cost) = entry
            .job
            .budgets
            .iter()
            .filter(|b| b.key == budget_key)
            .map(|b| b.cost)
            .max()
        else {
            continue;
        };

        let group = String::from_utf8(key[1..sep].to_vec()).map_err(|e| {
            StoreError::Corruption(format!("cron group name is not valid UTF-8: {e}"))
        })?;

        usage = Some(match usage {
            None => CronBudgetUsage {
                max_cost: cost,
                entries: 1,
                example: format!("{group}/{}", entry.name),
            },
            Some(prev) => CronBudgetUsage {
                max_cost: prev.max_cost.max(cost),
                entries: prev.entries + 1,
                example: prev.example,
            },
        });
    }

    Ok(usage)
}

/// A change to the cron records.
pub(in crate::store) enum CronChange {
    ReplaceGroup {
        group: String,
        opts: ReplaceCronGroupOptions,
    },
    AddEntry {
        group: String,
        opts: CronEntryOptions,
    },
    PutEntry {
        group: String,
        opts: CronEntryOptions,
    },
    PatchEntry {
        group: String,
        name: String,
        paused: bool,
    },
    DeleteEntry {
        group: String,
        name: String,
    },
    PatchGroup {
        group: String,
        opts: PatchCronGroupOptions,
    },
    DeleteGroup {
        group: String,
    },
    DeleteAllGroups,
    #[cfg(test)]
    SetExpressionUnvalidated {
        group: String,
        name: String,
        expression: String,
    },
}

/// What a cron change answers with.
pub(in crate::store) enum CronWritten {
    /// A replaced group, with its entries.
    Group(CronGroup, Vec<CronEntry>),
    /// An added or put entry.
    Entry(CronEntry),
    /// A patched entry, or `None` if the group or entry doesn't exist.
    MaybeEntry(Option<CronEntry>),
    /// A patched group, or `None` if it doesn't exist.
    MaybeGroup(Option<CronGroup>),
    /// How many entries or groups were deleted.
    Deleted(usize),
}

/// Everything a cron change writes, decided before any of it is.
#[derive(Default)]
struct CronWrites {
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    removes: Vec<Vec<u8>>,
    created_budgets: Vec<(String, Budget)>,
    /// `(next_enqueue_at, group, entry)` to take out of the schedule
    /// index once durable, then to put in.
    unschedule: Vec<(u64, String, String)>,
    schedule: Vec<(u64, String, String)>,
    clear_index: bool,
    /// Whether to announce `CronScheduleChanged`.
    changed: bool,
}

impl CronWrites {
    fn writes_anything(&self) -> bool {
        !self.puts.is_empty() || !self.removes.is_empty() || !self.created_budgets.is_empty()
    }
}

/// A change to the cron records, applied in a writer pass.
///
/// Decides first, then writes: every read, check and serialization comes
/// before anything goes into the transaction. A refusal (`Conflict`, an
/// invalid expression, budgets that don't resolve) is then this op's
/// answer alone, and the rest of the pass still commits. Reads go through
/// the transaction, so they see what the pass already wrote.
pub(in crate::store) struct CronOp {
    change: CronChange,
    now: u64,
    live: Arc<Budgets>,
    cron_index: Arc<CronScheduleIndex>,
    /// Filled in by `apply`. Boxed, being much larger than any other
    /// op's state, which every op in a pass would otherwise be sized to.
    decided: Option<Box<Result<(CronWrites, CronWritten), StoreError>>>,
    reply: JobReply<CronWritten>,
}

impl CronOp {
    fn decide(
        &self,
        tx: &fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<(CronWrites, CronWritten), StoreError> {
        let now = self.now;
        let mut w = CronWrites::default();

        match &self.change {
            CronChange::ReplaceGroup { group, opts } => {
                let group_key = make_cron_group_key(group);
                let group_timezone = opts.timezone.clone();

                // Load or create group metadata, applying pause state if
                // requested. The timezone is applied unconditionally —
                // this is a full replace, so an absent timezone clears
                // any existing one.
                let mut old_group_timezone: Option<String> = None;
                let existing_group: CronGroup = match tx.get(&ks.data, &group_key)? {
                    Some(bytes) => {
                        let mut g: CronGroup = rmp_serde::from_slice(&bytes)?;
                        old_group_timezone = g.timezone.clone();

                        let mut dirty = g.timezone != group_timezone;
                        g.timezone = group_timezone.clone();

                        if let Some(paused) = opts.paused {
                            if paused != g.paused {
                                match (g.paused, paused) {
                                    (false, true) => g.paused_at = Some(now),
                                    (true, false) => g.resumed_at = Some(now),
                                    _ => {}
                                }
                                g.paused = paused;
                            }
                            dirty = true;
                        }

                        if dirty {
                            w.puts
                                .push((group_key.clone(), rmp_serde::to_vec_named(&g)?));
                        }
                        g
                    }
                    None => {
                        let g = CronGroup {
                            paused: opts.paused.unwrap_or(false),
                            paused_at: if opts.paused == Some(true) {
                                Some(now)
                            } else {
                                None
                            },
                            timezone: group_timezone.clone(),
                            ..CronGroup::default()
                        };
                        w.puts
                            .push((group_key.clone(), rmp_serde::to_vec_named(&g)?));
                        g
                    }
                };

                let existing_entries = group_entries(tx, ks, group)?;
                let input_names: HashSet<&str> =
                    opts.entries.iter().map(|e| e.name.as_str()).collect();

                // Build the new entries with smart merge logic.
                let mut new_entries = Vec::with_capacity(opts.entries.len());
                for input in &opts.entries {
                    let tz = input.timezone.as_deref().or(group_timezone.as_deref());
                    let computed = cron_next_after(&input.expression, now, tz)?;
                    new_entries.push(merge_cron_entry(
                        input.clone(),
                        existing_entries.get(&input.name),
                        computed,
                        now,
                        old_group_timezone.as_deref(),
                        group_timezone.as_deref(),
                    ));
                }

                // Resolve every entry's budgets, creating any the
                // templates ask for. Done at install rather than at first
                // firing so that an entry accepted into a schedule is
                // guaranteed to be fireable: deferring creation would
                // leave a window in which the server reaches its budget
                // cap and the entry can never run.
                w.created_budgets = match plan_budgets(
                    tx,
                    ks,
                    new_entries.iter().flat_map(|e| e.job.budgets.iter()),
                    now,
                )? {
                    BudgetPlan::Proceed(planned) => planned,
                    BudgetPlan::Reject(e) => return Err(e),
                };

                // Delete removed entries (present in existing but not in
                // input).
                for name in existing_entries.keys() {
                    if !input_names.contains(name.as_str()) {
                        w.removes.push(make_cron_entry_key(group, name));
                    }
                }
                for entry in &new_entries {
                    w.puts.push((
                        make_cron_entry_key(group, &entry.name),
                        rmp_serde::to_vec_named(entry)?,
                    ));
                }

                for (name, old_entry) in &existing_entries {
                    if let Some(next) = old_entry.next_enqueue_at {
                        w.unschedule.push((next, group.clone(), name.clone()));
                    }
                }
                for entry in &new_entries {
                    if let Some(next) = entry.next_enqueue_at {
                        w.schedule.push((next, group.clone(), entry.name.clone()));
                    }
                }
                w.changed = true;

                Ok((w, CronWritten::Group(existing_group, new_entries)))
            }

            CronChange::AddEntry { group, opts } => {
                let group_key = make_cron_group_key(group);
                let entry_key = make_cron_entry_key(group, &opts.name);

                // Load or create group metadata.
                let group_meta: CronGroup = match tx.get(&ks.data, &group_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => {
                        let group_meta = CronGroup::default();
                        w.puts
                            .push((group_key, rmp_serde::to_vec_named(&group_meta)?));
                        group_meta
                    }
                };

                // Validates the expression; None means no future
                // occurrences.
                let tz = opts.timezone.as_deref().or(group_meta.timezone.as_deref());
                let next_enqueue_at = cron_next_after(&opts.expression, now, tz)?;

                if tx.get(&ks.data, &entry_key)?.is_some() {
                    return Err(StoreError::Conflict(format!(
                        "cron entry '{}' already exists in group '{}'",
                        opts.name, group
                    )));
                }

                let entry = CronEntry {
                    name: opts.name.clone(),
                    expression: opts.expression.clone(),
                    timezone: opts.timezone.clone(),
                    paused: opts.paused.unwrap_or(false),
                    paused_at: None,
                    resumed_at: None,
                    job: opts.job.clone(),
                    next_enqueue_at,
                    last_enqueue_at: None,
                };

                w.created_budgets = match plan_budgets(tx, ks, entry.job.budgets.iter(), now)? {
                    BudgetPlan::Proceed(planned) => planned,
                    BudgetPlan::Reject(e) => return Err(e),
                };
                w.puts.push((entry_key, rmp_serde::to_vec_named(&entry)?));
                if let Some(next) = entry.next_enqueue_at {
                    w.schedule.push((next, group.clone(), entry.name.clone()));
                }
                w.changed = true;

                Ok((w, CronWritten::Entry(entry)))
            }

            CronChange::PutEntry { group, opts } => {
                let group_key = make_cron_group_key(group);
                let entry_key = make_cron_entry_key(group, &opts.name);

                // Load or create group metadata. The group's timezone is
                // needed to resolve the entry's effective timezone.
                let group_meta: CronGroup = match tx.get(&ks.data, &group_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => {
                        let group_meta = CronGroup::default();
                        w.puts
                            .push((group_key, rmp_serde::to_vec_named(&group_meta)?));
                        group_meta
                    }
                };

                let tz = opts.timezone.as_deref().or(group_meta.timezone.as_deref());
                let next_enqueue_at = cron_next_after(&opts.expression, now, tz)?;

                // Load existing entry for smart merge.
                let old_entry: Option<CronEntry> = match tx.get(&ks.data, &entry_key)? {
                    Some(bytes) => Some(rmp_serde::from_slice(&bytes)?),
                    None => None,
                };

                // The group timezone is unchanged by this call, so the
                // entry's old and new effective timezones resolve against
                // the same group.
                let entry = merge_cron_entry(
                    opts.clone(),
                    old_entry.as_ref(),
                    next_enqueue_at,
                    now,
                    group_meta.timezone.as_deref(),
                    group_meta.timezone.as_deref(),
                );

                w.created_budgets = match plan_budgets(tx, ks, entry.job.budgets.iter(), now)? {
                    BudgetPlan::Proceed(planned) => planned,
                    BudgetPlan::Reject(e) => return Err(e),
                };
                w.puts.push((entry_key, rmp_serde::to_vec_named(&entry)?));

                if let Some(next) = old_entry.as_ref().and_then(|old| old.next_enqueue_at) {
                    w.unschedule.push((next, group.clone(), entry.name.clone()));
                }
                if let Some(next) = entry.next_enqueue_at {
                    w.schedule.push((next, group.clone(), entry.name.clone()));
                }
                w.changed = true;

                Ok((w, CronWritten::Entry(entry)))
            }

            CronChange::PatchEntry {
                group,
                name,
                paused,
            } => {
                let group_key = make_cron_group_key(group);
                let entry_key = make_cron_entry_key(group, name);

                if tx.get(&ks.data, &group_key)?.is_none() {
                    return Ok((w, CronWritten::MaybeEntry(None)));
                }
                let mut entry: CronEntry = match tx.get(&ks.data, &entry_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => return Ok((w, CronWritten::MaybeEntry(None))),
                };

                if *paused != entry.paused {
                    match (entry.paused, *paused) {
                        (false, true) => entry.paused_at = Some(now),
                        (true, false) => entry.resumed_at = Some(now),
                        _ => {}
                    }
                    entry.paused = *paused;
                    w.puts.push((entry_key, rmp_serde::to_vec_named(&entry)?));
                    w.changed = true;
                }

                Ok((w, CronWritten::MaybeEntry(Some(entry))))
            }

            CronChange::DeleteEntry { group, name } => {
                let group_key = make_cron_group_key(group);
                let entry_key = make_cron_entry_key(group, name);

                if tx.get(&ks.data, &group_key)?.is_none() {
                    return Ok((w, CronWritten::Deleted(0)));
                }
                let entry: CronEntry = match tx.get(&ks.data, &entry_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => return Ok((w, CronWritten::Deleted(0))),
                };

                w.removes.push(entry_key);
                if let Some(next) = entry.next_enqueue_at {
                    w.unschedule.push((next, group.clone(), name.clone()));
                }
                w.changed = true;

                Ok((w, CronWritten::Deleted(1)))
            }

            CronChange::PatchGroup { group, opts } => {
                let group_key = make_cron_group_key(group);

                let mut group_meta: CronGroup = match tx.get(&ks.data, &group_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => return Ok((w, CronWritten::MaybeGroup(None))),
                };

                let mut dirty = false;

                if let Some(paused) = opts.paused
                    && paused != group_meta.paused
                {
                    match (group_meta.paused, paused) {
                        (false, true) => group_meta.paused_at = Some(now),
                        (true, false) => group_meta.resumed_at = Some(now),
                        _ => {}
                    }
                    group_meta.paused = paused;
                    dirty = true;
                }

                let timezone_changed = match &opts.timezone {
                    Some(tz) if *tz != group_meta.timezone => {
                        group_meta.timezone = tz.clone();
                        dirty = true;
                        true
                    }
                    _ => false,
                };

                if !dirty {
                    return Ok((w, CronWritten::MaybeGroup(Some(group_meta))));
                }

                w.puts
                    .push((group_key, rmp_serde::to_vec_named(&group_meta)?));

                // Reschedule entries that inherit the group's timezone.
                // Their expressions are unchanged, but the wall-clock
                // times those expressions denote have moved.
                if timezone_changed {
                    for mut entry in group_entries(tx, ks, group)?.into_values() {
                        // An entry naming its own timezone ignores the
                        // group's.
                        if entry.timezone.is_some() {
                            continue;
                        }

                        let next = cron_next_after(
                            &entry.expression,
                            now,
                            group_meta.timezone.as_deref(),
                        )?;
                        if next == entry.next_enqueue_at {
                            continue;
                        }

                        if let Some(old) = entry.next_enqueue_at {
                            w.unschedule.push((old, group.clone(), entry.name.clone()));
                        }
                        if let Some(next) = next {
                            w.schedule.push((next, group.clone(), entry.name.clone()));
                        }
                        entry.next_enqueue_at = next;
                        w.puts.push((
                            make_cron_entry_key(group, &entry.name),
                            rmp_serde::to_vec_named(&entry)?,
                        ));
                    }
                }
                w.changed = true;

                Ok((w, CronWritten::MaybeGroup(Some(group_meta))))
            }

            CronChange::DeleteGroup { group } => {
                let group_key = make_cron_group_key(group);
                let prefix = make_cron_group_prefix(group);

                if tx.get(&ks.data, &group_key)?.is_none() {
                    return Ok((w, CronWritten::Deleted(0)));
                }

                let mut range_end = prefix.clone();
                *range_end.last_mut().unwrap() = 1;

                for guard in tx.range::<Vec<u8>, _>(
                    &ks.data,
                    (Bound::Included(prefix.clone()), Bound::Excluded(range_end)),
                ) {
                    let (key, value) = guard.into_inner()?;
                    // Everything goes — group metadata and all entries.
                    // Entries also leave the schedule index (the group
                    // key itself is the prefix, so shorter).
                    if key.len() > prefix.len() {
                        let entry: CronEntry = rmp_serde::from_slice(&value)?;
                        if let Some(next) = entry.next_enqueue_at {
                            w.unschedule.push((next, group.clone(), entry.name));
                        }
                    }
                    w.removes.push(key.to_vec());
                }
                w.changed = true;

                Ok((w, CronWritten::Deleted(1)))
            }

            CronChange::DeleteAllGroups => {
                let start: Vec<u8> = vec![RecordKind::Cron as u8];
                let end: Vec<u8> = vec![RecordKind::Cron as u8 + 1];
                let mut groups_seen: HashSet<Vec<u8>> = HashSet::new();

                for guard in
                    tx.range::<Vec<u8>, _>(&ks.data, (Bound::Included(start), Bound::Excluded(end)))
                {
                    let (key, _) = guard.into_inner()?;
                    let key_bytes = key.as_ref();

                    // Key layout: C{group}\0... — deduped by the prefix up
                    // to and including the first \0.
                    let group_end = key_bytes[1..]
                        .iter()
                        .position(|&b| b == 0)
                        .map(|p| 1 + p)
                        .unwrap_or(key_bytes.len());
                    groups_seen.insert(key_bytes[..group_end].to_vec());
                    w.removes.push(key_bytes.to_vec());
                }

                if !groups_seen.is_empty() {
                    w.clear_index = true;
                    w.changed = true;
                }
                Ok((w, CronWritten::Deleted(groups_seen.len())))
            }

            #[cfg(test)]
            CronChange::SetExpressionUnvalidated {
                group,
                name,
                expression,
            } => {
                let entry_key = make_cron_entry_key(group, name);
                let mut entry: CronEntry = match tx.get(&ks.data, &entry_key)? {
                    Some(bytes) => rmp_serde::from_slice(&bytes)?,
                    None => {
                        return Err(StoreError::InvalidOperation(format!(
                            "no such cron entry: {group}/{name}"
                        )));
                    }
                };
                entry.expression = expression.clone();
                w.puts.push((entry_key, rmp_serde::to_vec_named(&entry)?));
                Ok((w, CronWritten::MaybeEntry(Some(entry))))
            }
        }
    }
}

/// Every entry in a group, by name, read through the transaction. The
/// group's own record shares the entries' prefix and is left out.
fn group_entries(
    tx: &fjall::SingleWriterWriteTx<'_>,
    ks: &Keyspaces,
    group: &str,
) -> Result<std::collections::HashMap<String, CronEntry>, StoreError> {
    let prefix = make_cron_group_prefix(group);
    let mut range_end = prefix.clone();
    *range_end.last_mut().unwrap() = 1; // \0 -> \1

    tx.range::<Vec<u8>, _>(
        &ks.data,
        (Bound::Included(prefix.clone()), Bound::Excluded(range_end)),
    )
    .filter_map(|guard| match guard.into_inner() {
        Ok((key, _)) if *key == *prefix => None,
        Ok((_, value)) => Some(
            rmp_serde::from_slice::<CronEntry>(&value)
                .map(|entry| (entry.name.clone(), entry))
                .map_err(StoreError::from),
        ),
        Err(e) => Some(Err(e.into())),
    })
    .collect()
}

impl JobWrite for CronOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        let decided = self.decide(tx, ks);
        let mut applied = Applied::Nothing;
        if let Ok((writes, _)) = &decided
            && writes.writes_anything()
        {
            write_created_budgets(tx, ks, &writes.created_budgets)?;
            for key in &writes.removes {
                tx.remove(&ks.data, key);
            }
            for (key, value) in &writes.puts {
                tx.insert(&ks.data, key, value);
            }
            applied = Applied::Wrote;
        }
        self.decided = Some(Box::new(decided));
        Ok(applied)
    }

    fn finish(self, cx: &WriterContext) {
        let decided = self.decided.map_or_else(
            || Err(StoreError::Internal("cron write never applied".into())),
            |decided| *decided,
        );
        let written = decided.map(|(writes, written)| {
            sync_created_budgets(&self.live, &writes.created_budgets, self.now);

            if writes.clear_index {
                self.cron_index.clear();
            }
            for (next, group, name) in &writes.unschedule {
                self.cron_index.remove(*next, group, name);
            }
            for (next, group, name) in writes.schedule {
                self.cron_index.insert(next, group, name);
            }
            if writes.changed {
                let _ = cx.event_tx.send(StoreEvent::CronScheduleChanged);
            }
            written
        });
        let _ = self.reply.send(written.map(JobOutcome::Done));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

/// A due cron entry's firing, prepared by its caller: the schedule
/// advanced, and the entry's job enqueued unless it is paused.
pub(in crate::store) struct CronPromoteOp {
    group: String,
    entry_name: String,
    entry_key: Vec<u8>,
    pre_bytes: Slice,
    /// The entry as pre-read.
    entry: CronEntry,
    next: Option<u64>,
    /// The job to enqueue, or `None` if the group or entry is paused.
    prepared: Option<PreparedEnqueue>,
    now: u64,
    live: Arc<Budgets>,
    cron_index: Arc<CronScheduleIndex>,
    /// Filled in by `apply`: the entry advanced, once it is known
    /// whether this tick fires.
    advanced: Option<CronEntry>,
    creations: Vec<(String, Budget)>,
    enqueued: Option<EnqueueResult>,
    staged: bool,
    reply: JobReply<CronEntry>,
}

impl CronPromoteOp {
    /// Pre-read the entry and its group, and prepare the job. `None` if
    /// the entry is gone or not due.
    fn prepare(
        ks: &Keyspaces,
        group: &str,
        entry_name: &str,
        now: u64,
        live: Arc<Budgets>,
        cron_index: Arc<CronScheduleIndex>,
        reply: JobReply<CronEntry>,
    ) -> Result<Option<Self>, StoreError> {
        let entry_key = make_cron_entry_key(group, entry_name);
        let Some(pre_bytes) = ks.data.get(&entry_key)? else {
            return Ok(None); // Entry deleted.
        };
        let entry: CronEntry = rmp_serde::from_slice(&pre_bytes)?;

        // Not due: either no future occurrence or not yet due.
        match entry.next_enqueue_at {
            None => return Ok(None),
            Some(next) if next > now => return Ok(None),
            _ => {}
        }

        // Check if group or entry is paused, and pick up the group's
        // timezone for entries that do not name one.
        let (group_paused, group_timezone) = match ks.data.get(make_cron_group_key(group))? {
            Some(bytes) => {
                let g: CronGroup = rmp_serde::from_slice(&bytes)?;
                (g.paused, g.timezone)
            }
            None => (false, None),
        };

        // Compute next occurrence (None if no future occurrences).
        let tz = entry.timezone.as_deref().or(group_timezone.as_deref());
        let next = cron_next_after(&entry.expression, now, tz)?;

        // Prepare the job enqueue (unless paused). Pure — the entry is not
        // advanced until the budget pre-pass has said whether this tick
        // actually fires.
        let prepared = if group_paused || entry.paused {
            None
        } else {
            Some(prepare_enqueue(entry.job.clone(), now)?)
        };

        Ok(Some(Self {
            group: group.to_string(),
            entry_name: entry_name.to_string(),
            entry_key,
            pre_bytes,
            entry,
            next,
            prepared,
            now,
            live,
            cron_index,
            advanced: None,
            creations: Vec::new(),
            enqueued: None,
            staged: false,
            reply,
        }))
    }

    /// The job this firing created, for the budget accounting.
    fn fired(&self) -> Option<&Job> {
        self.enqueued.as_ref().and_then(|r| match r {
            EnqueueResult::Created(job) => Some(job),
            EnqueueResult::Duplicate(_) | EnqueueResult::Folded(_) => None,
        })
    }
}

impl JobWrite for CronPromoteOp {
    fn apply(
        &mut self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        ks: &Keyspaces,
    ) -> Result<Applied, StoreError> {
        if tx.get(&ks.data, &self.entry_key)?.as_deref() != Some(&*self.pre_bytes) {
            return Ok(Applied::Conflict);
        }

        // Cron fires through `apply_enqueue` directly rather than as an
        // enqueue request, so it runs the budget pre-pass itself —
        // otherwise a scheduled job would reference budgets nobody
        // resolved, and a `create_with` on the template would never
        // create anything.
        //
        // Resolution succeeding is an invariant, not a hope: an entry is
        // validated before it enters a schedule, and a budget it
        // references cannot be deleted or shrunk out from under it. A
        // rejection here therefore means one of those protections has a
        // hole, which is why it is logged as an error rather than treated
        // as a normal outcome.
        self.creations.clear();
        let mut fires = self.prepared.is_some();
        if let Some(p) = &self.prepared {
            match plan_op_budgets(tx, ks, std::slice::from_ref(p))? {
                BudgetPlan::Proceed(creations) => self.creations = creations,
                BudgetPlan::Reject(e) => {
                    // Skip the tick rather than failing the promotion.
                    // Returning an error would discard the schedule
                    // advance with it, leaving the entry perpetually due
                    // and the scheduler spinning on it — a far worse way
                    // to surface a bug than a gap and a log line.
                    tracing::error!(
                        group = %self.group,
                        entry = %self.entry_name,
                        error = %e,
                        "cron entry references budgets that no longer resolve — \
                         this should be impossible, as entries are validated on \
                         install and their budgets protected from deletion and \
                         shrinking. Skipping this tick."
                    );
                    fires = false;
                }
            }
        }

        // Advance the schedule (time moves forward regardless of pause,
        // or of a tick that could not fire).
        let mut entry = self.entry.clone();
        if fires {
            entry.last_enqueue_at = Some(self.now);
        }
        entry.next_enqueue_at = self.next;
        tx.insert(&ks.data, &self.entry_key, rmp_serde::to_vec_named(&entry)?);

        self.enqueued = match (fires, &self.prepared) {
            (true, Some(p)) => {
                write_created_budgets(tx, ks, &self.creations)?;
                Some(apply_enqueue(tx, ks, p)?)
            }
            _ => None,
        };

        // Staged before the commit, while no other write can come between
        // — see `stage_budgets`. Cron fires jobs like any other enqueue,
        // so it needs the same protection against a budget being deleted
        // in the gap between durability and accounting.
        stage_budgets(&self.live, &self.creations, self.fired(), self.now);
        self.staged = true;
        self.advanced = Some(entry);
        Ok(Applied::Wrote)
    }

    fn enqueued(&self) -> bool {
        self.enqueued.is_some()
    }

    fn discard(&mut self) {
        if std::mem::take(&mut self.staged) {
            unstage_budgets(&self.live, &self.creations, self.fired());
        }
    }

    fn finish(self, cx: &WriterContext) {
        if let Some(old) = self.entry.next_enqueue_at {
            self.cron_index.remove(old, &self.group, &self.entry_name);
        }
        if let Some(next) = self.next {
            self.cron_index
                .insert(next, self.group.clone(), self.entry_name.clone());
        }

        if let Some(result) = &self.enqueued {
            finalize_enqueue(result, &cx.dispatch, &cx.scheduled_index, &cx.event_tx);
        }

        let entry = self.advanced.unwrap_or(self.entry);
        let _ = self.reply.send(Ok(JobOutcome::Done(entry)));
    }

    fn conflict(self) {
        let _ = self.reply.send(Ok(JobOutcome::Conflict));
    }

    fn fail(self, msg: &str) {
        let _ = self.reply.send(Err(StoreError::Internal(msg.to_string())));
    }
}

/// Build a cron group metadata key: `C{group}\0`.
fn make_cron_group_key(group: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + group.len());
    key.push(RecordKind::Cron as u8);
    key.extend_from_slice(group.as_bytes());
    key.push(0);
    key
}

/// Build a cron entry key: `C{group}\0{entry_name}`.
fn make_cron_entry_key(group: &str, entry_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + group.len() + entry_name.len());
    key.push(RecordKind::Cron as u8);
    key.extend_from_slice(group.as_bytes());
    key.push(0);
    key.extend_from_slice(entry_name.as_bytes());
    key
}

/// Build a prefix for scanning all cron entries in a group: `C{group}\0`.
///
/// This is the same as the group metadata key — entries sort immediately
/// after the group record because their names are non-empty.
fn make_cron_group_prefix(group: &str) -> Vec<u8> {
    make_cron_group_key(group)
}

/// Merge a `CronEntryOptions` with an optional existing `CronEntry`.
///
/// When an existing entry is present and neither the expression nor the
/// effective timezone has changed, preserves `next_enqueue_at` and
/// `last_enqueue_at`. When `paused` is omitted in the options, preserves
/// the existing pause state. Tracks `paused_at` / `resumed_at` transitions.
///
/// `computed_next` is the pre-computed next enqueue time from the new
/// expression and effective timezone — used only when either has changed,
/// or the entry is new.
///
/// The effective timezone is the entry's own, falling back to the group's.
/// The group's timezone is taken before and after the write because a
/// replace can change it in the same operation.
fn merge_cron_entry(
    opts: CronEntryOptions,
    existing: Option<&CronEntry>,
    computed_next: Option<u64>,
    now: u64,
    old_group_timezone: Option<&str>,
    new_group_timezone: Option<&str>,
) -> CronEntry {
    if let Some(old) = existing {
        let old_timezone = old.timezone.as_deref().or(old_group_timezone);
        let new_timezone = opts.timezone.as_deref().or(new_group_timezone);

        let (next, last) = if old.expression == opts.expression && old_timezone == new_timezone {
            (old.next_enqueue_at, old.last_enqueue_at)
        } else {
            (computed_next, old.last_enqueue_at)
        };

        let paused = opts.paused.unwrap_or(old.paused);
        let (paused_at, resumed_at) = match (old.paused, paused) {
            (false, true) => (Some(now), old.resumed_at),
            (true, false) => (old.paused_at, Some(now)),
            _ => (old.paused_at, old.resumed_at),
        };

        CronEntry {
            name: opts.name,
            expression: opts.expression,
            timezone: opts.timezone,
            paused,
            paused_at,
            resumed_at,
            job: opts.job,
            next_enqueue_at: next,
            last_enqueue_at: last,
        }
    } else {
        CronEntry {
            name: opts.name,
            expression: opts.expression,
            timezone: opts.timezone,
            paused: opts.paused.unwrap_or(false),
            paused_at: None,
            resumed_at: None,
            job: opts.job,
            next_enqueue_at: computed_next,
            last_enqueue_at: None,
        }
    }
}

/// Parse an IANA timezone name, if one is given.
///
/// `None` in means `None` out — the caller treats that as the system's
/// local timezone.
fn parse_timezone(timezone: Option<&str>) -> Result<Option<chrono_tz::Tz>, StoreError> {
    match timezone {
        Some(name) => {
            let tz: chrono_tz::Tz = name
                .parse()
                .map_err(|_| StoreError::InvalidOperation(format!("invalid timezone: {name:?}")))?;
            Ok(Some(tz))
        }
        None => Ok(None),
    }
}

/// Reject an unparseable timezone name.
///
/// Used where a timezone is stored without any expression being evaluated
/// against it — a group's timezone with no entries inheriting it would
/// otherwise be accepted and only fail later.
fn validate_timezone(timezone: Option<&str>) -> Result<(), StoreError> {
    parse_timezone(timezone).map(|_| ())
}

/// Compute the next occurrence of a cron expression after `now_ms`.
///
/// When `timezone` is `Some`, the expression is evaluated in that IANA
/// timezone (e.g. "Australia/Melbourne"). When `None`, the system's
/// local timezone is used.
///
/// Callers pass the entry's *effective* timezone — its own, or the
/// group's when it does not name one.
///
/// Returns `None` if the expression has no future occurrences.
fn cron_next_after(
    expression: &str,
    now_ms: u64,
    timezone: Option<&str>,
) -> Result<Option<u64>, StoreError> {
    // `sloppy_ranges` keeps single-number step syntax (`0/15`, meaning
    // "every 15 starting at 0") parsing. It is not OCPS-compliant, but
    // it is what Quartz and its many descendants emit, and schedules
    // written that way are already installed in the wild.
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .sloppy_ranges(true)
        .build()
        .parse(expression)
        .map_err(|e| StoreError::InvalidOperation(format!("invalid cron expression: {e}")))?;

    let now_secs = (now_ms / 1000) as i64;
    let dt = chrono::DateTime::from_timestamp(now_secs, 0)
        .ok_or_else(|| StoreError::InvalidOperation("invalid timestamp".to_string()))?;

    if let Some(tz) = parse_timezone(timezone)? {
        let dt_tz = dt.with_timezone(&tz);
        match cron.find_next_occurrence(&dt_tz, false) {
            Ok(next) => Ok(Some(next.timestamp() as u64 * 1000)),
            Err(_) => Ok(None),
        }
    } else {
        let local = dt.with_timezone(&chrono::Local);
        match cron.find_next_occurrence(&local, false) {
            Ok(next) => Ok(Some(next.timestamp() as u64 * 1000)),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::options::{
        CronEntryOptions, EnqueueOptions, ListJobsOptions, PatchCronGroupOptions,
        ReplaceCronGroupOptions,
    };
    use super::super::super::test_support::test_store;
    use crate::store::StoreError;
    use crate::time::now_millis;

    /// Fixed timestamp for cron tests: 2023-11-14 22:13:20 UTC.
    /// Chosen to be mid-minute so cron calculations are deterministic.
    const CRON_NOW: u64 = 1_700_000_000_000;

    fn cron_entry_opts(
        name: &str,
        expression: &str,
        queue: &str,
        job_type: &str,
    ) -> CronEntryOptions {
        CronEntryOptions {
            name: name.to_string(),
            expression: expression.to_string(),
            timezone: None,
            paused: None,
            job: EnqueueOptions::new(job_type, queue, serde_json::json!({})),
        }
    }

    /// A patch that only changes the group's pause state.
    fn pause_group(paused: bool) -> PatchCronGroupOptions {
        PatchCronGroupOptions {
            paused: Some(paused),
            timezone: None,
        }
    }

    /// A patch that only changes the group's timezone.
    fn set_group_timezone(timezone: Option<&str>) -> PatchCronGroupOptions {
        PatchCronGroupOptions {
            paused: None,
            timezone: Some(timezone.map(str::to_string)),
        }
    }

    #[tokio::test]
    async fn replace_cron_group_creates_new_group() {
        let store = test_store();
        let now = CRON_NOW;

        let (group, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("every-minute", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert!(!group.paused);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "every-minute");
        assert_eq!(entries[0].expression, "* * * * *");
        assert!(entries[0].next_enqueue_at.is_some());
        assert!(entries[0].next_enqueue_at.unwrap() > now);
        assert!(entries[0].last_enqueue_at.is_none());
        assert!(!entries[0].paused);
    }

    #[tokio::test]
    async fn replace_cron_group_preserves_state_when_expression_unchanged() {
        let store = test_store();
        let now = CRON_NOW;

        let (_, entries1) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "*/5 * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let original_next = entries1[0].next_enqueue_at;

        // Replace with the same expression — next_enqueue_at should be preserved.
        let (_, entries2) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "*/5 * * * *", "q", "test")],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert_eq!(entries2[0].next_enqueue_at, original_next);
    }

    #[tokio::test]
    async fn replace_cron_group_recomputes_next_when_expression_changes() {
        let store = test_store();
        let now = CRON_NOW;

        let (_, entries1) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let original_next = entries1[0].next_enqueue_at;

        // Replace with a very different expression — next_enqueue_at should change.
        let (_, entries2) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "0 0 * * *", "q", "test")],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert_ne!(entries2[0].next_enqueue_at, original_next);
    }

    #[tokio::test]
    async fn replace_cron_group_removes_absent_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "* * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        // Replace with only e1 — e2 should be removed.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "e1");

        // Verify e2 is gone from disk.
        let e2 = store.get_cron_entry("default", "e2").await.unwrap();
        assert!(e2.is_none());
    }

    #[tokio::test]
    async fn replace_cron_group_empty_entries_removes_entries_but_keeps_group() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Replace with empty entries — entries removed, group persists.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![],
                },
                now,
            )
            .await
            .unwrap();

        assert!(entries.is_empty());

        // Entry should be gone.
        let e1 = store.get_cron_entry("default", "e1").await.unwrap();
        assert!(e1.is_none());

        // Group should still exist.
        let group = store.get_cron_group("default").await.unwrap();
        assert!(group.is_some());
    }

    #[tokio::test]
    async fn replace_cron_group_invalid_expression_returns_error() {
        let store = test_store();
        let now = CRON_NOW;

        let result = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("bad", "not a cron expr", "q", "test")],
                },
                now,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replace_cron_group_preserves_pause_when_omitted() {
        let store = test_store();
        let now = CRON_NOW;

        // Create an entry and manually pause it.
        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![CronEntryOptions {
                        paused: Some(true),
                        ..cron_entry_opts("e1", "* * * * *", "q", "test")
                    }],
                },
                now,
            )
            .await
            .unwrap();

        // Replace with paused omitted — should preserve paused state.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert!(entries[0].paused);
    }

    #[tokio::test]
    async fn replace_cron_group_explicit_pause_overrides() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Explicitly pause.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![CronEntryOptions {
                        paused: Some(true),
                        ..cron_entry_opts("e1", "* * * * *", "q", "test")
                    }],
                },
                now + 1000,
            )
            .await
            .unwrap();

        assert!(entries[0].paused);
        assert!(entries[0].paused_at.is_some());
    }

    #[tokio::test]
    async fn get_cron_entry_returns_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let entry = store.get_cron_entry("default", "e1").await.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name, "e1");
    }

    #[tokio::test]
    async fn get_cron_entry_returns_none_for_missing() {
        let store = test_store();

        let entry = store.get_cron_entry("default", "missing").await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn promote_cron_entry_enqueues_job_and_advances() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Get the entry's next_enqueue_at and call promote at that time.
        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        let result = store
            .promote_cron_entry("default", "e1", due_at)
            .await
            .unwrap();

        let updated = result.unwrap();
        assert!(updated.next_enqueue_at.is_some());
        assert!(updated.next_enqueue_at.unwrap() > due_at);
        assert_eq!(updated.last_enqueue_at, Some(due_at));

        // Verify a job was enqueued.
        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["q".to_string()].into()))
            .await
            .unwrap();
        assert_eq!(jobs.jobs.len(), 1);
        assert_eq!(jobs.jobs[0].job_type, "test");
    }

    #[tokio::test]
    async fn promote_cron_entry_skips_when_not_due() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Call promote before the entry is due.
        let result = store
            .promote_cron_entry("default", "e1", now)
            .await
            .unwrap();

        assert!(result.is_none());

        // No job should be enqueued.
        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["q".to_string()].into()))
            .await
            .unwrap();
        assert!(jobs.jobs.is_empty());
    }

    #[tokio::test]
    async fn promote_cron_entry_skips_paused_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![CronEntryOptions {
                        paused: Some(true),
                        ..cron_entry_opts("e1", "* * * * *", "q", "test")
                    }],
                },
                now,
            )
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        let result = store
            .promote_cron_entry("default", "e1", due_at)
            .await
            .unwrap();

        // Entry should be returned (schedule advanced) but no job enqueued.
        let updated = result.unwrap();
        assert!(updated.next_enqueue_at.unwrap() > due_at);
        assert!(updated.last_enqueue_at.is_none()); // Not set when paused.

        let jobs = store
            .list_jobs(ListJobsOptions::new().queues(["q".to_string()].into()))
            .await
            .unwrap();
        assert!(jobs.jobs.is_empty());
    }

    #[tokio::test]
    async fn promote_cron_entry_skips_deleted_entry() {
        let store = test_store();

        let result = store
            .promote_cron_entry("default", "missing", now_millis())
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn rebuild_cron_index_populates_from_disk() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "*/5 * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        // Verify the index has entries.
        assert!(store.cron_next_due_at().is_some());

        // Simulate a restart by rebuilding.
        let (_, _, cron) = store.rebuild_indexes().await.unwrap();
        assert_eq!(cron, 2);

        // Index should still work.
        assert!(store.cron_next_due_at().is_some());
    }

    #[tokio::test]
    async fn cron_next_due_at_reflects_schedule() {
        let store = test_store();
        let now = CRON_NOW;

        assert!(store.cron_next_due_at().is_none());

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let next = store.cron_next_due_at();
        assert!(next.is_some());
        assert!(next.unwrap() > now);
    }

    #[tokio::test]
    async fn next_due_cron_entries_returns_due_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        let entry = store
            .get_cron_entry("default", "e1")
            .await
            .unwrap()
            .unwrap();
        let due_at = entry.next_enqueue_at.unwrap();

        // Before due time — nothing.
        let due = store.next_due_cron_entries(now);
        assert!(due.is_empty());

        // At due time.
        let due = store.next_due_cron_entries(due_at);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, "default");
        assert_eq!(due[0].2, "e1");
    }

    #[tokio::test]
    async fn replace_cron_group_respects_timezone() {
        let store = test_store();
        let now = CRON_NOW; // 2023-11-14 22:13:20 UTC

        // Create two entries with the same expression but different timezones.
        // "0 9 * * *" = 9:00 AM daily.
        // In UTC, next 9:00 AM is 2023-11-15 09:00 UTC.
        // In Australia/Melbourne (UTC+11 in Nov), next 9:00 AM local is
        // 2023-11-14 22:00 UTC (already passed) → 2023-11-15 22:00 UTC.
        let utc_entry = CronEntryOptions {
            timezone: Some("UTC".to_string()),
            ..cron_entry_opts("utc-9am", "0 9 * * *", "q", "test")
        };
        let melb_entry = CronEntryOptions {
            timezone: Some("Australia/Melbourne".to_string()),
            ..cron_entry_opts("melb-9am", "0 9 * * *", "q", "test")
        };

        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![utc_entry, melb_entry],
                },
                now,
            )
            .await
            .unwrap();

        let utc_next = entries
            .iter()
            .find(|e| e.name == "utc-9am")
            .unwrap()
            .next_enqueue_at
            .unwrap();
        let melb_next = entries
            .iter()
            .find(|e| e.name == "melb-9am")
            .unwrap()
            .next_enqueue_at
            .unwrap();

        // They should be different — Melbourne 9 AM is a different UTC time than UTC 9 AM.
        assert_ne!(utc_next, melb_next);

        // UTC 9 AM should be earlier (it's ~11 hours before Melbourne 9 AM in UTC).
        assert!(utc_next < melb_next);
    }

    #[tokio::test]
    async fn replace_cron_group_invalid_timezone_returns_error() {
        let store = test_store();
        let now = CRON_NOW;

        let entry = CronEntryOptions {
            timezone: Some("Not/A/Timezone".to_string()),
            ..cron_entry_opts("bad-tz", "* * * * *", "q", "test")
        };

        let result = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![entry],
                },
                now,
            )
            .await;
        assert!(result.is_err());
    }

    /// "0 9 * * *" evaluated in UTC, from `CRON_NOW` — 2023-11-15 09:00 UTC.
    const NEXT_9AM_UTC: u64 = 1_700_038_800_000;

    /// The same expression in Australia/Melbourne (UTC+11 in November).
    /// 9 AM local on the 15th is 22:00 UTC on the 14th, which `CRON_NOW`
    /// has already passed, so the next occurrence is a day later.
    const NEXT_9AM_MELBOURNE: u64 = 1_700_085_600_000;

    #[tokio::test]
    async fn replace_cron_group_applies_group_timezone_to_unscoped_entries() {
        let store = test_store();

        let (group, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Australia/Melbourne".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        assert_eq!(group.timezone.as_deref(), Some("Australia/Melbourne"));
        // The entry keeps no timezone of its own — it inherits.
        assert_eq!(entries[0].timezone, None);
        assert_eq!(entries[0].next_enqueue_at, Some(NEXT_9AM_MELBOURNE));
    }

    #[tokio::test]
    async fn replace_cron_group_entry_timezone_wins_over_group() {
        let store = test_store();

        let entry = CronEntryOptions {
            timezone: Some("UTC".to_string()),
            ..cron_entry_opts("9am", "0 9 * * *", "q", "test")
        };

        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Australia/Melbourne".to_string()),
                    entries: vec![entry],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        assert_eq!(entries[0].next_enqueue_at, Some(NEXT_9AM_UTC));
    }

    #[tokio::test]
    async fn replace_cron_group_reschedules_when_group_timezone_changes() {
        let store = test_store();

        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("UTC".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();
        assert_eq!(entries[0].next_enqueue_at, Some(NEXT_9AM_UTC));

        // Same expression, different group timezone. The expression is
        // unchanged but the wall-clock time it denotes has moved.
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Australia/Melbourne".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();
        assert_eq!(entries[0].next_enqueue_at, Some(NEXT_9AM_MELBOURNE));
    }

    #[tokio::test]
    async fn replace_cron_group_reschedules_when_entry_timezone_changes() {
        let store = test_store();

        let utc = CronEntryOptions {
            timezone: Some("UTC".to_string()),
            ..cron_entry_opts("9am", "0 9 * * *", "q", "test")
        };
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![utc],
                },
                CRON_NOW,
            )
            .await
            .unwrap();
        assert_eq!(entries[0].next_enqueue_at, Some(NEXT_9AM_UTC));

        let melbourne = CronEntryOptions {
            timezone: Some("Australia/Melbourne".to_string()),
            ..cron_entry_opts("9am", "0 9 * * *", "q", "test")
        };
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![melbourne],
                },
                CRON_NOW,
            )
            .await
            .unwrap();
        assert_eq!(entries[0].next_enqueue_at, Some(NEXT_9AM_MELBOURNE));
    }

    #[tokio::test]
    async fn replace_cron_group_omitting_timezone_clears_it() {
        let store = test_store();

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Australia/Melbourne".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        // A replace is a full replace — an absent timezone clears it rather
        // than preserving what was there.
        let (group, _) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        assert_eq!(group.timezone, None);
        let (group, _) = store.get_cron_group("default").await.unwrap().unwrap();
        assert_eq!(group.timezone, None);
    }

    #[tokio::test]
    async fn replace_cron_group_rejects_invalid_group_timezone() {
        let store = test_store();

        // No entries at all — nothing else would evaluate the timezone.
        let result = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Not/A/Timezone".to_string()),
                    entries: vec![],
                },
                CRON_NOW,
            )
            .await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
        assert!(store.get_cron_group("default").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn patch_cron_group_sets_timezone_and_reschedules() {
        let store = test_store();

        let scoped = CronEntryOptions {
            timezone: Some("UTC".to_string()),
            ..cron_entry_opts("scoped", "0 9 * * *", "q", "test")
        };
        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("inherits", "0 9 * * *", "q", "test"),
                        scoped,
                    ],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        let group = store
            .patch_cron_group(
                "default",
                set_group_timezone(Some("Australia/Melbourne")),
                CRON_NOW,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(group.timezone.as_deref(), Some("Australia/Melbourne"));

        let (_, entries) = store.get_cron_group("default").await.unwrap().unwrap();
        let inherits = entries.iter().find(|e| e.name == "inherits").unwrap();
        let scoped = entries.iter().find(|e| e.name == "scoped").unwrap();

        assert_eq!(inherits.next_enqueue_at, Some(NEXT_9AM_MELBOURNE));
        // An entry naming its own timezone is untouched.
        assert_eq!(scoped.next_enqueue_at, Some(NEXT_9AM_UTC));

        // The in-memory schedule index moved with the rescheduled entry.
        assert_eq!(store.cron_next_due_at(), Some(NEXT_9AM_UTC));
        let due = store.next_due_cron_entries(NEXT_9AM_MELBOURNE);
        assert!(
            due.iter()
                .any(|(ts, _, name)| *ts == NEXT_9AM_MELBOURNE && name == "inherits"),
            "rescheduled entry should be indexed at its new time: {due:?}"
        );
    }

    #[tokio::test]
    async fn patch_cron_group_clears_timezone_with_null() {
        let store = test_store();

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("UTC".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        let group = store
            .patch_cron_group("default", set_group_timezone(None), CRON_NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(group.timezone, None);
    }

    #[tokio::test]
    async fn patch_cron_group_leaves_absent_fields_alone() {
        let store = test_store();

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: Some(true),
                    timezone: Some("UTC".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        // Patching only the timezone leaves the pause state alone...
        let group = store
            .patch_cron_group(
                "default",
                set_group_timezone(Some("Australia/Melbourne")),
                CRON_NOW,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(group.paused);

        // ...and patching only the pause state leaves the timezone alone.
        let group = store
            .patch_cron_group("default", pause_group(false), CRON_NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(group.timezone.as_deref(), Some("Australia/Melbourne"));
    }

    #[tokio::test]
    async fn patch_cron_group_rejects_invalid_timezone() {
        let store = test_store();

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        let result = store
            .patch_cron_group(
                "default",
                set_group_timezone(Some("Not/A/Timezone")),
                CRON_NOW,
            )
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        let (group, _) = store.get_cron_group("default").await.unwrap().unwrap();
        assert_eq!(group.timezone, None);
    }

    #[tokio::test]
    async fn add_and_put_cron_entry_inherit_group_timezone() {
        let store = test_store();

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Australia/Melbourne".to_string()),
                    entries: vec![],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        let added = store
            .add_cron_entry(
                "default",
                cron_entry_opts("added", "0 9 * * *", "q", "test"),
                CRON_NOW,
            )
            .await
            .unwrap();
        assert_eq!(added.next_enqueue_at, Some(NEXT_9AM_MELBOURNE));

        let put = store
            .put_cron_entry(
                "default",
                cron_entry_opts("put", "0 9 * * *", "q", "test"),
                CRON_NOW,
            )
            .await
            .unwrap();
        assert_eq!(put.next_enqueue_at, Some(NEXT_9AM_MELBOURNE));
    }

    #[tokio::test]
    async fn promote_cron_entry_advances_in_group_timezone() {
        let store = test_store();

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: Some("Australia/Melbourne".to_string()),
                    entries: vec![cron_entry_opts("9am", "0 9 * * *", "q", "test")],
                },
                CRON_NOW,
            )
            .await
            .unwrap();

        // Promote at the moment it fires; the next occurrence is a day on,
        // in Melbourne rather than the server's local time.
        let entry = store
            .promote_cron_entry("default", "9am", NEXT_9AM_MELBOURNE)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(entry.last_enqueue_at, Some(NEXT_9AM_MELBOURNE));
        assert_eq!(
            entry.next_enqueue_at,
            Some(NEXT_9AM_MELBOURNE + 24 * 60 * 60 * 1000)
        );
    }

    #[tokio::test]
    async fn replace_cron_group_accepts_6_field_expression() {
        let store = test_store();
        let now = CRON_NOW;

        // 6-field: second minute hour dom month dow
        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("every-30s", "*/30 * * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        // With seconds, next occurrence should be within 30 seconds.
        let next = entries[0].next_enqueue_at.unwrap();
        assert!(next > now);
        assert!(next <= now + 30_000);
    }

    /// Single-number step syntax (`0/15`, "every 15 starting at 0") is
    /// what Quartz and its descendants emit, and croner rejects it by
    /// default from 4.0 onward. Schedules written that way are already
    /// installed in the wild, so the parser opts into `sloppy_ranges`
    /// and this pins that decision.
    #[tokio::test]
    async fn accepts_single_number_step_expressions() {
        let store = test_store();
        let now = CRON_NOW;

        let (_, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("quarter-hourly", "0/15 * * * *", "q", "test"),
                        cron_entry_opts("from-five", "5/5 * * * *", "q", "test"),
                        cron_entry_opts("with-seconds", "10/30 * * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        assert_eq!(entries.len(), 3);
        for entry in &entries {
            assert!(
                entry.next_enqueue_at.is_some_and(|next| next > now),
                "{} did not schedule a future occurrence",
                entry.name
            );
        }
    }

    #[tokio::test]
    async fn list_cron_groups_returns_names() {
        let store = test_store();
        let now = CRON_NOW;

        assert!(store.list_cron_groups().await.unwrap().is_empty());

        for name in &["alpha", "beta", "gamma"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        timezone: None,
                        entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        let mut groups = store.list_cron_groups().await.unwrap();
        groups.sort();
        assert_eq!(groups, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn list_cron_groups_handles_common_prefixes() {
        let store = test_store();
        let now = CRON_NOW;

        // Groups with shared prefixes that could confuse prefix scanning.
        for name in &["billing", "billing-events", "billing-payments"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        timezone: None,
                        entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        let mut groups = store.list_cron_groups().await.unwrap();
        groups.sort();
        assert_eq!(
            groups,
            vec!["billing", "billing-events", "billing-payments"]
        );
    }

    #[tokio::test]
    async fn delete_cron_group_removes_group_and_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "* * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        assert!(store.delete_cron_group("default").await.unwrap());

        // Group should be gone.
        assert!(store.get_cron_group("default").await.unwrap().is_none());

        // Entries should be gone.
        assert!(
            store
                .get_cron_entry("default", "e1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_cron_entry("default", "e2")
                .await
                .unwrap()
                .is_none()
        );

        // Should not appear in listing.
        assert!(store.list_cron_groups().await.unwrap().is_empty());

        // Index should be empty.
        assert!(store.cron_next_due_at().is_none());
    }

    #[tokio::test]
    async fn delete_cron_group_returns_false_for_missing() {
        let store = test_store();
        assert!(!store.delete_cron_group("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn delete_cron_groups_returns_zero_for_empty_store() {
        let store = test_store();
        let count = store.delete_cron_groups().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_cron_groups_removes_all_groups_and_entries() {
        let store = test_store();
        let now = CRON_NOW;

        for name in &["g1", "g2", "g3"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        timezone: None,
                        entries: vec![
                            cron_entry_opts("e1", "* * * * *", "q", "test"),
                            cron_entry_opts("e2", "*/5 * * * *", "q", "test"),
                        ],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        let count = store.delete_cron_groups().await.unwrap();
        assert_eq!(count, 3);

        assert!(store.list_cron_groups().await.unwrap().is_empty());
        assert!(store.cron_next_due_at().is_none());
        assert!(store.get_cron_entry("g1", "e1").await.unwrap().is_none());
        assert!(store.get_cron_group("g2").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_cron_group_does_not_affect_other_groups() {
        let store = test_store();
        let now = CRON_NOW;

        for name in &["keep", "delete"] {
            store
                .replace_cron_group(
                    name,
                    ReplaceCronGroupOptions {
                        paused: None,
                        timezone: None,
                        entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                    },
                    now,
                )
                .await
                .unwrap();
        }

        store.delete_cron_group("delete").await.unwrap();

        let groups = store.list_cron_groups().await.unwrap();
        assert_eq!(groups, vec!["keep"]);

        // The kept group should still have its entry.
        assert!(store.get_cron_entry("keep", "e1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn get_cron_group_returns_group_and_entries() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "*/5 * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        let (group, entries) = store.get_cron_group("default").await.unwrap().unwrap();
        assert!(!group.paused);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "e1");
        assert_eq!(entries[1].name, "e2");
    }

    #[tokio::test]
    async fn get_cron_group_returns_none_for_missing() {
        let store = test_store();
        assert!(store.get_cron_group("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn patch_cron_group_pauses_and_unpauses() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Pause.
        let group = store
            .patch_cron_group("default", pause_group(true), now + 1000)
            .await
            .unwrap()
            .unwrap();
        assert!(group.paused);
        assert_eq!(group.paused_at, Some(now + 1000));
        assert!(group.resumed_at.is_none());

        // Unpause.
        let group = store
            .patch_cron_group("default", pause_group(false), now + 2000)
            .await
            .unwrap()
            .unwrap();
        assert!(!group.paused);
        assert_eq!(group.paused_at, Some(now + 1000));
        assert_eq!(group.resumed_at, Some(now + 2000));
    }

    #[tokio::test]
    async fn patch_cron_group_returns_none_for_missing() {
        let store = test_store();
        assert!(
            store
                .patch_cron_group("missing", pause_group(true), CRON_NOW)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_cron_entry_removes_single_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![
                        cron_entry_opts("e1", "* * * * *", "q", "test"),
                        cron_entry_opts("e2", "* * * * *", "q", "test"),
                    ],
                },
                now,
            )
            .await
            .unwrap();

        assert!(store.delete_cron_entry("default", "e1").await.unwrap());

        // e1 is gone, e2 remains.
        assert!(
            store
                .get_cron_entry("default", "e1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_cron_entry("default", "e2")
                .await
                .unwrap()
                .is_some()
        );

        // Group still exists.
        assert!(store.get_cron_group("default").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_cron_entry_returns_false_for_missing_group() {
        let store = test_store();
        assert!(!store.delete_cron_entry("nonexistent", "e1").await.unwrap());
    }

    #[tokio::test]
    async fn delete_cron_entry_returns_false_for_missing_entry() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert!(!store.delete_cron_entry("default", "missing").await.unwrap());
    }

    #[tokio::test]
    async fn delete_cron_entry_leaves_empty_group() {
        let store = test_store();
        let now = CRON_NOW;

        store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        assert!(store.delete_cron_entry("default", "e1").await.unwrap());

        // Group persists with no entries.
        let (group, entries) = store.get_cron_group("default").await.unwrap().unwrap();
        assert!(!group.paused);
        assert!(entries.is_empty());
    }

    // --- Batched cron entries: enqueue-time validation ---

    fn cron_entry_with_batch(
        name: &str,
        payload: serde_json::Value,
        cfg: crate::store::BatchConfig,
    ) -> CronEntryOptions {
        CronEntryOptions {
            name: name.to_string(),
            expression: "0 * * * *".to_string(),
            timezone: None,
            paused: None,
            job: EnqueueOptions::new("t", "q", payload).batch(cfg),
        }
    }

    #[tokio::test]
    async fn add_cron_entry_rejects_invalid_batch_expression() {
        let store = test_store();
        let bad = crate::store::BatchConfig {
            key: "k".into(),
            when: ".[*]".into(),
            fold: "$existing + $new".into(),
        };

        let err = store
            .add_cron_entry(
                "default",
                cron_entry_with_batch("e1", serde_json::json!([1]), bad),
                CRON_NOW,
            )
            .await
            .err()
            .expect("expected InvalidOperation");
        assert!(matches!(err, StoreError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn add_cron_entry_rejects_batch_dry_run_shape_error() {
        let store = test_store();
        let cfg = crate::store::BatchConfig {
            key: "k".into(),
            when: "true".into(),
            fold: "$existing | .items += $new.items".into(),
        };

        let err = store
            .add_cron_entry(
                "default",
                cron_entry_with_batch("e1", serde_json::json!([1, 2]), cfg),
                CRON_NOW,
            )
            .await
            .err()
            .expect("expected InvalidOperation from dry-run");
        let msg = match err {
            StoreError::InvalidOperation(m) => m,
            other => panic!("expected InvalidOperation, got {other:?}"),
        };
        assert!(msg.contains("dry-run"), "got: {msg}");
    }

    #[tokio::test]
    async fn put_cron_entry_rejects_invalid_batch_expression() {
        let store = test_store();
        let bad = crate::store::BatchConfig {
            key: "k".into(),
            when: ".[*]".into(),
            fold: "$existing + $new".into(),
        };

        let err = store
            .put_cron_entry(
                "default",
                cron_entry_with_batch("e1", serde_json::json!([1]), bad),
                CRON_NOW,
            )
            .await
            .err()
            .expect("expected InvalidOperation");
        assert!(matches!(err, StoreError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn replace_cron_group_rejects_invalid_batch_expression() {
        let store = test_store();
        let bad = crate::store::BatchConfig {
            key: "k".into(),
            when: ".[*]".into(),
            fold: "$existing + $new".into(),
        };

        let opts = ReplaceCronGroupOptions {
            paused: None,
            timezone: None,
            entries: vec![cron_entry_with_batch("e1", serde_json::json!([1]), bad)],
        };
        let err = store
            .replace_cron_group("default", opts, CRON_NOW)
            .await
            .err()
            .expect("expected InvalidOperation");
        assert!(matches!(err, StoreError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn add_cron_entry_accepts_valid_batch_expression() {
        let store = test_store();
        let cfg = crate::store::BatchConfig {
            key: "k".into(),
            when: "true".into(),
            fold: "$existing + $new".into(),
        };

        let entry = store
            .add_cron_entry(
                "default",
                cron_entry_with_batch("e1", serde_json::json!([1]), cfg),
                CRON_NOW,
            )
            .await
            .unwrap();
        assert!(entry.job.batch.is_some());
    }
}

#[cfg(test)]
mod budget_tests {
    use super::super::super::budget::{
        BudgetBinding, BudgetPolicy, BudgetStrategy, make_budget_key,
    };
    use super::super::super::options::{
        CronEntryOptions, EnqueueOptions, ListJobsOptions, ReplaceCronGroupOptions,
    };
    use super::super::super::test_support::{run_job_ops, test_store};
    use super::super::super::writer::{Applied, JobOutcome, JobWrite};
    use super::{CronChange, CronOp, CronPromoteOp, CronWritten};
    use crate::store::{Store, StoreError};

    const NOW: u64 = 1_700_000_000_000;

    type CronReply = tokio::sync::oneshot::Receiver<Result<JobOutcome<CronWritten>, StoreError>>;

    /// `change`, as the store submits it to the writer.
    fn cron_op(store: &Store, change: CronChange) -> (CronOp, CronReply) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = CronOp {
            change,
            now: NOW,
            live: store.budgets.clone(),
            cron_index: store.cron_index.clone(),
            decided: None,
            reply,
        };
        (op, rx)
    }

    /// A replace into a new group, whose entry names a budget that
    /// doesn't exist, is refused before anything is written: the group
    /// isn't created, though the pass it shares commits another change.
    #[tokio::test]
    async fn a_refused_replace_writes_nothing_in_a_shared_pass() {
        let store = test_store();
        let unresolvable = EnqueueOptions::new("t", "q", serde_json::json!(null))
            .budget(BudgetBinding::new("missing"));
        let (refused, refused_rx) = cron_op(
            &store,
            CronChange::ReplaceGroup {
                group: "g".into(),
                opts: ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![entry_with(unresolvable)],
                },
            },
        );
        let (other, other_rx) = cron_op(
            &store,
            CronChange::AddEntry {
                group: "h".into(),
                opts: entry_with(EnqueueOptions::new("t", "q", serde_json::json!(null))),
            },
        );

        run_job_ops(&store, vec![refused.into(), other.into()]).await;

        assert!(refused_rx.await.unwrap().is_err());
        assert!(matches!(
            other_rx.await.unwrap(),
            Ok(JobOutcome::Done(CronWritten::Entry(_)))
        ));
        assert!(store.get_cron_group("g").await.unwrap().is_none());
        assert!(
            store
                .get_cron_entry("h", "nightly")
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Prepare the firing of `g/nightly` at `now`, as the store would.
    fn promote_op(
        store: &Store,
        now: u64,
    ) -> (
        CronPromoteOp,
        tokio::sync::oneshot::Receiver<Result<JobOutcome<super::CronEntry>, StoreError>>,
    ) {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let op = CronPromoteOp::prepare(
            &store.ks,
            "g",
            "nightly",
            now,
            store.budgets.clone(),
            store.cron_index.clone(),
            reply,
        )
        .unwrap()
        .expect("the entry should be due");
        (op, rx)
    }

    /// A firing prepared before its entry changed (here, paused) is told
    /// to retry, and enqueues nothing. Retried, it advances the paused
    /// entry without firing it.
    #[tokio::test]
    async fn a_firing_of_a_changed_entry_conflicts_and_enqueues_nothing() {
        let store = test_store();
        install(
            &store,
            EnqueueOptions::new("t", "q", serde_json::json!(null)),
        )
        .await;
        let later = NOW + 120_000;
        let (op, rx) = promote_op(&store, later);

        store
            .patch_cron_entry("g", "nightly", true, later)
            .await
            .unwrap();
        run_job_ops(&store, vec![Box::new(op).into()]).await;

        assert!(matches!(rx.await.unwrap().unwrap(), JobOutcome::Conflict));
        assert_eq!(store.dispatch.len(), 0);

        let entry = store
            .promote_cron_entry("g", "nightly", later)
            .await
            .unwrap()
            .unwrap();
        assert!(entry.last_enqueue_at.is_none());
        assert!(entry.next_enqueue_at.unwrap() > later);
        assert_eq!(store.dispatch.len(), 0);
    }

    /// A firing reports its enqueue, so the pass commits with the enqueue
    /// commit mode, and, discarded after it applied, leaves no budget
    /// accounting behind.
    #[tokio::test]
    async fn a_discarded_firing_leaves_no_accounting() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        install(
            &store,
            EnqueueOptions::new("t", "q", serde_json::json!(null))
                .budget(BudgetBinding::new("stripe")),
        )
        .await;
        let (mut op, _rx) = promote_op(&store, NOW + 120_000);

        let ks = store.ks.clone();
        let live = store.budgets.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            assert_eq!(op.apply(&mut tx, &ks).unwrap(), Applied::Wrote);
            assert!(op.enqueued());
            assert_eq!(live.tracked("stripe"), 1);
            op.discard();
            assert_eq!(live.tracked("stripe"), 0);
        })
        .await
        .unwrap();
    }

    fn entry_with(job: EnqueueOptions) -> CronEntryOptions {
        CronEntryOptions {
            name: "nightly".to_string(),
            // Every minute, so it is due almost immediately.
            expression: "* * * * *".to_string(),
            timezone: Some("UTC".to_string()),
            paused: None,
            job,
        }
    }

    async fn install(store: &Store, job: EnqueueOptions) {
        store
            .replace_cron_group(
                "g",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![entry_with(job)],
                },
                NOW,
            )
            .await
            .unwrap();
    }

    /// Cron fires through `apply_enqueue` directly, so it has to run
    /// the budget pre-pass itself or a `create_with` on the template
    /// would never create anything.
    #[tokio::test]
    async fn firing_creates_a_budget_from_the_template() {
        let store = test_store();
        let job = EnqueueOptions::new("t", "q", serde_json::json!({})).budget(
            BudgetBinding::new("stripe").create_with(BudgetPolicy {
                allocation: 10,
                strategy: BudgetStrategy::WhileInFlight,
            }),
        );
        install(&store, job).await;

        let fired = store
            .promote_cron_entry("g", "nightly", NOW + 120_000)
            .await
            .unwrap();
        assert!(fired.is_some());

        let budget = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(budget.allocation, 10);
    }

    /// A cron firing is an enqueue like any other, so the job it
    /// creates counts against its budgets. Installing the entry alone
    /// must not — an entry is a standing claim guarded separately, and
    /// counting it here would double-count every schedule.
    #[tokio::test]
    async fn firing_tracks_the_job_it_enqueues() {
        let store = test_store();
        let job = EnqueueOptions::new("t", "q", serde_json::json!({})).budget(
            BudgetBinding::new("stripe")
                .cost(6)
                .create_with(BudgetPolicy {
                    allocation: 10,
                    strategy: BudgetStrategy::WhileInFlight,
                }),
        );
        install(&store, job).await;

        assert_eq!(store.budgets.tracked("stripe"), 0);

        store
            .promote_cron_entry("g", "nightly", NOW + 120_000)
            .await
            .unwrap();

        assert_eq!(store.budgets.tracked("stripe"), 1);
        assert_eq!(store.budgets.max_cost("stripe"), Some(6));
    }

    /// Defence in depth for a state the install-time and deletion
    /// protections are supposed to make unreachable.
    ///
    /// There is deliberately no API route into it — installing
    /// validates, and deleting a referenced budget is refused — so the
    /// store is corrupted directly to reach the branch. The property
    /// under test is not that skipping is *correct*; it is that an
    /// invariant violation degrades into a gap rather than a wedged
    /// scheduler. Failing the promotion would discard the schedule
    /// advance with it, leaving the entry perpetually due and the
    /// scheduler spinning on it.
    #[tokio::test]
    async fn an_unresolvable_budget_degrades_to_a_skipped_tick() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        // No `create_with`, so the entry has no way to reconstitute the
        // budget — install accepts it only because the budget exists.
        let job = EnqueueOptions::new("t", "q", serde_json::json!({}))
            .budget(BudgetBinding::new("stripe"));
        install(&store, job).await;

        // Rip the budget out from under the entry, behind the
        // protections that exist precisely to stop this.
        let ks = store.ks.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = ks.write_tx();
            tx.remove(&ks.data, make_budget_key("stripe"));
            ks.commit(tx, ks.default_commit_mode).unwrap();
        })
        .await
        .unwrap();

        let before = store
            .get_cron_entry("g", "nightly")
            .await
            .unwrap()
            .unwrap()
            .next_enqueue_at
            .unwrap();

        let entry = store
            .promote_cron_entry("g", "nightly", before)
            .await
            .unwrap()
            .unwrap();

        // Advanced, so the scheduler moves on rather than retrying.
        assert!(entry.next_enqueue_at.unwrap() > before);
        // Did not fire.
        assert_eq!(entry.last_enqueue_at, None);
        assert_eq!(store.count_jobs(ListJobsOptions::new()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn installing_creates_a_budget_from_the_template() {
        let store = test_store();
        let job = EnqueueOptions::new("t", "q", serde_json::json!({})).budget(
            BudgetBinding::new("stripe").create_with(BudgetPolicy {
                allocation: 10,
                strategy: BudgetStrategy::WhileInFlight,
            }),
        );

        install(&store, job).await;

        // Created at install, not deferred to the first firing — so the
        // entry cannot be accepted and then become unfireable because
        // the server hit its budget cap in the meantime.
        let budget = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(budget.allocation, 10);
    }

    #[tokio::test]
    async fn installing_rejects_an_unknown_budget_without_create_with() {
        let store = test_store();
        let job = EnqueueOptions::new("t", "q", serde_json::json!({}))
            .budget(BudgetBinding::new("absent"));

        let result = store
            .replace_cron_group(
                "g",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![entry_with(job)],
                },
                NOW,
            )
            .await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
        // The whole install is refused, entry included.
        assert!(store.get_cron_group("g").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn installing_rejects_a_cost_above_the_allocation() {
        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let job = EnqueueOptions::new("t", "q", serde_json::json!({}))
            .budget(BudgetBinding::new("stripe").cost(11));

        let result = store
            .replace_cron_group(
                "g",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![entry_with(job)],
                },
                NOW,
            )
            .await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    #[tokio::test]
    async fn adding_a_single_entry_resolves_its_budgets() {
        let store = test_store();
        let job = EnqueueOptions::new("t", "q", serde_json::json!({}))
            .budget(BudgetBinding::new("absent"));

        let result = store.add_cron_entry("g", entry_with(job), NOW).await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }

    #[tokio::test]
    async fn putting_a_single_entry_resolves_its_budgets() {
        let store = test_store();
        let job = EnqueueOptions::new("t", "q", serde_json::json!({}))
            .budget(BudgetBinding::new("absent"));

        let result = store.put_cron_entry("g", entry_with(job), NOW).await;

        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
    }
}
