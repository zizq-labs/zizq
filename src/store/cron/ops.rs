// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Cron group/entry CRUD, scheduler integration, and recovery rebuild.
//!
//! Lives in a submodule of `cron/` so that helpers (key builders,
//! `merge_cron_entry`, `cron_next_after`) stay package-private to the
//! cron subsystem.

use std::collections::HashSet;
use std::ops::Bound;

use fjall::{Readable, Slice};
use tokio::task;

use super::super::enqueue::{apply_enqueue, finalize_enqueue, prepare_enqueue};
use super::super::options::{CronEntryOptions, ReplaceCronGroupOptions};
use super::super::store::{RecordKind, Store, StoreEvent};
use super::super::types::StoreError;
use super::{CronEntry, CronGroup};

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
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let group_paused = opts.paused;
        let entries = opts.entries;

        task::spawn_blocking(move || -> Result<(CronGroup, Vec<CronEntry>), StoreError> {
            let group_key = make_cron_group_key(&group);
            let prefix = make_cron_group_prefix(&group);

            // Validate all cron expressions before acquiring the write lock.
            let mut computed_next: Vec<Option<u64>> = Vec::with_capacity(entries.len());
            for input in &entries {
                // Validates the expression; None means no future occurrences.
                let next = cron_next_after(&input.expression, now, input.timezone.as_deref())?;
                computed_next.push(next);
            }

            // Acquire the single-writer lock. All reads within this scope
            // are consistent — no other writer can modify data.
            let mut tx = ks.write_tx();

            // Load or create group metadata, applying pause state if requested.
            let existing_group: CronGroup = match ks.data.get(&group_key)? {
                Some(bytes) => {
                    let mut g: CronGroup = rmp_serde::from_slice(&bytes)?;
                    if let Some(paused) = group_paused {
                        if paused != g.paused {
                            match (g.paused, paused) {
                                (false, true) => g.paused_at = Some(now),
                                (true, false) => g.resumed_at = Some(now),
                                _ => {}
                            }
                            g.paused = paused;
                        }
                        tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&g)?);
                    }
                    g
                }
                None => {
                    let g = CronGroup {
                        paused: group_paused.unwrap_or(false),
                        paused_at: if group_paused == Some(true) {
                            Some(now)
                        } else {
                            None
                        },
                        ..CronGroup::default()
                    };
                    tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&g)?);
                    g
                }
            };

            // Load all existing entries for this group.
            let mut range_end = prefix.clone();
            *range_end.last_mut().unwrap() = 1; // \0 -> \1

            let data: &fjall::Keyspace = ks.data.as_ref();
            let existing_entries: std::collections::HashMap<String, CronEntry> = data
                .range::<Vec<u8>, _>((Bound::Included(prefix.clone()), Bound::Excluded(range_end)))
                .skip(1) // skip the group metadata key
                .map(|guard| {
                    let (_, value) = guard.into_inner()?;
                    let entry: CronEntry = rmp_serde::from_slice(&value)?;
                    Ok((entry.name.clone(), entry))
                })
                .collect::<Result<_, StoreError>>()?;

            // Collect input names before consuming entries (needed for
            // deletion check below).
            let input_names: std::collections::HashSet<String> =
                entries.iter().map(|e| e.name.clone()).collect();

            // Build the new entries with smart merge logic.
            let new_entries: Vec<CronEntry> = entries
                .into_iter()
                .zip(computed_next)
                .map(|(input, computed)| {
                    let existing = existing_entries.get(&input.name);
                    merge_cron_entry(input, existing, computed, now)
                })
                .collect();

            // Delete removed entries (present in existing but not in input).
            for (name, _) in &existing_entries {
                if !input_names.contains(name) {
                    tx.remove(&ks.data, &make_cron_entry_key(&group, name));
                }
            }

            // Write new/updated entries.
            for entry in &new_entries {
                let entry_key = make_cron_entry_key(&group, &entry.name);
                let entry_bytes = rmp_serde::to_vec_named(entry)?;
                tx.insert(&ks.data, &entry_key, &entry_bytes);
            }

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----

            // Remove all old entries from the index.
            for (name, old_entry) in &existing_entries {
                if let Some(next) = old_entry.next_enqueue_at {
                    cron_index.remove(next, &group, name);
                }
            }

            // Insert all new entries into the index.
            for entry in &new_entries {
                if let Some(next) = entry.next_enqueue_at {
                    cron_index.insert(next, group.clone(), entry.name.clone());
                }
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok((existing_group, new_entries))
        })
        .await?
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
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<CronEntry, StoreError> {
            // Validate the cron expression before acquiring the write lock.
            let next_enqueue_at = cron_next_after(&opts.expression, now, opts.timezone.as_deref())?;

            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &opts.name);

            let mut tx = ks.write_tx();

            // Load or create group metadata.
            if ks.data.get(&group_key)?.is_none() {
                let group_meta = CronGroup::default();
                tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&group_meta)?);
            }

            // Check for conflict.
            if ks.data.get(&entry_key)?.is_some() {
                return Err(StoreError::Conflict(format!(
                    "cron entry '{}' already exists in group '{}'",
                    opts.name, group
                )));
            }

            let entry = CronEntry {
                name: opts.name,
                expression: opts.expression,
                timezone: opts.timezone,
                paused: opts.paused.unwrap_or(false),
                paused_at: None,
                resumed_at: None,
                job: opts.job,
                next_enqueue_at,
                last_enqueue_at: None,
            };

            tx.insert(&ks.data, &entry_key, &rmp_serde::to_vec_named(&entry)?);

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            if let Some(next) = entry.next_enqueue_at {
                cron_index.insert(next, group.clone(), entry.name.clone());
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(entry)
        })
        .await?
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
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<CronEntry, StoreError> {
            let next_enqueue_at = cron_next_after(&opts.expression, now, opts.timezone.as_deref())?;

            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &opts.name);

            let mut tx = ks.write_tx();

            // Load or create group metadata.
            if ks.data.get(&group_key)?.is_none() {
                let group_meta = CronGroup::default();
                tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&group_meta)?);
            }

            // Load existing entry for smart merge.
            let old_entry: Option<CronEntry> = match ks.data.get(&entry_key)? {
                Some(bytes) => Some(rmp_serde::from_slice(&bytes)?),
                None => None,
            };

            let entry = merge_cron_entry(opts, old_entry.as_ref(), next_enqueue_at, now);

            tx.insert(&ks.data, &entry_key, &rmp_serde::to_vec_named(&entry)?);

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            if let Some(old) = &old_entry {
                if let Some(next) = old.next_enqueue_at {
                    cron_index.remove(next, &group, &entry.name);
                }
            }
            if let Some(next) = entry.next_enqueue_at {
                cron_index.insert(next, group.clone(), entry.name.clone());
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(entry)
        })
        .await?
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
        let ks = self.ks.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &entry_name);

            // Check group exists.
            if ks.data.get(&group_key)?.is_none() {
                return Ok(None);
            }

            let mut tx = ks.write_tx();

            let mut entry: CronEntry = match ks.data.get(&entry_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(None),
            };

            if paused != entry.paused {
                match (entry.paused, paused) {
                    (false, true) => entry.paused_at = Some(now),
                    (true, false) => entry.resumed_at = Some(now),
                    _ => {}
                }
                entry.paused = paused;
                tx.insert(&ks.data, &entry_key, &rmp_serde::to_vec_named(&entry)?);
                ks.commit(tx, ks.default_commit_mode)?;
                let _ = event_tx.send(StoreEvent::CronScheduleChanged);
            }

            Ok(Some(entry))
        })
        .await?
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
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        task::spawn_blocking(move || -> Result<bool, StoreError> {
            let group_key = make_cron_group_key(&group);
            let entry_key = make_cron_entry_key(&group, &entry_name);

            // Check group exists.
            if ks.data.get(&group_key)?.is_none() {
                return Ok(false);
            }

            // Load entry for index cleanup.
            let entry: CronEntry = match ks.data.get(&entry_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(false),
            };

            let mut tx = ks.write_tx();
            tx.remove(&ks.data, &entry_key);
            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            if let Some(next) = entry.next_enqueue_at {
                cron_index.remove(next, &group, &entry_name);
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(true)
        })
        .await?
    }

    /// Update group-level metadata (currently just pause state).
    ///
    /// Returns the updated group, or `None` if the group does not exist.
    pub async fn patch_cron_group(
        &self,
        group: &str,
        paused: bool,
        now: u64,
    ) -> Result<Option<CronGroup>, StoreError> {
        let ks = self.ks.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<Option<CronGroup>, StoreError> {
            let group_key = make_cron_group_key(&group);

            let mut tx = ks.write_tx();

            let mut group_meta: CronGroup = match ks.data.get(&group_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(None),
            };

            if paused != group_meta.paused {
                match (group_meta.paused, paused) {
                    (false, true) => group_meta.paused_at = Some(now),
                    (true, false) => group_meta.resumed_at = Some(now),
                    _ => {}
                }
                group_meta.paused = paused;
                tx.insert(&ks.data, &group_key, &rmp_serde::to_vec_named(&group_meta)?);
                ks.commit(tx, ks.default_commit_mode)?;
                let _ = event_tx.send(StoreEvent::CronScheduleChanged);
            }

            Ok(Some(group_meta))
        })
        .await?
    }

    /// Delete every cron group and entry in a single transaction.
    ///
    /// Returns the number of groups deleted. Emits a single
    /// `CronScheduleChanged` event regardless of how many groups were
    /// removed.
    pub async fn delete_cron_groups(&self) -> Result<usize, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();

        let count = task::spawn_blocking(move || -> Result<usize, StoreError> {
            let data: &fjall::Keyspace = ks.data.as_ref();
            let start: Vec<u8> = vec![RecordKind::Cron as u8];
            let end: Vec<u8> = vec![RecordKind::Cron as u8 + 1];

            let mut tx = ks.write_tx();
            let mut groups_seen: HashSet<Vec<u8>> = HashSet::new();

            for guard in data.range::<Vec<u8>, _>((Bound::Included(start), Bound::Excluded(end))) {
                let (key, _) = guard.into_inner()?;
                let key_bytes = key.as_ref();

                // Key layout: C{group}\0... — the group-meta key is C{group}
                // (no trailing \0). We dedupe by the prefix up to and
                // including the first \0 (or the whole key for the meta).
                let group_end = key_bytes[1..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|p| 1 + p)
                    .unwrap_or(key_bytes.len());
                groups_seen.insert(key_bytes[..group_end].to_vec());

                tx.remove(&ks.data, key_bytes);
            }

            if groups_seen.is_empty() {
                return Ok(0);
            }

            ks.commit(tx, ks.default_commit_mode)?;

            cron_index.clear();

            Ok(groups_seen.len())
        })
        .await??;

        if count > 0 {
            let _ = event_tx.send(StoreEvent::CronScheduleChanged);
        }

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
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();

        task::spawn_blocking(move || -> Result<bool, StoreError> {
            let group_key = make_cron_group_key(&group);
            let prefix = make_cron_group_prefix(&group);

            let mut tx = ks.write_tx();

            // Check if the group exists.
            if ks.data.get(&group_key)?.is_none() {
                return Ok(false);
            }

            // Collect all entry keys and their next_enqueue_at for index cleanup.
            let mut range_end = prefix.clone();
            *range_end.last_mut().unwrap() = 1;

            let data: &fjall::Keyspace = ks.data.as_ref();
            let mut entries_to_remove: Vec<(String, Option<u64>)> = Vec::new();

            for guard in data
                .range::<Vec<u8>, _>((Bound::Included(prefix.clone()), Bound::Excluded(range_end)))
            {
                let (key, value) = guard.into_inner()?;
                // Remove everything — group metadata and all entries.
                tx.remove(&ks.data, key.as_ref());

                // Track entry next_enqueue_at for index cleanup (skip group key).
                if key.len() > prefix.len() {
                    let entry: CronEntry = rmp_serde::from_slice(&value)?;
                    entries_to_remove.push((entry.name, entry.next_enqueue_at));
                }
            }

            // Remove the group metadata key itself.
            tx.remove(&ks.data, &group_key);

            ks.commit(tx, ks.default_commit_mode)?;

            // ---- outside tx: update in-memory cron index ----
            for (entry_name, next) in &entries_to_remove {
                if let Some(next) = next {
                    cron_index.remove(*next, &group, entry_name);
                }
            }

            let _ = event_tx.send(StoreEvent::CronScheduleChanged);

            Ok(true)
        })
        .await?
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
    /// Pre-reads the entry and group outside the write tx, then uses
    /// `fetch_update` to CAS the entry inside the tx. Retries if the
    /// entry was modified concurrently. Returns early without writing
    /// if the entry is paused, not actually due, or no longer exists.
    pub async fn promote_cron_entry(
        &self,
        group: &str,
        entry_name: &str,
        now: u64,
    ) -> Result<Option<CronEntry>, StoreError> {
        let ks = self.ks.clone();
        let cron_index = self.cron_index.clone();
        let ready_index = self.ready_index.clone();
        let scheduled_index = self.scheduled_index.clone();
        let event_tx = self.event_tx.clone();
        let group = group.to_string();
        let entry_name = entry_name.to_string();

        task::spawn_blocking(move || -> Result<Option<CronEntry>, StoreError> {
            let entry_key = make_cron_entry_key(&group, &entry_name);
            let group_key = make_cron_group_key(&group);

            loop {
                // ---- outside tx: pre-read ----
                let pre_bytes = match ks.data.get(&entry_key)? {
                    Some(bytes) => bytes,
                    None => return Ok(None), // Entry deleted.
                };
                let mut entry: CronEntry = rmp_serde::from_slice(&pre_bytes)?;

                // Not due: either no future occurrence or not yet due.
                match entry.next_enqueue_at {
                    None => return Ok(None),
                    Some(next) if next > now => return Ok(None),
                    _ => {}
                }

                // Check if group or entry is paused.
                let group_paused = match ks.data.get(&group_key)? {
                    Some(bytes) => {
                        let g: CronGroup = rmp_serde::from_slice(&bytes)?;
                        g.paused
                    }
                    None => false,
                };
                let is_paused = group_paused || entry.paused;

                // Compute next occurrence (None if no future occurrences).
                let next = cron_next_after(&entry.expression, now, entry.timezone.as_deref())?;

                // Prepare the job enqueue (unless paused).
                let prepared = if !is_paused {
                    entry.last_enqueue_at = Some(now);
                    Some(prepare_enqueue(entry.job.clone(), now)?)
                } else {
                    None
                };

                // Advance the schedule (time moves forward regardless of pause).
                let old_next = entry.next_enqueue_at;
                entry.next_enqueue_at = next;

                let updated_entry_bytes: Slice = rmp_serde::to_vec_named(&entry)?.into();

                // ---- inside tx: CAS + enqueue ----
                let mut tx = ks.write_tx();

                // CAS the cron entry — retry if it changed since pre-read.
                let prev =
                    tx.fetch_update(&ks.data, &entry_key, |_| Some(updated_entry_bytes.clone()))?;

                if prev.as_deref() != Some(&*pre_bytes) {
                    drop(tx);
                    continue; // Entry changed — retry.
                }

                // Enqueue the job if not paused.
                let enqueue_result = if let Some(ref p) = prepared {
                    Some(apply_enqueue(&mut tx, &ks, p)?)
                } else {
                    None
                };

                ks.commit(tx, ks.enqueue_commit_mode)?;

                // ---- outside tx: update in-memory indexes ----

                // Update cron schedule index.
                if let Some(old) = old_next {
                    cron_index.remove(old, &group, &entry_name);
                }
                if let Some(next) = next {
                    cron_index.insert(next, group.clone(), entry_name.clone());
                }

                // Finalize job enqueue (in-memory indexes + events).
                if let Some(ref result) = enqueue_result {
                    finalize_enqueue(result, &ready_index, &scheduled_index, &event_tx);
                }

                return Ok(Some(entry));
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
/// When an existing entry is present and the expression hasn't changed,
/// preserves `next_enqueue_at` and `last_enqueue_at`. When `paused` is
/// omitted in the options, preserves the existing pause state. Tracks
/// `paused_at` / `resumed_at` transitions.
///
/// `computed_next` is the pre-computed next enqueue time from the new
/// expression — used only when the expression has changed or the entry
/// is new.
fn merge_cron_entry(
    opts: CronEntryOptions,
    existing: Option<&CronEntry>,
    computed_next: Option<u64>,
    now: u64,
) -> CronEntry {
    if let Some(old) = existing {
        let (next, last) = if old.expression == opts.expression {
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

/// Compute the next occurrence of a cron expression after `now_ms`.
///
/// When `timezone` is `Some`, the expression is evaluated in that IANA
/// timezone (e.g. "Australia/Melbourne"). When `None`, the system's
/// local timezone is used.
///
/// Returns `None` if the expression has no future occurrences.
fn cron_next_after(
    expression: &str,
    now_ms: u64,
    timezone: Option<&str>,
) -> Result<Option<u64>, StoreError> {
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse(expression)
        .map_err(|e| StoreError::InvalidOperation(format!("invalid cron expression: {e}")))?;

    let now_secs = (now_ms / 1000) as i64;
    let dt = chrono::DateTime::from_timestamp(now_secs, 0)
        .ok_or_else(|| StoreError::InvalidOperation("invalid timestamp".to_string()))?;

    if let Some(tz_name) = timezone {
        let tz: chrono_tz::Tz = tz_name
            .parse()
            .map_err(|_| StoreError::InvalidOperation(format!("invalid timezone: {tz_name:?}")))?;
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
        CronEntryOptions, EnqueueOptions, ListJobsOptions, ReplaceCronGroupOptions,
    };
    use super::super::super::test_support::test_store;
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

    #[tokio::test]
    async fn replace_cron_group_creates_new_group() {
        let store = test_store();
        let now = CRON_NOW;

        let (group, entries) = store
            .replace_cron_group(
                "default",
                ReplaceCronGroupOptions {
                    paused: None,
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
                    entries: vec![entry],
                },
                now,
            )
            .await;
        assert!(result.is_err());
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
                    entries: vec![cron_entry_opts("e1", "* * * * *", "q", "test")],
                },
                now,
            )
            .await
            .unwrap();

        // Pause.
        let group = store
            .patch_cron_group("default", true, now + 1000)
            .await
            .unwrap()
            .unwrap();
        assert!(group.paused);
        assert_eq!(group.paused_at, Some(now + 1000));
        assert!(group.resumed_at.is_none());

        // Unpause.
        let group = store
            .patch_cron_group("default", false, now + 2000)
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
                .patch_cron_group("missing", true, CRON_NOW)
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
}
