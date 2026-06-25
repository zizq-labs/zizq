// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Maintenance ops that don't fit any single job-lifecycle pass:
//!
//! - `compact_all`: force a full LSM major compaction across both
//!   keyspaces to reclaim tombstones leveled compaction would otherwise
//!   leave in upper levels.
//! - `backup_snapshot`: take a consistent point-in-time copy of the
//!   database to a new directory.

use std::path::Path;
use std::sync::Arc;

use fjall::Readable;
use fjall::config::{FilterPolicy, PinningPolicy};
use tokio::task;

use super::store::Store;
use super::types::StoreError;

impl Store {
    /// Force a full LSM compaction of both keyspaces, reclaiming tombstones
    /// that leveled compaction has left in upper levels.
    ///
    /// Leveled compaction triggers on level-size ratios, so after a large
    /// bulk delete (or any operation that produces many tombstones without
    /// matching live writes) the upper levels can sit on garbage for a long
    /// time on a quiet database. A full compaction merges every level down
    /// and drops tombstones whose seqnos are safe to GC.
    ///
    /// Runs on a blocking thread and may take seconds for large keyspaces.
    /// Excludes other (background leveled) compactions while running, but
    /// reads and writes at the LSM level proceed normally — they don't
    /// touch the compaction lock. Concurrent zizq transactions also aren't
    /// serialized against this since we bypass the txn writer lock.
    //
    // `major_compact` is `#[doc(hidden)]` in fjall 3.0.x / 3.1.x. It works
    // and is the intended escape hatch for this use case, but the API is
    // not formally stable yet; recheck on each fjall upgrade.
    pub async fn compact_all(&self) -> Result<(), StoreError> {
        let ks = self.ks.clone();
        task::spawn_blocking(move || -> Result<(), StoreError> {
            ks.data.inner().major_compact()?;
            ks.index.inner().major_compact()?;
            Ok(())
        })
        .await?
    }

    /// Create a consistent backup of the database at the given path.
    ///
    /// Takes a read snapshot of the live database and copies all key/value
    /// pairs from both keyspaces into a new database at `dest`. The new
    /// database is created with the same `StorageConfig` as the live one.
    ///
    /// This runs entirely from a point-in-time snapshot, so it does not
    /// block or lock the live database.
    pub async fn backup_snapshot(
        &self,
        dest: impl AsRef<Path> + Send + 'static,
    ) -> Result<(), StoreError> {
        let ks = self.ks.clone();
        let config = self.config.clone();

        task::spawn_blocking(move || {
            let snapshot = ks.db.read_tx();
            let dest = dest.as_ref();

            // Open a new database with matching settings.
            let backup_db = fjall::Database::builder(dest)
                .cache_size(config.cache_size)
                .max_journaling_size(config.journal_size)
                .open()
                .map_err(|e| {
                    StoreError::Internal(format!("failed to open backup database: {e}"))
                })?;

            let data_compaction = Arc::new(
                fjall::compaction::Leveled::default()
                    .with_l0_threshold(config.l0_threshold)
                    .with_table_target_size(config.data_table_size),
            );
            let index_compaction = Arc::new(
                fjall::compaction::Leveled::default()
                    .with_l0_threshold(config.l0_threshold)
                    .with_table_target_size(config.index_table_size),
            );

            let backup_data = backup_db
                .keyspace("data", || {
                    fjall::KeyspaceCreateOptions::default()
                        .compaction_strategy(data_compaction)
                        .max_memtable_size(config.data_table_size)
                        .filter_block_pinning_policy(PinningPolicy::all(true))
                        .index_block_pinning_policy(PinningPolicy::all(true))
                })
                .map_err(|e| {
                    StoreError::Internal(format!("failed to create backup data keyspace: {e}"))
                })?;

            let backup_index = backup_db
                .keyspace("index", || {
                    fjall::KeyspaceCreateOptions::default()
                        .compaction_strategy(index_compaction)
                        .max_memtable_size(config.index_table_size)
                        .filter_policy(FilterPolicy::disabled())
                        .index_block_pinning_policy(PinningPolicy::all(true))
                })
                .map_err(|e| {
                    StoreError::Internal(format!("failed to create backup index keyspace: {e}"))
                })?;

            // Copy all data keyspace entries.
            for entry in snapshot.range::<Vec<u8>, _>(&ks.data, ..) {
                let (key, value) = entry.into_inner()?;
                backup_data
                    .insert(&*key, &*value)
                    .map_err(|e| StoreError::Internal(format!("backup data write failed: {e}")))?;
            }

            // Copy all index keyspace entries.
            for entry in snapshot.range::<Vec<u8>, _>(&ks.index, ..) {
                let (key, value) = entry.into_inner()?;
                backup_index
                    .insert(&*key, &*value)
                    .map_err(|e| StoreError::Internal(format!("backup index write failed: {e}")))?;
            }

            // Flush and close cleanly.
            backup_db
                .persist(fjall::PersistMode::SyncAll)
                .map_err(|e| StoreError::Internal(format!("backup flush failed: {e}")))?;

            drop(backup_db);
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::super::options::{BulkDeleteOptions, EnqueueOptions, ListJobsOptions};
    use super::super::store::Store;
    use super::super::test_support::test_store;
    use crate::time::now_millis;

    #[tokio::test]
    async fn compact_all_succeeds_on_empty_store() {
        let store = test_store();
        store.compact_all().await.unwrap();
    }

    #[tokio::test]
    async fn compact_all_succeeds_after_writes_and_deletes() {
        let store = test_store();
        let now = now_millis();

        for _ in 0..50 {
            store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
                .await
                .unwrap();
        }
        let count = store.delete_jobs(BulkDeleteOptions::new()).await.unwrap();
        assert_eq!(count, 50);

        store.compact_all().await.unwrap();
    }

    #[tokio::test]
    async fn backup_snapshot_copies_all_data() {
        let store = test_store();
        let now = now_millis();

        // Enqueue some jobs across different queues.
        for i in 0..5 {
            store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", &format!("q{i}"), serde_json::json!({"i": i})),
                )
                .await
                .unwrap();
        }

        // Create the backup.
        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join("data");
        store.backup_snapshot(backup_path.clone()).await.unwrap();

        // Open the backup as a new store and verify the data.
        let restored = Store::open(&backup_path, Default::default()).unwrap();
        let opts = ListJobsOptions::new().limit(100).now(now);
        let page = restored.list_jobs(opts).await.unwrap();
        assert_eq!(page.jobs.len(), 5);

        let queues = restored.list_queues().await.unwrap();
        assert_eq!(queues, vec!["q0", "q1", "q2", "q3", "q4"]);
    }

    #[tokio::test]
    async fn backup_snapshot_is_point_in_time() {
        let store = test_store();
        let now = now_millis();

        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("before")),
            )
            .await
            .unwrap();

        // Take the backup.
        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join("data");
        store.backup_snapshot(backup_path.clone()).await.unwrap();

        // Enqueue more after the backup.
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("after")),
            )
            .await
            .unwrap();

        // The backup should only contain the job from before.
        let restored = Store::open(&backup_path, Default::default()).unwrap();
        let opts = ListJobsOptions::new().limit(100).now(now);
        let page = restored.list_jobs(opts).await.unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].payload, Some(serde_json::json!("before")));
    }
}
