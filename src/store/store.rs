// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Persistent storage layer to manage job queues.
//!
//! Wraps fjall (database) to provide transactional queue operations across
//! two keyspaces plus two in-memory indexes:
//!
//! - `data`: jobs (`J` tag), payloads (`P` tag), and error records (`E` tag).
//!   Uses bloom filters and a larger memtable. See `RecordKind` for key layouts.
//! - `index`: status (`S` tag), queue (`Q` tag), type (`T` tag), and purge-at
//!   (`A` tag) secondary indexes. No bloom filters, smaller memtable. See
//!   `IndexKind` for key layouts.
//! - In-memory `ReadyIndex`: lock-free crossbeam skip-list priority index of
//!   ready jobs, rebuilt from the status index on startup.
//! - In-memory `ScheduledIndex`: lock-free crossbeam skip-set chronological
//!   index of scheduled jobs, rebuilt from the status index on startup.
//!
//! Each tag byte occupies a disjoint prefix range so that all record/index
//! types coexist without collision. Combined filters (e.g. queue + status)
//! use sorted stream intersection across the tag-prefixed ranges.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fjall::config::{FilterPolicy, PinningPolicy};
use fjall::{Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use tokio::sync::broadcast;
use tokio::task;

use super::complete::CompleteBatcher;
use super::cron::CronScheduleIndex;
use super::enqueue::EnqueueBatcher;
use super::group_committer::GroupCommitter;
use super::ready_index::ReadyIndex;
use super::scheduled_index::ScheduledIndex;
use super::storage_config::StorageConfig;
use super::types::{BackoffConfig, CommitMode, StoreError};

/// Events broadcast by the store when job state changes.
///
/// Workers use these events to decide when to check for new work:
///
/// - `JobCreated`: a job became ready. Workers check the queue name, then
///   try to atomically claim the one-shot `token`. Only the winner calls
///   `take_next_job`, avoiding thundering-herd DB contention.
/// - `JobCompleted` / `JobFailed`: workers prune their in-flight set;
///   no DB call needed.
/// - `JobScheduled`: only the scheduler cares; workers ignore it.
///
/// On `Lagged`, workers fall back to a full reconciliation (re-check the
/// DB and reconcile their in-flight set), since they may have missed both
/// creation and completion events.

#[derive(Debug, Clone)]
pub enum StoreEvent {
    /// A job became ready for processing (newly enqueued, requeued, or
    /// promoted from the scheduled index).
    ///
    /// Contains the queue name so workers can cheaply filter irrelevant
    /// events, and a single-use claim token (`Arc<AtomicBool>`) so that
    /// only one worker per event hits the database.
    JobCreated {
        id: String,
        queue: String,
        token: Arc<AtomicBool>,
    },

    /// A job was taken from the ready queue and is now being worked on.
    JobInFlight { id: String },

    /// A job was successfully completed and removed.
    JobCompleted { id: String },

    /// A job failed (either rescheduled for retry or killed).
    ///
    /// Workers use this to prune their in-flight set and free capacity.
    /// The `attempts` count identifies which attempt failed — workers
    /// compare it against the attempt they took so that stale events
    /// from a previous retry cycle are safely ignored.
    ///
    /// For rescheduled jobs a `JobScheduled` event follows immediately
    /// after, waking the scheduler at the new `ready_at`. For killed
    /// jobs this is the only event.
    JobFailed { id: String, attempts: u32 },

    /// A job was enqueued with a future `ready_at` timestamp.
    ///
    /// The scheduler listens for this to wake up early if the new job is
    /// due sooner than whatever it was sleeping until. Workers ignore it.
    JobScheduled { id: String, ready_at: u64 },

    /// A job was explicitly deleted via the API (not the normal lifecycle purge).
    ///
    /// Workers use this to release capacity when an in-flight job is deleted
    /// externally. The take stream treats this like a completion — the job
    /// is gone and the worker should not attempt to ack/nack it.
    JobDeleted { id: String },

    /// A job was modified via PATCH.
    ///
    /// The admin layer uses this to re-diff all tabs (ready, scheduled,
    /// in-flight) since a patch can change queue, priority, status, or
    /// any combination. Workers and the scheduler ignore this — they
    /// receive `JobCreated`/`JobScheduled` separately when a status
    /// transition occurs.
    JobPatched { id: String },

    /// A cron schedule was modified (created, replaced, paused, deleted, etc.).
    ///
    /// The cron scheduler listens for this to recalculate its sleep timer.
    CronScheduleChanged,

    /// Recovery and index rebuilding completed.
    ///
    /// Emitted once when `rebuild_indexes()` finishes. Wakes sleeping
    /// workers (so they attempt a drain) and the scheduler (so it polls
    /// immediately for due jobs).
    IndexRebuilt,
}

/// Provides a handle to the persistent store.
#[derive(Clone)]
pub struct Store {
    /// The storage configuration used to open this store, retained so that
    /// backup snapshots can recreate the database with matching settings.
    pub(super) config: StorageConfig,

    /// Database handle and all disk keyspaces, shared via `Arc` so that
    /// cloning the `Store` (or moving into `spawn_blocking`) is a single
    /// reference-count bump instead of eight.
    pub(super) ks: Arc<Keyspaces>,

    /// In-memory priority index for ready jobs (lock-free).
    ///
    /// Uses `crossbeam-skiplist` SkipMap + `dashmap` DashMap internally —
    /// no external mutex needed.
    pub(super) ready_index: Arc<ReadyIndex>,

    /// In-memory chronological index of scheduled jobs.
    ///
    /// Ordered by `(ready_at, job_id)`. Only jobs in the `Scheduled` state
    /// appear here. A background task scans this index to promote jobs to
    /// `Ready` once their time arrives.
    pub(super) scheduled_index: Arc<ScheduledIndex>,

    /// In-memory index of cron entries ordered by `next_enqueue_at`.
    pub(super) cron_index: Arc<CronScheduleIndex>,

    /// Default retention period for dead jobs (milliseconds).
    pub(super) default_dead_retention_ms: u64,

    /// Default maximum retries before a failed job is killed.
    pub(super) default_retry_limit: u32,

    /// Default backoff config applied to jobs that don't specify one.
    pub(super) default_backoff: BackoffConfig,

    /// Whether the in-memory indexes have been fully rebuilt.
    ///
    /// Set to `true` in `open()` (fresh/empty indexes are trivially
    /// consistent) and toggled to `false`/`true` around `rebuild_indexes()`.
    /// Guards on `take_next_job`, `list_ready_jobs`, and `scan_ready_ids`
    /// return empty results while `false`.
    pub(super) index_ready: Arc<AtomicBool>,

    /// Current number of in-flight jobs across all connections.
    ///
    /// Incremented when jobs are taken (`take_next_n_jobs`), decremented
    /// when jobs complete, fail, or are requeued. Used by the HTTP layer
    /// to enforce `global_in_flight_limit` without hitting disk.
    pub(super) in_flight_count: Arc<AtomicU64>,

    /// Broadcast channel for store events.
    ///
    /// Subscribers receive notifications when jobs are enqueued, completed,
    /// or otherwise change state. Take handlers use this both to wake up
    /// when new work is available and to prune their in-flight tracking.
    pub(super) event_tx: broadcast::Sender<StoreEvent>,

    /// Server-side auto-batcher for concurrent single-job enqueues.
    /// Owned in an `Arc` so spawn_blocking tasks can cheaply clone the
    /// handle without holding a `&Store` borrow across the .await.
    /// Shut down by dropping (the inner `SyncSender` closes, the worker
    /// drains queued ops and exits).
    pub(super) enqueue_batcher: Arc<EnqueueBatcher>,

    /// Server-side auto-batcher for concurrent completions (acks).
    /// Mirrors `enqueue_batcher` in structure and lifecycle.
    pub(super) complete_batcher: Arc<CompleteBatcher>,
}

/// Groups the fjall database handle and all disk keyspaces into a single
/// cheaply-cloneable unit. Wrapped in `Arc` inside `Store` so that every
/// async method only bumps one reference count instead of three.
pub(super) struct Keyspaces {
    /// Connection to the underlying database.
    pub(super) db: SingleWriterTxDatabase,

    /// Data keyspace — stores jobs (`J` tag), payloads (`P` tag), and
    /// error records (`E` tag). Uses bloom filters and a larger memtable.
    /// See `RecordKind` for the tag layout.
    pub(super) data: SingleWriterTxKeyspace,

    /// Index keyspace — stores status (`S` tag), queue (`Q` tag), type
    /// (`T` tag), and purge-at (`A` tag) secondary indexes. No bloom
    /// filters, smaller memtable. See `IndexKind` for the tag layout.
    pub(super) index: SingleWriterTxKeyspace,

    /// Group committer for batching journal persists.
    ///
    /// Shutdown is driven by dropping this field (which drops the
    /// `SyncSender`, closing the channel). The background thread
    /// drains remaining waiters and performs a final `SyncAll`.
    group_committer: GroupCommitter,

    /// Default commit mode for most operations (dequeue, complete, fail, etc.).
    pub(super) default_commit_mode: CommitMode,

    /// Commit mode for enqueue operations (resolved at construction;
    /// inherits the default when not overridden).
    pub(super) enqueue_commit_mode: CommitMode,
}

impl Keyspaces {
    /// Acquire the single-writer transaction lock.
    pub(super) fn write_tx(&self) -> fjall::SingleWriterWriteTx<'_> {
        self.db.write_tx()
    }

    /// Commit a transaction and block until durable.
    ///
    /// Calls fjall's `tx.commit()` to buffer the write, then sends a sync
    /// request to the group committer and blocks until it completes.
    /// Since this is always called inside `spawn_blocking`, blocking is safe.
    pub(super) fn commit(
        &self,
        tx: fjall::SingleWriterWriteTx<'_>,
        mode: CommitMode,
    ) -> Result<(), StoreError> {
        tx.commit()?;
        let rx = self.group_committer.persist(mode);
        match rx.blocking_recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(StoreError::Db(fjall::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                msg,
            )))),
            Err(_) => Err(StoreError::Db(fjall::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "group committer channel closed",
            )))),
        }
    }
}

impl Store {
    /// Open or create a store at the given path.
    ///
    /// The path refers to the directory in which fjall stores its keyspace
    /// data. `default_completed_retention_ms` and `default_dead_retention_ms`
    /// control how long completed/dead jobs remain visible before purging.
    pub fn open(
        path: impl AsRef<std::path::Path>,
        config: StorageConfig,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let db = SingleWriterTxDatabase::builder(path)
            .cache_size(config.cache_size)
            .manual_journal_persist(true)
            .max_journaling_size(config.journal_size)
            .open()?;

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

        // Size the memtable (in-memory write buffer) to match each table
        // target so that flushes produce appropriately-sized L0 tables.
        //
        // Index keyspaces also disable bloom filters since they are only
        // ever range-scanned, never point-read.
        let data_opts = || {
            fjall::KeyspaceCreateOptions::default()
                .compaction_strategy(data_compaction.clone())
                .max_memtable_size(config.data_table_size)
                .filter_block_pinning_policy(PinningPolicy::all(true))
                .index_block_pinning_policy(PinningPolicy::all(true))
        };
        let index_opts = || {
            fjall::KeyspaceCreateOptions::default()
                .compaction_strategy(index_compaction.clone())
                .max_memtable_size(config.index_table_size)
                .filter_policy(FilterPolicy::disabled())
                .index_block_pinning_policy(PinningPolicy::all(true))
        };

        let data = db.keyspace("data", &data_opts)?;
        let index = db.keyspace("index", &index_opts)?;

        let (event_tx, _) = broadcast::channel(1024);

        // Resolve per-operation commit modes: enqueue inherits the
        // default when not explicitly overridden.
        let default_commit_mode = config.default_commit_mode;
        let enqueue_commit_mode = config.enqueue_commit_mode.unwrap_or(default_commit_mode);

        // Start the group committer — a dedicated OS thread that batches
        // journal persists. Shuts down when Keyspaces is dropped (the
        // SyncSender closes, the thread drains and does a final SyncAll).
        let group_committer = GroupCommitter::start(db.clone(), path.to_path_buf());

        let ks = Arc::new(Keyspaces {
            db,
            data,
            index,
            group_committer,
            default_commit_mode,
            enqueue_commit_mode,
        });
        let ready_index = Arc::new(ReadyIndex::new());
        let scheduled_index = Arc::new(ScheduledIndex::new());

        // Start the enqueue auto-batcher — a dedicated OS thread that
        // coalesces concurrent enqueue requests (singular + bulk) into
        // one tx.commit(). Shuts down when this Store (and any
        // spawn_blocking clones of the Arc) drops — the inner
        // SyncSender closes, the worker drains remaining ops and exits.
        let enqueue_batcher = Arc::new(EnqueueBatcher::start(
            ks.clone(),
            ready_index.clone(),
            scheduled_index.clone(),
            event_tx.clone(),
            config.enqueue_batch_size,
        ));

        let in_flight_count = Arc::new(AtomicU64::new(0));

        // Start the complete auto-batcher — same shape as the enqueue
        // batcher, coalesces concurrent completion (ack) requests.
        let complete_batcher = Arc::new(CompleteBatcher::start(
            ks.clone(),
            ready_index.clone(),
            in_flight_count.clone(),
            event_tx.clone(),
            config.default_completed_retention_ms,
            config.complete_batch_size,
        ));

        Ok(Self {
            config: config.clone(),
            ks,
            ready_index,
            scheduled_index,
            cron_index: Arc::new(CronScheduleIndex::new()),
            default_dead_retention_ms: config.default_dead_retention_ms,
            default_retry_limit: config.default_retry_limit,
            default_backoff: config.default_backoff,
            index_ready: Arc::new(AtomicBool::new(true)),
            in_flight_count,
            event_tx,
            enqueue_batcher,
            complete_batcher,
        })
    }

    /// Subscribe to store events.
    pub fn subscribe(&self) -> broadcast::Receiver<StoreEvent> {
        self.event_tx.subscribe()
    }

    /// Total number of ready jobs across all queues.
    pub fn ready_count(&self) -> usize {
        self.ready_index.len()
    }

    /// Total number of in-flight jobs across all connections.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_count.load(Ordering::Relaxed) as usize
    }

    /// Total number of scheduled jobs.
    pub fn scheduled_count(&self) -> usize {
        self.scheduled_index.len()
    }

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

/// Compute the backoff delay in milliseconds for a given attempt count.
///
/// Formula: `delay_ms = attempts^exponent + base_ms + rand(0..jitter_ms) * (attempts + 1)`
///
/// The jitter component scales linearly with the attempt count so that
/// later retries spread further apart, reducing collision likelihood when
/// many jobs fail at similar times.
pub(super) fn compute_backoff(attempts: u32, backoff: &BackoffConfig) -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let base_delay = (attempts as f32).powf(backoff.exponent) + backoff.base_ms as f32;

    // Cheap random value using the same approach as rand_id() in http.rs.
    let rand_frac = (RandomState::new().build_hasher().finish() as f64) / (u64::MAX as f64); // 0.0..1.0

    let jitter = rand_frac * backoff.jitter_ms as f64 * (attempts as f64 + 1.0);

    (base_delay as f64 + jitter) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::store::options::{BulkDeleteOptions, EnqueueOptions, ListJobsOptions};
    use crate::store::storage_config::StorageConfig;
    use crate::store::test_support::test_store;
    use crate::store::types::JobStatus;
    use crate::time::now_millis;

    // --- compute_backoff tests ---

    #[test]
    fn compute_backoff_zero_jitter_is_deterministic() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0,
        };

        // attempts=1: 1^2 + 100 = 101
        assert_eq!(compute_backoff(1, &backoff), 101);
        // attempts=2: 2^2 + 100 = 104
        assert_eq!(compute_backoff(2, &backoff), 104);
        // attempts=3: 3^2 + 100 = 109
        assert_eq!(compute_backoff(3, &backoff), 109);
    }

    #[test]
    fn compute_backoff_with_jitter_stays_in_range() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 50,
        };

        // Run many times to exercise randomness.
        for attempts in 1..=10 {
            let delay = compute_backoff(attempts, &backoff);
            let base = (attempts as f64).powf(2.0) + 100.0;
            let max_jitter = 50.0 * (attempts as f64 + 1.0);
            assert!(
                delay >= base as u64,
                "delay {delay} below base {base} for attempt {attempts}"
            );
            assert!(
                delay <= (base + max_jitter) as u64,
                "delay {delay} above max {} for attempt {attempts}",
                base + max_jitter
            );
        }
    }

    #[test]
    fn compute_backoff_zero_attempts() {
        let backoff = BackoffConfig {
            exponent: 2.0,
            base_ms: 100,
            jitter_ms: 0,
        };

        // attempts=0: 0^2 + 100 = 100
        assert_eq!(compute_backoff(0, &backoff), 100);
    }

    #[tokio::test]
    async fn fsync_commit_mode_smoke_test() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_commit_mode = CommitMode::Fsync;
        let store = Store::open(dir.path().join("data"), config).unwrap();

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();

        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, job.id);
        assert_eq!(fetched.status, u8::from(JobStatus::Ready));
    }

    #[tokio::test]
    async fn fsync_enqueue_commit_mode_smoke_test() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config.default_commit_mode = CommitMode::Buffered;
        config.enqueue_commit_mode = Some(CommitMode::Fsync);
        let store = Store::open(dir.path().join("data"), config).unwrap();

        let job = store
            .enqueue(
                now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!({"x": 1})),
            )
            .await
            .unwrap()
            .into_job();

        let fetched = store.get_job(now_millis(), &job.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, job.id);
        assert_eq!(fetched.status, u8::from(JobStatus::Ready));
    }

    // --- ready_count / scheduled_count tests ---

    #[tokio::test]
    async fn ready_count_tracks_enqueue_and_take() {
        let store = test_store();
        let now = now_millis();

        assert_eq!(store.ready_count(), 0);

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
        assert_eq!(store.ready_count(), 2);

        // Taking a job should decrement the ready count.
        store.take_next_job(now, &HashSet::new()).await.unwrap();
        assert_eq!(store.ready_count(), 1);
    }

    #[tokio::test]
    async fn ready_count_excludes_scheduled() {
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
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(store.ready_count(), 1);
    }

    #[tokio::test]
    async fn scheduled_count_tracks_enqueue_and_promote() {
        let store = test_store();
        let now = now_millis();

        assert_eq!(store.scheduled_count(), 0);

        let job = store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();
        store
            .enqueue(
                now,
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 120_000),
            )
            .await
            .unwrap()
            .into_job();
        assert_eq!(store.scheduled_count(), 2);

        // Promoting a scheduled job should decrement the scheduled count.
        store.promote_scheduled(&job).await.unwrap();
        assert_eq!(store.scheduled_count(), 1);
    }

    #[tokio::test]
    async fn scheduled_count_excludes_ready() {
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
                EnqueueOptions::new("test", "default", serde_json::json!(null))
                    .ready_at(now + 60_000),
            )
            .await
            .unwrap()
            .into_job();

        assert_eq!(store.scheduled_count(), 1);
    }

    // --- compact_all + auto-compact ---

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
