// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Recovery latency benchmark for the Zanxio job queue.
//!
//! Simulates repeated restart cycles with a growing backlog of unprocessed
//! jobs. Each cycle enqueues K jobs, takes only half, then restarts. This
//! builds up fragmented L0 files across restarts — matching the real-world
//! pattern where a server is restarted while jobs are still queued.
//!
//! After the warmup cycles, one final cycle measures per-operation latency.
//!
//! Configure via environment variables:
//!   BENCH_CYCLES           — restart cycles before measuring (default 3)
//!   BENCH_ENQUEUE          — jobs enqueued per cycle (default 5000)
//!   BENCH_MEASURE          — jobs to take in the final measurement cycle (default 1000)
//!   BENCH_DATA_TABLE_SIZE  — StorageConfig.data_table_size (default 1048576)
//!   BENCH_INDEX_TABLE_SIZE — StorageConfig.index_table_size (default 262144)
//!   BENCH_L0_THRESHOLD     — L0 compaction trigger (default 4)

use std::collections::HashSet;
use std::time::{Duration, Instant};

use zanxio::store::{EnqueueOptions, StorageConfig, Store};
use zanxio::time::now_millis;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Percentile reporting
// ---------------------------------------------------------------------------

struct Stats {
    min: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
    mean: Duration,
}

fn compute_stats(times: &mut [Duration]) -> Stats {
    times.sort();
    let n = times.len();
    let sum: Duration = times.iter().sum();
    Stats {
        min: times[0],
        p50: times[n / 2],
        p95: times[n * 95 / 100],
        p99: times[n * 99 / 100],
        max: times[n - 1],
        mean: sum / n as u32,
    }
}

fn print_stats(label: &str, stats: &Stats) {
    println!(
        "  {label:<10} min={:<10} p50={:<10} p95={:<10} p99={:<10} max={:<10} mean={}",
        format_duration(stats.min),
        format_duration(stats.p50),
        format_duration(stats.p95),
        format_duration(stats.p99),
        format_duration(stats.max),
        format_duration(stats.mean),
    );
}

fn format_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us}us")
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Total size of a directory tree, formatted for humans.
fn dir_size_human(path: &std::path::Path) -> String {
    fn dir_size(path: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let ft = entry.file_type().unwrap();
                if ft.is_file() {
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                } else if ft.is_dir() {
                    total += dir_size(&entry.path());
                }
            }
        }
        total
    }
    let bytes = dir_size(path);
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Enqueue `n` jobs into the store.
async fn enqueue_jobs(store: &Store, n: usize) {
    for _ in 0..n {
        let now = now_millis();
        store
            .enqueue(
                now,
                EnqueueOptions::new(
                    "bench",
                    "bench",
                    serde_json::json!({"hello": "recovery benchmark"}),
                ),
            )
            .await
            .unwrap();
    }
}

/// Take `n` jobs from the store and mark them completed.
async fn drain_jobs(store: &Store, queues: &HashSet<String>, n: usize) {
    for _ in 0..n {
        let now = now_millis();
        let taken = store.take_next_job(now, queues).await.unwrap();
        assert!(taken.is_some(), "expected a job to be available");
        let job = taken.unwrap();
        store.mark_completed(&job.id).await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn main() {
    let cycles = env_or("BENCH_CYCLES", 3) as usize;
    let enqueue_per_cycle = env_or("BENCH_ENQUEUE", 5_000) as usize;
    let measure_n = env_or("BENCH_MEASURE", 1_000) as usize;
    let take_per_cycle = enqueue_per_cycle / 2;
    let config = StorageConfig {
        data_table_size: env_or("BENCH_DATA_TABLE_SIZE", 1024 * 1024),
        index_table_size: env_or("BENCH_INDEX_TABLE_SIZE", 256 * 1024),
        l0_threshold: env_or("BENCH_L0_THRESHOLD", 4) as u8,
        ..Default::default()
    };

    println!("Recovery latency benchmark");
    println!(
        "  cycles={cycles}  enqueue_per_cycle={enqueue_per_cycle}  take_per_cycle={take_per_cycle}  measure={measure_n}"
    );
    println!(
        "  data_table_size={}  index_table_size={}  l0_threshold={}",
        config.data_table_size, config.index_table_size, config.l0_threshold
    );
    println!();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let queues: HashSet<String> = ["bench".to_string()].into_iter().collect();

    // Warmup cycles: enqueue K jobs, take K/2, restart. Each cycle leaves
    // half its jobs in ready state and builds up L0 file history across
    // restarts. The backlog grows with each cycle.
    for cycle in 0..cycles {
        let store = Store::open(&path, config).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        println!(
            "Cycle {}/{cycles}: enqueue {enqueue_per_cycle}, take {take_per_cycle}, restart...",
            cycle + 1
        );
        rt.block_on(async {
            enqueue_jobs(&store, enqueue_per_cycle).await;
            drain_jobs(&store, &queues, take_per_cycle).await;
        });

        drop(rt);
        drop(store);
        println!("  data dir size: {}", dir_size_human(&path));
    }

    // Final cycle: reopen and measure per-operation latency.
    println!();
    println!("Measure: reopen + {measure_n} enqueue/take/complete cycles...");

    let reopen_start = Instant::now();
    let store = Store::open(&path, config).unwrap();
    let reopen_time = reopen_start.elapsed();

    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut enqueue_times = Vec::with_capacity(measure_n);
    let mut take_times = Vec::with_capacity(measure_n);
    let mut complete_times = Vec::with_capacity(measure_n);

    rt.block_on(async {
        for _ in 0..measure_n {
            let now = now_millis();

            let t = Instant::now();
            let job = store
                .enqueue(
                    now,
                    EnqueueOptions::new("bench", "bench", serde_json::json!({})),
                )
                .await
                .unwrap();
            enqueue_times.push(t.elapsed());

            let t = Instant::now();
            let taken = store.take_next_job(now, &queues).await.unwrap();
            assert!(taken.is_some(), "expected a job to be available");
            take_times.push(t.elapsed());

            let t = Instant::now();
            store.mark_completed(&job.id).await.unwrap();
            complete_times.push(t.elapsed());
        }
    });

    // Report.
    println!();
    println!("Results:");
    println!("  reopen     {}", format_duration(reopen_time));
    print_stats("enqueue", &compute_stats(&mut enqueue_times));
    print_stats("take", &compute_stats(&mut take_times));
    print_stats("complete", &compute_stats(&mut complete_times));
}
