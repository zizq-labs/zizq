// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Throughput benchmarks for the Zanxio job queue.
//!
//! Each scenario starts an in-process axum server on a random port and
//! drives it with reqwest as the HTTP client. Queue names are unique per
//! iteration so accumulated data doesn't interfere between samples.
//!
//! Configure via environment variables:
//!   BENCH_N        — jobs per iteration (default 100)
//!   BENCH_WORKERS  — parallel workers for multi-worker scenarios (default 4)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use reqwest::Client;
use serde::Serialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use zanxio::http::{self, AppState, DEFAULT_GLOBAL_WORKING_LIMIT, DEFAULT_HEARTBEAT_SECONDS};
use zanxio::store::Store;

// ---------------------------------------------------------------------------
// Bench configuration
// ---------------------------------------------------------------------------

/// HTTP protocol version.
#[derive(Clone, Copy)]
enum Protocol {
    Http1,
    H2c,
}

/// Whether the client reuses TCP connections across requests.
#[derive(Clone, Copy)]
enum Persistence {
    /// New TCP connection (and h2 session) per request.
    NewConn,
    /// Keep-alive (HTTP/1.1) or multiplexed (h2) connection reuse.
    KeepAlive,
}

/// Wire format for request/response bodies.
#[derive(Clone, Copy)]
enum Format {
    Json,
    MsgPack,
}

/// Fully describes how a benchmark talks to the server.
#[derive(Clone)]
struct BenchConfig {
    protocol: Protocol,
    persistence: Persistence,
    format: Format,
}

impl BenchConfig {
    fn new(protocol: Protocol, persistence: Persistence, format: Format) -> Self {
        Self {
            protocol,
            persistence,
            format,
        }
    }

    /// Build a reqwest client matching this configuration.
    fn client(&self) -> Client {
        let mut builder = Client::builder();
        builder = match self.protocol {
            Protocol::Http1 => builder.http1_only(),
            Protocol::H2c => builder.http2_prior_knowledge(),
        };
        if matches!(self.persistence, Persistence::NewConn) {
            builder = builder.pool_max_idle_per_host(0);
        }
        builder.tcp_nodelay(true).build().unwrap()
    }

    /// Human-readable label for criterion output.
    fn label(&self) -> String {
        let proto = match self.protocol {
            Protocol::Http1 => "http1",
            Protocol::H2c => "h2c",
        };
        let persist = match self.persistence {
            Persistence::NewConn => "new-conn",
            Persistence::KeepAlive => "keep-alive",
        };
        let fmt = match self.format {
            Format::Json => "json",
            Format::MsgPack => "msgpack",
        };
        format!("{proto} {persist} {fmt}")
    }

    /// MIME type for the `Content-Type` header.
    fn content_type(&self) -> &'static str {
        match self.format {
            Format::Json => "application/json",
            Format::MsgPack => "application/msgpack",
        }
    }

    /// MIME type for the `Accept` header.
    fn accept(&self) -> &'static str {
        self.content_type()
    }

    /// Serialize a value in the configured format.
    fn encode<T: Serialize>(&self, value: &T) -> Vec<u8> {
        match self.format {
            Format::Json => serde_json::to_vec(value).unwrap(),
            Format::MsgPack => rmp_serde::to_vec_named(value).unwrap(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Atomic counter for unique queue names across all iterations.
static QUEUE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Return a unique queue name for this iteration.
fn unique_queue() -> String {
    let n = QUEUE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("bench-{n}")
}

/// Read an env var as a u64, falling back to `default`.
fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Start an in-process server on a random port. Returns the base URL and
/// the server task handle. The tempdir is leaked so it outlives the bench.
async fn start_server() -> (String, JoinHandle<()>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    // Leak so the directory isn't cleaned up while the bench runs.
    std::mem::forget(dir);

    let store = Store::open(&path).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(());
    // Leak the sender so the shutdown signal is never triggered.
    std::mem::forget(shutdown_tx);

    let state = Arc::new(AppState {
        store: store.clone(),
        heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_SECONDS),
        global_working_limit: DEFAULT_GLOBAL_WORKING_LIMIT,
        global_in_flight: AtomicU64::new(0),
        shutdown: shutdown_rx.clone(),
    });

    // Spawn the background scheduler.
    tokio::spawn(zanxio::scheduler::run(store, shutdown_rx));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Accept connections ourselves so we can set TCP_NODELAY before handing
    // them to hyper. axum::serve doesn't expose this and Nagle's algorithm
    // combined with delayed ACKs devastates h2 latency.
    let app = http::app(state);
    let handle = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            let svc = hyper_util::service::TowerToHyperService::new(app.clone());
            tokio::spawn(async move {
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await
                    .ok();
            });
        }
    });

    (format!("http://{addr}"), handle)
}

// ---------------------------------------------------------------------------
// Scenario: Enqueue
// ---------------------------------------------------------------------------

/// Request body for POST /jobs.
#[derive(Serialize)]
struct EnqueueBody<'a> {
    queue: &'a str,
    payload: serde_json::Value,
}

/// Enqueue N jobs and return the wall-clock time (excluding setup).
async fn enqueue(cfg: &BenchConfig, base_url: &str, n: usize) -> Duration {
    let client = cfg.client();
    let queue = unique_queue();
    let url = format!("{base_url}/jobs");

    let start = Instant::now();
    for i in 0..n {
        let body = EnqueueBody {
            queue: &queue,
            payload: json!({"i": i}),
        };
        let resp = client
            .post(&url)
            .header("content-type", cfg.content_type())
            .header("accept", cfg.accept())
            .body(cfg.encode(&body))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "enqueue failed: {}",
            resp.status()
        );
        // Consume the full response body so the connection lifecycle completes.
        let _ = resp.bytes().await;
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Scenario: Concurrent Enqueue
// ---------------------------------------------------------------------------

/// Enqueue N jobs concurrently (all requests in-flight at once) and return the
/// wall-clock time. This exercises h2 multiplexing: many requests share one TCP
/// connection simultaneously, like APNS-style workloads. For HTTP/1.1 each
/// concurrent request needs its own connection, so h2 should have an advantage.
async fn enqueue_concurrent(cfg: &BenchConfig, base_url: &str, n: usize) -> Duration {
    let client = cfg.client();
    let queue = unique_queue();
    let url: Arc<str> = format!("{base_url}/jobs").into();
    let content_type = cfg.content_type();
    let accept = cfg.accept();

    // Pre-encode all request bodies so serialization doesn't count against
    // the concurrent send timing.
    let bodies: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            cfg.encode(&EnqueueBody {
                queue: &queue,
                payload: json!({"i": i}),
            })
        })
        .collect();

    let start = Instant::now();

    let handles: Vec<_> = bodies
        .into_iter()
        .map(|body| {
            let client = client.clone();
            let url = Arc::clone(&url);
            tokio::spawn(async move {
                let resp = client
                    .post(&*url)
                    .header("content-type", content_type)
                    .header("accept", accept)
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                assert!(
                    resp.status().is_success(),
                    "enqueue failed: {}",
                    resp.status()
                );
                let _ = resp.bytes().await;
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    start.elapsed()
}

// ---------------------------------------------------------------------------
// Criterion wiring
// ---------------------------------------------------------------------------

fn bench_enqueue(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let url = rt.block_on(async { start_server().await.0 });
    let n = env_or("BENCH_N", 100) as usize;

    let configs = [
        BenchConfig::new(Protocol::Http1, Persistence::NewConn, Format::Json),
        BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::Json),
        BenchConfig::new(Protocol::Http1, Persistence::NewConn, Format::MsgPack),
        BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::MsgPack),
        BenchConfig::new(Protocol::H2c, Persistence::NewConn, Format::Json),
        BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::Json),
        BenchConfig::new(Protocol::H2c, Persistence::NewConn, Format::MsgPack),
        BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::MsgPack),
    ];

    let mut group = c.benchmark_group("enqueue");
    group.throughput(Throughput::Elements(n as u64));
    group.sample_size(10);

    for cfg in &configs {
        group.bench_function(cfg.label(), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let url = url.clone();
                let cfg = cfg.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += enqueue(&cfg, &url, n).await;
                    }
                    total
                }
            });
        });
    }

    group.finish();
}

fn bench_enqueue_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let url = rt.block_on(async { start_server().await.0 });
    let n = env_or("BENCH_N", 100) as usize;

    // Only keep-alive configs make sense for concurrent sends — the whole
    // point is many requests multiplexed on one connection (h2) vs many
    // pipelined on one connection (http1). new-conn would just be N
    // parallel connections which isn't what we're testing.
    let configs = [
        BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::Json),
        BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::MsgPack),
        BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::Json),
        BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::MsgPack),
    ];

    let mut group = c.benchmark_group("enqueue-concurrent");
    group.throughput(Throughput::Elements(n as u64));
    group.sample_size(10);

    for cfg in &configs {
        group.bench_function(cfg.label(), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let url = url.clone();
                let cfg = cfg.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += enqueue_concurrent(&cfg, &url, n).await;
                    }
                    total
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_enqueue, bench_enqueue_concurrent);
criterion_main!(benches);
