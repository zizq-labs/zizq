// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Throughput benchmarks for the Zanxio job queue.
//!
//! Each scenario starts an in-process axum server on a random port and
//! drives it with reqwest as the HTTP client. Queue names are unique per
//! iteration so accumulated data doesn't interfere between samples.
//!
//! Configure via environment variables:
//!   BENCH_N          — jobs per iteration (default 100)
//!   BENCH_ENQUEUERS  — parallel enqueue workers (default 4)
//!   BENCH_DEQUEUERS  — parallel dequeue workers (default 4)
//!   BENCH_PREFETCH   — per-stream prefetch limit for dequeue (default 10)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures_util::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
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

/// How acks are dispatched relative to stream reads.
#[derive(Clone, Copy)]
enum AckStrategy {
    /// Wait for ack response before reading the next job from the stream.
    Sequential,
    /// Fire ack without waiting; overlap with next job read via select!.
    Overlapped,
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

    /// Accept header value for streaming `GET /jobs/take`.
    fn stream_accept(&self) -> &'static str {
        match self.format {
            Format::Json => "application/x-ndjson",
            Format::MsgPack => "application/vnd.zanxio.msgpack-stream",
        }
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

/// Enqueue N jobs across K workers and return the wall-clock time. Workers pull
/// pre-encoded bodies from a shared channel, so the degree of parallelism is
/// controlled independently from the job count. This exercises h2 multiplexing
/// (many requests on one connection) vs HTTP/1.1 (one connection per worker).
async fn enqueue_concurrent(
    cfg: &BenchConfig,
    base_url: &str,
    n: usize,
    workers: usize,
) -> Duration {
    let client = cfg.client();
    let queue = unique_queue();
    let url: Arc<str> = format!("{base_url}/jobs").into();
    let content_type = cfg.content_type();
    let accept = cfg.accept();

    // Pre-encode all request bodies so serialization doesn't count against
    // the concurrent send timing.
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(n);
    for i in 0..n {
        tx.send(cfg.encode(&EnqueueBody {
            queue: &queue,
            payload: json!({"i": i}),
        }))
        .await
        .unwrap();
    }
    // Close the sender so workers drain to completion.
    drop(tx);

    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    let start = Instant::now();

    let handles: Vec<_> = (0..workers)
        .map(|_| {
            let client = client.clone();
            let url = Arc::clone(&url);
            let rx = Arc::clone(&rx);
            tokio::spawn(async move {
                while let Some(body) = rx.lock().await.recv().await {
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
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    start.elapsed()
}

// ---------------------------------------------------------------------------
// Scenario: Dequeue (stream + ack)
// ---------------------------------------------------------------------------

/// Minimal deserialization target — we only need the job id for acking.
#[derive(Deserialize)]
struct TakeJob {
    id: String,
}

/// Format-aware stream reader that wraps a `reqwest::Response` and yields
/// deserialized job objects. Handles both NDJSON (line-buffered text) and
/// length-prefixed msgpack (binary).
enum JobStream {
    Ndjson {
        resp: reqwest::Response,
        buf: String,
    },
    MsgPack {
        resp: reqwest::Response,
        buf: Vec<u8>,
    },
}

impl JobStream {
    fn new(format: Format, resp: reqwest::Response) -> Self {
        match format {
            Format::Json => JobStream::Ndjson {
                resp,
                buf: String::new(),
            },
            Format::MsgPack => JobStream::MsgPack {
                resp,
                buf: Vec::new(),
            },
        }
    }

    /// Read the next job from the stream, returning `None` when the stream
    /// closes (server sent EOF).
    async fn next_job(&mut self) -> Option<TakeJob> {
        match self {
            JobStream::Ndjson { resp, buf } => {
                // Buffer chunks and split on newlines. Empty lines are
                // heartbeats — skip them and keep reading.
                loop {
                    if let Some(pos) = buf.find('\n') {
                        let line: String = buf.drain(..=pos).collect();
                        let line = line.trim();
                        if line.is_empty() {
                            // Heartbeat — skip.
                            continue;
                        }
                        return Some(serde_json::from_str(line).unwrap());
                    }
                    // Need more data from the network.
                    let chunk = resp.chunk().await.ok()??;
                    buf.push_str(std::str::from_utf8(&chunk).unwrap());
                }
            }
            JobStream::MsgPack { resp, buf } => {
                // Length-prefixed binary framing: 4-byte BE u32 length,
                // then that many bytes of msgpack payload. Zero length
                // means heartbeat.
                loop {
                    // Accumulate at least 4 bytes for the length prefix.
                    while buf.len() < 4 {
                        let chunk = resp.chunk().await.ok()??;
                        buf.extend_from_slice(&chunk);
                    }
                    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                    buf.drain(..4);

                    if len == 0 {
                        // Heartbeat — skip.
                        continue;
                    }

                    // Accumulate the full msgpack payload.
                    while buf.len() < len {
                        let chunk = resp.chunk().await.ok()??;
                        buf.extend_from_slice(&chunk);
                    }
                    let payload: Vec<u8> = buf.drain(..len).collect();
                    return Some(rmp_serde::from_slice(&payload).unwrap());
                }
            }
        }
    }
}

/// Enqueue N jobs to a given queue as untimed setup for dequeue benchmarks.
/// Always uses JSON for simplicity — the format being benchmarked is the
/// stream/ack format, not the enqueue format.
async fn pre_enqueue(base_url: &str, queue: &str, n: usize) {
    let client = Client::builder().tcp_nodelay(true).build().unwrap();
    let url = format!("{base_url}/jobs");

    for i in 0..n {
        let body = EnqueueBody {
            queue,
            payload: json!({"i": i}),
        };
        let resp = client
            .post(&url)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap())
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "pre_enqueue failed: {}",
            resp.status()
        );
        let _ = resp.bytes().await;
    }
}

/// Take N jobs from a stream and ack each one. Runs as one of potentially
/// many concurrent workers.
async fn dequeue_worker(
    cfg: &BenchConfig,
    ack_strategy: AckStrategy,
    base_url: &str,
    queue: &str,
    prefetch: u64,
    acked: Arc<AtomicU64>,
    target: u64,
    done_tx: Arc<watch::Sender<bool>>,
    mut done_rx: watch::Receiver<bool>,
) {
    let client = cfg.client();
    let stream_url = format!("{base_url}/jobs/take?queue={queue}&prefetch={prefetch}");

    let resp = client
        .get(&stream_url)
        .header("accept", cfg.stream_accept())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream open failed: {}",
        resp.status()
    );

    let mut stream = JobStream::new(cfg.format, resp);

    match ack_strategy {
        AckStrategy::Sequential => loop {
            tokio::select! {
                job = stream.next_job() => {
                    let Some(job) = job else { break };
                    let ack_url = format!(
                        "{base_url}/jobs/{}/success",
                        job.id
                    );
                    let resp = client.post(&ack_url).send().await.unwrap();
                    assert_eq!(resp.status(), 204, "ack failed: {}", resp.status());
                    let prev = acked.fetch_add(1, Ordering::Relaxed);
                    if prev + 1 >= target {
                        let _ = done_tx.send(true);
                    }
                }
                _ = done_rx.changed() => break,
            }
        },
        AckStrategy::Overlapped => {
            let mut in_flight = FuturesUnordered::new();
            loop {
                tokio::select! {
                    // Prefer draining completed acks first so the in-flight
                    // set doesn't grow unboundedly.
                    biased;

                    result = in_flight.next(), if !in_flight.is_empty() => {
                        // Ack completed — unwrap the join result.
                        result.unwrap();
                        let prev = acked.fetch_add(1, Ordering::Relaxed);
                        if prev + 1 >= target {
                            let _ = done_tx.send(true);
                        }
                    }

                    job = stream.next_job() => {
                        let Some(job) = job else { break };
                        let client = client.clone();
                        let url = format!(
                            "{base_url}/jobs/{}/success",
                            job.id
                        );
                        in_flight.push(async move {
                            let resp = client.post(&url).send().await.unwrap();
                            assert_eq!(resp.status(), 204, "ack failed: {}", resp.status());
                        });
                    }

                    _ = done_rx.changed() => break,
                }
            }
            // Drain remaining in-flight acks.
            while in_flight.next().await.is_some() {
                let prev = acked.fetch_add(1, Ordering::Relaxed);
                if prev + 1 >= target {
                    let _ = done_tx.send(true);
                }
            }
        }
    }
}

/// Dequeue N jobs via streaming + ack and return the wall-clock time.
/// Pre-enqueue happens before the timer starts.
async fn dequeue(
    cfg: &BenchConfig,
    ack_strategy: AckStrategy,
    base_url: &str,
    n: usize,
    workers: usize,
    prefetch: u64,
) -> Duration {
    let queue = unique_queue();

    // Untimed setup: seed the queue with jobs.
    pre_enqueue(base_url, &queue, n).await;

    let target = n as u64;
    let acked = Arc::new(AtomicU64::new(0));
    let (done_tx, done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);

    let start = Instant::now();

    let handles: Vec<_> = (0..workers)
        .map(|_| {
            let cfg = cfg.clone();
            let base_url = base_url.to_owned();
            let queue = queue.clone();
            let acked = Arc::clone(&acked);
            let done_tx = Arc::clone(&done_tx);
            let done_rx = done_rx.clone();
            tokio::spawn(async move {
                dequeue_worker(
                    &cfg,
                    ack_strategy,
                    &base_url,
                    &queue,
                    prefetch,
                    acked,
                    target,
                    done_tx,
                    done_rx,
                )
                .await;
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

    let mut group = c.benchmark_group(format!("enqueue (n={n})"));
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
    let enqueuers = env_or("BENCH_ENQUEUERS", 4) as usize;

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

    let mut group = c.benchmark_group(format!("enqueue-concurrent (n={n} enqueuers={enqueuers})"));
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
                        total += enqueue_concurrent(&cfg, &url, n, enqueuers).await;
                    }
                    total
                }
            });
        });
    }

    group.finish();
}

fn bench_dequeue(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let url = rt.block_on(async { start_server().await.0 });
    let n = env_or("BENCH_N", 100) as usize;
    let dequeuers = env_or("BENCH_DEQUEUERS", 4) as usize;
    let prefetch = env_or("BENCH_PREFETCH", 10);

    // All dequeue configs use KeepAlive — the stream is inherently a
    // persistent connection, and acks benefit from reuse too.
    let configs: Vec<(BenchConfig, AckStrategy)> = vec![
        (
            BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::Json),
            AckStrategy::Sequential,
        ),
        (
            BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::MsgPack),
            AckStrategy::Sequential,
        ),
        (
            BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::Json),
            AckStrategy::Sequential,
        ),
        (
            BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::MsgPack),
            AckStrategy::Sequential,
        ),
        (
            BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::Json),
            AckStrategy::Overlapped,
        ),
        (
            BenchConfig::new(Protocol::Http1, Persistence::KeepAlive, Format::MsgPack),
            AckStrategy::Overlapped,
        ),
        (
            BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::Json),
            AckStrategy::Overlapped,
        ),
        (
            BenchConfig::new(Protocol::H2c, Persistence::KeepAlive, Format::MsgPack),
            AckStrategy::Overlapped,
        ),
    ];

    let mut group = c.benchmark_group(format!(
        "dequeue (n={n} dequeuers={dequeuers} prefetch={prefetch})"
    ));
    group.throughput(Throughput::Elements(n as u64));
    group.sample_size(10);

    for (cfg, ack_strategy) in &configs {
        let proto = match cfg.protocol {
            Protocol::Http1 => "http1",
            Protocol::H2c => "h2c",
        };
        let fmt = match cfg.format {
            Format::Json => "json",
            Format::MsgPack => "msgpack",
        };
        let ack = match ack_strategy {
            AckStrategy::Sequential => "sequential",
            AckStrategy::Overlapped => "overlapped",
        };
        let label = format!("{proto} {fmt} {ack}");

        group.bench_function(&label, |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let url = url.clone();
                let cfg = cfg.clone();
                let ack_strategy = *ack_strategy;
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += dequeue(&cfg, ack_strategy, &url, n, dequeuers, prefetch).await;
                    }
                    total
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue,
    bench_enqueue_concurrent,
    bench_dequeue
);
criterion_main!(benches);
