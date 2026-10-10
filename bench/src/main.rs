//! Benchmark harness for the Zizq server.
//!
//! Starts the given server binary with a fresh root directory, pushes
//! `--jobs` jobs through it, and samples throughput, memory, CPU and disk
//! usage as it goes. Each run writes one NDJSON file: a header line
//! describing the run, a line per sample, and a summary line.
//!
//! ```text
//! cargo run --release -- --server path/to/zizq --jobs 500000
//! cargo run --release -- --server path/to/zizq --mode concurrent \
//!     -- --default-completed-job-retention 7d
//! ```
//!
//! Arguments after `--` are passed to `zizq serve`.

mod load;
mod sampler;
mod server;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use serde::Serialize;
use sysinfo::System;
use zizq::{Client, Format};

use load::Counters;
use sampler::{Phase, PhaseCell};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Version of the `zizq` client crate, pinned in `Cargo.toml`.
const CLIENT_VERSION: &str = "0.7.0";

#[derive(Parser, Serialize)]
#[command(about = "Benchmark a Zizq server binary")]
struct Args {
    /// Path to the `zizq` server binary to benchmark.
    #[arg(long)]
    server: PathBuf,

    /// Name for this build, to tell apart builds of the same version
    /// (e.g. `musl`, `glibc`). Defaults to a prefix of its SHA-256.
    #[arg(long)]
    label: Option<String>,

    /// `upfront` enqueues every job before starting the workers.
    /// `concurrent` starts the workers first and enqueues while they drain.
    #[arg(long, value_enum, default_value_t = Mode::Upfront)]
    mode: Mode,

    /// Number of jobs to push through the queue.
    #[arg(long, default_value_t = 50_000)]
    jobs: u64,

    /// Number of workers, each with its own take connection.
    #[arg(long, default_value_t = 1)]
    workers: usize,

    /// Jobs each worker processes at once.
    #[arg(long, default_value_t = 100)]
    concurrency: usize,

    /// Jobs each worker buffers ahead. Defaults to twice `--concurrency`.
    #[arg(long)]
    prefetch: Option<usize>,

    /// Jobs per bulk enqueue request.
    #[arg(long, default_value_t = 1000)]
    batch_size: u64,

    /// Bulk enqueue requests in flight at once.
    #[arg(long, default_value_t = 4)]
    enqueue_concurrency: usize,

    /// Wire format used by the client.
    #[arg(long, value_enum, default_value_t = WireFormat::Json)]
    format: WireFormat,

    /// Milliseconds between samples.
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,

    /// Directory results are written under, as `<version>/<file>.ndjson`.
    #[arg(long, default_value = "results")]
    out: PathBuf,

    /// Keep the server's root directory after the run, for inspection.
    #[arg(long)]
    keep_root: bool,

    /// Extra arguments passed to `zizq serve`.
    #[arg(last = true)]
    server_args: Vec<String>,
}

#[derive(Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Upfront,
    Concurrent,
}

#[derive(Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireFormat {
    Json,
    Msgpack,
}

/// First line of the results file.
#[derive(Serialize)]
struct Header<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    version: &'a str,
    label: &'a str,
    sha256: &'a str,
    client_version: &'static str,
    started_at: u64,
    host: Host,
    args: &'a Args,
}

/// The machine the benchmark ran on. Results are only comparable between
/// runs on the same host.
#[derive(Serialize)]
struct Host {
    os: Option<String>,
    arch: String,
    cpu: String,
    cores: Option<usize>,
    memory: u64,
}

/// Last line of the results file.
#[derive(Serialize)]
struct Summary {
    #[serde(rename = "type")]
    kind: &'static str,
    enqueue_secs: f64,
    enqueue_rate: f64,
    drain_secs: f64,
    drain_rate: f64,
    total_secs: f64,
    peak_rss: u64,
    final_disk: u64,
    shutdown_secs: f64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let (version, sha256) = server::identify(&args.server).await?;
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| sha256[..8].to_string());
    let started_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let dir = args.out.join(&version);
    std::fs::create_dir_all(&dir)?;
    let mode = match args.mode {
        Mode::Upfront => "upfront",
        Mode::Concurrent => "concurrent",
    };
    let name = format!("{label}-{mode}-{}-{started_at}", args.jobs);
    let results_path = dir.join(format!("{name}.ndjson"));
    let out = Arc::new(Mutex::new(BufWriter::new(File::create(&results_path)?)));

    write_line(
        &out,
        &Header {
            kind: "header",
            version: &version,
            label: &label,
            sha256: &sha256,
            client_version: CLIENT_VERSION,
            started_at,
            host: host(),
            args: &args,
        },
    )?;

    let root_dir = std::env::temp_dir().join(format!("zizq-bench-{name}"));
    let server = server::start(
        &args.server,
        &root_dir,
        File::create(dir.join(format!("{name}.log")))?,
        &args.server_args,
    )
    .await?;

    // Producers and workers get separate clients, and so separate
    // connections, as they would as separate processes in production.
    let client = || {
        Client::builder()
            .url(&server.url)
            .format(match args.format {
                WireFormat::Json => Format::Json,
                WireFormat::Msgpack => Format::MessagePack,
            })
            .build()
    };

    let counters = Arc::new(Counters::default());
    let phase = Arc::new(PhaseCell::default());
    let started = Instant::now();
    let sampler = sampler::start(
        out.clone(),
        Duration::from_millis(args.interval_ms),
        server.pid,
        root_dir.clone(),
        counters.clone(),
        phase.clone(),
        started,
    );

    let enqueue = load::enqueue(
        client()?,
        args.jobs,
        args.batch_size,
        args.enqueue_concurrency,
        counters.clone(),
    );
    let drain = load::drain(
        client()?,
        args.jobs,
        args.workers,
        args.concurrency,
        args.prefetch.unwrap_or(args.concurrency * 2),
        counters,
    );

    let (enqueue_secs, drain_secs) = match args.mode {
        Mode::Upfront => {
            phase.set(Phase::Enqueue);
            enqueue.await?;
            let enqueue_secs = started.elapsed().as_secs_f64();

            phase.set(Phase::Drain);
            drain.await?;
            (enqueue_secs, started.elapsed().as_secs_f64() - enqueue_secs)
        }
        Mode::Concurrent => {
            phase.set(Phase::Both);
            let drain = tokio::spawn(drain);
            enqueue.await?;
            let enqueue_secs = started.elapsed().as_secs_f64();

            phase.set(Phase::Drain);
            drain.await??;
            (enqueue_secs, started.elapsed().as_secs_f64())
        }
    };
    let total_secs = started.elapsed().as_secs_f64();

    let peak_rss = sampler.stop().await?;
    let final_disk = sampler::dir_size(&root_dir);
    let shutdown_secs = server.stop().await?.as_secs_f64();

    let jobs = args.jobs as f64;
    let summary = Summary {
        kind: "summary",
        enqueue_secs,
        enqueue_rate: jobs / enqueue_secs,
        drain_secs,
        drain_rate: jobs / drain_secs,
        total_secs,
        peak_rss,
        final_disk,
        shutdown_secs,
    };
    write_line(&out, &summary)?;

    if !args.keep_root {
        std::fs::remove_dir_all(&root_dir)?;
    }

    eprintln!(
        "{version} ({label}), {mode}, {} jobs: enqueue {:.0}/s, drain {:.0}/s, peak RSS {} MiB",
        args.jobs,
        summary.enqueue_rate,
        summary.drain_rate,
        peak_rss / (1024 * 1024),
    );
    eprintln!("Results: {}", results_path.display());

    Ok(())
}

fn write_line(out: &Mutex<impl Write>, record: &impl Serialize) -> Result<(), Error> {
    let mut out = out.lock().unwrap();
    serde_json::to_writer(&mut *out, record)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn host() -> Host {
    let mut system = System::new();
    system.refresh_cpu_list(sysinfo::CpuRefreshKind::nothing());
    system.refresh_memory();

    Host {
        os: System::long_os_version(),
        arch: System::cpu_arch(),
        cpu: system
            .cpus()
            .first()
            .map(|cpu| cpu.brand().to_string())
            .unwrap_or_default(),
        cores: System::physical_core_count(),
        memory: system.total_memory(),
    }
}
