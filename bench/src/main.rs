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
//!
//! With `--remote`, the server runs on another machine instead, under an
//! agent started over SSH (see `remote`), and `--server` is its path
//! there. With `--remote-client`, the server runs here and the load runs
//! on another machine (see `remote_client`).

mod load;
mod remote;
mod remote_client;
mod sampler;
mod server;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
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
    /// Path to the `zizq` server binary to benchmark: on the remote
    /// machine, with `--remote`.
    #[arg(long, required_unless_present = "load_agent")]
    server: Option<PathBuf>,

    /// Run the server on another machine, given as an SSH destination
    /// (e.g. `user@host`). Needs `zizq-bench` installed there too, and
    /// SSH access without a password prompt.
    #[arg(long)]
    remote: Option<String>,

    /// How to run `zizq-bench` on the remote machine. Not quoted, so it
    /// can carry a prefix, e.g. `env TMPDIR=/dev/shm ~/bin/zizq-bench`.
    #[arg(long, default_value = "zizq-bench")]
    remote_bench: String,

    /// Address to reach the remote server at, if not the SSH destination's
    /// host (e.g. a private network address).
    #[arg(long)]
    remote_addr: Option<String>,

    /// Run the load (the producers and workers) on another machine, given
    /// as an SSH destination, against a server here. Needs `zizq-bench`
    /// installed there too, and SSH access without a password prompt.
    #[arg(long, conflicts_with = "remote")]
    remote_client: Option<String>,

    /// Address the remote client reaches this machine's server at.
    /// Defaults to this machine's hostname.
    #[arg(long, requires = "remote_client")]
    server_addr: Option<String>,

    /// Run as the agent for a benchmark on another machine.
    #[arg(long, hide = true)]
    agent: bool,

    /// Run as the load agent for a benchmark on another machine, against
    /// the server at `--url`.
    #[arg(long, hide = true, requires = "url")]
    load_agent: bool,

    /// The server the load agent runs against.
    #[arg(long, hide = true)]
    url: Option<String>,

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

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Upfront => "upfront",
            Mode::Concurrent => "concurrent",
        }
    }
}

#[derive(Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireFormat {
    Json,
    Msgpack,
}

impl WireFormat {
    fn name(self) -> &'static str {
        match self {
            WireFormat::Json => "json",
            WireFormat::Msgpack => "msgpack",
        }
    }
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
    /// The machine the benchmark (the client) ran on.
    host: &'a Host,
    /// The machine the server ran on, when not the same one.
    #[serde(skip_serializing_if = "Option::is_none")]
    server_host: Option<&'a Host>,
    args: &'a Args,
}

/// A machine the benchmark ran on. Results are only comparable between
/// runs on the same hosts.
#[derive(Serialize, Deserialize)]
pub struct Host {
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

/// The server under test, on this machine or another.
enum Running {
    Local {
        server: server::Server,
        root_dir: PathBuf,
    },
    Remote(remote::RemoteServer),
}

impl Running {
    fn url(&self) -> &str {
        match self {
            Running::Local { server, .. } => &server.url,
            Running::Remote(server) => &server.url,
        }
    }

    fn metrics(&self) -> sampler::Source {
        match self {
            Running::Local { server, root_dir } => sampler::Source::Local {
                pid: server.pid,
                root_dir: root_dir.clone(),
            },
            Running::Remote(server) => sampler::Source::Remote(server.metrics.clone()),
        }
    }

    /// Stop the server, returning how long it took and its final disk
    /// use.
    async fn stop(self, keep_root: bool) -> Result<(f64, u64), Error> {
        match self {
            Running::Local { server, root_dir } => {
                let final_disk = sampler::dir_size(&root_dir);
                let shutdown_secs = server.stop().await?.as_secs_f64();
                if !keep_root {
                    std::fs::remove_dir_all(&root_dir)?;
                }
                Ok((shutdown_secs, final_disk))
            }
            Running::Remote(server) => {
                let (shutdown, final_disk) = server.stop().await?;
                Ok((shutdown.as_secs_f64(), final_disk))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    if args.load_agent {
        return remote_client::agent(&args).await;
    }
    let server_path = args.server.clone().ok_or("--server is required")?;
    if args.agent {
        return remote::agent(remote::AgentArgs {
            server: &server_path,
            server_args: &args.server_args,
            interval: Duration::from_millis(args.interval_ms),
            keep_root: args.keep_root,
        })
        .await;
    }

    let started_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let mode = args.mode.name();
    let this_host = host();

    // A remote server reports its version once its agent has started it,
    // so it starts before the results file can be named. A local one is
    // identified first, and started once the file is.
    let mut remote = match &args.remote {
        Some(destination) => {
            std::fs::create_dir_all(&args.out)?;
            Some(
                remote::start(remote::RemoteArgs {
                    destination,
                    bench: &args.remote_bench,
                    addr: args.remote_addr.as_deref(),
                    agent: remote::AgentArgs {
                        server: &server_path,
                        server_args: &args.server_args,
                        interval: Duration::from_millis(args.interval_ms),
                        keep_root: args.keep_root,
                    },
                    log: args.out.join(format!(".remote-{started_at}.log")),
                })
                .await?,
            )
        }
        None => None,
    };
    let (version, sha256) = match &remote {
        Some(server) => (server.version.clone(), server.sha256.clone()),
        None => server::identify(&server_path).await?,
    };
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| sha256[..8].to_string());

    let dir = args.out.join(&version);
    std::fs::create_dir_all(&dir)?;
    let name = format!("{label}-{mode}-{}-{started_at}", args.jobs);
    let results_path = dir.join(format!("{name}.ndjson"));
    let out = Arc::new(Mutex::new(BufWriter::new(File::create(&results_path)?)));

    let log_path = dir.join(format!("{name}.log"));
    let running = match remote.take() {
        Some(mut server) => {
            server.log_to(&log_path)?;
            Running::Remote(server)
        }
        None => {
            // Listening on the network only when the load comes from
            // another machine.
            let listen = match args.remote_client {
                Some(_) => server::Listen::Network,
                None => server::Listen::Local,
            };
            let root_dir = std::env::temp_dir().join(format!("zizq-bench-{name}"));
            let server = server::start(
                &server_path,
                &root_dir,
                listen,
                File::create(&log_path)?,
                &args.server_args,
            )
            .await?;
            Running::Local { server, root_dir }
        }
    };

    let counters = Arc::new(Counters::default());
    let phase = Arc::new(PhaseCell::default());

    // The load agent waits to be told to begin, once the server and the
    // sampler are ready.
    let remote_load = match &args.remote_client {
        Some(destination) => {
            let port = running
                .url()
                .rsplit(':')
                .next()
                .ok_or("server URL has no port")?;
            let addr = match &args.server_addr {
                Some(addr) => addr.clone(),
                None => System::host_name().ok_or("no hostname: pass --server-addr")?,
            };
            let started = remote_client::start(remote_client::RemoteLoadArgs {
                destination,
                bench: &args.remote_bench,
                url: &format!("http://{addr}:{port}"),
                args: &args,
                counters: counters.clone(),
                phase: phase.clone(),
                log: &dir.join(format!("{name}.client.log")),
            })
            .await;
            match started {
                Ok(load) => Some(load),
                Err(e) => {
                    // Not leaving the server, or its root directory, behind.
                    let _ = running.stop(args.keep_root).await;
                    return Err(e);
                }
            }
        }
        None => None,
    };

    // `host` is always the client's machine, and `server_host` the
    // server's when the two differ.
    let (client_host, server_host) = match (&running, &remote_load) {
        (Running::Remote(server), _) => (&this_host, Some(&server.host)),
        (_, Some(load)) => (&load.host, Some(&this_host)),
        _ => (&this_host, None),
    };
    write_line(
        &out,
        &Header {
            kind: "header",
            version: &version,
            label: &label,
            sha256: &sha256,
            client_version: CLIENT_VERSION,
            started_at,
            host: client_host,
            server_host,
            args: &args,
        },
    )?;

    let started = Instant::now();
    let sampler = sampler::start(
        out.clone(),
        Duration::from_millis(args.interval_ms),
        running.metrics(),
        match &remote_load {
            Some(load) => sampler::ClientCpu::Remote(load.cpu_ms.clone()),
            None => sampler::ClientCpu::Local,
        },
        counters.clone(),
        phase.clone(),
        started,
    );

    let timings = match remote_load {
        Some(load) => load.run().await,
        None => run_load(&args, running.url(), counters, |p| phase.set(p)).await,
    };
    let (enqueue_secs, drain_secs, total_secs) = match timings {
        Ok(timings) => timings,
        Err(e) => {
            let _ = sampler.stop().await;
            let _ = running.stop(args.keep_root).await;
            return Err(e);
        }
    };

    let peak_rss = sampler.stop().await?;
    let (shutdown_secs, final_disk) = running.stop(args.keep_root).await?;

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

/// Push `--jobs` through the server at `url`, reporting each phase as it
/// starts. Returns the seconds spent enqueueing, draining, and in all.
async fn run_load(
    args: &Args,
    url: &str,
    counters: Arc<Counters>,
    on_phase: impl Fn(Phase),
) -> Result<(f64, f64, f64), Error> {
    // Producers and workers get separate clients, and so separate
    // connections, as they would as separate processes in production.
    let client = || {
        Client::builder()
            .url(url)
            .format(match args.format {
                WireFormat::Json => Format::Json,
                WireFormat::Msgpack => Format::MessagePack,
            })
            .build()
    };

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

    let started = Instant::now();
    let (enqueue_secs, drain_secs) = match args.mode {
        Mode::Upfront => {
            on_phase(Phase::Enqueue);
            enqueue.await?;
            let enqueue_secs = started.elapsed().as_secs_f64();

            on_phase(Phase::Drain);
            drain.await?;
            (enqueue_secs, started.elapsed().as_secs_f64() - enqueue_secs)
        }
        Mode::Concurrent => {
            on_phase(Phase::Both);
            let drain = tokio::spawn(drain);
            enqueue.await?;
            let enqueue_secs = started.elapsed().as_secs_f64();

            on_phase(Phase::Drain);
            drain.await??;
            (enqueue_secs, started.elapsed().as_secs_f64())
        }
    };
    Ok((enqueue_secs, drain_secs, started.elapsed().as_secs_f64()))
}

fn write_line(out: &Mutex<impl Write>, record: &impl Serialize) -> Result<(), Error> {
    let mut out = out.lock().unwrap();
    serde_json::to_writer(&mut *out, record)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

pub fn host() -> Host {
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
