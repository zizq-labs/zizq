//! Periodic sampling of the server process and benchmark progress.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use crate::Error;
use crate::load::Counters;

/// What the benchmark is doing, recorded with each sample so charts can
/// shade the phases.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Phase {
    Enqueue = 0,
    Drain = 1,
    Both = 2,
}

/// The current phase, shared between the benchmark and the sampler.
#[derive(Default)]
pub struct PhaseCell(AtomicU8);

impl PhaseCell {
    pub fn set(&self, phase: Phase) {
        self.0.store(phase as u8, Ordering::Relaxed);
    }

    fn get(&self) -> Phase {
        match self.0.load(Ordering::Relaxed) {
            0 => Phase::Enqueue,
            1 => Phase::Drain,
            _ => Phase::Both,
        }
    }
}

/// One line of the results file.
#[derive(Serialize)]
pub struct Sample {
    #[serde(rename = "type")]
    kind: &'static str,
    /// Seconds since the benchmark started.
    t: f64,
    phase: Phase,
    /// Resident memory of the server, in bytes.
    rss: u64,
    /// Peak resident memory of the server so far, in bytes. Linux only.
    #[serde(skip_serializing_if = "Option::is_none")]
    rss_peak: Option<u64>,
    /// CPU used by the server since the last sample, as a percentage of
    /// one core.
    server_cpu: f64,
    /// CPU used by the benchmark itself since the last sample. If this is
    /// pegged while the server is not, the client is the bottleneck.
    bench_cpu: f64,
    /// Total size of the files in the server's root directory, in bytes.
    disk: u64,
    enqueued: u64,
    completed: u64,
}

/// The server's resource use at one moment.
#[derive(Clone, Copy, Default, Serialize, Deserialize)]
pub struct Metrics {
    /// Resident memory, in bytes.
    pub rss: u64,
    /// Peak resident memory, in bytes. Linux only.
    pub rss_peak: Option<u64>,
    /// CPU time used since the server started, in milliseconds.
    pub cpu_ms: u64,
    /// Total size of the files in the server's root directory, in bytes.
    pub disk: u64,
}

/// Where the server's metrics come from.
pub enum Source {
    /// A process on this machine, measured directly.
    Local { pid: u32, root_dir: PathBuf },
    /// A server on another machine, as last reported by its agent.
    Remote(Arc<Mutex<Metrics>>),
}

/// Where the client's CPU use comes from.
pub enum ClientCpu {
    /// This process, which is running the load.
    Local,
    /// A load agent on another machine: its CPU time in milliseconds, as
    /// last reported.
    Remote(Arc<AtomicU64>),
}

/// Measure the server process `pid` on this machine. `system` must have
/// just refreshed the process's memory and CPU.
pub fn measure(system: &System, pid: u32, root_dir: &Path) -> Metrics {
    let (rss, cpu_ms) = usage(system, Pid::from_u32(pid));
    Metrics {
        rss,
        rss_peak: peak_rss(pid),
        cpu_ms,
        disk: dir_size(root_dir),
    }
}

/// What `measure` needs refreshed for a process.
pub fn refresh_kind() -> ProcessRefreshKind {
    ProcessRefreshKind::nothing().with_memory().with_cpu()
}

/// Handle to a running sampler.
pub struct Sampler {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<u64, Error>>,
}

impl Sampler {
    /// Take a final sample and stop, returning the highest resident memory
    /// seen.
    pub async fn stop(self) -> Result<u64, Error> {
        let _ = self.stop.send(());
        self.task.await?
    }
}

/// Start sampling every `interval`, writing each sample to `out`.
pub fn start(
    out: Arc<Mutex<impl Write + Send + 'static>>,
    interval: Duration,
    server: Source,
    client: ClientCpu,
    counters: Arc<Counters>,
    phase: Arc<PhaseCell>,
    started: Instant,
) -> Sampler {
    let (stop, mut stopped) = oneshot::channel();

    let task = tokio::spawn(async move {
        let bench = sysinfo::get_current_pid()?;
        let mut pids = vec![bench];
        if let Source::Local { pid, .. } = &server {
            pids.push(Pid::from_u32(*pid));
        }
        let mut system = System::new();
        let read = |system: &System| match &server {
            Source::Local { pid, root_dir } => measure(system, *pid, root_dir),
            Source::Remote(latest) => *latest.lock().unwrap(),
        };
        let client_cpu_ms = |system: &System| match &client {
            ClientCpu::Local => usage(system, bench).1,
            ClientCpu::Remote(cpu_ms) => cpu_ms.load(Ordering::Relaxed),
        };

        // Baseline, so the first sample's CPU covers only its own interval.
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&pids), true, refresh_kind());
        let mut last = (Instant::now(), read(&system).cpu_ms, client_cpu_ms(&system));
        let mut max_rss = 0;

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let last_sample = tokio::select! {
                _ = ticker.tick() => false,
                _ = &mut stopped => true,
            };

            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&pids),
                true,
                refresh_kind(),
            );
            let now = Instant::now();
            let server = read(&system);
            let bench_cpu_ms = client_cpu_ms(&system);
            let elapsed_ms = now.duration_since(last.0).as_secs_f64() * 1000.0;
            max_rss = max_rss.max(server.rss);

            let sample = Sample {
                kind: "sample",
                t: now.duration_since(started).as_secs_f64(),
                phase: phase.get(),
                rss: server.rss,
                rss_peak: server.rss_peak,
                server_cpu: percent(server.cpu_ms.saturating_sub(last.1), elapsed_ms),
                bench_cpu: percent(bench_cpu_ms.saturating_sub(last.2), elapsed_ms),
                disk: server.disk,
                enqueued: counters.enqueued.load(Ordering::Relaxed),
                completed: counters.completed.load(Ordering::Relaxed),
            };
            last = (now, server.cpu_ms, bench_cpu_ms);

            let mut out = out.lock().unwrap();
            serde_json::to_writer(&mut *out, &sample)?;
            writeln!(out)?;
            out.flush()?;

            if last_sample {
                return Ok(max_rss.max(sample.rss_peak.unwrap_or(0)));
            }
        }
    });

    Sampler { stop, task }
}

/// Resident memory in bytes and accumulated CPU time in milliseconds.
fn usage(system: &System, pid: Pid) -> (u64, u64) {
    system
        .process(pid)
        .map(|p| (p.memory(), p.accumulated_cpu_time()))
        .unwrap_or_default()
}

fn percent(cpu_ms: u64, elapsed_ms: f64) -> f64 {
    if elapsed_ms > 0.0 {
        cpu_ms as f64 / elapsed_ms * 100.0
    } else {
        0.0
    }
}

/// The kernel's record of the process's peak resident memory.
#[cfg(target_os = "linux")]
fn peak_rss(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

#[cfg(not(target_os = "linux"))]
fn peak_rss(_pid: u32) -> Option<u64> {
    None
}

/// Total size of the files under `dir`. Files can vanish mid-walk as
/// compaction replaces them, so errors are skipped rather than fatal.
pub fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_size(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}
