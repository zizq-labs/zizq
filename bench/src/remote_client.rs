//! Running the load on another machine, over SSH.
//!
//! The mirror image of `remote`: with `--remote-client`, the server runs
//! here and is sampled here, and the producers and workers run on another
//! machine, under `zizq-bench --load-agent` started over SSH. Only this
//! machine connects out; the other only needs to reach the server's port.
//!
//! The agent reports on its standard output, one JSON line at a time:
//!
//! - `started`: the machine it runs on. It then waits for a line on its
//!   standard input before starting the load, so the load can't begin
//!   before the server and the sampler here are ready
//! - `phase`: each phase as it begins
//! - `progress`: jobs enqueued and completed, and its own CPU time, every
//!   `--interval-ms`
//! - `done`: its own timings of the enqueue and the drain

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sysinfo::{ProcessesToUpdate, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStdin};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use crate::load::Counters;
use crate::remote::{quote, ssh};
use crate::sampler::{self, Phase, PhaseCell};
use crate::{Args, Error, Host};

/// How long the load agent waits to connect to the server before giving
/// up on it.
const REACH_TIMEOUT: Duration = Duration::from_secs(5);

/// One line of the load agent's report.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Report {
    Started {
        host: Host,
    },
    Phase {
        phase: Phase,
    },
    Progress {
        enqueued: u64,
        completed: u64,
        cpu_ms: u64,
    },
    Done {
        enqueue_secs: f64,
        drain_secs: f64,
        total_secs: f64,
    },
}

/// Run as the load agent: report the machine, wait to be told to begin,
/// then push the load through the server at `--url`.
pub async fn agent(args: &Args) -> Result<(), Error> {
    let url = args.url.as_deref().ok_or("--url is required")?;

    // Checked before reporting in, because the client would retry a
    // server it can't reach for ever, rather than fail.
    let addr = url.trim_start_matches("http://");
    match tokio::time::timeout(REACH_TIMEOUT, TcpStream::connect(addr)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(format!("cannot reach the server at {url}: {e}").into()),
        Err(_) => return Err(format!("cannot reach the server at {url}: timed out").into()),
    }
    report(&Report::Started {
        host: crate::host(),
    })?;

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    if stdin.next_line().await?.is_none() {
        return Ok(()); // The benchmark gave up before starting.
    }
    // The benchmark closing the connection means it has gone, so there
    // is nobody to report to.
    tokio::spawn(async move {
        while let Ok(Some(_)) = stdin.next_line().await {}
        std::process::exit(1);
    });

    let counters = Arc::new(Counters::default());
    let progress = tokio::spawn({
        let counters = counters.clone();
        let interval = Duration::from_millis(args.interval_ms);
        async move {
            let mut system = System::new();
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // A report that can't be written means the benchmark has gone.
            loop {
                ticker.tick().await;
                if report_progress(&mut system, &counters).is_err() {
                    break;
                }
            }
        }
    });

    let result = crate::run_load(args, url, counters.clone(), |phase| {
        let _ = report(&Report::Phase { phase });
    })
    .await;
    progress.abort();
    let (enqueue_secs, drain_secs, total_secs) = result?;

    // The final counts, so the last sample sees every job done.
    report_progress(&mut System::new(), &counters)?;
    report(&Report::Done {
        enqueue_secs,
        drain_secs,
        total_secs,
    })
}

fn report_progress(system: &mut System, counters: &Counters) -> Result<(), Error> {
    let pid = sysinfo::get_current_pid()?;
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        sampler::refresh_kind(),
    );
    report(&Report::Progress {
        enqueued: counters.enqueued.load(Ordering::Relaxed),
        completed: counters.completed.load(Ordering::Relaxed),
        cpu_ms: system
            .process(pid)
            .map(|p| p.accumulated_cpu_time())
            .unwrap_or_default(),
    })
}

fn report(report: &Report) -> Result<(), Error> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, report)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

/// How to run the load on another machine.
pub struct RemoteLoadArgs<'a> {
    /// SSH destination, e.g. `user@host`.
    pub destination: &'a str,
    /// The command running `zizq-bench` there. Not quoted, so it can be
    /// prefixed.
    pub bench: &'a str,
    /// The server, as the other machine reaches it.
    pub url: &'a str,
    /// The load to run: mode, jobs, workers and so on.
    pub args: &'a Args,
    /// Updated from the agent's progress, for the sampler.
    pub counters: Arc<Counters>,
    /// Updated as the agent reports each phase, for the sampler.
    pub phase: Arc<PhaseCell>,
    /// Where the agent's errors are written.
    pub log: &'a Path,
}

/// A load agent on another machine, ready to begin.
pub struct RemoteLoad {
    ssh: Child,
    stdin: ChildStdin,
    /// The machine the load runs on.
    pub host: Host,
    /// The agent's CPU time in milliseconds, as last reported.
    pub cpu_ms: Arc<AtomicU64>,
    done: oneshot::Receiver<(f64, f64, f64)>,
    log: PathBuf,
}

/// Start the load agent over SSH, and wait for it to report in.
pub async fn start(args: RemoteLoadArgs<'_>) -> Result<RemoteLoad, Error> {
    let load = args.args;
    let mut command = format!(
        "{} --load-agent --url {} --mode {} --jobs {} --workers {} --concurrency {} \
         --batch-size {} --enqueue-concurrency {} --format {} --interval-ms {}",
        args.bench,
        quote(args.url),
        load.mode.name(),
        load.jobs,
        load.workers,
        load.concurrency,
        load.batch_size,
        load.enqueue_concurrency,
        load.format.name(),
        load.interval_ms,
    );
    if let Some(prefetch) = load.prefetch {
        command.push_str(&format!(" --prefetch {prefetch}"));
    }

    let mut ssh = ssh(args.destination, &command, File::create(args.log)?).spawn()?;
    let stdin = ssh.stdin.take().ok_or("no stdin for ssh")?;
    let mut lines = BufReader::new(ssh.stdout.take().ok_or("no stdout for ssh")?).lines();

    let host = match lines.next_line().await? {
        Some(line) => match serde_json::from_str(&line)? {
            Report::Started { host } => host,
            _ => return Err(format!("load agent did not start with a report: {line}").into()),
        },
        None => {
            return Err(format!(
                "load agent exited before starting; see {}",
                args.log.display()
            )
            .into());
        }
    };

    let cpu_ms = Arc::new(AtomicU64::new(0));
    let (done_tx, done) = oneshot::channel();
    tokio::spawn({
        let (counters, phase, cpu_ms) = (args.counters, args.phase, cpu_ms.clone());
        async move {
            let mut done_tx = Some(done_tx);
            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str(&line) {
                    Ok(Report::Phase { phase: p }) => phase.set(p),
                    Ok(Report::Progress {
                        enqueued,
                        completed,
                        cpu_ms: ms,
                    }) => {
                        counters.enqueued.store(enqueued, Ordering::Relaxed);
                        counters.completed.store(completed, Ordering::Relaxed);
                        cpu_ms.store(ms, Ordering::Relaxed);
                    }
                    Ok(Report::Done {
                        enqueue_secs,
                        drain_secs,
                        total_secs,
                    }) => {
                        if let Some(tx) = done_tx.take() {
                            let _ = tx.send((enqueue_secs, drain_secs, total_secs));
                        }
                    }
                    _ => eprintln!("unexpected line from load agent: {line}"),
                }
            }
        }
    });

    Ok(RemoteLoad {
        ssh,
        stdin,
        host,
        cpu_ms,
        done,
        log: args.log.to_path_buf(),
    })
}

impl RemoteLoad {
    /// Begin the load and wait for it to finish, returning the agent's
    /// timings of the enqueue, the drain, and the whole run.
    pub async fn run(mut self) -> Result<(f64, f64, f64), Error> {
        self.stdin.write_all(b"go\n").await?;
        self.stdin.flush().await?;

        let timings = self.done.await.map_err(|_| {
            format!(
                "load agent exited before finishing; see {}",
                self.log.display()
            )
        })?;

        self.stdin.shutdown().await?;
        drop(self.stdin);
        self.ssh.wait().await?;
        Ok(timings)
    }
}
