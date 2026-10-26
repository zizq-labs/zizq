//! Running the server on another machine, over SSH.
//!
//! With `--remote`, the benchmark runs `zizq-bench --agent` on the server's
//! machine. The agent starts the server listening on the network with a
//! fresh root directory, and reports on its standard output, one JSON line
//! at a time:
//!
//! - `started`: the server's version, SHA-256 and port, and the machine
//! - `metrics`: the server's resource use, every `--interval-ms`
//! - `stopped`: shutdown time and final disk use, once standard input
//!   closes, which is how the benchmark tells it to stop
//!
//! The server's own output goes to the agent's standard error, which SSH
//! carries back to the benchmark's log file. SSH is the only control
//! channel, so nothing else listens but the server.

use std::fs::File;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use crate::sampler::{self, Metrics};
use crate::server::{self, Listen};
use crate::{Error, Host};

/// One line of the agent's report.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Report {
    Started {
        version: String,
        sha256: String,
        port: u16,
        host: Host,
    },
    Metrics(Metrics),
    Stopped {
        shutdown_secs: f64,
        final_disk: u64,
    },
}

/// What the agent needs to know, from the benchmark's arguments.
pub struct AgentArgs<'a> {
    pub server: &'a Path,
    pub server_args: &'a [String],
    pub interval: Duration,
    pub keep_root: bool,
}

/// Run as the agent: start the server, report on it until standard input
/// closes, then stop it.
pub async fn agent(args: AgentArgs<'_>) -> Result<(), Error> {
    let (version, sha256) = server::identify(args.server).await?;
    let root_dir = std::env::temp_dir().join(format!("zizq-bench-agent-{}", std::process::id()));

    // The server's output goes where the agent's own errors do, keeping
    // standard output for the report.
    let log = File::from(std::io::stderr().as_fd().try_clone_to_owned()?);
    let server = server::start(
        args.server,
        &root_dir,
        Listen::Network,
        log,
        args.server_args,
    )
    .await?;
    let port = server
        .url
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or("server URL has no port")?;

    report(&Report::Started {
        version,
        sha256,
        port,
        host: crate::host(),
    })?;

    let mut system = System::new();
    let pid = [Pid::from_u32(server.pid)];
    let mut ticker = tokio::time::interval(args.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                system.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&pid),
                    true,
                    sampler::refresh_kind(),
                );
                report(&Report::Metrics(sampler::measure(&system, server.pid, &root_dir)))?;
            }
            line = stdin.next_line() => {
                // Nothing is ever sent: only the end of input matters.
                if line?.is_none() {
                    break;
                }
            }
        }
    }

    let final_disk = sampler::dir_size(&root_dir);
    let shutdown_secs = server.stop().await?.as_secs_f64();
    if !args.keep_root {
        std::fs::remove_dir_all(&root_dir)?;
    }
    report(&Report::Stopped {
        shutdown_secs,
        final_disk,
    })?;
    Ok(())
}

fn report(report: &Report) -> Result<(), Error> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, report)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

/// How to reach the agent, from the benchmark's arguments.
pub struct RemoteArgs<'a> {
    /// SSH destination, e.g. `user@host`.
    pub destination: &'a str,
    /// The command running `zizq-bench` there. Not quoted, so it can be
    /// prefixed, e.g. `env TMPDIR=/dev/shm ~/bin/zizq-bench`.
    pub bench: &'a str,
    /// Address the server is reached at. Defaults to the destination's
    /// host.
    pub addr: Option<&'a str>,
    pub agent: AgentArgs<'a>,
    /// Where the server's output is written, until `RemoteServer::log_to`
    /// moves it.
    pub log: PathBuf,
}

/// A server running on another machine, under its agent.
pub struct RemoteServer {
    ssh: Child,
    stdin: ChildStdin,
    pub version: String,
    pub sha256: String,
    pub host: Host,
    pub url: String,
    /// The latest metrics the agent reported, for the sampler.
    pub metrics: Arc<Mutex<Metrics>>,
    stopped: oneshot::Receiver<(f64, u64)>,
    log: PathBuf,
}

/// Start the agent over SSH and wait for the server to be up.
pub async fn start(args: RemoteArgs<'_>) -> Result<RemoteServer, Error> {
    let mut command = format!(
        "{} --agent --server {} --interval-ms {}",
        args.bench,
        quote(&args.agent.server.to_string_lossy()),
        args.agent.interval.as_millis(),
    );
    if args.agent.keep_root {
        command.push_str(" --keep-root");
    }
    command.push_str(" --");
    for arg in args.agent.server_args {
        command.push(' ');
        command.push_str(&quote(arg));
    }

    let mut ssh = ssh(args.destination, &command, File::create(&args.log)?).spawn()?;
    let stdin = ssh.stdin.take().ok_or("no stdin for ssh")?;
    let mut lines = BufReader::new(ssh.stdout.take().ok_or("no stdout for ssh")?).lines();

    let (version, sha256, port, host) = match lines.next_line().await? {
        Some(line) => match serde_json::from_str(&line)? {
            Report::Started {
                version,
                sha256,
                port,
                host,
            } => (version, sha256, port, host),
            _ => return Err(format!("agent did not start with a report: {line}").into()),
        },
        None => {
            return Err(format!(
                "agent exited before starting the server; see {}",
                args.log.display()
            )
            .into());
        }
    };

    // The agent reports straight away. Waited for, so the sampler's
    // baseline is a real reading rather than zeros, which would put all
    // the CPU used since the server started into its first sample.
    let first = match lines.next_line().await? {
        Some(line) => match serde_json::from_str(&line)? {
            Report::Metrics(m) => m,
            _ => return Err(format!("agent did not report metrics: {line}").into()),
        },
        None => return Err("agent exited after starting the server".into()),
    };
    let metrics = Arc::new(Mutex::new(first));
    let (stopped_tx, stopped) = oneshot::channel();
    tokio::spawn({
        let metrics = metrics.clone();
        async move {
            let mut stopped_tx = Some(stopped_tx);
            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str(&line) {
                    Ok(Report::Metrics(m)) => *metrics.lock().unwrap() = m,
                    Ok(Report::Stopped {
                        shutdown_secs,
                        final_disk,
                    }) => {
                        if let Some(tx) = stopped_tx.take() {
                            let _ = tx.send((shutdown_secs, final_disk));
                        }
                    }
                    _ => eprintln!("unexpected line from agent: {line}"),
                }
            }
        }
    });

    let addr = args
        .addr
        .unwrap_or_else(|| destination_host(args.destination));
    Ok(RemoteServer {
        ssh,
        stdin,
        version,
        sha256,
        host,
        url: format!("http://{addr}:{port}"),
        metrics,
        stopped,
        log: args.log,
    })
}

impl RemoteServer {
    /// Move the server's log, once the results directory is known.
    pub fn log_to(&mut self, path: &Path) -> Result<(), Error> {
        // Still being written through the open file, wherever it lives.
        std::fs::rename(&self.log, path)?;
        self.log = path.to_path_buf();
        Ok(())
    }

    /// Stop the server, returning how long it took to shut down and the
    /// final size of its root directory.
    pub async fn stop(mut self) -> Result<(Duration, u64), Error> {
        self.stdin.shutdown().await?;
        drop(self.stdin);
        let (shutdown_secs, final_disk) = self.stopped.await.map_err(|_| {
            format!(
                "agent exited without stopping the server; see {}",
                self.log.display()
            )
        })?;
        self.ssh.wait().await?;
        Ok((Duration::from_secs_f64(shutdown_secs), final_disk))
    }
}

/// The host part of an SSH destination: `user@host` or `host`.
fn destination_host(destination: &str) -> &str {
    destination.rsplit('@').next().unwrap_or(destination)
}

/// Run `command` at `destination` over SSH, talking to it over standard
/// input and output, and writing its errors to `log`. Never prompts, so a
/// missing key fails rather than hanging.
pub fn ssh(destination: &str, command: &str, log: File) -> Command {
    let mut ssh = Command::new("ssh");
    ssh.args(["-T", "-o", "BatchMode=yes", destination, command])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(log)
        .kill_on_drop(true);
    ssh
}

/// Quote `arg` for the remote shell.
pub fn quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_survives_single_quotes() {
        assert_eq!(quote("plain"), "'plain'");
        assert_eq!(quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn the_destination_host_drops_the_user() {
        assert_eq!(
            destination_host("pi@raspberrypi.local"),
            "raspberrypi.local"
        );
        assert_eq!(destination_host("10.0.0.2"), "10.0.0.2");
    }
}
