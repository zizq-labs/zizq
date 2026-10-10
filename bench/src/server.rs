//! Running the server under test.

use std::fs::File;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};

use crate::Error;

/// How long the server may take to start accepting connections.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the server may take to exit after SIGTERM.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(120);

/// A running `zizq serve` process.
pub struct Server {
    child: Child,
    pub pid: u32,
    pub url: String,
}

/// The version reported by `zizq --version` and the SHA-256 of the binary.
///
/// The hash tells apart builds of the same version, such as the musl
/// release binary and a local glibc build.
pub async fn identify(path: &Path) -> Result<(String, String), Error> {
    let output = Command::new(path).arg("--version").output().await?;
    let version = String::from_utf8(output.stdout)?
        .trim()
        .trim_start_matches("zizq ")
        .to_string();

    let sha256 = format!("{:x}", Sha256::digest(std::fs::read(path)?));

    Ok((version, sha256))
}

/// Start the server on a free port with a fresh root directory, and wait
/// until it accepts connections.
pub async fn start(
    path: &Path,
    root_dir: &Path,
    log: File,
    extra_args: &[String],
) -> Result<Server, Error> {
    let addr = free_addr().await?;

    let mut child = Command::new(path)
        .arg("serve")
        .arg("--root-dir")
        .arg(root_dir)
        .args(["--host", "127.0.0.1", "--port", &addr.port().to_string()])
        .arg("--no-admin")
        .args(extra_args)
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()?;

    let pid = child.id().ok_or("server exited immediately")?;
    let started = Instant::now();

    while TcpStream::connect(addr).await.is_err() {
        if let Some(status) = child.try_wait()? {
            return Err(format!("server exited during startup: {status}").into());
        }
        if started.elapsed() > STARTUP_TIMEOUT {
            return Err("server did not start accepting connections".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(Server {
        child,
        pid,
        url: format!("http://{addr}"),
    })
}

impl Server {
    /// Send SIGTERM and wait for the server to exit, returning how long
    /// that took.
    pub async fn stop(mut self) -> Result<Duration, Error> {
        let started = Instant::now();

        Command::new("kill")
            .args(["-TERM", &self.pid.to_string()])
            .status()
            .await?;

        tokio::time::timeout(SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .map_err(|_| "server did not exit after SIGTERM")??;

        Ok(started.elapsed())
    }
}

/// Find a port that is free right now by binding to port 0.
async fn free_addr() -> Result<SocketAddr, Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?)
}
