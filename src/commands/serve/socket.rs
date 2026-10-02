// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Socket options applied to every accepted connection.
//!
//! A client whose host vanishes without closing its connection (power
//! loss, kernel panic, network partition) sends nothing to say so. The
//! server's writes keep succeeding into the kernel's send buffer, so the
//! only signal is that they are never acknowledged, and by default Linux
//! retransmits for around 15 minutes before giving up. Until then any
//! jobs that client was holding stay in flight.
//!
//! TCP keepalive does not help here: keepalive probes are only sent on
//! an idle connection, and take streams are never idle because the
//! server writes heartbeats to them. The option that bounds this is the
//! one capping how long sent data may remain unacknowledged:
//! `TCP_USER_TIMEOUT` on Linux, `TCP_MAXRT` on Windows and
//! `TCP_RXT_CONNDROPTIME` on macOS.

use std::time::Duration;

use tokio::net::TcpStream;

/// Default time that sent data may remain unacknowledged before the
/// connection is dropped (milliseconds).
pub const DEFAULT_TCP_USER_TIMEOUT_MS: u64 = 30_000;

/// Apply socket options to a newly accepted connection.
///
/// A zero `user_timeout` leaves the operating system's default in place.
/// Failure to set an option is logged rather than returned, since the
/// connection is still usable without it.
pub fn configure_accepted(stream: &TcpStream, user_timeout: Duration) {
    if user_timeout.is_zero() {
        return;
    }

    if let Err(e) = set_user_timeout(stream, user_timeout) {
        tracing::warn!(error = %e, "failed to set TCP user timeout");
    }
}

#[cfg(target_os = "linux")]
fn set_user_timeout(stream: &TcpStream, timeout: Duration) -> std::io::Result<()> {
    socket2::SockRef::from(stream).set_tcp_user_timeout(Some(timeout))
}

#[cfg(windows)]
fn set_user_timeout(stream: &TcpStream, timeout: Duration) -> std::io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        IPPROTO_TCP, SOCKET, SOCKET_ERROR, TCP_MAXRT, WSAGetLastError, setsockopt,
    };

    // Stay below u32::MAX, which TCP_MAXRT takes to mean never time out.
    let secs = whole_secs(timeout) as u32;

    let rc = unsafe {
        setsockopt(
            stream.as_raw_socket() as SOCKET,
            IPPROTO_TCP,
            TCP_MAXRT,
            (&secs as *const u32).cast(),
            size_of::<u32>() as i32,
        )
    };

    if rc == SOCKET_ERROR {
        return Err(std::io::Error::from_raw_os_error(unsafe {
            WSAGetLastError()
        }));
    }

    Ok(())
}

/// Not exported by the `libc` crate. From XNU's `<netinet/tcp.h>`.
#[cfg(target_os = "macos")]
const TCP_RXT_CONNDROPTIME: libc::c_int = 0x80;

#[cfg(target_os = "macos")]
fn set_user_timeout(stream: &TcpStream, timeout: Duration) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let secs = whole_secs(timeout) as libc::c_int;

    let rc = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            TCP_RXT_CONNDROPTIME,
            (&secs as *const libc::c_int).cast(),
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    };

    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}

/// Convert a timeout to the whole seconds Windows and macOS expect.
///
/// Rounds up, so a sub-second timeout is not truncated to 0 (the system
/// default), and clamps to `i32::MAX`, which both platforms accept.
#[cfg(any(windows, target_os = "macos"))]
fn whole_secs(timeout: Duration) -> u64 {
    let secs = timeout.as_secs() + u64::from(timeout.subsec_nanos() > 0);
    secs.min(i32::MAX as u64)
}

#[cfg(not(any(target_os = "linux", windows, target_os = "macos")))]
fn set_user_timeout(_stream: &TcpStream, _timeout: Duration) -> std::io::Result<()> {
    Ok(())
}

#[cfg(all(test, any(target_os = "linux", windows, target_os = "macos")))]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Return the server side of a fresh loopback connection.
    async fn accepted_stream() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (server, client)
    }

    #[cfg(target_os = "linux")]
    mod linux {
        use super::*;

        fn user_timeout(stream: &TcpStream) -> Option<Duration> {
            socket2::SockRef::from(stream).tcp_user_timeout().unwrap()
        }

        #[tokio::test]
        async fn sets_user_timeout() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::from_secs(30));

            assert_eq!(user_timeout(&server), Some(Duration::from_secs(30)));
        }

        #[tokio::test]
        async fn zero_leaves_os_default() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::ZERO);

            assert_eq!(user_timeout(&server), None);
        }
    }

    #[cfg(windows)]
    mod windows {
        use super::*;
        use std::os::windows::io::AsRawSocket;
        use windows_sys::Win32::Networking::WinSock::{IPPROTO_TCP, SOCKET, TCP_MAXRT, getsockopt};

        /// Read back TCP_MAXRT, in seconds.
        fn max_rt(stream: &TcpStream) -> u32 {
            let mut secs: u32 = 0;
            let mut len = size_of::<u32>() as i32;
            let rc = unsafe {
                getsockopt(
                    stream.as_raw_socket() as SOCKET,
                    IPPROTO_TCP,
                    TCP_MAXRT,
                    (&mut secs as *mut u32).cast(),
                    &mut len,
                )
            };
            assert_eq!(rc, 0, "getsockopt(TCP_MAXRT) failed");
            secs
        }

        #[tokio::test]
        async fn sets_max_retransmission_time() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::from_secs(30));

            assert_eq!(max_rt(&server), 30);
        }

        #[tokio::test]
        async fn rounds_sub_second_timeout_up() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::from_millis(1500));

            assert_eq!(max_rt(&server), 2);
        }
    }

    #[cfg(target_os = "macos")]
    mod macos {
        use super::*;
        use std::os::fd::AsRawFd;

        /// Read back TCP_RXT_CONNDROPTIME, in seconds.
        fn conn_drop_time(stream: &TcpStream) -> libc::c_int {
            let mut secs: libc::c_int = 0;
            let mut len = size_of::<libc::c_int>() as libc::socklen_t;
            let rc = unsafe {
                libc::getsockopt(
                    stream.as_raw_fd(),
                    libc::IPPROTO_TCP,
                    TCP_RXT_CONNDROPTIME,
                    (&mut secs as *mut libc::c_int).cast(),
                    &mut len,
                )
            };
            assert_eq!(rc, 0, "getsockopt(TCP_RXT_CONNDROPTIME) failed");
            secs
        }

        #[tokio::test]
        async fn sets_retransmission_drop_time() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::from_secs(30));

            assert_eq!(conn_drop_time(&server), 30);
        }

        #[tokio::test]
        async fn zero_leaves_os_default() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::ZERO);

            assert_eq!(conn_drop_time(&server), 0);
        }

        #[tokio::test]
        async fn rounds_sub_second_timeout_up() {
            let (server, _client) = accepted_stream().await;

            configure_accepted(&server, Duration::from_millis(1500));

            assert_eq!(conn_drop_time(&server), 2);
        }
    }
}
