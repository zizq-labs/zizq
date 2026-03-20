// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TLS support for the primary HTTP listener.
//!
//! Provides PEM loading helpers, a rustls `ServerConfig` builder, and a
//! `TlsListener` that implements axum's `Listener` trait so that
//! `axum::serve` can run directly over TLS connections.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// Load PEM-encoded certificates from a file.
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("failed to open cert file {}: {e}", path.display()))?;
    let mut reader = io::BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse certs from {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("no certificates found in {}", path.display()));
    }
    Ok(certs)
}

/// Load a PEM-encoded private key from a file.
///
/// Accepts PKCS#8, RSA, or EC private keys.
pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("failed to open key file {}: {e}", path.display()))?;
    let mut reader = io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("failed to parse private key from {}: {e}", path.display()))?
        .ok_or_else(|| format!("no private key found in {}", path.display()))
}

/// Build a rustls `ServerConfig` for the server.
///
/// If `client_ca_path` is provided, configures client certificate
/// verification (mTLS) using the given CA certificate(s).
pub fn build_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: Option<&Path>,
) -> Result<Arc<ServerConfig>, String> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let builder = if let Some(ca_path) = client_ca_path {
        let ca_certs = load_certs(ca_path)?;
        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| format!("invalid CA certificate: {e}"))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| format!("failed to build client verifier: {e}"))?;
        ServerConfig::builder().with_client_cert_verifier(verifier)
    } else {
        ServerConfig::builder().with_no_client_auth()
    };

    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid server certificate/key: {e}"))?;

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Arc::new(config))
}

/// A TLS-wrapping listener that implements `axum::serve::Listener`.
///
/// Accepts TCP connections, performs TLS handshakes, and yields
/// `TlsStream<TcpStream>` to axum's serve loop. Failed handshakes
/// are logged and retried (the trait requires infallible accept).
pub struct TlsListener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    /// Create a new `TlsListener` wrapping an existing TCP listener.
    pub fn new(tcp: TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            tcp,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            // Accept the TCP connection (retry on TCP-level errors).
            let (stream, addr) = match self.tcp.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    if !is_connection_error(&e) {
                        tracing::error!(error = %e, "TCP accept error");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    continue;
                }
            };

            // Perform TLS handshake (retry on failure).
            match self.acceptor.accept(stream).await {
                Ok(tls_stream) => return (tls_stream, addr),
                Err(e) => {
                    tracing::warn!(peer = %addr, error = %e, "TLS handshake failed");
                    continue;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

fn is_connection_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Generate a self-signed certificate and private key PEM pair.
    fn gen_cert_and_key() -> (String, String) {
        let params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        (cert.pem(), key_pair.serialize_pem())
    }

    /// Generate a self-signed CA certificate and key.
    fn gen_ca() -> (String, String, rcgen::KeyPair) {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        (cert.pem(), key_pair.serialize_pem(), key_pair)
    }

    fn write_temp(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    // -- load_certs --

    #[test]
    fn load_certs_valid_pem() {
        let (cert_pem, _) = gen_cert_and_key();
        let f = write_temp(&cert_pem);
        let certs = load_certs(f.path()).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn load_certs_empty_file() {
        let f = write_temp("");
        let err = load_certs(f.path()).unwrap_err();
        assert!(err.contains("no certificates"), "got: {err}");
    }

    #[test]
    fn load_certs_not_pem() {
        let f = write_temp("not a pem file");
        let err = load_certs(f.path()).unwrap_err();
        assert!(err.contains("no certificates"), "got: {err}");
    }

    #[test]
    fn load_certs_missing_file() {
        let err = load_certs(Path::new("/tmp/zizq-nonexistent.pem")).unwrap_err();
        assert!(err.contains("failed to open"), "got: {err}");
    }

    // -- load_private_key --

    #[test]
    fn load_private_key_valid_pem() {
        let (_, key_pem) = gen_cert_and_key();
        let f = write_temp(&key_pem);
        load_private_key(f.path()).unwrap();
    }

    #[test]
    fn load_private_key_empty_file() {
        let f = write_temp("");
        let err = load_private_key(f.path()).unwrap_err();
        assert!(err.contains("no private key"), "got: {err}");
    }

    #[test]
    fn load_private_key_missing_file() {
        let err = load_private_key(Path::new("/tmp/zizq-nonexistent.pem")).unwrap_err();
        assert!(err.contains("failed to open"), "got: {err}");
    }

    // -- build_server_config --

    #[test]
    fn build_config_valid_cert_and_key() {
        let (cert_pem, key_pem) = gen_cert_and_key();
        let cert_file = write_temp(&cert_pem);
        let key_file = write_temp(&key_pem);

        let config = build_server_config(cert_file.path(), key_file.path(), None).unwrap();
        assert_eq!(
            config.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    #[test]
    fn build_config_mismatched_cert_and_key() {
        let (cert_pem, _) = gen_cert_and_key();
        let (_, other_key_pem) = gen_cert_and_key();
        let cert_file = write_temp(&cert_pem);
        let key_file = write_temp(&other_key_pem);

        let err = build_server_config(cert_file.path(), key_file.path(), None).unwrap_err();
        assert!(err.contains("invalid server certificate/key"), "got: {err}");
    }

    #[test]
    fn build_config_with_client_ca() {
        let (cert_pem, key_pem) = gen_cert_and_key();
        let (ca_pem, _, _) = gen_ca();
        let cert_file = write_temp(&cert_pem);
        let key_file = write_temp(&key_pem);
        let ca_file = write_temp(&ca_pem);

        // Succeeding here proves the mTLS code path ran — the CA was
        // parsed, added to the root store, and used to build a client
        // verifier. The ServerConfig verifier field is private, so we
        // can't inspect it directly.
        build_server_config(cert_file.path(), key_file.path(), Some(ca_file.path())).unwrap();
    }

    #[test]
    fn build_config_with_invalid_client_ca() {
        let (cert_pem, key_pem) = gen_cert_and_key();
        let cert_file = write_temp(&cert_pem);
        let key_file = write_temp(&key_pem);
        let ca_file = write_temp("not a cert");

        let err = build_server_config(cert_file.path(), key_file.path(), Some(ca_file.path()))
            .unwrap_err();
        assert!(err.contains("no certificates"), "got: {err}");
    }
}
