// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! CLI subcommand implementations.

use clap::Parser;

pub mod backup;
pub mod restore;
pub mod serve;
pub mod tls;
pub mod top;

/// Shared CLI arguments for connecting to the admin API.
#[derive(Parser)]
pub struct AdminClientArgs {
    /// Admin API base URL to connect to.
    #[arg(long, default_value = "http://127.0.0.1:8901", env = "ZIZQ_ADMIN_URL")]
    pub url: String,

    /// Path to a PEM-encoded CA certificate for verifying the admin server.
    #[arg(long, value_name = "PATH", env = "ZIZQ_ADMIN_TLS_CA")]
    pub ca_cert: Option<String>,

    /// Path to a PEM-encoded client certificate for mTLS with the admin server.
    /// Must be used together with --client-key.
    #[arg(long, value_name = "PATH", env = "ZIZQ_ADMIN_TLS_CLIENT_CERT")]
    pub client_cert: Option<String>,

    /// Path to a PEM-encoded client private key for mTLS with the admin server.
    /// Must be used together with --client-cert.
    #[arg(long, value_name = "PATH", env = "ZIZQ_ADMIN_TLS_CLIENT_KEY")]
    pub client_key: Option<String>,
}

impl AdminClientArgs {
    /// Validate TLS argument combinations.
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        match (&self.client_cert, &self.client_key) {
            (Some(_), None) | (None, Some(_)) => {
                Err("--client-cert and --client-key must be provided together".into())
            }
            _ => Ok(()),
        }
    }

    /// Build a `reqwest::Client` with optional TLS configuration.
    pub fn build_http_client(&self) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
        let mut builder = reqwest::Client::builder();

        if let Some(ref ca_path) = self.ca_cert {
            let pem = std::fs::read(ca_path)
                .map_err(|e| format!("failed to read CA cert {ca_path}: {e}"))?;
            let cert = reqwest::tls::Certificate::from_pem(&pem)
                .map_err(|e| format!("failed to parse CA cert {ca_path}: {e}"))?;
            builder = builder.add_root_certificate(cert);
        }

        if let (Some(cert_path), Some(key_path)) = (&self.client_cert, &self.client_key) {
            let cert_pem = std::fs::read(cert_path)
                .map_err(|e| format!("failed to read client cert {cert_path}: {e}"))?;
            let key_pem = std::fs::read(key_path)
                .map_err(|e| format!("failed to read client key {key_path}: {e}"))?;
            let mut identity_pem = cert_pem;
            identity_pem.extend_from_slice(&key_pem);
            let identity = reqwest::tls::Identity::from_pem(&identity_pem)
                .map_err(|e| format!("failed to build client identity: {e}"))?;
            builder = builder.identity(identity);
        }

        builder.build().map_err(|e| e.into())
    }
}
