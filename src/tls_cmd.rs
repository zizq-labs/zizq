// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! CLI subcommands for generating TLS certificates.
//!
//! Provides `zizq tls init`, `zizq tls ca`, `zizq tls server-cert`, and
//! `zizq tls client-cert` for bootstrapping TLS and mTLS without external
//! tools like `openssl`.

use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair};

/// Generate TLS certificates for use with `zizq serve'.
#[derive(Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate CA, server, and (optionally) client certificates.
    Init(InitArgs),

    /// Generate a Certificate Authority key pair.
    Ca(CaArgs),

    /// Generate a server certificate signed by a CA.
    ServerCert(ServerCertArgs),

    /// Generate a client certificate signed by a CA.
    ClientCert(ClientCertArgs),
}

#[derive(Parser)]
#[command(after_long_help = "\
Examples:
  zizq tls init
  zizq tls init --san localhost --san myhost.local --client worker1 --client worker2
  zizq tls init --ca-cert my-ca.pem --ca-key my-ca-key.pem --client worker1
  zizq tls init --overwrite")]
struct InitArgs {
    /// Subject Alternative Names for the server certificate (repeatable).
    #[arg(long = "san", default_value = "localhost")]
    sans: Vec<String>,

    /// Validity period for server and client certificates in days.
    #[arg(long, default_value_t = 365)]
    days: u32,

    /// Validity period for the CA certificate in days.
    #[arg(long, default_value_t = 3650)]
    ca_days: u32,

    /// Common Name for the CA certificate.
    #[arg(long = "cn", default_value = "Zizq CA")]
    common_name: String,

    /// Path to an existing CA certificate (skips CA generation).
    #[arg(long = "ca-cert")]
    ca_cert: Option<PathBuf>,

    /// Path to an existing CA private key (skips CA generation).
    #[arg(long = "ca-key")]
    ca_key: Option<PathBuf>,

    /// Client names to generate certificates for (repeatable).
    #[arg(long = "client")]
    clients: Vec<String>,

    /// Output directory for generated certificates [default: <root-dir>/tls].
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,

    /// Overwrite existing files instead of skipping.
    #[arg(long)]
    overwrite: bool,
}

#[derive(Parser)]
#[command(after_long_help = "\
Examples:
  zizq tls ca
  zizq tls ca --cn 'My Org CA' --days 7300")]
struct CaArgs {
    /// Validity period in days.
    #[arg(long, default_value_t = 3650)]
    days: u32,

    /// Common Name for the CA certificate.
    #[arg(long = "cn", default_value = "Zizq CA")]
    common_name: String,

    /// Output directory for generated certificates [default: <root-dir>/tls].
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,

    /// Overwrite existing files.
    #[arg(long)]
    overwrite: bool,
}

#[derive(Parser)]
#[command(after_long_help = "\
Examples:
  zizq tls server-cert
  zizq tls server-cert --san localhost --san 192.168.1.10
  zizq tls server-cert --ca-cert my-ca.pem --ca-key my-ca-key.pem")]
struct ServerCertArgs {
    /// Subject Alternative Names (repeatable).
    #[arg(long = "san", default_value = "localhost")]
    sans: Vec<String>,

    /// Validity period in days.
    #[arg(long, default_value_t = 365)]
    days: u32,

    /// Common Name for the server certificate.
    #[arg(long = "cn", default_value = "Zizq Server")]
    common_name: String,

    /// Path to the CA certificate.
    #[arg(long = "ca-cert")]
    ca_cert: Option<PathBuf>,

    /// Path to the CA private key.
    #[arg(long = "ca-key")]
    ca_key: Option<PathBuf>,

    /// Output directory for generated certificates [default: <root-dir>/tls].
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,

    /// Overwrite existing files.
    #[arg(long)]
    overwrite: bool,
}

#[derive(Parser)]
#[command(after_long_help = "\
Examples:
  zizq tls client-cert --name worker1
  zizq tls client-cert --name worker1 --ca-cert my-ca.pem --ca-key my-ca-key.pem")]
struct ClientCertArgs {
    /// Client name — used as CN and output file prefix.
    #[arg(long)]
    name: String,

    /// Validity period in days.
    #[arg(long, default_value_t = 365)]
    days: u32,

    /// Path to the CA certificate.
    #[arg(long = "ca-cert")]
    ca_cert: Option<PathBuf>,

    /// Path to the CA private key.
    #[arg(long = "ca-key")]
    ca_key: Option<PathBuf>,

    /// Output directory for generated certificates [default: <root-dir>/tls].
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,

    /// Overwrite existing files.
    #[arg(long)]
    overwrite: bool,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_out_dir(root_dir: &str, out_dir_override: Option<&Path>) -> PathBuf {
    match out_dir_override {
        Some(p) => p.to_path_buf(),
        None => Path::new(root_dir).join("tls"),
    }
}

/// Write PEM content to a file.
///
/// Returns `Ok(true)` if the file was written, `Ok(false)` if it was
/// skipped (already exists and `!overwrite`), or `Err` on I/O failure.
fn write_pem(path: &Path, contents: &str, overwrite: bool) -> Result<bool, String> {
    if path.exists() && !overwrite {
        println!("  skip: {} (already exists)", path.display());
        return Ok(false);
    }
    fs::write(path, contents).map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    println!("  wrote: {}", path.display());
    Ok(true)
}

/// Validate that the given PEM strings can be parsed as a CA cert + key.
fn validate_ca_pem(cert_pem: &str, key_pem: &str) -> Result<(), String> {
    let key_pair =
        KeyPair::from_pem(key_pem).map_err(|e| format!("failed to parse CA key: {e}"))?;
    Issuer::from_ca_cert_pem(cert_pem, &key_pair)
        .map_err(|e| format!("failed to parse CA cert: {e}"))?;
    Ok(())
}

/// Generate a new CA certificate and key, returning (cert_pem, key_pem).
fn gen_ca(cn: &str, days: u32) -> Result<(String, String), String> {
    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|e| format!("failed to create CA params: {e}"))?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(i64::from(days));

    let key_pair = KeyPair::generate().map_err(|e| format!("failed to generate CA key: {e}"))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("failed to self-sign CA cert: {e}"))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate a certificate signed by the given CA, returning (cert_pem, key_pem).
fn gen_signed_cert(
    mut params: CertificateParams,
    ca_cert_pem: &str,
    ca_key_pem: &str,
) -> Result<(String, String), String> {
    let ca_key =
        KeyPair::from_pem(ca_key_pem).map_err(|e| format!("failed to parse CA key: {e}"))?;
    let issuer = Issuer::from_ca_cert_pem(ca_cert_pem, &ca_key)
        .map_err(|e| format!("failed to parse CA cert: {e}"))?;

    // Ensure this is not a CA certificate.
    params.is_ca = IsCa::NoCa;

    let key_pair = KeyPair::generate().map_err(|e| format!("failed to generate key: {e}"))?;
    let cert = params
        .signed_by(&key_pair, &issuer)
        .map_err(|e| format!("failed to sign cert: {e}"))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

fn make_server_params(sans: &[String], cn: &str, days: u32) -> Result<CertificateParams, String> {
    let mut params =
        CertificateParams::new(sans.to_vec()).map_err(|e| format!("invalid SANs: {e}"))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(i64::from(days));
    params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    Ok(params)
}

fn make_client_params(name: &str, days: u32) -> Result<CertificateParams, String> {
    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|e| format!("failed to create client params: {e}"))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(i64::from(days));
    params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    Ok(params)
}

// ---------------------------------------------------------------------------
// Subcommand implementations
// ---------------------------------------------------------------------------

fn run_init(root_dir: &str, args: InitArgs) -> Result<(), String> {
    let out_dir = resolve_out_dir(root_dir, args.out_dir.as_deref());
    fs::create_dir_all(&out_dir)
        .map_err(|e| format!("failed to create {}: {e}", out_dir.display()))?;

    let ca_cert_path = out_dir.join("ca-cert.pem");
    let ca_key_path = out_dir.join("ca-key.pem");

    // Resolve or generate CA.
    let (ca_cert_pem, ca_key_pem) =
        if let (Some(ext_cert), Some(ext_key)) = (&args.ca_cert, &args.ca_key) {
            // External CA provided — load it.
            println!("Using external CA:");
            println!("  cert: {}", ext_cert.display());
            println!("  key:  {}", ext_key.display());
            let cert_pem =
                fs::read_to_string(ext_cert).map_err(|e| format!("failed to read CA cert: {e}"))?;
            let key_pem =
                fs::read_to_string(ext_key).map_err(|e| format!("failed to read CA key: {e}"))?;
            // Validate it parses as a CA.
            validate_ca_pem(&cert_pem, &key_pem)?;
            (cert_pem, key_pem)
        } else if ca_cert_path.exists() && ca_key_path.exists() && !args.overwrite {
            // Existing CA in out_dir — reuse it.
            println!("CA already exists, reusing:");
            println!("  skip: {} (already exists)", ca_cert_path.display());
            println!("  skip: {} (already exists)", ca_key_path.display());
            let cert_pem = fs::read_to_string(&ca_cert_path)
                .map_err(|e| format!("failed to read existing CA cert: {e}"))?;
            let key_pem = fs::read_to_string(&ca_key_path)
                .map_err(|e| format!("failed to read existing CA key: {e}"))?;
            validate_ca_pem(&cert_pem, &key_pem)?;
            (cert_pem, key_pem)
        } else {
            // Generate new CA.
            println!("Generating CA ({}):", args.common_name);
            let (cert_pem, key_pem) = gen_ca(&args.common_name, args.ca_days)?;
            write_pem(&ca_cert_path, &cert_pem, args.overwrite)?;
            write_pem(&ca_key_path, &key_pem, args.overwrite)?;
            (cert_pem, key_pem)
        };

    // Generate server certificate.
    println!(
        "Generating server certificate (SANs: {}):",
        args.sans.join(", ")
    );
    let server_params = make_server_params(&args.sans, "Zizq Server", args.days)?;
    let (server_cert, server_key) = gen_signed_cert(server_params, &ca_cert_pem, &ca_key_pem)?;
    write_pem(
        &out_dir.join("server-cert.pem"),
        &server_cert,
        args.overwrite,
    )?;
    write_pem(&out_dir.join("server-key.pem"), &server_key, args.overwrite)?;

    // Generate client certificates.
    for name in &args.clients {
        println!("Generating client certificate ({name}):");
        let client_params = make_client_params(name, args.days)?;
        let (client_cert, client_key) = gen_signed_cert(client_params, &ca_cert_pem, &ca_key_pem)?;
        write_pem(
            &out_dir.join(format!("client-{name}-cert.pem")),
            &client_cert,
            args.overwrite,
        )?;
        write_pem(
            &out_dir.join(format!("client-{name}-key.pem")),
            &client_key,
            args.overwrite,
        )?;
    }

    Ok(())
}

fn run_ca(root_dir: &str, args: CaArgs) -> Result<(), String> {
    let out_dir = resolve_out_dir(root_dir, args.out_dir.as_deref());
    fs::create_dir_all(&out_dir)
        .map_err(|e| format!("failed to create {}: {e}", out_dir.display()))?;

    let cert_path = out_dir.join("ca-cert.pem");
    let key_path = out_dir.join("ca-key.pem");

    if (cert_path.exists() || key_path.exists()) && !args.overwrite {
        return Err(format!(
            "CA files already exist in {}. Use --overwrite to replace them.",
            out_dir.display()
        ));
    }

    println!("Generating CA ({}):", args.common_name);
    let (cert_pem, key_pem) = gen_ca(&args.common_name, args.days)?;
    write_pem(&cert_path, &cert_pem, args.overwrite)?;
    write_pem(&key_path, &key_pem, args.overwrite)?;

    Ok(())
}

fn run_server_cert(root_dir: &str, args: ServerCertArgs) -> Result<(), String> {
    let out_dir = resolve_out_dir(root_dir, args.out_dir.as_deref());
    fs::create_dir_all(&out_dir)
        .map_err(|e| format!("failed to create {}: {e}", out_dir.display()))?;

    let ca_cert_path = args.ca_cert.unwrap_or_else(|| out_dir.join("ca-cert.pem"));
    let ca_key_path = args.ca_key.unwrap_or_else(|| out_dir.join("ca-key.pem"));

    let ca_cert_pem = fs::read_to_string(&ca_cert_path)
        .map_err(|e| format!("failed to read CA cert {}: {e}", ca_cert_path.display()))?;
    let ca_key_pem = fs::read_to_string(&ca_key_path)
        .map_err(|e| format!("failed to read CA key {}: {e}", ca_key_path.display()))?;

    println!(
        "Generating server certificate (SANs: {}):",
        args.sans.join(", ")
    );
    let params = make_server_params(&args.sans, &args.common_name, args.days)?;
    let (cert_pem, key_pem) = gen_signed_cert(params, &ca_cert_pem, &ca_key_pem)?;

    let cert_path = out_dir.join("server-cert.pem");
    let key_path = out_dir.join("server-key.pem");

    if (cert_path.exists() || key_path.exists()) && !args.overwrite {
        return Err(format!(
            "Server cert files already exist in {}. Use --overwrite to replace them.",
            out_dir.display()
        ));
    }

    write_pem(&cert_path, &cert_pem, args.overwrite)?;
    write_pem(&key_path, &key_pem, args.overwrite)?;

    Ok(())
}

fn run_client_cert(root_dir: &str, args: ClientCertArgs) -> Result<(), String> {
    let out_dir = resolve_out_dir(root_dir, args.out_dir.as_deref());
    fs::create_dir_all(&out_dir)
        .map_err(|e| format!("failed to create {}: {e}", out_dir.display()))?;

    let ca_cert_path = args.ca_cert.unwrap_or_else(|| out_dir.join("ca-cert.pem"));
    let ca_key_path = args.ca_key.unwrap_or_else(|| out_dir.join("ca-key.pem"));

    let ca_cert_pem = fs::read_to_string(&ca_cert_path)
        .map_err(|e| format!("failed to read CA cert {}: {e}", ca_cert_path.display()))?;
    let ca_key_pem = fs::read_to_string(&ca_key_path)
        .map_err(|e| format!("failed to read CA key {}: {e}", ca_key_path.display()))?;

    let name = &args.name;
    println!("Generating client certificate ({name}):");
    let params = make_client_params(name, args.days)?;
    let (cert_pem, key_pem) = gen_signed_cert(params, &ca_cert_pem, &ca_key_pem)?;

    let cert_path = out_dir.join(format!("client-{name}-cert.pem"));
    let key_path = out_dir.join(format!("client-{name}-key.pem"));

    if (cert_path.exists() || key_path.exists()) && !args.overwrite {
        return Err(format!(
            "Client cert files for '{name}' already exist in {}. Use --overwrite to replace them.",
            out_dir.display()
        ));
    }

    write_pem(&cert_path, &cert_pem, args.overwrite)?;
    write_pem(&key_path, &key_pem, args.overwrite)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run(args: Args, root_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let result = match args.command {
        Command::Init(cmd) => run_init(root_dir, cmd),
        Command::Ca(cmd) => run_ca(root_dir, cmd),
        Command::ServerCert(cmd) => run_server_cert(root_dir, cmd),
        Command::ClientCert(cmd) => run_client_cert(root_dir, cmd),
    };

    result.map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn gen_ca_produces_valid_pem() {
        let (cert_pem, key_pem) = gen_ca("Test CA", 365).unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN PRIVATE KEY"));
        // Round-trip: can load back.
        validate_ca_pem(&cert_pem, &key_pem).unwrap();
    }

    #[test]
    fn gen_signed_server_cert() {
        let (ca_cert, ca_key) = gen_ca("Test CA", 365).unwrap();
        let params = make_server_params(&["localhost".into()], "Test Server", 30).unwrap();
        let (cert_pem, key_pem) = gen_signed_cert(params, &ca_cert, &ca_key).unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn gen_signed_client_cert() {
        let (ca_cert, ca_key) = gen_ca("Test CA", 365).unwrap();
        let params = make_client_params("worker1", 30).unwrap();
        let (cert_pem, key_pem) = gen_signed_cert(params, &ca_cert, &ca_key).unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn write_pem_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.pem");
        let written = write_pem(&path, "content", false).unwrap();
        assert!(written);
        assert_eq!(fs::read_to_string(&path).unwrap(), "content");
    }

    #[test]
    fn write_pem_skips_existing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.pem");
        fs::write(&path, "original").unwrap();
        let written = write_pem(&path, "new", false).unwrap();
        assert!(!written);
        assert_eq!(fs::read_to_string(&path).unwrap(), "original");
    }

    #[test]
    fn write_pem_overwrites_when_flagged() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.pem");
        fs::write(&path, "original").unwrap();
        let written = write_pem(&path, "new", true).unwrap();
        assert!(written);
        assert_eq!(fs::read_to_string(&path).unwrap(), "new");
    }

    #[test]
    fn init_generates_all_files() {
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("tls");
        run_init(
            dir.path().to_str().unwrap(),
            InitArgs {
                sans: vec!["localhost".into()],
                days: 30,
                ca_days: 365,
                common_name: "Test CA".into(),
                ca_cert: None,
                ca_key: None,
                clients: vec!["worker1".into()],
                out_dir: Some(out.clone()),
                overwrite: false,
            },
        )
        .unwrap();

        assert!(out.join("ca-cert.pem").exists());
        assert!(out.join("ca-key.pem").exists());
        assert!(out.join("server-cert.pem").exists());
        assert!(out.join("server-key.pem").exists());
        assert!(out.join("client-worker1-cert.pem").exists());
        assert!(out.join("client-worker1-key.pem").exists());
    }

    #[test]
    fn init_reuses_existing_ca() {
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("tls");

        // First run generates CA.
        run_init(
            dir.path().to_str().unwrap(),
            InitArgs {
                sans: vec!["localhost".into()],
                days: 30,
                ca_days: 365,
                common_name: "Test CA".into(),
                ca_cert: None,
                ca_key: None,
                clients: vec![],
                out_dir: Some(out.clone()),
                overwrite: false,
            },
        )
        .unwrap();

        let ca_cert_before = fs::read_to_string(out.join("ca-cert.pem")).unwrap();

        // Second run reuses CA (no --overwrite).
        run_init(
            dir.path().to_str().unwrap(),
            InitArgs {
                sans: vec!["localhost".into()],
                days: 30,
                ca_days: 365,
                common_name: "Test CA".into(),
                ca_cert: None,
                ca_key: None,
                clients: vec![],
                out_dir: Some(out.clone()),
                overwrite: false,
            },
        )
        .unwrap();

        let ca_cert_after = fs::read_to_string(out.join("ca-cert.pem")).unwrap();
        assert_eq!(ca_cert_before, ca_cert_after);
    }

    #[test]
    fn init_with_overwrite_regenerates_ca() {
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("tls");

        run_init(
            dir.path().to_str().unwrap(),
            InitArgs {
                sans: vec!["localhost".into()],
                days: 30,
                ca_days: 365,
                common_name: "Test CA".into(),
                ca_cert: None,
                ca_key: None,
                clients: vec![],
                out_dir: Some(out.clone()),
                overwrite: false,
            },
        )
        .unwrap();

        let ca_key_before = fs::read_to_string(out.join("ca-key.pem")).unwrap();

        run_init(
            dir.path().to_str().unwrap(),
            InitArgs {
                sans: vec!["localhost".into()],
                days: 30,
                ca_days: 365,
                common_name: "Test CA".into(),
                ca_cert: None,
                ca_key: None,
                clients: vec![],
                out_dir: Some(out.clone()),
                overwrite: true,
            },
        )
        .unwrap();

        let ca_key_after = fs::read_to_string(out.join("ca-key.pem")).unwrap();
        assert_ne!(ca_key_before, ca_key_after);
    }

    #[test]
    fn ca_refuses_overwrite_by_default() {
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("tls");
        fs::create_dir_all(&out).unwrap();
        fs::write(out.join("ca-cert.pem"), "existing").unwrap();

        let err = run_ca(
            dir.path().to_str().unwrap(),
            CaArgs {
                days: 365,
                common_name: "Test CA".into(),
                out_dir: Some(out),
                overwrite: false,
            },
        )
        .unwrap_err();

        assert!(err.contains("already exist"), "got: {err}");
    }

    #[test]
    fn init_with_external_ca() {
        let dir = TempDir::new().unwrap();
        let out = dir.path().join("tls");
        let ext_dir = dir.path().join("ext");
        fs::create_dir_all(&ext_dir).unwrap();

        // Generate external CA files.
        let (ca_cert, ca_key) = gen_ca("External CA", 365).unwrap();
        let ext_cert = ext_dir.join("ca.pem");
        let ext_key = ext_dir.join("ca-key.pem");
        fs::write(&ext_cert, &ca_cert).unwrap();
        fs::write(&ext_key, &ca_key).unwrap();

        run_init(
            dir.path().to_str().unwrap(),
            InitArgs {
                sans: vec!["localhost".into()],
                days: 30,
                ca_days: 365,
                common_name: "Ignored".into(),
                ca_cert: Some(ext_cert),
                ca_key: Some(ext_key),
                clients: vec![],
                out_dir: Some(out.clone()),
                overwrite: false,
            },
        )
        .unwrap();

        // CA files should NOT be written to out_dir when using external CA.
        assert!(!out.join("ca-cert.pem").exists());
        assert!(out.join("server-cert.pem").exists());
    }

    #[test]
    fn resolve_out_dir_uses_override() {
        let result = resolve_out_dir("/root", Some(Path::new("/custom")));
        assert_eq!(result, PathBuf::from("/custom"));
    }

    #[test]
    fn resolve_out_dir_defaults_to_root_tls() {
        let result = resolve_out_dir("/root", None);
        assert_eq!(result, PathBuf::from("/root/tls"));
    }
}
