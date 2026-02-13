// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Zanxio CLI `serve` command entry point.
//!
//! Initializes the database and starts the HTTP server.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::store::Store;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Default priority for jobs that don't specify one.
///
/// Sits in the middle of the u16 range so there is equal room for higher
/// and lower priority work.
const DEFAULT_PRIORITY: u16 = 32_768;

// --- Response types ---

/// Response shape for the health check.
#[derive(Serialize)]
struct HealthResponse {
    /// Status message.
    status: &'static str,
}

/// Response shape for all error responses.
#[derive(Serialize)]
struct ErrorResponse {
    /// Error message.
    error: String,
}

/// Response shape for content negotiation errors (406, 415).
#[derive(Serialize)]
struct UnsupportedFormatResponse {
    /// Error message.
    error: String,

    /// List of supported content types.
    supported: Vec<&'static str>,
}

/// Request shape for the job enqueue request.
///
/// Jobs are required to specify the queue that they target and a payload to be
/// provided when dequeued. Priority is optional, defaulting to the center of
/// the priority range.
#[derive(Deserialize)]
struct EnqueueRequest {
    /// The queue name onto which this job is pushed.
    queue: String,

    /// Optional job priority (lower is higher priority).
    priority: Option<u16>,

    /// Arbitrary job payload provided to the client on dequeue.
    payload: serde_json::Value,
}

/// Response shape for enqueue requests.
#[derive(Serialize)]
struct EnqueueResponse {
    /// The unique identifier for this job.
    ///
    /// This identifier must be supplied when marking jobs as completed or
    /// failed.
    id: String,

    /// The queue onto which this job was pushed.
    queue: String,

    /// The priority assigned to this job.
    priority: u16,
}

/// Location of the internal database within the root directory.
const DATABASE_DIR: &str = "data";

/// Shared server state, passed to all request handlers.
struct AppState {
    /// Persistent store for job queue operations.
    store: Store,
}

/// Arguments for the `serve` subcommand.
#[derive(Parser)]
pub struct Args {
    /// Root directory for all server data and configuration.
    #[arg(long, default_value = "./zanxio-root", env = "ZANXIO_ROOT_DIR")]
    root_dir: String,

    /// Address to bind the HTTP server to.
    #[arg(long, default_value = "127.0.0.1", env = "ZANXIO_HOST")]
    host: String,

    /// Port to listen for HTTP connections on.
    #[arg(long, default_value_t = 7890, env = "ZANXIO_PORT")]
    port: u16,
}

/// Initializes the database and starts the HTTP server.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Make sure the root dir exists.
    let root = std::path::Path::new(&args.root_dir);
    std::fs::create_dir_all(root)?;

    // Init/open the store (within the root dir).
    let store = Store::open(root.join(DATABASE_DIR))?;
    tracing::info!(root_dir = %root.display(), "store opened");

    // Initialize shared state accessible to all request handlers.
    let state = Arc::new(AppState { store });

    // Set up the TCP socket for incoming connections.
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening");

    eprintln!("Zanxio {}", env!("CARGO_PKG_VERSION"));
    eprintln!("Accepting connections on {addr}");

    // Make sure we catch signals to shut down cleanly.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    // Start the incoming connection/accept runloop.
    //
    // Each new connection is turned into a tokio IO stream and then handed off
    // to hyper to process the HTTP request, with HTTP/2 and HTTP/1 support
    // automatically negotiated.
    loop {
        tokio::select! {
            // Wait for the next connection.
            result = listener.accept() => {
                // Wrap the stream with tokio.
                let (stream, remote_addr) = result?;
                let io = TokioIo::new(stream);
                let state = state.clone();

                // Handle the connection in an async task using hyper,
                // dispatching to our request handler.
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let state = state.clone();
                        async move { handle(state, req).await }
                    });

                    if let Err(err) = Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        tracing::error!(%remote_addr, %err, "connection error");
                    }
                });
            }
            // Handle incoming signal for shutdown (see shutdown_signal() above).
            // We simply break to exit the run loop.
            () = &mut shutdown => {
                tracing::info!("shutdown signal received");
                eprintln!("Shutting down...");
                break;
            }
        }
    }

    eprintln!("Server stopped.");
    Ok(())
}

/// Request handler function dispatched by hyper.
///
/// This handles all routing to specific handlers.
async fn handle<B>(state: Arc<AppState>, req: Request<B>) -> Result<Response<BoxBody>, Infallible>
where
    B: hyper::body::Body,
    B::Error: std::fmt::Display,
{
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Determine response format from Accept header.
    let format = match negotiate_accept(req.headers()) {
        Some(f) => f,
        None => {
            let res = not_acceptable();
            tracing::debug!(%method, %path, status = 406, "request");
            return Ok(res);
        }
    };

    let res = match (method.as_str(), path.as_str()) {
        // Check the health status of the server process.
        ("GET", "/health") => respond(format, StatusCode::OK, &HealthResponse { status: "ok" }),
        // Enqueue a new job.
        ("POST", "/jobs") => enqueue(format, state, req).await,
        // Mark a job as successfully completed.
        ("POST", p) if p.starts_with("/jobs/") && p.ends_with("/success") => {
            let job_id = &p["/jobs/".len()..p.len() - "/success".len()];
            mark_completed(format, state, job_id).await
        }
        // Invalid path: 404.
        _ => respond(
            format,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "not found".into(),
            },
        ),
    };

    tracing::debug!(
        %method,
        %path,
        status = res.status().as_u16(),
        "request"
    );

    Ok(res)
}

/// Handle `POST /jobs` — enqueue a new job.
async fn enqueue<B>(format: Format, state: Arc<AppState>, req: Request<B>) -> Response<BoxBody>
where
    B: hyper::body::Body,
    B::Error: std::fmt::Display,
{
    // Determine how to parse the body from Content-Type.
    let input_format = match negotiate_content_type(req.headers()) {
        Some(f) => f,
        None => return unsupported_media_type(format),
    };

    // Read the request body (bytes).
    let body = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return respond(
                format,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("failed to read body: {e}"),
                },
            );
        }
    };

    // Deserialize the request.
    let enqueue_req: EnqueueRequest = match parse_body(input_format, &body) {
        Ok(r) => r,
        Err(msg) => {
            return respond(
                format,
                StatusCode::BAD_REQUEST,
                &ErrorResponse { error: msg },
            );
        }
    };

    let priority = enqueue_req.priority.unwrap_or(DEFAULT_PRIORITY);

    // Delegate to the store instance to enqueue the job and return that job to
    // the client.
    match state
        .store
        .enqueue(&enqueue_req.queue, priority, enqueue_req.payload)
        .await
    {
        Ok(job) => respond(
            format,
            StatusCode::CREATED,
            &EnqueueResponse {
                id: job.id,
                queue: job.queue,
                priority: job.priority,
            },
        ),
        Err(e) => {
            tracing::error!(%e, "enqueue failed");
            respond(
                format,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Handle `POST /jobs/{id}/success` — mark a job as completed.
async fn mark_completed(format: Format, state: Arc<AppState>, job_id: &str) -> Response<BoxBody> {
    match state.store.mark_completed(job_id).await {
        Ok(true) => no_content(),
        Ok(false) => respond(
            format,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found in working set".into(),
            },
        ),
        Err(e) => {
            tracing::error!(%e, "mark_completed failed");
            respond(
                format,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Supported serialization formats for requests and responses.
///
/// Currently both JSON and MessagePack formats are supported.
/// The default format is JSON due to its widespread support and human
/// readability but clients are recommended to use MessagePack in production
/// workloads.
#[derive(Clone, Copy)]
enum Format {
    Json,
    MsgPack,
}

impl Format {
    /// The MIME type for this format.
    fn mime(self) -> &'static str {
        match self {
            Format::Json => "application/json",
            Format::MsgPack => "application/msgpack",
        }
    }
}

/// All content types we support.
const SUPPORTED_TYPES: &[&str] = &["application/json", "application/msgpack"];

/// Determine the response format from the Accept header.
///
/// Returns `None` if the client requested a format we don't support.
/// Defaults to JSON when no Accept header is present (curl-friendly).
fn negotiate_accept(headers: &hyper::HeaderMap) -> Option<Format> {
    match headers.get(hyper::header::ACCEPT) {
        None => Some(Format::Json),
        Some(value) => {
            let value = value.to_str().unwrap_or("");
            if value.contains("application/msgpack") {
                Some(Format::MsgPack)
            } else if value.contains("application/json") || value.contains("*/*") {
                Some(Format::Json)
            } else {
                None
            }
        }
    }
}

/// Determine the request body format from the Content-Type header.
///
/// Returns `None` if the client sent a format we don't support.
/// Defaults to JSON when no Content-Type header is present.
fn negotiate_content_type(headers: &hyper::HeaderMap) -> Option<Format> {
    match headers.get(hyper::header::CONTENT_TYPE) {
        None => Some(Format::Json),
        Some(value) => {
            let value = value.to_str().unwrap_or("");
            if value.contains("application/msgpack") {
                Some(Format::MsgPack)
            } else if value.contains("application/json") {
                Some(Format::Json)
            } else {
                None
            }
        }
    }
}

/// Serialize a value and return it as an HTTP response in the negotiated format.
fn respond<T: Serialize>(format: Format, status: StatusCode, body: &T) -> Response<BoxBody> {
    let bytes = match format {
        Format::Json => serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec()),
        Format::MsgPack => rmp_serde::to_vec_named(body).unwrap_or_else(|_| b"{}".to_vec()),
    };
    let mut res = Response::new(full(bytes));
    *res.status_mut() = status;
    res.headers_mut()
        .insert(hyper::header::CONTENT_TYPE, format.mime().parse().unwrap());
    res
}

/// Return a 406 Not Acceptable response.
///
/// This is always JSON since we need *some* format to communicate the error,
/// and the client clearly isn't getting what they asked for anyway.
fn not_acceptable() -> Response<BoxBody> {
    respond(
        Format::Json,
        StatusCode::NOT_ACCEPTABLE,
        &UnsupportedFormatResponse {
            error: "not acceptable".into(),
            supported: SUPPORTED_TYPES.to_vec(),
        },
    )
}

/// Return a 204 No Content response with an empty body.
fn no_content() -> Response<BoxBody> {
    let mut res = Response::new(full(Bytes::new()));
    *res.status_mut() = StatusCode::NO_CONTENT;
    res
}

/// Return a 415 Unsupported Media Type response.
fn unsupported_media_type(format: Format) -> Response<BoxBody> {
    respond(
        format,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        &UnsupportedFormatResponse {
            error: "unsupported media type".into(),
            supported: SUPPORTED_TYPES.to_vec(),
        },
    )
}

/// Deserialize a request body according to the negotiated format.
fn parse_body<T: serde::de::DeserializeOwned>(format: Format, body: &[u8]) -> Result<T, String> {
    match format {
        Format::Json => serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}")),
        Format::MsgPack => {
            rmp_serde::from_slice(body).map_err(|e| format!("invalid MessagePack: {e}"))
        }
    }
}

/// Turn the provided data into a complete response sent in one chunk.
fn full(data: impl Into<Bytes>) -> BoxBody {
    Full::new(data.into())
        .map_err(|never| match never {})
        .boxed()
}

/// Async function that returns once a signal is received.
/// This is checked in the select! loop so the server can exit upon receipt of
/// one of the signals.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(windows)]
    {
        let mut ctrl_close =
            tokio::signal::windows::ctrl_close().expect("failed to register ctrl_close handler");
        let mut ctrl_shutdown = tokio::signal::windows::ctrl_shutdown()
            .expect("failed to register ctrl_shutdown handler");

        tokio::select! {
            _ = ctrl_c => {}
            _ = ctrl_close.recv() => {}
            _ = ctrl_shutdown.recv() => {}
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        ctrl_c.await.ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Empty;

    /// Create a temporary AppState with a fresh store for testing.
    fn test_state() -> Arc<AppState> {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        // Leak the TempDir so it isn't cleaned up while the store is open.
        std::mem::forget(dir);
        Arc::new(AppState { store })
    }

    fn empty_request(method: &str, uri: &str) -> Request<Empty<Bytes>> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Empty::new())
            .unwrap()
    }

    fn request_with_accept(method: &str, uri: &str, accept: &str) -> Request<Empty<Bytes>> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("accept", accept)
            .body(Empty::new())
            .unwrap()
    }

    fn json_request(method: &str, uri: &str, body: &serde_json::Value) -> Request<Full<Bytes>> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(body).unwrap())))
            .unwrap()
    }

    // Convert the response into a String for testing purposes.
    async fn response_body(res: Response<BoxBody>) -> String {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn health_returns_200() {
        let state = test_state();
        let req = empty_request("GET", "/health");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn health_returns_json_content_type() {
        let state = test_state();
        let req = empty_request("GET", "/health");
        let res = handle(state, req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let state = test_state();
        let req = empty_request("GET", "/nope");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "not found");
    }

    #[tokio::test]
    async fn accept_json_returns_200() {
        let state = test_state();
        let req = request_with_accept("GET", "/health", "application/json");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accept_wildcard_returns_200() {
        let state = test_state();
        let req = request_with_accept("GET", "/health", "*/*");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accept_xml_returns_406() {
        let state = test_state();
        let req = request_with_accept("GET", "/health", "text/xml");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "not acceptable");
        let supported = body["supported"].as_array().unwrap();
        assert!(supported.contains(&"application/json".into()));
        assert!(supported.contains(&"application/msgpack".into()));
    }

    #[tokio::test]
    async fn accept_msgpack_returns_msgpack() {
        let state = test_state();
        let req = request_with_accept("GET", "/health", "application/msgpack");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/msgpack"
        );

        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn enqueue_returns_201() {
        let state = test_state();
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn enqueue_returns_job_id() {
        let state = test_state();
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = handle(state, req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["id"].is_string());
        assert!(!body["id"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn enqueue_defaults_priority() {
        let state = test_state();
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = handle(state, req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["priority"], DEFAULT_PRIORITY);
    }

    #[tokio::test]
    async fn enqueue_accepts_custom_queue_and_priority() {
        let state = test_state();
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "emails", "priority": 5, "payload": "test"}),
        );
        let res = handle(state, req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["queue"], "emails");
        assert_eq!(body["priority"], 5);
    }

    #[tokio::test]
    async fn enqueue_rejects_missing_queue() {
        let state = test_state();
        let req = json_request("POST", "/jobs", &serde_json::json!({"payload": "test"}));
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn enqueue_rejects_invalid_json() {
        let state = test_state();
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .body(Full::new(Bytes::from("not json")))
            .unwrap();
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn enqueue_rejects_unsupported_content_type() {
        let state = test_state();
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/xml")
            .body(Full::new(Bytes::from("<job/>")))
            .unwrap();
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let supported = body["supported"].as_array().unwrap();
        assert!(supported.contains(&"application/json".into()));
        assert!(supported.contains(&"application/msgpack".into()));
    }

    #[tokio::test]
    async fn enqueue_accepts_msgpack_request_body() {
        #[derive(Serialize)]
        struct MsgPackEnqueue {
            queue: &'static str,
            priority: u16,
            payload: &'static str,
        }

        let state = test_state();
        let body = rmp_serde::to_vec_named(&MsgPackEnqueue {
            queue: "events",
            priority: 3,
            payload: "mp",
        })
        .unwrap();

        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/msgpack")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["queue"], "events");
        assert_eq!(body["priority"], 3);
    }

    #[tokio::test]
    async fn enqueue_msgpack_round_trip() {
        #[derive(Serialize)]
        struct MsgPackEnqueue {
            queue: &'static str,
            priority: u16,
            payload: &'static str,
        }

        let state = test_state();
        let body = rmp_serde::to_vec_named(&MsgPackEnqueue {
            queue: "events",
            priority: 7,
            payload: "round-trip",
        })
        .unwrap();

        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/msgpack")
            .header("accept", "application/msgpack")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/msgpack"
        );

        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(body["queue"], "events");
        assert_eq!(body["priority"], 7);
        assert!(body["id"].is_string());
    }

    #[tokio::test]
    async fn enqueue_rejects_msgpack_binary_payload() {
        // Construct raw MessagePack with a binary (bin) type payload.
        // serde_json::Value has no binary variant, so deserialization must fail.
        let mut buf = Vec::new();
        rmp::encode::write_map_len(&mut buf, 2).unwrap();
        rmp::encode::write_str(&mut buf, "queue").unwrap();
        rmp::encode::write_str(&mut buf, "default").unwrap();
        rmp::encode::write_str(&mut buf, "payload").unwrap();
        rmp::encode::write_bin(&mut buf, &[0x00, 0xff, 0xfe]).unwrap();

        let state = test_state();
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/msgpack")
            .body(Full::new(Bytes::from(buf)))
            .unwrap();
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn success_returns_204() {
        let state = test_state();

        // Enqueue and take a job so it's in the working set.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = handle(state.clone(), req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        state.store.take_next_job().await.unwrap();

        // Mark it as completed.
        let req = empty_request("POST", &format!("/jobs/{job_id}/success"));
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn success_returns_404_for_unknown_job() {
        let state = test_state();
        let req = empty_request("POST", "/jobs/nonexistent/success");
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn success_returns_404_for_pending_job() {
        let state = test_state();

        // Enqueue but don't take — job is pending, not working.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = handle(state.clone(), req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        let req = empty_request("POST", &format!("/jobs/{job_id}/success"));
        let res = handle(state, req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }
}
