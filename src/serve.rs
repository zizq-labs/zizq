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

/// Response shape for the 406 - not acceptable response.
#[derive(Serialize)]
struct NotAcceptableResponse {
    /// Error message.
    error: String,

    /// List of valid content types.
    acceptable: Vec<&'static str>,
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

    // Check content negotiation before routing.
    if !accepts_json(req.headers()) {
        let res = not_acceptable();
        tracing::debug!(%method, %path, status = 406, "request");
        return Ok(res);
    }

    let res = match (method.as_str(), path.as_str()) {
        // Check the health status of the server process.
        ("GET", "/health") => respond(StatusCode::OK, &HealthResponse { status: "ok" }),
        // Enqueue a new job.
        ("POST", "/jobs") => enqueue(state, req).await,
        // Invalid path: 404.
        _ => respond(
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
async fn enqueue<B>(state: Arc<AppState>, req: Request<B>) -> Response<BoxBody>
where
    B: hyper::body::Body,
    B::Error: std::fmt::Display,
{
    // Read and parse the request body.
    let body = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return respond(
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("failed to read body: {e}"),
                },
            );
        }
    };

    let enqueue_req: EnqueueRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return respond(
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid JSON: {e}"),
                },
            );
        }
    };

    let priority = enqueue_req.priority.unwrap_or(DEFAULT_PRIORITY);

    match state
        .store
        .enqueue(&enqueue_req.queue, priority, enqueue_req.payload)
        .await
    {
        Ok(job) => respond(
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
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Returns true if the request accepts JSON responses.
///
/// Accepts if the Accept header is absent, contains `application/json`,
/// or contains `*/*`.
fn accepts_json(headers: &hyper::HeaderMap) -> bool {
    match headers.get(hyper::header::ACCEPT) {
        None => true,
        Some(value) => {
            let value = value.to_str().unwrap_or("");
            value.contains("application/json") || value.contains("*/*")
        }
    }
}

/// Serialize a value as JSON and return it as an HTTP response.
fn respond<T: Serialize>(status: StatusCode, body: &T) -> Response<BoxBody> {
    let json = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let mut res = Response::new(full(json));
    *res.status_mut() = status;
    res.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    res
}

/// Return a 406 Not Acceptable response.
///
/// This is always JSON since we need *some* format to communicate the error,
/// and the client clearly isn't getting what they asked for anyway.
fn not_acceptable() -> Response<BoxBody> {
    respond(
        StatusCode::NOT_ACCEPTABLE,
        &NotAcceptableResponse {
            error: "not acceptable".into(),
            acceptable: vec!["application/json"],
        },
    )
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
        assert!(
            body["acceptable"]
                .as_array()
                .unwrap()
                .contains(&"application/json".into())
        );
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
}
