// Copyright (c) 2025 Chris Corbyn <chris@zanxio.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! HTTP layer for the Zanxio server.
//!
//! Defines the router, request handlers, content negotiation, stream framing,
//! and all request/response types.

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{self, StatusCode};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::{Event, Sse};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::store::{self, ListJobsOptions, ScanDirection, Store, StoreEvent};

/// Default priority for jobs that don't specify one.
///
/// Sits in the middle of the u16 range so there is equal room for higher
/// and lower priority work.
const DEFAULT_PRIORITY: u16 = 32_768;

/// Default interval between heartbeat frames on idle streams.
pub const DEFAULT_HEARTBEAT_SECONDS: u64 = 15;

/// Default maximum number of jobs in the working set across all streams.
pub const DEFAULT_GLOBAL_WORKING_LIMIT: u64 = 1024;

// --- Stream types ---

/// Supported stream serialization formats, negotiated via Accept header.
#[derive(Clone, Copy)]
enum StreamFormat {
    Sse,
    NdJson,
    MsgPackStream,
}

/// All stream content types we support.
const SUPPORTED_STREAM_TYPES: &[&str] = &[
    "text/event-stream",
    "application/x-ndjson",
    "application/vnd.zanxio.msgpack-stream",
];

/// Extractor that reads the `Accept` header and resolves a `StreamFormat`.
struct StreamAccept(StreamFormat);

impl<S> FromRequestParts<S> for StreamAccept
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        match parts.headers.get(ACCEPT) {
            None => Ok(StreamAccept(StreamFormat::NdJson)),
            Some(value) => {
                let value = value.to_str().unwrap_or("");
                if value.contains("text/event-stream") {
                    Ok(StreamAccept(StreamFormat::Sse))
                } else if value.contains("application/vnd.zanxio.msgpack-stream")
                    || value.contains("application/msgpack")
                {
                    Ok(StreamAccept(StreamFormat::MsgPackStream))
                } else if value.contains("application/x-ndjson")
                    || value.contains("application/json")
                    || value.contains("*/*")
                {
                    Ok(StreamAccept(StreamFormat::NdJson))
                } else {
                    Err(respond(
                        Format::Json,
                        StatusCode::NOT_ACCEPTABLE,
                        &UnsupportedFormatResponse {
                            error: "not acceptable".into(),
                            supported: SUPPORTED_STREAM_TYPES.to_vec(),
                        },
                    ))
                }
            }
        }
    }
}

/// Messages sent over the stream channel.
enum StreamMessage {
    Job(Arc<Job>), // http::Job
    Heartbeat,
}

// --- Job type ---

/// Lifecycle status of a job as returned in API responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// The job is in the store, but is scheduled to be queued at a later time.
    Scheduled,

    /// The job is in the priority queue ready to be worked.
    Ready,

    /// The job is currently being processed by a worker.
    Working,

    /// The job was successfully completed by a worker.
    Completed,

    /// The job failed too many times and exhausted its retry policy.
    Dead,
}

impl From<store::JobStatus> for JobStatus {
    fn from(s: store::JobStatus) -> Self {
        match s {
            store::JobStatus::Scheduled => Self::Scheduled,
            store::JobStatus::Ready => Self::Ready,
            store::JobStatus::Working => Self::Working,
            store::JobStatus::Completed => Self::Completed,
            store::JobStatus::Dead => Self::Dead,
        }
    }
}

impl From<JobStatus> for store::JobStatus {
    fn from(s: JobStatus) -> Self {
        match s {
            JobStatus::Scheduled => Self::Scheduled,
            JobStatus::Ready => Self::Ready,
            JobStatus::Working => Self::Working,
            JobStatus::Completed => Self::Completed,
            JobStatus::Dead => Self::Dead,
        }
    }
}

/// HTTP representation of a job.
///
/// Mirrors `store::Job` for now, but exists as a separate type so the HTTP
/// layer can evolve independently (e.g. adding virtual fields, renaming, or
/// changing serialization) without affecting storage.
#[derive(Debug, Clone, Serialize)]
pub struct Job {
    /// Unique job identifier.
    pub id: String,

    /// Queue this job belongs to.
    pub queue: String,

    /// Priority (lower number = higher priority).
    pub priority: u16,

    /// Current lifecycle status.
    pub status: JobStatus,

    /// Arbitrary payload provided by the client.
    pub payload: serde_json::Value,
}

impl From<store::Job> for Job {
    fn from(job: store::Job) -> Self {
        Self {
            id: job.id,
            queue: job.queue,
            priority: job.priority,
            status: store::JobStatus::try_from(job.status)
                .unwrap_or(store::JobStatus::Ready)
                .into(),
            payload: job.payload,
        }
    }
}

impl From<Job> for store::Job {
    fn from(job: Job) -> Self {
        Self {
            id: job.id,
            queue: job.queue,
            priority: job.priority,
            payload: job.payload,
            status: store::JobStatus::from(job.status).into(),
        }
    }
}

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

/// Query parameters for `GET /jobs`.
#[derive(Deserialize)]
struct ListJobsParams {
    /// Cursor: start after this job ID (exclusive).
    from: Option<String>,

    /// Sort order: "asc" (oldest first) or "desc" (newest first).
    #[serde(default)]
    order: Order,

    /// Maximum number of jobs to return (1–200, default 50).
    limit: Option<u16>,
}

/// Sort order for job listings.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

impl From<Order> for ScanDirection {
    fn from(o: Order) -> Self {
        match o {
            Order::Asc => Self::Asc,
            Order::Desc => Self::Desc,
        }
    }
}

impl From<ScanDirection> for Order {
    fn from(d: ScanDirection) -> Self {
        match d {
            ScanDirection::Asc => Self::Asc,
            ScanDirection::Desc => Self::Desc,
        }
    }
}

/// Default page size for job listings.
const DEFAULT_PAGE_LIMIT: u16 = 50;

/// Maximum page size for job listings.
const MAX_PAGE_LIMIT: u16 = 200;

/// Response shape for paginated job listings.
#[derive(Serialize)]
struct ListJobsResponse {
    /// The jobs on this page in the requested order.
    jobs: Vec<Job>,

    /// Information about self, next, prev pages.
    pages: ListJobsPages,
}

/// Pagination links in a job listing response.
#[derive(Serialize)]
struct ListJobsPages {
    /// URL for the current page.
    #[serde(rename = "self")]
    self_url: String,

    /// URL for the next page, or null if this is the last page.
    next: Option<String>,

    /// URL for the previous page, or null if this is the first page.
    prev: Option<String>,
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

/// Shared server state, passed to all request handlers.
pub struct AppState {
    /// Persistent store for job queue operations.
    pub store: Store,

    /// Interval between heartbeat frames on idle streams.
    pub heartbeat_interval: Duration,

    /// Maximum number of jobs in the working set across all streams.
    /// 0 means no limit.
    pub global_working_limit: u64,

    /// Current number of in-flight jobs across all streams.
    pub global_in_flight: AtomicU64,

    /// Cloneable receiver for graceful shutdown signaling.
    pub shutdown: watch::Receiver<()>,
}

// --- Content negotiation ---

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

// --- Custom extractors ---

/// Extractor that reads the `Accept` header and resolves a response `Format`.
///
/// Defaults to JSON when no Accept header is present (curl-friendly).
/// Rejects with 406 if the client requested an unsupported format.
struct AcceptFormat(Format);

impl<S> FromRequestParts<S> for AcceptFormat
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        match parts.headers.get(ACCEPT) {
            None => Ok(AcceptFormat(Format::Json)),
            Some(value) => {
                let value = value.to_str().unwrap_or("");
                if value.contains("application/msgpack") {
                    Ok(AcceptFormat(Format::MsgPack))
                } else if value.contains("application/json") || value.contains("*/*") {
                    Ok(AcceptFormat(Format::Json))
                } else {
                    Err(respond(
                        Format::Json,
                        StatusCode::NOT_ACCEPTABLE,
                        &UnsupportedFormatResponse {
                            error: "not acceptable".into(),
                            supported: SUPPORTED_TYPES.to_vec(),
                        },
                    ))
                }
            }
        }
    }
}

/// Extractor that reads the `Content-Type` header, reads the body, and
/// deserializes it as `T` using the negotiated format.
///
/// Defaults to JSON when no Content-Type header is present.
/// Rejects with 415 for unknown Content-Type, 400 for malformed body.
struct NegotiatedBody<T>(T);

impl<S, T> FromRequest<S> for NegotiatedBody<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        // Determine the output format from Accept for error responses.
        // We'll fall back on JSON as a last effort.
        let error_format = match req.headers().get(ACCEPT) {
            None => Format::Json,
            Some(v) => {
                let v = v.to_str().unwrap_or("");
                if v.contains("application/msgpack") {
                    Format::MsgPack
                } else {
                    Format::Json
                }
            }
        };

        // Determine input format from Content-Type.
        let input_format = match req.headers().get(CONTENT_TYPE) {
            None => Format::Json,
            Some(value) => {
                let value = value.to_str().unwrap_or("");
                if value.contains("application/msgpack") {
                    Format::MsgPack
                } else if value.contains("application/json") {
                    Format::Json
                } else {
                    return Err(respond(
                        error_format,
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        &UnsupportedFormatResponse {
                            error: "unsupported media type".into(),
                            supported: SUPPORTED_TYPES.to_vec(),
                        },
                    ));
                }
            }
        };

        // Read body bytes.
        let bytes = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
            Ok(b) => b,
            Err(e) => {
                return Err(respond(
                    error_format,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("failed to read body: {e}"),
                    },
                ));
            }
        };

        // Deserialize.
        let value: T = match input_format {
            Format::Json => serde_json::from_slice(&bytes).map_err(|e| {
                respond(
                    error_format,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid JSON: {e}"),
                    },
                )
            })?,
            Format::MsgPack => rmp_serde::from_slice(&bytes).map_err(|e| {
                respond(
                    error_format,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid MessagePack: {e}"),
                    },
                )
            })?,
        };

        Ok(NegotiatedBody(value))
    }
}

// --- Response helpers ---

/// Serialize a value and return it as an HTTP response in the negotiated format.
fn respond<T: Serialize>(format: Format, status: StatusCode, body: &T) -> Response {
    let bytes = match format {
        Format::Json => serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec()),
        Format::MsgPack => rmp_serde::to_vec_named(body).unwrap_or_else(|_| b"{}".to_vec()),
    };
    let mut res = Response::new(Body::from(bytes));
    *res.status_mut() = status;
    res.headers_mut()
        .insert(CONTENT_TYPE, format.mime().parse().unwrap());
    res
}

/// Return a 204 No Content response with an empty body.
fn no_content() -> Response {
    let mut res = Response::new(Body::empty());
    *res.status_mut() = StatusCode::NO_CONTENT;
    res
}

// --- Stream framing ---

/// Return a stream frame in newline delimited JSON format.
///
/// Heartbeat messages are empty (newline only).
fn frame_ndjson(msg: &StreamMessage) -> Bytes {
    match msg {
        StreamMessage::Job(job) => {
            let mut bytes = serde_json::to_vec(&**job).unwrap();
            bytes.push(b'\n');
            Bytes::from(bytes)
        }
        StreamMessage::Heartbeat => Bytes::from_static(b"\n"),
    }
}

/// Return a stream frame in length-prefixed MessagePack format.
///
/// Heartbeat messages are empty (zero prefix, no data).
fn frame_msgpack_stream(msg: &StreamMessage) -> Bytes {
    match msg {
        StreamMessage::Job(job) => {
            let payload = rmp_serde::to_vec_named(&**job).unwrap();
            let len = (payload.len() as u32).to_be_bytes();
            let mut bytes = Vec::with_capacity(4 + payload.len());
            bytes.extend_from_slice(&len);
            bytes.extend_from_slice(&payload);
            Bytes::from(bytes)
        }
        StreamMessage::Heartbeat => Bytes::from_static(&[0, 0, 0, 0]),
    }
}

/// Return a SSE event containing the given stream message.
///
/// Heartbeats are just comments.
fn frame_sse(msg: &StreamMessage) -> Result<Event, Infallible> {
    match msg {
        StreamMessage::Job(job) => Ok(Event::default().json_data(&**job).unwrap()),
        StreamMessage::Heartbeat => Ok(Event::default().comment("heartbeat")),
    }
}

// --- Stream response builders ---

/// Create a SSE streaming response which emits a message for each event in the
/// receiver channel.
fn build_sse_response(rx: mpsc::Receiver<StreamMessage>) -> Response {
    let stream = ReceiverStream::new(rx).map(|msg| frame_sse(&msg));
    Sse::new(stream).into_response()
}

/// Create a newline delimited JSON streaming response that emits a JSON
/// message for every event on the receiver channel.
fn build_ndjson_response(rx: mpsc::Receiver<StreamMessage>) -> Response {
    let stream = ReceiverStream::new(rx).map(|msg| Ok::<_, Infallible>(frame_ndjson(&msg)));
    let mut res = Response::new(Body::from_stream(stream));
    res.headers_mut()
        .insert(CONTENT_TYPE, "application/x-ndjson".parse().unwrap());
    res
}

/// Create a length-prefixed MessagePack streaming response that emits a
/// MessagePack message for every event in the receiver channel.
fn build_msgpack_stream_response(rx: mpsc::Receiver<StreamMessage>) -> Response {
    let stream = ReceiverStream::new(rx).map(|msg| Ok::<_, Infallible>(frame_msgpack_stream(&msg)));
    let mut res = Response::new(Body::from_stream(stream));
    res.headers_mut().insert(
        CONTENT_TYPE,
        "application/vnd.zanxio.msgpack-stream".parse().unwrap(),
    );
    res
}

// --- Router ---

/// Build the axum router with all routes and shared state.
pub fn app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/jobs", get(list_jobs).post(enqueue))
        .route("/jobs/stream", get(stream_jobs))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/success", post(mark_completed))
        .fallback(not_found)
        .with_state(state)
}

// --- Handlers ---

/// Handle `GET /health`.
async fn health(AcceptFormat(fmt): AcceptFormat) -> Response {
    respond(fmt, StatusCode::OK, &HealthResponse { status: "ok" })
}

/// Handle `POST /jobs` — enqueue a new job.
async fn enqueue(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    NegotiatedBody(enqueue_req): NegotiatedBody<EnqueueRequest>,
) -> Response {
    let priority = enqueue_req.priority.unwrap_or(DEFAULT_PRIORITY);

    match state
        .store
        .enqueue(&enqueue_req.queue, priority, enqueue_req.payload)
        .await
    {
        Ok(job) => respond(
            fmt,
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
                fmt,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Handle `POST /jobs/{id}/success` — mark a job as completed.
async fn mark_completed(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.mark_completed(&id).await {
        Ok(true) => no_content(),
        Ok(false) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found in working set".into(),
            },
        ),
        Err(e) => {
            tracing::error!(%e, "mark_completed failed");
            respond(
                fmt,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Handle `GET /jobs/{id}` — look up a job by ID.
async fn get_job(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.get_job(&id).await {
        Ok(Some(job)) => respond(fmt, StatusCode::OK, &Job::from(job)),
        Ok(None) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found".into(),
            },
        ),
        Err(e) => {
            tracing::error!(%e, "get_job failed");
            respond(
                fmt,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Handle `GET /jobs` — list jobs with cursor-based pagination.
async fn list_jobs(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    params: Result<Query<ListJobsParams>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            return respond(
                Format::Json,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid query parameters: {e}"),
                },
            );
        }
    };

    let limit = params.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if limit < 1 || limit > MAX_PAGE_LIMIT {
        return respond(
            fmt,
            StatusCode::BAD_REQUEST,
            &ErrorResponse {
                error: format!("limit must be between 1 and {MAX_PAGE_LIMIT}"),
            },
        );
    }

    let opts = ListJobsOptions {
        from: params.from.clone(),
        direction: params.order.into(),
        limit: limit as usize,
    };

    match state.store.list_jobs(opts).await {
        Ok(page) => {
            let self_url = build_page_url(params.from.as_deref(), params.order, limit);
            let next = page
                .next
                .map(|o| build_page_url(o.from.as_deref(), o.direction.into(), limit));
            let prev = page
                .prev
                .map(|o| build_page_url(o.from.as_deref(), o.direction.into(), limit));

            let jobs = page.jobs.into_iter().map(Job::from).collect();

            respond(
                fmt,
                StatusCode::OK,
                &ListJobsResponse {
                    jobs,
                    pages: ListJobsPages {
                        self_url,
                        next,
                        prev,
                    },
                },
            )
        }
        Err(e) => {
            tracing::error!(%e, "list_jobs failed");
            respond(
                fmt,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            )
        }
    }
}

/// Build a pagination URL for the jobs listing endpoint.
fn build_page_url(from: Option<&str>, order: Order, limit: u16) -> String {
    let mut url = String::from("/jobs?");
    if let Some(cursor) = from {
        url.push_str(&format!("from={cursor}&"));
    }
    let order_str = match order {
        Order::Asc => "asc",
        Order::Desc => "desc",
    };
    url.push_str(&format!("order={order_str}&limit={limit}"));
    url
}

/// Default per-stream prefetch limit.
///
/// With a prefetch of 1, the server sends one job and waits for the client to
/// acknowledge it before sending the next. Clients that can handle concurrent
/// work should set `?prefetch=N` on the stream URL. 0 means unlimited.
const DEFAULT_PREFETCH: usize = 1;

/// Query parameters for `GET /jobs/stream`.
#[derive(Deserialize)]
struct StreamParams {
    /// Maximum number of unacknowledged jobs to send before waiting.
    /// 0 means unlimited. Defaults to 1.
    #[serde(default = "default_prefetch")]
    prefetch: usize,
}

fn default_prefetch() -> usize {
    DEFAULT_PREFETCH
}

/// Handle `GET /jobs/stream` — stream jobs to workers.
async fn stream_jobs(
    StreamAccept(fmt): StreamAccept,
    State(state): State<Arc<AppState>>,
    Query(params): Query<StreamParams>,
) -> Response {
    let prefetch = params.prefetch;
    // Channel we'll use to emit events to the client. We reserve a slot
    // before dequeuing a job, so we never take work we can't deliver.
    let (tx, rx) = mpsc::channel::<StreamMessage>(1);

    // Subscription to the store events broadcast channel which serves two
    // purposes:
    //
    // 1. Waking the run loop while it is waiting on new jobs.
    // 2. Receiving notifications about completed jobs so they can be removed
    //    from the in_flight set.
    let mut event_rx = state.store.subscribe();
    let mut shutdown = state.shutdown.clone();

    tokio::spawn(async move {
        // Maintain a set of in-flight job IDs. We need this because once we
        // remove a job from the queue and return it to the client, the job
        // will forever stay in the working queue until the client acknowledges
        // or fails the job. If the client disconnects without doing this, we
        // need to clean up any in-flight jobs by moving them back to the
        // queue.
        //
        // In the extremely rare case of a crash or shutdown before the client
        // finishes processing the job, recovery on server restart will move
        // the job from the working set back into the queue.
        let mut in_flight = HashSet::<String>::new();

        // Pin the heartbeat timer outside the loop so it isn't reset
        // when broadcast events wake the select. It is only reset
        // after a heartbeat is actually sent.
        let heartbeat_sleep = tokio::time::sleep(state.heartbeat_interval);
        tokio::pin!(heartbeat_sleep);

        loop {
            // Drain any pending events.
            while let Ok(event) = event_rx.try_recv() {
                if let StoreEvent::JobCompleted(id) = event {
                    if in_flight.remove(&id) {
                        let _ = state.global_in_flight.fetch_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |v| Some(v.saturating_sub(1)),
                        );
                    }
                }
            }

            // Only attempt to take a job if the per-stream prefetch
            // limit AND the global working limit haven't been reached.
            let can_take = (prefetch == 0 || in_flight.len() < prefetch)
                && (state.global_working_limit == 0
                    || state.global_in_flight.load(Ordering::Relaxed) < state.global_working_limit);

            if can_take {
                // Wait until the client can accept a message before
                // dequeuing. This ensures we never take a job we can't
                // deliver.
                let permit = tokio::select! {
                    result = tx.reserve() => match result {
                        Ok(permit) => permit,
                        Err(_) => break, // client disconnected
                    },
                    _ = shutdown.changed() => break, // server shutting down
                };

                match state.store.take_next_job().await {
                    Ok(Some(job)) => {
                        in_flight.insert(job.id.clone());
                        state.global_in_flight.fetch_add(1, Ordering::Relaxed);
                        permit.send(StreamMessage::Job(Arc::new(Job::from(job))));
                        // Reset the heartbeat timer since we just sent
                        // data — the client knows we're alive.
                        heartbeat_sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + state.heartbeat_interval);
                        continue;
                    }
                    Ok(None) => drop(permit),
                    Err(e) => {
                        tracing::error!(%e, "take_next_job failed in stream");
                        break;
                    }
                }
            }

            // No job available or at the working limit. Wait for a
            // store event or heartbeat timeout. Any enqueue that
            // happened between take_next_job() returning None and
            // entering recv() is already buffered in the broadcast
            // channel.
            tokio::select! {
                _ = &mut heartbeat_sleep => {
                    if tx.send(StreamMessage::Heartbeat).await.is_err() {
                        break;
                    }
                    // Reset for the next heartbeat.
                    heartbeat_sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + state.heartbeat_interval);
                }
                event = event_rx.recv() => {
                    if let Ok(StoreEvent::JobCompleted(id)) = event {
                        if in_flight.remove(&id) {
                            let _ = state.global_in_flight.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |v| Some(v.saturating_sub(1)),
                            );
                        }
                    }
                }
                _ = shutdown.changed() => break, // server shutting down
            }
        }

        // The client disconnected. If there were any in-flight jobs re-queue
        // them so other clients can pick them up.
        for id in &in_flight {
            if let Err(e) = state.store.requeue(id).await {
                tracing::error!(job_id = %id, %e, "requeue failed");
            }
        }

        // Bulk decrement the global in-flight count.
        let _ = state
            .global_in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(in_flight.len() as u64))
            });
    });

    // Start streaming using any of the valid formats.
    match fmt {
        StreamFormat::Sse => build_sse_response(rx),
        StreamFormat::NdJson => build_ndjson_response(rx),
        StreamFormat::MsgPackStream => build_msgpack_stream_response(rx),
    }
}

/// Fallback handler for unmatched routes.
async fn not_found(AcceptFormat(fmt): AcceptFormat) -> Response {
    respond(
        fmt,
        StatusCode::NOT_FOUND,
        &ErrorResponse {
            error: "not found".into(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    /// Create a shared state and router for tests that need direct store access.
    fn test_state_and_app() -> (Arc<AppState>, Router) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        // Leak the TempDir so it isn't cleaned up while the store is open.
        std::mem::forget(dir);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        // Leak the sender so the shutdown signal is never triggered.
        std::mem::forget(shutdown_tx);
        let state = Arc::new(AppState {
            store,
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_SECONDS),
            global_working_limit: 0,
            global_in_flight: AtomicU64::new(0),
            shutdown: shutdown_rx,
        });
        let router = app(state.clone());
        (state, router)
    }

    /// Create a test router with a fresh store.
    fn test_app() -> Router {
        test_state_and_app().1
    }

    fn empty_request(method: &str, uri: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    fn request_with_accept(method: &str, uri: &str, accept: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("accept", accept)
            .body(Body::empty())
            .unwrap()
    }

    fn json_request(method: &str, uri: &str, body: &serde_json::Value) -> Request {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap()
    }

    // Convert the response into a String for testing purposes.
    async fn response_body(res: Response) -> String {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn health_returns_200() {
        let req = empty_request("GET", "/health");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn health_returns_json_content_type() {
        let req = empty_request("GET", "/health");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let req = empty_request("GET", "/nope");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "not found");
    }

    #[tokio::test]
    async fn accept_json_returns_200() {
        let req = request_with_accept("GET", "/health", "application/json");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accept_wildcard_returns_200() {
        let req = request_with_accept("GET", "/health", "*/*");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accept_xml_returns_406() {
        let req = request_with_accept("GET", "/health", "text/xml");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "not acceptable");
        let supported = body["supported"].as_array().unwrap();
        assert!(supported.contains(&"application/json".into()));
        assert!(supported.contains(&"application/msgpack".into()));
    }

    #[tokio::test]
    async fn accept_msgpack_returns_msgpack() {
        let req = request_with_accept("GET", "/health", "application/msgpack");
        let res = test_app().oneshot(req).await.unwrap();

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
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn enqueue_returns_job_id() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["id"].is_string());
        assert!(!body["id"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn enqueue_defaults_priority() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["priority"], DEFAULT_PRIORITY);
    }

    #[tokio::test]
    async fn enqueue_accepts_custom_queue_and_priority() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "emails", "priority": 5, "payload": "test"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["queue"], "emails");
        assert_eq!(body["priority"], 5);
    }

    #[tokio::test]
    async fn enqueue_rejects_missing_queue() {
        let req = json_request("POST", "/jobs", &serde_json::json!({"payload": "test"}));
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn enqueue_rejects_invalid_json() {
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .body(Body::from("not json"))
            .unwrap();
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn enqueue_rejects_unsupported_content_type() {
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/xml")
            .body(Body::from("<job/>"))
            .unwrap();
        let res = test_app().oneshot(req).await.unwrap();

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
            .body(Body::from(body))
            .unwrap();
        let res = test_app().oneshot(req).await.unwrap();

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
            .body(Body::from(body))
            .unwrap();
        let res = test_app().oneshot(req).await.unwrap();

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

        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/msgpack")
            .body(Body::from(buf))
            .unwrap();
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_job_returns_pending_job() {
        let (_, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "emails", "priority": 5, "payload": {"to": "a@b.c"}}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let created: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = created["id"].as_str().unwrap();

        let req = empty_request("GET", &format!("/jobs/{job_id}"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["id"], job_id);
        assert_eq!(body["queue"], "emails");
        assert_eq!(body["priority"], 5);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["payload"]["to"], "a@b.c");
    }

    #[tokio::test]
    async fn get_job_returns_working_job() {
        let (state, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let created: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = created["id"].as_str().unwrap();

        // Take the job so it moves to working.
        state.store.take_next_job().await.unwrap();

        let req = empty_request("GET", &format!("/jobs/{job_id}"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["id"], job_id);
        assert_eq!(body["status"], "working");
    }

    #[tokio::test]
    async fn get_job_returns_404_for_completed_job() {
        let (state, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let created: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = created["id"].as_str().unwrap();

        state.store.take_next_job().await.unwrap();
        state.store.mark_completed(job_id).await.unwrap();

        let req = empty_request("GET", &format!("/jobs/{job_id}"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_job_returns_404_for_unknown_id() {
        let req = empty_request("GET", "/jobs/nonexistent");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    // --- GET /jobs tests ---

    #[tokio::test]
    async fn list_jobs_returns_empty_page() {
        let req = empty_request("GET", "/jobs");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"], serde_json::json!([]));
        assert!(body["pages"]["next"].is_null());
        assert!(body["pages"]["prev"].is_null());
    }

    #[tokio::test]
    async fn list_jobs_returns_jobs_with_status() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["status"], "ready");
        assert_eq!(jobs[0]["payload"], "a");
    }

    #[tokio::test]
    async fn list_jobs_returns_json_content_type() {
        let req = empty_request("GET", "/jobs");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn list_jobs_returns_msgpack_when_requested() {
        let (state, app) = test_state_and_app();
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();

        let req = request_with_accept("GET", "/jobs", "application/msgpack");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/msgpack"
        );
    }

    #[tokio::test]
    async fn list_jobs_paginates_with_limit() {
        let (state, app) = test_state_and_app();
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("c"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?limit=2");
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(body["pages"]["next"].is_string());
    }

    #[tokio::test]
    async fn list_jobs_next_page_returns_remaining() {
        let (state, app) = test_state_and_app();
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("c"))
            .await
            .unwrap();

        // First page.
        let req = empty_request("GET", "/jobs?limit=2");
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let next_url = body["pages"]["next"].as_str().unwrap();

        // Second page via next URL.
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["payload"], "c");
        assert!(body["pages"]["next"].is_null());
        assert!(body["pages"]["prev"].is_string());
    }

    #[tokio::test]
    async fn list_jobs_desc_order() {
        let (state, app) = test_state_and_app();
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?order=desc");
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["payload"], "b");
        assert_eq!(jobs[1]["payload"], "a");
    }

    #[tokio::test]
    async fn list_jobs_from_cursor() {
        let (state, app) = test_state_and_app();
        let a = state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();

        let req = empty_request("GET", &format!("/jobs?from={}", a.id));
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["payload"], "b");
    }

    #[tokio::test]
    async fn list_jobs_self_url_present() {
        let req = empty_request("GET", "/jobs");
        let res = test_app().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["pages"]["self"].is_string());
    }

    #[tokio::test]
    async fn list_jobs_rejects_invalid_order() {
        let req = empty_request("GET", "/jobs?order=sideways");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("invalid query parameters")
        );
    }

    #[tokio::test]
    async fn list_jobs_rejects_non_numeric_limit() {
        let req = empty_request("GET", "/jobs?limit=abc");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_jobs_rejects_limit_zero() {
        let req = empty_request("GET", "/jobs?limit=0");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["error"].as_str().unwrap().contains("limit"));
    }

    #[tokio::test]
    async fn list_jobs_rejects_limit_over_max() {
        let req = empty_request("GET", "/jobs?limit=201");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["error"].as_str().unwrap().contains("limit"));
    }

    #[tokio::test]
    async fn list_jobs_accepts_max_limit() {
        let req = empty_request("GET", "/jobs?limit=200");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    // --- POST /jobs/{id}/success tests ---

    #[tokio::test]
    async fn success_returns_204() {
        let (state, app) = test_state_and_app();

        // Enqueue and take a job so it's in the working set.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        state.store.take_next_job().await.unwrap();

        // Mark it as completed.
        let req = empty_request("POST", &format!("/jobs/{job_id}/success"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn success_returns_404_for_unknown_job() {
        let req = empty_request("POST", "/jobs/nonexistent/success");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn success_returns_404_for_pending_job() {
        let (_, app) = test_state_and_app();

        // Enqueue but don't take — job is pending, not working.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        let req = empty_request("POST", &format!("/jobs/{job_id}/success"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    // --- Streaming tests ---

    async fn next_body_bytes(body: &mut axum::body::Body) -> Bytes {
        use http_body_util::BodyExt as _;
        body.frame().await.unwrap().unwrap().into_data().unwrap()
    }

    #[tokio::test]
    async fn stream_delivers_pending_jobs_as_ndjson() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue("q1", 0, serde_json::json!("first"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q2", 5, serde_json::json!("second"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/stream?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // First job (priority 0, enqueued first).
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "q1");
        assert_eq!(job["payload"], "first");
        assert_eq!(job["status"], "working");
        assert!(job["id"].is_string());

        // Second job (priority 5).
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "q2");
        assert_eq!(job["payload"], "second");
        assert_eq!(job["status"], "working");
    }

    #[tokio::test]
    async fn stream_waits_and_delivers_new_jobs() {
        let (state, app) = test_state_and_app();

        let req = empty_request("GET", "/jobs/stream");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Enqueue after stream is open.
        let enqueued = state
            .store
            .enqueue("default", 0, serde_json::json!("delayed"))
            .await
            .unwrap();

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["id"], enqueued.id);
        assert_eq!(job["payload"], "delayed");
        assert_eq!(job["status"], "working");
    }

    #[tokio::test]
    async fn stream_returns_sse_when_requested() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue("default", 0, serde_json::json!("sse-test"))
            .await
            .unwrap();

        let req = request_with_accept("GET", "/jobs/stream", "text/event-stream");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "text/event-stream"
        );

        let mut body = res.into_body();
        let bytes = next_body_bytes(&mut body).await;
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(
            text.contains("data:"),
            "SSE frame should contain 'data:' prefix"
        );
        // Extract the JSON from the data: line.
        let data_line = text.lines().find(|l| l.starts_with("data:")).unwrap();
        let json_str = data_line.strip_prefix("data:").unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(job["payload"], "sse-test");
        assert_eq!(job["status"], "working");
    }

    #[tokio::test]
    async fn stream_returns_msgpack_when_requested() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue("default", 0, serde_json::json!("mp-test"))
            .await
            .unwrap();

        let req = request_with_accept(
            "GET",
            "/jobs/stream",
            "application/vnd.zanxio.msgpack-stream",
        );
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/vnd.zanxio.msgpack-stream"
        );

        let mut body = res.into_body();
        let bytes = next_body_bytes(&mut body).await;

        // First 4 bytes are big-endian u32 length prefix.
        assert!(bytes.len() > 4);
        let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        assert_eq!(bytes.len(), 4 + len);

        let job: serde_json::Value = rmp_serde::from_slice(&bytes[4..]).unwrap();
        assert_eq!(job["payload"], "mp-test");
        assert_eq!(job["status"], "working");
    }

    #[tokio::test]
    async fn stream_returns_406_for_unsupported_accept() {
        let req = request_with_accept("GET", "/jobs/stream", "text/xml");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "not acceptable");
        let supported = body["supported"].as_array().unwrap();
        assert!(supported.contains(&"text/event-stream".into()));
        assert!(supported.contains(&"application/x-ndjson".into()));
        assert!(supported.contains(&"application/vnd.zanxio.msgpack-stream".into()));
    }

    #[tokio::test]
    async fn stream_sets_ndjson_content_type() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue("default", 0, serde_json::json!("ct-test"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/stream");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/x-ndjson"
        );
    }

    #[tokio::test]
    async fn stream_requeues_on_disconnect() {
        let (state, app) = test_state_and_app();

        let enqueued = state
            .store
            .enqueue("default", 0, serde_json::json!("requeue-me"))
            .await
            .unwrap();

        // Use prefetch=2 so the task attempts to reserve a second slot
        // after sending the first job. When the body is dropped, the
        // reserve() call detects the closed channel and triggers requeue.
        let req = empty_request("GET", "/jobs/stream?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read the job from the stream.
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["id"], enqueued.id);

        // Drop the body to simulate client disconnect.
        drop(body);

        // Give the spawned task time to detect disconnect and requeue.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drain jobs from queue — our original job should be among them.
        let mut ids = Vec::new();
        while let Some(job) = state.store.take_next_job().await.unwrap() {
            ids.push(job.id);
        }
        assert!(
            ids.contains(&enqueued.id),
            "original job should have been requeued"
        );
    }

    #[tokio::test]
    async fn stream_does_not_requeue_acked_jobs_on_disconnect() {
        let (state, app) = test_state_and_app();

        let job_a = state
            .store
            .enqueue("default", 0, serde_json::json!("acked"))
            .await
            .unwrap();
        let job_b = state
            .store
            .enqueue("default", 0, serde_json::json!("unacked"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/stream?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read both jobs from the stream.
        let _ = next_body_bytes(&mut body).await;
        let _ = next_body_bytes(&mut body).await;

        // Ack the first job while the stream is still open.
        assert!(state.store.mark_completed(&job_a.id).await.unwrap());

        // Drop the body to simulate client disconnect.
        drop(body);

        // Give the spawned task time to detect disconnect and requeue.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Only the unacked job should be requeued, not the acked one.
        let mut ids = Vec::new();
        while let Some(job) = state.store.take_next_job().await.unwrap() {
            ids.push(job.id);
        }
        assert!(
            ids.contains(&job_b.id),
            "unacked job should have been requeued"
        );
        assert!(
            !ids.contains(&job_a.id),
            "acked job should NOT have been requeued"
        );
    }

    #[tokio::test]
    async fn stream_respects_working_limit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        std::mem::forget(dir);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        std::mem::forget(shutdown_tx);
        let state = Arc::new(AppState {
            store,
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_SECONDS),
            global_working_limit: 2,
            global_in_flight: AtomicU64::new(0),
            shutdown: shutdown_rx,
        });
        let router = app(state.clone());

        // Enqueue 3 jobs.
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("c"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/stream?prefetch=3");
        let res = router.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read 2 jobs — should succeed since working_limit is 2.
        let bytes = next_body_bytes(&mut body).await;
        let job_a: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        let _ = next_body_bytes(&mut body).await;

        assert_eq!(state.global_in_flight.load(Ordering::Relaxed), 2);

        // Ack the first job to free a slot.
        state
            .store
            .mark_completed(job_a["id"].as_str().unwrap())
            .await
            .unwrap();

        // Third job should now be delivered.
        let bytes = next_body_bytes(&mut body).await;
        let job_c: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_c["payload"], "c");
    }

    #[tokio::test]
    async fn stream_defaults_to_prefetch_one() {
        let (state, app) = test_state_and_app();

        // Enqueue 2 jobs.
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();

        // Stream with default prefetch (1).
        let req = empty_request("GET", "/jobs/stream");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // First job should arrive.
        let bytes = next_body_bytes(&mut body).await;
        let job_a: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_a["payload"], "a");

        // Second job should NOT arrive until the first is acked.
        // Ack the first job.
        state
            .store
            .mark_completed(job_a["id"].as_str().unwrap())
            .await
            .unwrap();

        // Now the second job should come through.
        let bytes = next_body_bytes(&mut body).await;
        let job_b: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_b["payload"], "b");
    }

    #[tokio::test]
    async fn stream_prefetch_allows_multiple_in_flight() {
        let (state, app) = test_state_and_app();

        // Enqueue 3 jobs.
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("c"))
            .await
            .unwrap();

        // Stream with prefetch=2.
        let req = empty_request("GET", "/jobs/stream?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Should get 2 jobs without any acks.
        let bytes = next_body_bytes(&mut body).await;
        let job_a: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_a["payload"], "a");

        let bytes = next_body_bytes(&mut body).await;
        let job_b: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_b["payload"], "b");

        // Third job should be held back. Ack one to free a slot.
        state
            .store
            .mark_completed(job_a["id"].as_str().unwrap())
            .await
            .unwrap();

        let bytes = next_body_bytes(&mut body).await;
        let job_c: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_c["payload"], "c");
    }

    #[tokio::test]
    async fn stream_prefetch_zero_means_unlimited() {
        let (state, app) = test_state_and_app();

        // Enqueue 3 jobs.
        state
            .store
            .enqueue("q", 0, serde_json::json!("a"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("b"))
            .await
            .unwrap();
        state
            .store
            .enqueue("q", 0, serde_json::json!("c"))
            .await
            .unwrap();

        // Stream with prefetch=0 (unlimited).
        let req = empty_request("GET", "/jobs/stream?prefetch=0");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // All 3 jobs should arrive without any acks.
        let bytes = next_body_bytes(&mut body).await;
        let job: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job["payload"], "a");

        let bytes = next_body_bytes(&mut body).await;
        let job: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job["payload"], "b");

        let bytes = next_body_bytes(&mut body).await;
        let job: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job["payload"], "c");
    }

    #[tokio::test]
    async fn stream_resets_global_counter_on_disconnect() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue("q", 0, serde_json::json!("x"))
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/stream?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        let _ = next_body_bytes(&mut body).await;
        assert_eq!(state.global_in_flight.load(Ordering::Relaxed), 1);

        // Disconnect.
        drop(body);

        // Give the spawned task time to detect disconnect.
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(state.global_in_flight.load(Ordering::Relaxed), 0);
    }
}
