// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Primary API request handlers.
//!
//! Defines the router, request handlers, content negotiation, and take
//! framing for the main job queue API.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::store::{self, ListErrorsOptions, ListJobsOptions, StoreError, StoreEvent};
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
use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::api::middleware;
use crate::filter::PayloadFilter;
use crate::state::AppState;

use super::types::{
    BulkEnqueueRequest, BulkEnqueueResponse, BulkSuccessNotFoundResponse, BulkSuccessRequest,
    CommaSet, CountJobsParams, CountJobsResponse, DeleteJobsParams, EnqueueRequest, ErrorRecord,
    ErrorResponse, FailureRequest, HealthResponse, Job, JobStatus, ListErrorsPages,
    ListErrorsParams, ListErrorsResponse, ListJobsPages, ListJobsParams, ListJobsResponse,
    ListQueuesResponse, Order, PatchJobBody, PatchJobsParams, TakeParams,
    UnsupportedFormatResponse, VersionResponse, parse_unique_while,
};

/// Default priority for jobs that don't specify one.
///
/// Sits in the middle of the u16 range so there is equal room for higher
/// and lower priority work.
const DEFAULT_PRIORITY: u16 = 32_768;

/// Default page size for job listings.
const DEFAULT_PAGE_LIMIT: u16 = 50;

/// Maximum page size for job listings.
const MAX_PAGE_LIMIT: u16 = 2000;

// --- Take types ---

/// Supported serialization formats for the take endpoint, negotiated via Accept header.
#[derive(Clone, Copy)]
enum TakeFormat {
    Sse,
    NdJson,
    MsgPackStream,
}

/// All content types supported by the take endpoint.
const SUPPORTED_TAKE_TYPES: &[&str] = &[
    "text/event-stream",
    "application/x-ndjson",
    "application/vnd.zizq.msgpack-stream",
];

/// Extractor that reads the `Accept` header and resolves a `TakeFormat` for the take endpoint.
struct TakeAccept(TakeFormat);

impl<S> FromRequestParts<S> for TakeAccept
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        match parts.headers.get(ACCEPT) {
            None => Ok(TakeAccept(TakeFormat::NdJson)),
            Some(value) => {
                let value = value.to_str().unwrap_or("");
                if value.contains("text/event-stream") {
                    Ok(TakeAccept(TakeFormat::Sse))
                } else if value.contains("application/vnd.zizq.msgpack-stream")
                    || value.contains("application/msgpack")
                {
                    Ok(TakeAccept(TakeFormat::MsgPackStream))
                } else if value.contains("application/x-ndjson")
                    || value.contains("application/json")
                    || value.contains("*/*")
                {
                    Ok(TakeAccept(TakeFormat::NdJson))
                } else {
                    Err(respond(
                        Format::Json,
                        StatusCode::NOT_ACCEPTABLE,
                        &UnsupportedFormatResponse {
                            error: "not acceptable".into(),
                            supported: SUPPORTED_TAKE_TYPES.to_vec(),
                        },
                    ))
                }
            }
        }
    }
}

/// Messages sent over the take channel.
enum TakeMessage {
    Job(Arc<Job>), // http::Job
    Heartbeat,
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

// --- Take framing ---

/// Return a take frame in newline delimited JSON format.
///
/// Heartbeat messages are empty (newline only).
fn frame_ndjson(msg: &TakeMessage) -> Bytes {
    match msg {
        TakeMessage::Job(job) => {
            let mut bytes = serde_json::to_vec(&**job).unwrap();
            bytes.push(b'\n');
            Bytes::from(bytes)
        }
        TakeMessage::Heartbeat => Bytes::from_static(b"\n"),
    }
}

/// Return a take frame in length-prefixed MessagePack format.
///
/// Heartbeat messages are empty (zero prefix, no data).
fn frame_msgpack_stream(msg: &TakeMessage) -> Bytes {
    match msg {
        TakeMessage::Job(job) => {
            let payload = rmp_serde::to_vec_named(&**job).unwrap();
            let len = (payload.len() as u32).to_be_bytes();
            let mut bytes = Vec::with_capacity(4 + payload.len());
            bytes.extend_from_slice(&len);
            bytes.extend_from_slice(&payload);
            Bytes::from(bytes)
        }
        TakeMessage::Heartbeat => Bytes::from_static(&[0, 0, 0, 0]),
    }
}

/// Return a SSE event containing the given take message.
///
/// Heartbeats are just comments.
fn frame_sse(msg: &TakeMessage) -> Result<Event, Infallible> {
    match msg {
        TakeMessage::Job(job) => Ok(Event::default().json_data(&**job).unwrap()),
        TakeMessage::Heartbeat => Ok(Event::default().comment("heartbeat")),
    }
}

// --- Take response builders ---

/// Create a SSE response which emits a message for each event in the
/// receiver channel.
fn build_sse_response(rx: mpsc::Receiver<TakeMessage>) -> Response {
    let stream = ReceiverStream::new(rx).map(|msg| frame_sse(&msg));
    Sse::new(stream).into_response()
}

/// Create a newline delimited JSON response that emits a JSON message for
/// every event on the receiver channel.
fn build_ndjson_response(rx: mpsc::Receiver<TakeMessage>) -> Response {
    let stream = ReceiverStream::new(rx).map(|msg| Ok::<_, Infallible>(frame_ndjson(&msg)));
    let mut res = Response::new(Body::from_stream(stream));
    res.headers_mut()
        .insert(CONTENT_TYPE, "application/x-ndjson".parse().unwrap());
    res
}

/// Create a length-prefixed MessagePack response that emits a MessagePack
/// message for every event in the receiver channel.
fn build_msgpack_stream_response(rx: mpsc::Receiver<TakeMessage>) -> Response {
    let stream = ReceiverStream::new(rx).map(|msg| Ok::<_, Infallible>(frame_msgpack_stream(&msg)));
    let mut res = Response::new(Body::from_stream(stream));
    res.headers_mut().insert(
        CONTENT_TYPE,
        "application/vnd.zizq.msgpack-stream".parse().unwrap(),
    );
    res
}

// --- Router ---

/// Build the axum router with all routes and shared state.
pub fn app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/queues", get(list_queues))
        .route(
            "/jobs",
            get(list_jobs)
                .post(enqueue)
                .patch(bulk_patch_jobs)
                .delete(bulk_delete_jobs),
        )
        .route("/jobs/bulk", post(bulk_enqueue))
        .route("/jobs/success", post(bulk_mark_completed))
        .route("/jobs/count", get(count_jobs))
        .route("/jobs/take", get(take_jobs))
        .route(
            "/jobs/{id}",
            get(get_job).patch(patch_job).delete(delete_job),
        )
        .route("/jobs/{id}/success", post(mark_completed))
        .route("/jobs/{id}/failure", post(report_failure))
        .route("/jobs/{id}/errors", get(list_errors))
        .route("/jobs/{id}/errors/{attempt}", get(get_error))
        .fallback(not_found)
        .layer(axum::middleware::from_fn(
            middleware::primary_request_logging,
        ))
        .with_state(state)
}

/// Generate a random u32 for use as a fallback worker ID.
fn rand_id() -> u32 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish() as u32
}

// --- Handlers ---

/// Handle `GET /health`.
async fn health(AcceptFormat(fmt): AcceptFormat) -> Response {
    respond(fmt, StatusCode::OK, &HealthResponse { status: "ok" })
}

/// Handle `GET /version`.
async fn version(AcceptFormat(fmt): AcceptFormat) -> Response {
    respond(
        fmt,
        StatusCode::OK,
        &VersionResponse {
            version: env!("CARGO_PKG_VERSION"),
        },
    )
}

/// Handle `GET /queues` — list all distinct queue names.
async fn list_queues(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
) -> Response {
    match state.store.list_queues().await {
        Ok(queues) => respond(fmt, StatusCode::OK, &ListQueuesResponse { queues }),
        Err(e) => {
            tracing::error!(%e, "list_queues failed");
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

/// Validate a string field used as an index key (queue name, job type, etc.).
///
/// Rejects empty strings, commas (used as a set delimiter in query
/// parameters), null bytes (used as a separator in index keys), and
/// glob characters (reserved for future pattern matching support).
fn validate_name(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.contains(',') {
        return Err(format!("{field} must not contain ','"));
    }
    if value.contains('\0') {
        return Err(format!("{field} must not contain null bytes"));
    }
    if let Some(c) = value
        .chars()
        .find(|c| matches!(c, '*' | '?' | '[' | ']' | '{' | '}' | '\\'))
    {
        return Err(format!("{field} must not contain '{c}' (reserved)"));
    }
    Ok(())
}

/// Check that a string looks like a valid scru128 job ID (25 lowercase
/// alphanumeric characters).
fn is_valid_job_id(id: &str) -> bool {
    id.len() == 25
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Handle `POST /jobs` — enqueue a new job.
async fn enqueue(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    NegotiatedBody(enqueue_req): NegotiatedBody<EnqueueRequest>,
) -> Response {
    if let Err(msg) = validate_name("type", &enqueue_req.job_type) {
        return respond(fmt, StatusCode::BAD_REQUEST, &ErrorResponse { error: msg });
    }
    if let Err(msg) = validate_name("queue", &enqueue_req.queue) {
        return respond(fmt, StatusCode::BAD_REQUEST, &ErrorResponse { error: msg });
    }

    // Validate uniqueness fields.
    if enqueue_req.unique_while.is_some() && enqueue_req.unique_key.is_none() {
        return respond(
            fmt,
            StatusCode::BAD_REQUEST,
            &ErrorResponse {
                error: "unique_while requires unique_key".into(),
            },
        );
    }

    // License check for unique jobs.
    if enqueue_req.unique_key.is_some() {
        let now_ms = (state.clock)();
        if let Err(e) = state
            .license
            .read()
            .unwrap()
            .require(now_ms, crate::license::Feature::UniqueJobs)
        {
            return respond(fmt, StatusCode::FORBIDDEN, &ErrorResponse { error: e });
        }
    }

    let priority = enqueue_req.priority.unwrap_or(DEFAULT_PRIORITY);

    let mut opts =
        store::EnqueueOptions::new(enqueue_req.job_type, enqueue_req.queue, enqueue_req.payload)
            .priority(priority);

    if let Some(ready_at) = enqueue_req.ready_at {
        opts = opts.ready_at(ready_at);
    }
    if let Some(retry_limit) = enqueue_req.retry_limit {
        opts = opts.retry_limit(retry_limit);
    }
    if let Some(backoff) = enqueue_req.backoff {
        opts = opts.backoff(backoff.into());
    }
    if let Some(retention) = enqueue_req.retention {
        opts = opts.retention(retention.into());
    }
    if let Some(unique_key) = enqueue_req.unique_key {
        opts = opts.unique_key(unique_key);
    }
    if let Some(ref unique_while) = enqueue_req.unique_while {
        match parse_unique_while(unique_while) {
            Ok(scope) => opts = opts.unique_while(scope),
            Err(msg) => {
                return respond(fmt, StatusCode::BAD_REQUEST, &ErrorResponse { error: msg });
            }
        }
    }

    let now = (state.clock)();
    match state
        .store
        .enqueue(now, opts)
        .await
        .and_then(Job::try_from_result)
    {
        Ok(mut job) => {
            let status_code = if job.duplicate == Some(true) {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            tracing::debug!(
                job_id = %job.id,
                job_type = %job.job_type,
                queue = %job.queue,
                priority = job.priority,
                status = %job.status,
                ready_at = job.ready_at,
                duplicate = job.duplicate.unwrap_or(false),
                "job enqueued"
            );
            // Drop the payload from the response — the enqueue confirmation
            // returns metadata only.
            job.payload = None;
            respond(fmt, status_code, &job)
        }
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

/// Handle `POST /jobs/bulk` — enqueue a batch of jobs atomically.
async fn bulk_enqueue(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    NegotiatedBody(req): NegotiatedBody<BulkEnqueueRequest>,
) -> Response {
    // Validate all jobs up front.
    let mut has_unique = false;
    for (i, job) in req.jobs.iter().enumerate() {
        if let Err(msg) = validate_name("type", &job.job_type) {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("jobs[{i}]: {msg}"),
                },
            );
        }
        if let Err(msg) = validate_name("queue", &job.queue) {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("jobs[{i}]: {msg}"),
                },
            );
        }
        if job.unique_while.is_some() && job.unique_key.is_none() {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("jobs[{i}]: unique_while requires unique_key"),
                },
            );
        }
        if let Some(ref uw) = job.unique_while {
            if let Err(msg) = parse_unique_while(uw) {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("jobs[{i}]: {msg}"),
                    },
                );
            }
        }
        if job.unique_key.is_some() {
            has_unique = true;
        }
    }

    // License check for unique jobs (once for the whole batch).
    if has_unique {
        let now_ms = (state.clock)();
        if let Err(e) = state
            .license
            .read()
            .unwrap()
            .require(now_ms, crate::license::Feature::UniqueJobs)
        {
            return respond(fmt, StatusCode::FORBIDDEN, &ErrorResponse { error: e });
        }
    }

    // Convert to store options.
    let opts: Vec<store::EnqueueOptions> = req
        .jobs
        .into_iter()
        .map(|enqueue_req| {
            let priority = enqueue_req.priority.unwrap_or(DEFAULT_PRIORITY);
            let mut opts = store::EnqueueOptions::new(
                enqueue_req.job_type,
                enqueue_req.queue,
                enqueue_req.payload,
            )
            .priority(priority);

            if let Some(ready_at) = enqueue_req.ready_at {
                opts = opts.ready_at(ready_at);
            }
            if let Some(retry_limit) = enqueue_req.retry_limit {
                opts = opts.retry_limit(retry_limit);
            }
            if let Some(backoff) = enqueue_req.backoff {
                opts = opts.backoff(backoff.into());
            }
            if let Some(retention) = enqueue_req.retention {
                opts = opts.retention(retention.into());
            }
            if let Some(unique_key) = enqueue_req.unique_key {
                opts = opts.unique_key(unique_key);
            }
            if let Some(unique_while) = enqueue_req.unique_while {
                // Already validated above.
                opts = opts.unique_while(parse_unique_while(&unique_while).unwrap());
            }
            opts
        })
        .collect();

    let now = (state.clock)();
    match state.store.enqueue_bulk(now, opts).await {
        Ok(store_results) => {
            let all_duplicates =
                !store_results.is_empty() && store_results.iter().all(|r| r.is_duplicate());
            let jobs: Result<Vec<Job>, StoreError> = store_results
                .into_iter()
                .map(|r| {
                    let mut job = Job::try_from_result(r)?;
                    job.payload = None;
                    Ok(job)
                })
                .collect();
            match jobs {
                Ok(jobs) => {
                    let status_code = if all_duplicates {
                        StatusCode::OK
                    } else {
                        StatusCode::CREATED
                    };
                    tracing::debug!(count = jobs.len(), "bulk enqueue succeeded");
                    respond(fmt, status_code, &BulkEnqueueResponse { jobs })
                }
                Err(e) => {
                    tracing::error!(%e, "bulk enqueue job conversion failed");
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
        Err(e) => {
            tracing::error!(%e, "bulk enqueue failed");
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
    let now = (state.clock)();
    match state.store.mark_completed(now, &id).await {
        Ok(true) => {
            tracing::debug!(job_id = %id, "job marked completed");
            no_content()
        }
        Ok(false) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found in in-flight set".into(),
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

/// Handle `POST /jobs/success` — bulk mark jobs as completed.
async fn bulk_mark_completed(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    NegotiatedBody(req): NegotiatedBody<BulkSuccessRequest>,
) -> Response {
    if req.ids.is_empty() {
        return respond(
            fmt,
            StatusCode::BAD_REQUEST,
            &ErrorResponse {
                error: "ids must not be empty".into(),
            },
        );
    }

    let now = (state.clock)();
    match state.store.mark_completed_bulk(now, &req.ids).await {
        Ok(result) => {
            if result.not_found.is_empty() {
                tracing::debug!(count = result.completed.len(), "bulk success completed");
                no_content()
            } else {
                tracing::debug!(
                    completed = result.completed.len(),
                    not_found = result.not_found.len(),
                    "bulk success partially completed"
                );
                respond(
                    fmt,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &BulkSuccessNotFoundResponse {
                        not_found: result.not_found,
                    },
                )
            }
        }
        Err(e) => {
            tracing::error!(%e, "bulk mark_completed failed");
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

/// Handle `POST /jobs/{id}/failure` — report that a job has failed.
///
/// The server decides whether to retry (rescheduling with backoff) or kill
/// the job based on the retry limit. The client can override the decision
/// with `retry_at` (force reschedule) or `kill` (force kill).
///
/// Returns the updated job metadata (without payload) so the client can
/// see the new status, next `ready_at`, and attempt count.
async fn report_failure(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    NegotiatedBody(failure_req): NegotiatedBody<FailureRequest>,
) -> Response {
    let opts = store::FailureOptions {
        message: failure_req.message,
        error_type: failure_req.error_type,
        backtrace: failure_req.backtrace,
        retry_at: failure_req.retry_at,
        kill: failure_req.kill,
    };

    let now = (state.clock)();
    match state
        .store
        .record_failure(now, &id, opts)
        .await
        .and_then(|opt| opt.map(Job::try_from).transpose())
    {
        Ok(Some(job)) => {
            tracing::debug!(
                job_id = %job.id,
                job_type = %job.job_type,
                status = %job.status,
                attempts = job.attempts,
                "job failure recorded"
            );
            respond(fmt, StatusCode::OK, &job)
        }
        Ok(None) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found in in-flight set".into(),
            },
        ),
        Err(e) => {
            tracing::error!(%e, "record_failure failed");
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
    let now = (state.clock)();
    match state.store.get_job(now, &id).await {
        Ok(Some(job)) => match Job::try_from(job) {
            Ok(job) => respond(fmt, StatusCode::OK, &job),
            Err(e) => {
                tracing::error!(%e, "corrupt job data");
                respond(
                    fmt,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ErrorResponse {
                        error: "internal server error".into(),
                    },
                )
            }
        },
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

/// Handle `PATCH /jobs/{id}` — update a job's mutable fields.
///
/// Mutable fields:
/// * `queue`
/// * `priority`
/// * `ready_at` — setting a future time on a Ready job moves it to Scheduled;
///   clearing (null) or setting a past time on a Scheduled job makes it Ready
/// * `retry_limit`
/// * `backoff`
/// * `retention`
///
/// Immutable fields:
/// * `type`
/// * `payload`
///
/// Only fields to be patched are included in the request. If a field is
/// present but set to `null` that field is cleared in the store (for
/// nullable fields). `queue` and `priority` cannot be null.
async fn patch_job(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    NegotiatedBody(body): NegotiatedBody<PatchJobBody>,
) -> Response {
    let now = (state.clock)();

    match &body.queue {
        Some(None) => {
            return respond(
                fmt,
                StatusCode::UNPROCESSABLE_ENTITY,
                &ErrorResponse {
                    error: "queue cannot be null".into(),
                },
            );
        }
        Some(Some(queue)) => {
            if let Err(e) = validate_name("queue", queue) {
                return respond(
                    fmt,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &ErrorResponse { error: e },
                );
            }
        }
        None => {}
    }

    if let Some(None) = body.priority {
        return respond(
            fmt,
            StatusCode::UNPROCESSABLE_ENTITY,
            &ErrorResponse {
                error: "priority cannot be null".into(),
            },
        );
    }

    let mut patch = store::PatchJobOptions::default();

    patch.queue = body.queue.flatten();
    patch.priority = body.priority.flatten();
    if let Some(ready_at) = body.ready_at {
        patch.ready_at = Some(ready_at);
    }
    if let Some(retry_limit) = body.retry_limit {
        patch.retry_limit = Some(retry_limit);
    }
    if let Some(backoff) = body.backoff {
        patch.backoff = Some(backoff.map(Into::into));
    }
    if let Some(retention) = body.retention {
        patch.retention = Some(retention.map(Into::into));
    }

    match state.store.patch_job(now, &id, patch).await {
        Ok(Some(job)) => match Job::try_from(job) {
            Ok(mut job) => {
                job.payload = None;
                respond(fmt, StatusCode::OK, &job)
            }
            Err(e) => {
                tracing::error!(%e, "corrupt job data");
                respond(
                    fmt,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ErrorResponse {
                        error: "internal server error".into(),
                    },
                )
            }
        },
        Ok(None) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found".into(),
            },
        ),
        Err(StoreError::InvalidOperation(msg)) => respond(
            fmt,
            StatusCode::UNPROCESSABLE_ENTITY,
            &ErrorResponse { error: msg },
        ),
        Err(e) => {
            tracing::error!(%e, "patch_job failed");
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

/// Handle `DELETE /jobs/{id}` — delete a job by ID.
async fn delete_job(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.delete_job(&id).await {
        Ok(true) => respond(fmt, StatusCode::NO_CONTENT, &()),
        Ok(false) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "job not found".into(),
            },
        ),
        Err(e) => {
            tracing::error!(%e, "delete_job failed");
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

/// Handle `DELETE /jobs` — bulk delete jobs matching filters.
async fn bulk_delete_jobs(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    params: Result<Query<DeleteJobsParams>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid query parameters: {e}"),
                },
            );
        }
    };

    // Validate IDs.
    for id in params.id.iter() {
        if !is_valid_job_id(id) {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid job ID: {id:?}"),
                },
            );
        }
    }

    // Compile payload filter.
    let filter = if let Some(ref expr) = params.filter {
        match PayloadFilter::compile(expr) {
            Ok(f) => Some(std::sync::Arc::new(f)),
            Err(e) => {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid filter: {e}"),
                    },
                );
            }
        }
    } else {
        None
    };

    let mut opts = store::BulkDeleteOptions::new();
    if !params.id.is_empty() {
        opts.ids = params.id.0;
    }
    if !params.status.is_empty() {
        opts.statuses = params
            .status
            .iter()
            .map(|s| store::JobStatus::from(*s))
            .collect();
    }
    if !params.queue.is_empty() {
        opts.queues = params.queue.0;
    }
    if !params.job_type.is_empty() {
        opts.types = params.job_type.0;
    }
    opts.filter = filter;

    match state.store.delete_jobs(opts).await {
        Ok(count) => {
            tracing::debug!(count, "bulk delete completed");
            respond(
                fmt,
                StatusCode::OK,
                &serde_json::json!({ "deleted": count }),
            )
        }
        Err(e) => {
            tracing::error!(%e, "bulk_delete_jobs failed");
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

/// Handle `PATCH /jobs` — bulk patch jobs matching filters.
///
/// Applies the same patch body to all matching jobs. Terminal jobs
/// (completed/dead) are silently skipped unless explicitly requested
/// via `?status=`, in which case the request is rejected with 422.
async fn bulk_patch_jobs(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    params: Result<Query<PatchJobsParams>, axum::extract::rejection::QueryRejection>,
    NegotiatedBody(body): NegotiatedBody<PatchJobBody>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid query parameters: {e}"),
                },
            );
        }
    };

    // Reject if status filter includes terminal statuses.
    for s in params.status.iter() {
        if matches!(s, JobStatus::Completed | JobStatus::Dead) {
            return respond(
                fmt,
                StatusCode::UNPROCESSABLE_ENTITY,
                &ErrorResponse {
                    error: format!("cannot patch jobs in {s:?} status"),
                },
            );
        }
    }

    // Validate IDs.
    for id in params.id.iter() {
        if !is_valid_job_id(id) {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid job ID: {id:?}"),
                },
            );
        }
    }

    // Validate queue if provided.
    match &body.queue {
        Some(None) => {
            return respond(
                fmt,
                StatusCode::UNPROCESSABLE_ENTITY,
                &ErrorResponse {
                    error: "queue cannot be null".into(),
                },
            );
        }
        Some(Some(queue)) => {
            if let Err(e) = validate_name("queue", queue) {
                return respond(
                    fmt,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &ErrorResponse { error: e },
                );
            }
        }
        None => {}
    }

    // Validate priority if provided.
    if let Some(None) = body.priority {
        return respond(
            fmt,
            StatusCode::UNPROCESSABLE_ENTITY,
            &ErrorResponse {
                error: "priority cannot be null".into(),
            },
        );
    }

    // Compile payload filter.
    let filter = if let Some(ref expr) = params.filter {
        match PayloadFilter::compile(expr) {
            Ok(f) => Some(std::sync::Arc::new(f)),
            Err(e) => {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid filter: {e}"),
                    },
                );
            }
        }
    } else {
        None
    };

    // Build patch options.
    let mut patch = store::PatchJobOptions::default();
    patch.queue = body.queue.flatten();
    patch.priority = body.priority.flatten();
    if let Some(ready_at) = body.ready_at {
        patch.ready_at = Some(ready_at);
    }
    if let Some(retry_limit) = body.retry_limit {
        patch.retry_limit = Some(retry_limit);
    }
    if let Some(backoff) = body.backoff {
        patch.backoff = Some(backoff.map(Into::into));
    }
    if let Some(retention) = body.retention {
        patch.retention = Some(retention.map(Into::into));
    }

    let opts = store::BulkPatchOptions {
        ids: if params.id.is_empty() {
            HashSet::new()
        } else {
            params.id.0
        },
        statuses: if params.status.is_empty() {
            HashSet::new()
        } else {
            params
                .status
                .iter()
                .map(|s| store::JobStatus::from(*s))
                .collect()
        },
        queues: if params.queue.is_empty() {
            HashSet::new()
        } else {
            params.queue.0
        },
        types: if params.job_type.is_empty() {
            HashSet::new()
        } else {
            params.job_type.0
        },
        filter,
        patch,
    };

    match state.store.patch_jobs(opts).await {
        Ok(count) => {
            tracing::debug!(count, "bulk patch completed");
            respond(
                fmt,
                StatusCode::OK,
                &serde_json::json!({ "patched": count }),
            )
        }
        Err(e) => {
            tracing::error!(%e, "bulk_patch_jobs failed");
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
                fmt,
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

    let now = (state.clock)();
    let mut opts = ListJobsOptions::new()
        .direction(params.order.into())
        .limit(limit as usize)
        .now(now);
    if let Some(from) = &params.from {
        opts = opts.from(from.clone());
    }
    if !params.status.is_empty() {
        let statuses = params
            .status
            .iter()
            .map(|s| store::JobStatus::from(*s))
            .collect();
        opts = opts.statuses(statuses);
    }
    if !params.queue.is_empty() {
        opts = opts.queues(params.queue.0.clone());
    }
    if !params.job_type.is_empty() {
        opts = opts.types(params.job_type.0.clone());
    }
    if !params.id.is_empty() {
        for id in params.id.iter() {
            if !is_valid_job_id(id) {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid job ID: {id:?}"),
                    },
                );
            }
        }
        opts = opts.ids(params.id.0.clone());
    }
    if let Some(ref expr) = params.filter {
        match PayloadFilter::compile(expr) {
            Ok(f) => opts.filter = Some(std::sync::Arc::new(f)),
            Err(e) => {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid filter: {e}"),
                    },
                );
            }
        }
    }

    match state.store.list_jobs(opts).await {
        Ok(page) => {
            let filter_expr = params.filter.as_deref();
            let self_url = build_page_url(
                params.from.as_deref(),
                params.order,
                limit,
                &params.status,
                &params.queue,
                &params.job_type,
                &params.id,
                filter_expr,
            );
            let next = page.next.map(|o| {
                build_page_url(
                    o.from.as_deref(),
                    o.direction.into(),
                    limit,
                    &params.status,
                    &params.queue,
                    &params.job_type,
                    &params.id,
                    filter_expr,
                )
            });
            let prev = page.prev.map(|o| {
                build_page_url(
                    o.from.as_deref(),
                    o.direction.into(),
                    limit,
                    &params.status,
                    &params.queue,
                    &params.job_type,
                    &params.id,
                    filter_expr,
                )
            });

            let jobs: Result<Vec<Job>, _> = page.jobs.into_iter().map(Job::try_from).collect();

            match jobs {
                Ok(jobs) => respond(
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
                ),
                Err(e) => {
                    tracing::error!(%e, "corrupt job data");
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
fn build_page_url(
    from: Option<&str>,
    order: Order,
    limit: u16,
    statuses: &CommaSet<JobStatus>,
    queues: &CommaSet<String>,
    types: &CommaSet<String>,
    ids: &CommaSet<String>,
    filter: Option<&str>,
) -> String {
    let mut url = String::from("/jobs?");
    if let Some(cursor) = from {
        url.push_str(&format!("from={cursor}&"));
    }
    let order = serde_json::to_value(order).unwrap();
    url.push_str(&format!("order={}&limit={limit}", order.as_str().unwrap()));
    if !statuses.is_empty() {
        url.push_str(&format!("&status={statuses}"));
    }
    if !queues.is_empty() {
        url.push_str(&format!("&queue={queues}"));
    }
    if !types.is_empty() {
        url.push_str(&format!("&type={types}"));
    }
    if !ids.is_empty() {
        url.push_str(&format!("&id={ids}"));
    }
    if let Some(expr) = filter {
        url.push_str(&format!("&filter={}", urlencoding::encode(expr)));
    }
    url
}

/// Build a pagination URL for the errors listing endpoint.
fn build_errors_page_url(job_id: &str, from: Option<u32>, order: Order, limit: u16) -> String {
    let mut url = format!("/jobs/{job_id}/errors?");
    if let Some(cursor) = from {
        url.push_str(&format!("from={cursor}&"));
    }
    let order = serde_json::to_value(order).unwrap();
    url.push_str(&format!("order={}&limit={limit}", order.as_str().unwrap()));
    url
}

/// Handle `GET /jobs/count` — count jobs matching the given filters.
async fn count_jobs(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    params: Result<Query<CountJobsParams>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            return respond(
                fmt,
                StatusCode::BAD_REQUEST,
                &ErrorResponse {
                    error: format!("invalid query parameters: {e}"),
                },
            );
        }
    };

    let now = (state.clock)();
    let mut opts = ListJobsOptions::new().now(now);

    if !params.status.is_empty() {
        let statuses = params
            .status
            .iter()
            .map(|s| store::JobStatus::from(*s))
            .collect();
        opts = opts.statuses(statuses);
    }
    if !params.queue.is_empty() {
        opts = opts.queues(params.queue.0.clone());
    }
    if !params.job_type.is_empty() {
        opts = opts.types(params.job_type.0.clone());
    }
    if !params.id.is_empty() {
        for id in params.id.iter() {
            if !is_valid_job_id(id) {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid job ID: {id:?}"),
                    },
                );
            }
        }
        opts = opts.ids(params.id.0.clone());
    }
    if let Some(ref expr) = params.filter {
        match PayloadFilter::compile(expr) {
            Ok(f) => opts.filter = Some(std::sync::Arc::new(f)),
            Err(e) => {
                return respond(
                    fmt,
                    StatusCode::BAD_REQUEST,
                    &ErrorResponse {
                        error: format!("invalid filter: {e}"),
                    },
                );
            }
        }
    }

    match state.store.count_jobs(opts).await {
        Ok(count) => respond(fmt, StatusCode::OK, &CountJobsResponse { count }),
        Err(e) => respond(
            fmt,
            StatusCode::INTERNAL_SERVER_ERROR,
            &ErrorResponse {
                error: format!("{e}"),
            },
        ),
    }
}

/// Handle `GET /jobs/{id}/errors` — list error records for a job.
async fn list_errors(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    params: Result<Query<ListErrorsParams>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            return respond(
                fmt,
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

    // Verify the job exists — 404 if not found, consistent with GET /jobs/{id}.
    let now = (state.clock)();
    match state.store.get_job(now, &id).await {
        Ok(None) => {
            return respond(
                fmt,
                StatusCode::NOT_FOUND,
                &ErrorResponse {
                    error: "job not found".into(),
                },
            );
        }
        Err(e) => {
            tracing::error!(%e, "list_errors: get_job failed");
            return respond(
                fmt,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            );
        }
        Ok(Some(_)) => {}
    }

    let mut opts = ListErrorsOptions::new(&id)
        .direction(params.order.into())
        .limit(limit as usize);
    if let Some(from) = params.from {
        opts = opts.from(from);
    }

    match state.store.list_errors(opts).await {
        Ok(page) => {
            let self_url = build_errors_page_url(&id, params.from, params.order, limit);
            let next = page
                .next
                .map(|o| build_errors_page_url(&id, o.from, o.direction.into(), limit));
            let prev = page
                .prev
                .map(|o| build_errors_page_url(&id, o.from, o.direction.into(), limit));

            let errors: Vec<ErrorRecord> = page.errors.into_iter().map(ErrorRecord::from).collect();

            respond(
                fmt,
                StatusCode::OK,
                &ListErrorsResponse {
                    errors,
                    pages: ListErrorsPages {
                        self_url,
                        next,
                        prev,
                    },
                },
            )
        }
        Err(e) => {
            tracing::error!(%e, "list_errors failed");
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

/// Handle `GET /jobs/{id}/errors/{attempt}` — get a single error record.
async fn get_error(
    AcceptFormat(fmt): AcceptFormat,
    State(state): State<Arc<AppState>>,
    Path((id, attempt)): Path<(String, u32)>,
) -> Response {
    // Verify the job exists.
    let now = (state.clock)();
    match state.store.get_job(now, &id).await {
        Ok(None) => {
            return respond(
                fmt,
                StatusCode::NOT_FOUND,
                &ErrorResponse {
                    error: "job not found".into(),
                },
            );
        }
        Err(e) => {
            tracing::error!(%e, "get_error: get_job failed");
            return respond(
                fmt,
                StatusCode::INTERNAL_SERVER_ERROR,
                &ErrorResponse {
                    error: "internal server error".into(),
                },
            );
        }
        Ok(Some(_)) => {}
    }

    match state.store.get_error(&id, attempt).await {
        Ok(Some(record)) => respond(fmt, StatusCode::OK, &ErrorRecord::from(record)),
        Ok(None) => respond(
            fmt,
            StatusCode::NOT_FOUND,
            &ErrorResponse {
                error: "error record not found".into(),
            },
        ),
        Err(e) => {
            tracing::error!(%e, "get_error failed");
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

/// Default per-connection prefetch limit.
///
/// With a prefetch of 1, the server sends one job and waits for the client to
/// acknowledge it before sending the next. Clients that can handle concurrent
/// work should set `?prefetch=N` on the take URL. 0 means unlimited.

/// Re-check each in-flight job against the store and remove any that are no
/// longer in the in-flight state (e.g. completed while we were lagged).
///
/// This is the recovery path for broadcast channel overflow. It's not a hot
/// path — it only runs when we've missed events, which requires 1024+
/// events to buffer while this task is blocked.
async fn reconcile_in_flight(state: &AppState, in_flight: &mut HashMap<String, u32>) {
    let mut gone = Vec::new();
    let now = (state.clock)();
    for id in in_flight.keys() {
        match state.store.get_job(now, id).await {
            Ok(Some(job)) if job.status == store::JobStatus::InFlight as u8 => {
                // Still in-flight — keep tracking it.
            }
            _ => {
                // Missing (completed/deleted) or no longer in-flight.
                gone.push(id.clone());
            }
        }
    }
    for id in &gone {
        in_flight.remove(id);
        tracing::debug!(job_id = %id, "reconciled: job no longer in-flight");
    }
}

/// Handle `GET /jobs/take` — take jobs from the queue and deliver to workers.
async fn take_jobs(
    TakeAccept(fmt): TakeAccept,
    State(state): State<Arc<AppState>>,
    Query(params): Query<TakeParams>,
    headers: http::HeaderMap,
) -> Response {
    let prefetch = params.prefetch;
    let queues = params.queue.0;

    // The client can provide a Worker-Id: header to specify its own ID that
    // shows up in logs, but when one is not set we assign a new worker ID for
    // each connection.
    let worker_id = headers
        .get("worker-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| format!("{:08x}", rand_id()));

    // Channel we'll use to emit events to the client. We reserve a slot
    // before dequeuing a job, so we never take work we can't deliver.
    let (tx, rx) = mpsc::channel::<TakeMessage>(1);

    // Subscription to the store events broadcast channel which serves two
    // purposes:
    //
    // 1. Waking the run loop while it is waiting on new jobs.
    // 2. Receiving notifications about completed jobs so they can be removed
    //    from the in_flight set.
    let mut event_rx = state.store.subscribe();
    let mut shutdown = state.shutdown.clone();

    // For visibility make sure empty queue list presents as "*" in logs.
    let queues_display = if queues.is_empty() {
        "*".to_string()
    } else {
        let mut sorted: Vec<_> = queues.iter().cloned().collect();
        sorted.sort();
        sorted.join(",")
    };

    // Open a span for the duration of the run loop (long-lived). All logs
    // within the span are annotated with details of the span.
    let span = tracing::debug_span!("take", worker_id = %worker_id, queues = %queues_display);

    tokio::spawn(tracing::Instrument::instrument(
        async move {
            tracing::debug!(prefetch, "worker connected");

            // Maintain a set of in-flight job IDs. We need this because once we
            // remove a job from the queue and return it to the client, the job
            // will forever stay in the in-flight queue until the client acknowledges
            // or fails the job. If the client disconnects without doing this, we
            // need to clean up any in-flight jobs by moving them back to the
            // queue.
            //
            // In the extremely rare case of a crash or shutdown before the client
            // finishes processing the job, recovery on server restart will move
            // the job from the in-flight set back into the queue.
            let mut in_flight = HashMap::<String, u32>::new();

            // Pin the heartbeat timer outside the loop so it isn't reset
            // when broadcast events wake the select. It is only reset
            // after a heartbeat is actually sent.
            let heartbeat_sleep = tokio::time::sleep(state.heartbeat_interval_ms);
            tokio::pin!(heartbeat_sleep);

            // The take loop uses a single select! driven by a claim
            // token. The token starts unclaimed so we immediately try
            // to drain pre-existing ready jobs on startup. After each
            // successful take we mint a new local unclaimed token so
            // the drain continues. When the queue is empty the CAS
            // leaves the token claimed (true) and can_take becomes
            // false — we sit idle until a JobCreated event overwrites
            // the token with a fresh one.
            //
            // This keeps reserve(), the capacity check, and the token
            // CAS in one place instead of splitting them across two
            // phases.
            let mut claim_token: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

            loop {
                // We're not yet at our prefetch limit, and nobody else has
                // taken the claim.
                let can_take = !claim_token.load(Ordering::Relaxed)
                    && (prefetch == 0 || in_flight.len() < prefetch)
                    && (state.global_in_flight_limit == 0
                        || (state.store.in_flight_count() as u64) < state.global_in_flight_limit);

                tokio::select! {
                    // Detect client disconnect without waiting for the
                    // next heartbeat or reserve() call.
                    _ = tx.closed() => break,
                    _ = &mut heartbeat_sleep => {
                        if tx.send(TakeMessage::Heartbeat).await.is_err() {
                            break;
                        }
                        heartbeat_sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + state.heartbeat_interval_ms);
                    }
                    // Drain phase.
                    //
                    // The reserve() is blocking, but we're inside select! so
                    // won't block if there are other events on the event
                    // channel, which is good because we can't afford to let
                    // the event channel lag behind, even though we have a
                    // costly fallback for Lagged handling.
                    //
                    // We're not even going to bother trying to take the claim
                    // if we can't send data to the client, or if we've hit our
                    // prefetch limit.
                    result = tx.reserve(), if can_take => {
                        // Client is able to receive more data, see if we can
                        // get the claim.
                        match result {
                            Ok(permit) => {
                                if claim_token
                                    .compare_exchange(
                                        false,
                                        true,
                                        Ordering::Acquire,
                                        Ordering::Relaxed,
                                    )
                                    .is_ok()
                                {
                                    // Won the claim — take a batch of jobs.
                                    let now = (state.clock)();
                                    let take_start = std::time::Instant::now();

                                    // Compute how many jobs we want.
                                    let wanted = if prefetch > 0 {
                                        prefetch - in_flight.len()
                                    } else {
                                        1 // unlimited: permit is the only backpressure
                                    };

                                    // Cap by global in-flight limit.
                                    let wanted = if state.global_in_flight_limit > 0 {
                                        let in_flight = state.store.in_flight_count() as u64;
                                        wanted.min(
                                            state.global_in_flight_limit
                                                .saturating_sub(in_flight) as usize,
                                        )
                                    } else {
                                        wanted
                                    };

                                    // Defensive: we already checked limits earlier
                                    // with the can_take condition.
                                    let wanted = wanted.max(1);

                                    match state.store.take_next_n_jobs(now, &queues, wanted).await {
                                        Ok(jobs) if !jobs.is_empty() => {
                                            let got = jobs.len();
                                            tracing::trace!(
                                                take_ms = take_start.elapsed().as_secs_f64() * 1000.0,
                                                count = got,
                                                "take loop: got batch"
                                            );

                                            // Release the reserved slot — we'll
                                            // send all jobs uniformly via tx.send().
                                            drop(permit);

                                            let mut send_err = false;
                                            for store_job in jobs {
                                                match Job::try_from(store_job) {
                                                    Ok(job) => {
                                                        in_flight.insert(job.id.clone(), job.attempts);
                                                        tracing::debug!(
                                                            job_id = %job.id,
                                                            job_type = %job.job_type,
                                                            queue = %job.queue,
                                                            priority = job.priority,
                                                            "job dispatched"
                                                        );
                                                        if tx.send(TakeMessage::Job(Arc::new(job))).await.is_err() {
                                                            send_err = true;
                                                            break;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(%e, "corrupt job data");
                                                        // Skip this job — it's marked InFlight
                                                        // in the store but we can't decode it.
                                                        // Don't track it in in_flight so the
                                                        // connection stays healthy.
                                                        continue;
                                                    }
                                                }
                                            }

                                            if send_err {
                                                break;
                                            }

                                            // Reset heartbeat — we just sent data.
                                            heartbeat_sleep
                                                .as_mut()
                                                .reset(tokio::time::Instant::now() + state.heartbeat_interval_ms);

                                            if got >= wanted {
                                                // Got everything — mint a new
                                                // token to continue draining.
                                                claim_token =
                                                    Arc::new(AtomicBool::new(false));
                                            }
                                            // If got < wanted, the queue is
                                            // exhausted. Token stays claimed
                                            // (true) — wait for events.
                                        }
                                        Ok(_) => {
                                            // Empty result — queue exhausted.
                                            tracing::trace!(
                                                take_ms = take_start.elapsed().as_secs_f64() * 1000.0,
                                                "take loop: queue empty"
                                            );
                                            drop(permit);
                                        }
                                        Err(e) => {
                                            tracing::error!(%e, "take_next_n_jobs failed");
                                            break;
                                        }
                                    }
                                }
                                // CAS failed: another worker claimed it.
                                // Token is now true so has_token will be
                                // false next iteration — no spin.
                            }
                            Err(_) => break, // client disconnected
                        }
                    }
                    // Wait phase.
                    //
                    // If we're not draining, we're waiting for events from the
                    // store. We only go back to the drain phase if we got a
                    // JobCreated event we care about. Other events are handled
                    // without going back to the top of the loop.
                    event = event_rx.recv() => {
                        match event {
                            Ok(StoreEvent::JobCreated { queue, token, .. }) => {
                                // Store the latest matching token. We
                                // don't CAS yet — we'll claim it when
                                // reserve() tells us the client can
                                // accept a message.
                                if queues.is_empty()
                                    || queues.contains(&queue)
                                {
                                    claim_token = token;
                                }
                            }
                            Ok(StoreEvent::JobCompleted { ref id }) => {
                                if in_flight.remove(id).is_some() {
                                    tracing::debug!(
                                        job_id = %id,
                                        reason = "completed",
                                        "job removed from in-flight set"
                                    );
                                }
                            }
                            Ok(StoreEvent::JobFailed {
                                ref id, attempts, ..
                            }) => {
                                // Only prune if the attempt count matches
                                // what we took. A stale JobFailed from a
                                // previous retry cycle will have a lower
                                // attempts count and is safely ignored.
                                if in_flight
                                    .get(id)
                                    .is_some_and(|&a| a == attempts)
                                {
                                    in_flight.remove(id);
                                    tracing::debug!(
                                        job_id = %id,
                                        reason = "failed",
                                        "job removed from in-flight set"
                                    );
                                }
                            }
                            Ok(StoreEvent::JobDeleted { ref id }) => {
                                if in_flight.remove(id).is_some() {
                                    tracing::debug!(
                                        job_id = %id,
                                        reason = "deleted",
                                        "job removed from in-flight set"
                                    );
                                }
                            }
                            Ok(StoreEvent::JobInFlight { .. }) => {}
                            Ok(StoreEvent::JobScheduled { .. }) => {}
                            Ok(StoreEvent::JobPatched { .. }) => {}
                            Ok(StoreEvent::CronScheduleChanged) => {}
                            Ok(StoreEvent::IndexRebuilt) => {
                                // Indexes just became available — mint
                                // an unclaimed token so the drain phase
                                // activates.
                                claim_token =
                                    Arc::new(AtomicBool::new(false));
                            }
                            Err(broadcast::error::RecvError::Lagged(n))
                            => {
                                tracing::warn!(
                                    missed = n,
                                    "broadcast lagged, \
                                     reconciling in-flight set"
                                );
                                reconcile_in_flight(
                                    &state,
                                    &mut in_flight,
                                ).await;
                                // After reconciliation we may have missed
                                // JobCreated events — mint an unclaimed
                                // token to trigger a drain.
                                claim_token =
                                    Arc::new(AtomicBool::new(false));
                            }
                            Err(
                                broadcast::error::RecvError::Closed,
                            ) => break,
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }

            // The client disconnected. If there were any in-flight jobs re-queue
            // them so other clients can pick them up.
            let in_flight_count = in_flight.len();
            for id in in_flight.keys() {
                tracing::debug!(job_id = %id, "job requeued");
                if let Err(e) = state.store.requeue(id).await {
                    tracing::error!(job_id = %id, %e, "requeue failed");
                }
            }

            tracing::debug!(in_flight = in_flight_count, "worker disconnected");
        },
        span,
    ));

    // Build the response in the negotiated format.
    match fmt {
        TakeFormat::Sse => build_sse_response(rx),
        TakeFormat::NdJson => build_ndjson_response(rx),
        TakeFormat::MsgPackStream => build_msgpack_stream_response(rx),
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
    use crate::license::License;
    use crate::state::DEFAULT_HEARTBEAT_INTERVAL_MS;
    use crate::store::{EnqueueOptions, Store};
    use axum::body::Body;
    use http_body_util::BodyExt;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::sync::watch;
    use tower::util::ServiceExt;

    /// Build a controllable clock backed by an `AtomicU64`.
    ///
    /// Returns the atomic (for advancing time) and an `Arc<dyn Fn>` suitable
    /// for `AppState::clock`. The initial value is seeded from real time so
    /// that timestamps look realistic.
    fn mock_clock() -> (Arc<AtomicU64>, Arc<dyn Fn() -> u64 + Send + Sync>) {
        let time = Arc::new(AtomicU64::new(crate::time::now_millis()));
        let t = Arc::clone(&time);
        (time, Arc::new(move || t.load(Ordering::Relaxed)))
    }

    /// Create a bare `AppState` with a mock clock for tests.
    ///
    /// Returns `(clock_handle, state)` so tests can read `now` from the
    /// clock and mutate the state (e.g. custom backoff, in-flight limit)
    /// before wrapping in `Arc` and building the router.
    fn test_app_state() -> (Arc<AtomicU64>, AppState) {
        test_app_state_with_config(Default::default())
    }

    fn test_app_state_with_config(config: store::StorageConfig) -> (Arc<AtomicU64>, AppState) {
        let (clock, clock_fn) = mock_clock();
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), config).unwrap();
        std::mem::forget(dir);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        std::mem::forget(shutdown_tx);
        let (admin_events, _) = broadcast::channel(64);
        let state = AppState {
            license: std::sync::RwLock::new(License::Free),
            store,
            heartbeat_interval_ms: Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS),
            global_in_flight_limit: 0,
            shutdown: shutdown_rx,
            clock: clock_fn,
            admin_events,
            start_time: std::time::Instant::now(),
        };
        (clock, state)
    }

    /// Create a shared state and router for tests that need direct store access.
    fn test_state_and_app() -> (Arc<AppState>, Router) {
        let (_, state) = test_app_state();
        let state = Arc::new(state);
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
    async fn version_returns_200_with_version() {
        let req = empty_request("GET", "/version");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
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
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn enqueue_returns_job_id() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
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
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
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
            &serde_json::json!({"type": "test", "queue": "emails", "priority": 5, "payload": "test"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["queue"], "emails");
        assert_eq!(body["priority"], 5);
    }

    #[tokio::test]
    async fn enqueue_rejects_missing_queue() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "payload": "test"}),
        );
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
            #[serde(rename = "type")]
            job_type: &'static str,
            queue: &'static str,
            priority: u16,
            payload: &'static str,
        }

        let body = rmp_serde::to_vec_named(&MsgPackEnqueue {
            job_type: "test",
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
            #[serde(rename = "type")]
            job_type: &'static str,
            queue: &'static str,
            priority: u16,
            payload: &'static str,
        }

        let body = rmp_serde::to_vec_named(&MsgPackEnqueue {
            job_type: "test",
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
        rmp::encode::write_map_len(&mut buf, 3).unwrap();
        rmp::encode::write_str(&mut buf, "type").unwrap();
        rmp::encode::write_str(&mut buf, "test").unwrap();
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
    async fn enqueue_rejects_queue_name_with_comma() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "a,b", "payload": "x"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["error"].as_str().unwrap().contains(","));
    }

    #[tokio::test]
    async fn enqueue_rejects_queue_name_with_glob_chars() {
        for name in ["foo*", "bar?", "baz[0]", "qux{ab}", "back\\slash"] {
            let req = json_request(
                "POST",
                "/jobs",
                &serde_json::json!({"type": "test", "queue": name, "payload": "x"}),
            );
            let res = test_app().oneshot(req).await.unwrap();

            assert_eq!(
                res.status(),
                StatusCode::BAD_REQUEST,
                "expected reject for queue: {name}"
            );
            let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
            assert!(
                body["error"].as_str().unwrap().contains("reserved"),
                "expected 'reserved' in error for queue: {name}, got: {}",
                body["error"]
            );
        }
    }

    #[tokio::test]
    async fn enqueue_rejects_empty_queue_name() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "", "payload": "x"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["error"].as_str().unwrap().contains("empty"));
    }

    #[tokio::test]
    async fn enqueue_returns_status_and_ready_at() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "q", "payload": "x"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["status"], "ready");
        assert!(body["ready_at"].as_u64().is_some());
    }

    #[tokio::test]
    async fn enqueue_with_future_ready_at_returns_scheduled() {
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "q", "payload": "x", "ready_at": future_ms}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::CREATED);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["status"], "scheduled");
        assert_eq!(body["ready_at"], future_ms);
    }

    #[tokio::test]
    async fn enqueue_with_retry_limit_stores_value() {
        let (_, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "test",
                "queue": "q",
                "payload": "x",
                "retry_limit": 5
            }),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let id = body["id"].as_str().unwrap();

        // Fetch the job back to confirm the value persisted.
        let req = empty_request("GET", &format!("/jobs/{id}"));
        let res = app.oneshot(req).await.unwrap();
        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(job["retry_limit"], 5);
    }

    #[tokio::test]
    async fn enqueue_with_backoff_stores_value() {
        let (_, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "test",
                "queue": "q",
                "payload": "x",
                "backoff": {
                    "exponent": 3.0,
                    "base_ms": 1000,
                    "jitter_ms": 50
                }
            }),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let id = body["id"].as_str().unwrap();

        // Fetch the job back to confirm the backoff config persisted with
        // human-readable field names (not the compact store keys).
        let req = empty_request("GET", &format!("/jobs/{id}"));
        let res = app.oneshot(req).await.unwrap();
        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let backoff = &job["backoff"];
        assert_eq!(backoff["exponent"], 3.0);
        assert_eq!(backoff["base_ms"], 1000);
        assert_eq!(backoff["jitter_ms"], 50);
    }

    #[tokio::test]
    async fn enqueue_without_retry_limit_omits_field() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "q", "payload": "x"}),
        );
        let res = test_app().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();

        // When no retry_limit is provided, the field should be absent (not
        // null) thanks to skip_serializing_if.
        assert!(body.get("retry_limit").is_none());
        assert!(body.get("backoff").is_none());
    }

    /// Integration test: enqueue a job with a per-job retry_limit of 1,
    /// then fail it once (rescheduled) and again (killed). Verifies the
    /// per-job retry_limit overrides the server default.
    #[tokio::test]
    async fn enqueue_per_job_retry_limit_overrides_default() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let router = app(state.clone());

        // Enqueue with retry_limit=1 — the job should survive one failure
        // and be killed on the second.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "test",
                "queue": "q",
                "payload": "x",
                "retry_limit": 1
            }),
        );
        let res = router.clone().oneshot(req).await.unwrap();
        let enq: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = enq["id"].as_str().unwrap().to_string();

        // Take the job.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // First failure — should reschedule (attempts=1 <= retry_limit=1).
        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "oops"}),
        );
        let res = router.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(
            job["status"], "scheduled",
            "first failure should reschedule"
        );

        // Promote the scheduled job back to ready and take it again.
        let stored = state.store.get_job(now, &job_id).await.unwrap().unwrap();
        state.store.promote_scheduled(&stored).await.unwrap();
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Second failure — should kill (attempts=2 > retry_limit=1).
        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "oops again"}),
        );
        let res = router.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(job["status"], "dead", "second failure should kill");
    }

    /// Integration test: enqueue a job with a per-job backoff config and
    /// verify that the failure uses it instead of the server default.
    #[tokio::test]
    async fn enqueue_per_job_backoff_overrides_default() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let router = app(state.clone());

        // Enqueue with a custom backoff: exponent=1, base_ms=200, jitter=0.
        // Backoff for attempt 1: 1^1 + 200 = 201ms.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "test",
                "queue": "q",
                "payload": "x",
                "backoff": {
                    "exponent": 1.0,
                    "base_ms": 200,
                    "jitter_ms": 0
                }
            }),
        );
        let res = router.clone().oneshot(req).await.unwrap();
        let enq: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = enq["id"].as_str().unwrap().to_string();

        // Take and fail the job.
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "timeout"}),
        );
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(job["status"], "scheduled");

        let ready_at = job["ready_at"].as_u64().unwrap();
        assert_eq!(
            ready_at,
            now + 201,
            "ready_at should use per-job backoff (1^1 + 200 = 201ms), not server default",
        );
    }

    #[tokio::test]
    async fn comma_set_ignores_trailing_commas() {
        let (_, app) = test_state_and_app();

        // Enqueue jobs in two queues.
        for queue in &["emails", "webhooks"] {
            let req = json_request(
                "POST",
                "/jobs",
                &serde_json::json!({"type": "test", "queue": queue, "payload": "x"}),
            );
            app.clone().oneshot(req).await.unwrap();
        }

        // Trailing comma should be ignored — same as ?queue=emails,webhooks
        let req = empty_request("GET", "/jobs?queue=emails,webhooks,");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn get_job_returns_pending_job() {
        let (_, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "emails", "priority": 5, "payload": {"to": "a@b.c"}}),
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
    async fn get_job_returns_in_flight_job() {
        let (state, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let created: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = created["id"].as_str().unwrap();

        // Take the job so it moves to in-flight.
        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        let req = empty_request("GET", &format!("/jobs/{job_id}"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["id"], job_id);
        assert_eq!(body["status"], "in_flight");
        assert!(
            body["dequeued_at"].is_u64(),
            "in-flight job should have dequeued_at set"
        );
    }

    #[tokio::test]
    async fn get_job_returns_404_for_completed_job() {
        let (state, app) = test_state_and_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let created: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = created["id"].as_str().unwrap();

        let now = (state.clock)();
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();
        state.store.mark_completed(now, job_id).await.unwrap();

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
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
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
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
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
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
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
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
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
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
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
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap()
            .into_job();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
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
    async fn list_jobs_desc_cursor_paginates_without_duplicates() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();
        for label in ["a", "b", "c"] {
            state
                .store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "q", serde_json::json!(label)),
                )
                .await
                .unwrap();
        }

        // Page 1: newest two (c, b)
        let req = empty_request("GET", "/jobs?order=desc&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page1 = body["jobs"].as_array().unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0]["payload"], "c");
        assert_eq!(page1[1]["payload"], "b");

        // Page 2: follow next link, should get only "a"
        let next_url = body["pages"]["next"].as_str().unwrap();
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page2 = body["jobs"].as_array().unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0]["payload"], "a");
    }

    #[tokio::test]
    async fn list_jobs_desc_cursor_with_queue_filter() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();
        for label in ["a", "b", "c"] {
            state
                .store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "emails", serde_json::json!(label)),
                )
                .await
                .unwrap();
        }
        // Decoy in another queue
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "other", serde_json::json!("x")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?queue=emails&order=desc&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page1 = body["jobs"].as_array().unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0]["payload"], "c");
        assert_eq!(page1[1]["payload"], "b");

        let next_url = body["pages"]["next"].as_str().unwrap();
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page2 = body["jobs"].as_array().unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0]["payload"], "a");
    }

    #[tokio::test]
    async fn list_jobs_desc_cursor_with_status_filter() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();
        for label in ["a", "b", "c"] {
            state
                .store
                .enqueue(
                    now,
                    EnqueueOptions::new("test", "q", serde_json::json!(label)),
                )
                .await
                .unwrap();
        }

        let req = empty_request("GET", "/jobs?status=ready&order=desc&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page1 = body["jobs"].as_array().unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0]["payload"], "c");
        assert_eq!(page1[1]["payload"], "b");

        let next_url = body["pages"]["next"].as_str().unwrap();
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page2 = body["jobs"].as_array().unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0]["payload"], "a");
    }

    #[tokio::test]
    async fn list_jobs_desc_cursor_with_type_filter() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();
        for label in ["a", "b", "c"] {
            state
                .store
                .enqueue(
                    now,
                    EnqueueOptions::new("send_email", "q", serde_json::json!(label)),
                )
                .await
                .unwrap();
        }
        // Decoy with different type
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("other_type", "q", serde_json::json!("x")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?type=send_email&order=desc&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page1 = body["jobs"].as_array().unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0]["payload"], "c");
        assert_eq!(page1[1]["payload"], "b");

        let next_url = body["pages"]["next"].as_str().unwrap();
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let page2 = body["jobs"].as_array().unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0]["payload"], "a");
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
        let req = empty_request("GET", "/jobs?limit=2001");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["error"].as_str().unwrap().contains("limit"));
    }

    #[tokio::test]
    async fn list_jobs_accepts_max_limit() {
        let req = empty_request("GET", "/jobs?limit=2000");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_status() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

        // Take the first job so it becomes in-flight.
        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?status=ready");
        let res = app.clone().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["status"], "ready");
        assert_eq!(jobs[0]["payload"], "b");

        let req = empty_request("GET", "/jobs?status=in_flight");
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["status"], "in_flight");
        assert_eq!(jobs[0]["payload"], "a");
    }

    #[tokio::test]
    async fn list_jobs_rejects_invalid_status() {
        let req = empty_request("GET", "/jobs?status=banana");
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
    async fn list_jobs_status_filter_preserves_in_pagination() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?status=ready&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

        // Next page URL should contain status=ready.
        let next_url = body["pages"]["next"].as_str().unwrap();
        assert!(next_url.contains("status=ready"));

        // Follow the next page.
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["payload"], "c");
        assert_eq!(jobs[0]["status"], "ready");
    }

    // --- GET /jobs comma-delimited filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_statuses() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        // Take the first job so it becomes in-flight.
        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?status=ready,in_flight");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[tokio::test]
    async fn list_jobs_multi_status_preserves_in_pagination() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?status=ready,in_flight&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

        let next_url = body["pages"]["next"].as_str().unwrap();
        // The next URL should contain both statuses.
        assert!(next_url.contains("status="));
        assert!(next_url.contains("ready"));
        assert!(next_url.contains("in_flight"));

        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_queue() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?queue=emails");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["queue"], "emails");
        assert_eq!(jobs[1]["queue"], "emails");
    }

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_queues() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?queue=emails,webhooks");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["payload"], "a");
        assert_eq!(jobs[1]["payload"], "c");
    }

    #[tokio::test]
    async fn list_jobs_queue_filter_preserves_in_pagination() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?queue=emails&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

        let next_url = body["pages"]["next"].as_str().unwrap();
        assert!(next_url.contains("queue=emails"));

        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(body["jobs"][0]["payload"], "c");
    }

    // --- list_jobs combined queue + status filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_queue_and_status() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

        // Take a so it becomes in-flight.
        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Only ready jobs in emails queue.
        let req = empty_request("GET", "/jobs?queue=emails&status=ready");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["payload"], "c");
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_preserves_in_pagination() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?queue=emails&status=ready&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

        let next_url = body["pages"]["next"].as_str().unwrap();
        // The next URL should contain both queue and status filters.
        assert!(next_url.contains("queue=emails"));
        assert!(next_url.contains("status=ready"));

        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(body["jobs"][0]["payload"], "c");
    }

    #[tokio::test]
    async fn list_jobs_queue_and_status_multiple_values() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap();

        // Take a so it becomes in-flight.
        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready + in-flight in emails + reports (excludes webhooks).
        let req = empty_request("GET", "/jobs?queue=emails,reports&status=ready,in_flight");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["payload"], "a"); // emails, in-flight
        assert_eq!(jobs[1]["payload"], "b"); // reports, ready
    }

    // --- list_jobs type filter tests ---

    #[tokio::test]
    async fn list_jobs_filters_by_type() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?type=send_email");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["type"], "send_email");
        assert_eq!(jobs[1]["type"], "send_email");
    }

    #[tokio::test]
    async fn list_jobs_filters_by_multiple_types() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("send_email", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("generate_report", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("send_sms", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs?type=send_email,send_sms");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["payload"], "a");
        assert_eq!(jobs[1]["payload"], "c");
    }

    #[tokio::test]
    async fn list_jobs_type_filter_preserves_in_pagination() {
        let (state, app) = test_state_and_app();

        for i in 0..3 {
            state
                .store
                .enqueue(
                    crate::time::now_millis(),
                    EnqueueOptions::new("send_email", "q", serde_json::json!(i)),
                )
                .await
                .unwrap();
        }

        let req = empty_request("GET", "/jobs?type=send_email&limit=2");
        let res = app.clone().oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

        let next_url = body["pages"]["next"].as_str().unwrap();
        assert!(next_url.contains("type=send_email"));

        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_type_and_queue_and_status() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("send_email", "high", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("generate_report", "high", serde_json::json!("c")),
            )
            .await
            .unwrap();

        // Take a so it becomes in-flight.
        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

        // Ready send_email in high queue.
        let req = empty_request("GET", "/jobs?type=send_email&queue=high&status=ready");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["payload"], "b");
    }

    // --- POST /jobs/{id}/success tests ---

    #[tokio::test]
    async fn success_returns_204() {
        let (state, app) = test_state_and_app();

        // Enqueue and take a job so it's in the in-flight set.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap();

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

        // Enqueue but don't take — job is pending, not in-flight.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "test"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        let req = empty_request("POST", &format!("/jobs/{job_id}/success"));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    // --- GET /jobs/take tests ---

    async fn next_body_bytes(body: &mut axum::body::Body) -> Bytes {
        use http_body_util::BodyExt as _;
        body.frame().await.unwrap().unwrap().into_data().unwrap()
    }

    #[tokio::test]
    async fn take_delivers_pending_jobs_as_ndjson() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q1", serde_json::json!("first")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q2", serde_json::json!("second")).priority(5),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/take?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // First job (priority 0, enqueued first).
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "q1");
        assert_eq!(job["payload"], "first");
        assert_eq!(job["status"], "in_flight");
        assert!(job["id"].is_string());
        assert!(
            job["dequeued_at"].is_u64(),
            "taken job should have dequeued_at set"
        );

        // Second job (priority 5).
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "q2");
        assert_eq!(job["payload"], "second");
        assert_eq!(job["status"], "in_flight");
        assert!(
            job["dequeued_at"].is_u64(),
            "taken job should have dequeued_at set"
        );
    }

    #[tokio::test]
    async fn take_waits_and_delivers_new_jobs() {
        let (state, app) = test_state_and_app();

        let req = empty_request("GET", "/jobs/take");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Enqueue after take connection is open.
        let enqueued = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("delayed")),
            )
            .await
            .unwrap()
            .into_job();

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["id"], enqueued.id);
        assert_eq!(job["payload"], "delayed");
        assert_eq!(job["status"], "in_flight");
    }

    #[tokio::test]
    async fn take_returns_sse_when_requested() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("sse-test")),
            )
            .await
            .unwrap();

        let req = request_with_accept("GET", "/jobs/take", "text/event-stream");
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
        assert_eq!(job["status"], "in_flight");
    }

    #[tokio::test]
    async fn take_returns_msgpack_when_requested() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("mp-test")),
            )
            .await
            .unwrap();

        let req = request_with_accept("GET", "/jobs/take", "application/vnd.zizq.msgpack-stream");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/vnd.zizq.msgpack-stream"
        );

        let mut body = res.into_body();
        let bytes = next_body_bytes(&mut body).await;

        // First 4 bytes are big-endian u32 length prefix.
        assert!(bytes.len() > 4);
        let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        assert_eq!(bytes.len(), 4 + len);

        let job: serde_json::Value = rmp_serde::from_slice(&bytes[4..]).unwrap();
        assert_eq!(job["payload"], "mp-test");
        assert_eq!(job["status"], "in_flight");
    }

    #[tokio::test]
    async fn take_returns_406_for_unsupported_accept() {
        let req = request_with_accept("GET", "/jobs/take", "text/xml");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_ACCEPTABLE);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "not acceptable");
        let supported = body["supported"].as_array().unwrap();
        assert!(supported.contains(&"text/event-stream".into()));
        assert!(supported.contains(&"application/x-ndjson".into()));
        assert!(supported.contains(&"application/vnd.zizq.msgpack-stream".into()));
    }

    #[tokio::test]
    async fn take_sets_ndjson_content_type() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("ct-test")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/take");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/x-ndjson"
        );
    }

    #[tokio::test]
    async fn take_requeues_on_disconnect() {
        let (state, app) = test_state_and_app();

        let enqueued = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("requeue-me")),
            )
            .await
            .unwrap()
            .into_job();

        // Use prefetch=2 so the task attempts to reserve a second slot
        // after sending the first job. When the body is dropped, the
        // reserve() call detects the closed channel and triggers requeue.
        let req = empty_request("GET", "/jobs/take?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read the job from the response.
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
        while let Some(job) = state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap()
        {
            ids.push(job.id);
        }
        assert!(
            ids.contains(&enqueued.id),
            "original job should have been requeued"
        );
    }

    #[tokio::test]
    async fn take_does_not_requeue_acked_jobs_on_disconnect() {
        let (state, app) = test_state_and_app();

        let job_a = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("acked")),
            )
            .await
            .unwrap()
            .into_job();
        let job_b = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!("unacked")),
            )
            .await
            .unwrap()
            .into_job();

        let req = empty_request("GET", "/jobs/take?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read both jobs from the response.
        let _ = next_body_bytes(&mut body).await;
        let _ = next_body_bytes(&mut body).await;

        // Ack the first job while the connection is still open.
        assert!(
            state
                .store
                .mark_completed(crate::time::now_millis(), &job_a.id)
                .await
                .unwrap()
        );

        // Drop the body to simulate client disconnect.
        drop(body);

        // Give the spawned task time to detect disconnect and requeue.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Only the unacked job should be requeued, not the acked one.
        let mut ids = Vec::new();
        while let Some(job) = state
            .store
            .take_next_job(crate::time::now_millis(), &HashSet::new())
            .await
            .unwrap()
        {
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
    async fn take_respects_in_flight_limit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Default::default()).unwrap();
        std::mem::forget(dir);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        std::mem::forget(shutdown_tx);
        let (admin_events, _) = broadcast::channel(64);
        let state = Arc::new(AppState {
            license: std::sync::RwLock::new(License::Free),
            store,
            heartbeat_interval_ms: Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS),
            global_in_flight_limit: 2,
            shutdown: shutdown_rx,
            clock: Arc::new(crate::time::now_millis),
            admin_events,
            start_time: std::time::Instant::now(),
        });
        let router = app(state.clone());

        // Enqueue 3 jobs.
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/take?prefetch=3");
        let res = router.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read 2 jobs — should succeed since in_flight_limit is 2.
        let bytes = next_body_bytes(&mut body).await;
        let job_a: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        let _ = next_body_bytes(&mut body).await;

        assert_eq!(state.store.in_flight_count(), 2);

        // Ack the first job to free a slot.
        state
            .store
            .mark_completed(crate::time::now_millis(), job_a["id"].as_str().unwrap())
            .await
            .unwrap();

        // Third job should now be delivered.
        let bytes = next_body_bytes(&mut body).await;
        let job_c: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_c["payload"], "c");
    }

    #[tokio::test]
    async fn take_defaults_to_prefetch_one() {
        let (state, app) = test_state_and_app();

        // Enqueue 2 jobs.
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

        // Take with default prefetch (1).
        let req = empty_request("GET", "/jobs/take");
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
            .mark_completed(crate::time::now_millis(), job_a["id"].as_str().unwrap())
            .await
            .unwrap();

        // Now the second job should come through.
        let bytes = next_body_bytes(&mut body).await;
        let job_b: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_b["payload"], "b");
    }

    #[tokio::test]
    async fn take_prefetch_allows_multiple_in_flight() {
        let (state, app) = test_state_and_app();

        // Enqueue 3 jobs.
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        // Take with prefetch=2.
        let req = empty_request("GET", "/jobs/take?prefetch=2");
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
            .mark_completed(crate::time::now_millis(), job_a["id"].as_str().unwrap())
            .await
            .unwrap();

        let bytes = next_body_bytes(&mut body).await;
        let job_c: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_c["payload"], "c");
    }

    #[tokio::test]
    async fn take_prefetch_zero_means_unlimited() {
        let (state, app) = test_state_and_app();

        // Enqueue 3 jobs.
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("c")),
            )
            .await
            .unwrap();

        // Take with prefetch=0 (unlimited).
        let req = empty_request("GET", "/jobs/take?prefetch=0");
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
    async fn take_resets_global_counter_on_disconnect() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "q", serde_json::json!("x")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/take?prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        let _ = next_body_bytes(&mut body).await;
        assert_eq!(state.store.in_flight_count(), 1);

        // Disconnect.
        drop(body);

        // Give the spawned task time to detect disconnect.
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(state.store.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn take_filters_by_single_queue() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/take?queue=reports");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "reports");
        assert_eq!(job["payload"], "b");
    }

    #[tokio::test]
    async fn take_filters_by_multiple_queues() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "webhooks", serde_json::json!("c")),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/take?queue=emails,webhooks&prefetch=2");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "emails");

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "webhooks");
    }

    #[tokio::test]
    async fn take_queue_filter_skips_other_queues() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "emails", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "reports", serde_json::json!("b")),
            )
            .await
            .unwrap();

        // Take only from emails — reports job should remain ready.
        let req = empty_request("GET", "/jobs/take?queue=emails");
        let res = app.clone().oneshot(req).await.unwrap();
        let mut body = res.into_body();

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "emails");

        // Ack the emails job so it isn't requeued on disconnect.
        state
            .store
            .mark_completed(crate::time::now_millis(), job["id"].as_str().unwrap())
            .await
            .unwrap();
        drop(body);

        // Give the spawned task time to detect disconnect.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The reports job should still be available via unfiltered take.
        let req = empty_request("GET", "/jobs/take");
        let res = app.oneshot(req).await.unwrap();
        let mut body = res.into_body();

        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job["queue"], "reports");
    }

    // --- POST /jobs/{id}/failure tests ---

    #[tokio::test]
    async fn failure_returns_200_with_scheduled_status() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let app = app(state.clone());

        // Enqueue and take a job so it's in the in-flight set.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "hello"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Report failure — should reschedule (default retry_limit > 0).
        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "connection timeout"}),
        );
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(job["status"], "scheduled");
        assert_eq!(job["attempts"], 1);
    }

    #[tokio::test]
    async fn failure_returns_200_with_dead_status_when_killed() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let app = app(state.clone());

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "hello"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "fatal", "kill": true}),
        );
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(job["status"], "dead");
    }

    #[tokio::test]
    async fn failure_returns_404_for_unknown_job() {
        let req = json_request(
            "POST",
            "/jobs/nonexistent/failure",
            &serde_json::json!({"message": "oops"}),
        );
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn failure_returns_404_for_ready_job() {
        let (_, app) = test_state_and_app();

        // Enqueue but don't take — job is ready, not in-flight.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "hello"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "oops"}),
        );
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn failure_does_not_return_payload() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let app = app(state.clone());

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": {"big": "data"}}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap();

        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "oops"}),
        );
        let res = app.oneshot(req).await.unwrap();

        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(
            job.get("payload").is_none(),
            "failure response should not include payload"
        );
    }

    /// Integration test: a job is taken via /take, fails with retry, gets
    /// promoted back to ready, and is re-delivered on the same /take stream.
    ///
    /// This exercises the full cycle: the take loop's in_flight tracking
    /// must correctly prune the failed job (via the JobFailed event with
    /// matching attempts), freeing capacity so the re-promoted job can be
    /// taken again.
    #[tokio::test]
    async fn take_redelivers_failed_job_after_reschedule() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let app = app(state.clone());

        // Enqueue one job.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("work")),
            )
            .await
            .unwrap();

        // Connect a /take stream (prefetch=1).
        let req = empty_request("GET", "/jobs/take?prefetch=1");
        let res = app.clone().oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // Read the first delivery.
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job: serde_json::Value = serde_json::from_str(line).unwrap();
        let job_id = job["id"].as_str().unwrap();
        assert_eq!(job["status"], "in_flight");
        assert_eq!(job["attempts"], 0);

        // Report failure with retry_at in the past so we can promote
        // immediately without relying on real-time scheduler delays.
        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "boom", "retry_at": 1}),
        );
        let fail_res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(fail_res.status(), StatusCode::OK);

        // Give the take loop a moment to process the JobFailed event
        // and prune in_flight.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The in-flight counter should be back to 0.
        assert_eq!(
            state.store.in_flight_count(),
            0,
            "global_in_flight should be 0 after failure prunes in-flight"
        );

        // Manually promote the rescheduled job (ready_at=1 is in the
        // past). This avoids depending on the background scheduler.
        let (due, _) = state.store.next_scheduled(u64::MAX, 10).await.unwrap();
        assert_eq!(due.len(), 1, "expected one scheduled job");
        state.store.promote_scheduled(&due[0]).await.unwrap();

        // The take stream should now re-deliver the job.
        let bytes = next_body_bytes(&mut body).await;
        let line = std::str::from_utf8(&bytes).unwrap().trim();
        let job2: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(job2["id"], job_id, "same job should be redelivered");
        assert_eq!(job2["status"], "in_flight");
        assert_eq!(job2["attempts"], 1, "attempts should be incremented");
    }

    /// Verify that the server's default backoff config produces the correct
    /// `ready_at` when no `retry_at` is provided by the client.
    ///
    /// Uses a mock clock so the assertion is exact — no timing margins.
    #[tokio::test]
    async fn failure_applies_default_backoff() {
        // Use a known backoff config with zero jitter so the result is
        // deterministic: delay = attempts^exponent + base_ms = 1^2 + 500 = 501.
        let mut config = store::StorageConfig::default();
        config.default_backoff = store::BackoffConfig {
            exponent: 2.0,
            base_ms: 500,
            jitter_ms: 0,
        };
        let (clock, state) = test_app_state_with_config(config);
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let router = app(state.clone());

        // Enqueue and take a job via the HTTP API.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "q", "payload": "x"}),
        );
        let res = router.clone().oneshot(req).await.unwrap();
        let enq: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = enq["id"].as_str().unwrap().to_string();

        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Fail the job — the handler reads (state.clock)() which returns
        // our fixed `now`, so ready_at = now + 501 exactly.
        let req = json_request(
            "POST",
            &format!("/jobs/{job_id}/failure"),
            &serde_json::json!({"message": "timeout"}),
        );
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let job: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(job["status"], "scheduled");

        // With exponent=2, base_ms=500, jitter=0, attempts=1:
        // delay = 1^2 + 500 = 501ms
        let ready_at = job["ready_at"].as_u64().unwrap();
        assert_eq!(
            ready_at,
            now + 501,
            "ready_at should be exactly now + 501ms (backoff for attempt 1)",
        );
    }

    /// Verify that failing a job frees global_in_flight capacity, allowing
    /// a different pending job to be taken.
    #[tokio::test]
    async fn take_failure_frees_capacity_for_other_jobs() {
        let (clock, mut state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        state.global_in_flight_limit = 1;
        let state = Arc::new(state);
        let router = app(state.clone());

        // Enqueue two jobs.
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("a")),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "q", serde_json::json!("b")),
            )
            .await
            .unwrap();

        // Take with prefetch=2 but global_in_flight_limit=1.
        let req = empty_request("GET", "/jobs/take?prefetch=2");
        let res = router.clone().oneshot(req).await.unwrap();
        let mut body = res.into_body();

        // First job delivered (hits global limit).
        let bytes = next_body_bytes(&mut body).await;
        let job_a: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        let job_a_id = job_a["id"].as_str().unwrap();
        assert_eq!(state.store.in_flight_count(), 1);

        // Fail the first job (kill it so it doesn't go back to the queue).
        let req = json_request(
            "POST",
            &format!("/jobs/{job_a_id}/failure"),
            &serde_json::json!({"message": "fatal", "kill": true}),
        );
        let fail_res = router.oneshot(req).await.unwrap();
        assert_eq!(fail_res.status(), StatusCode::OK);

        // Second job should now be delivered since capacity was freed.
        let bytes = next_body_bytes(&mut body).await;
        let job_b: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim()).unwrap();
        assert_eq!(job_b["payload"], "b");
    }

    // --- list_errors tests ---

    #[tokio::test]
    async fn list_errors_returns_200_with_empty_list() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();

        // Enqueue a job (no failures).
        let job = state
            .store
            .enqueue(
                now,
                store::EnqueueOptions::new("test", "q", serde_json::json!("x")),
            )
            .await
            .unwrap()
            .into_job();

        let req = empty_request("GET", &format!("/jobs/{}/errors", job.id));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["errors"].as_array().unwrap().len(), 0);
        assert!(body["pages"]["next"].is_null());
        assert!(body["pages"]["prev"].is_null());
    }

    #[tokio::test]
    async fn list_errors_returns_errors_after_failures() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();

        // Enqueue and take.
        state
            .store
            .enqueue(
                now,
                store::EnqueueOptions::new("test", "q", serde_json::json!("x")),
            )
            .await
            .unwrap();
        let job = state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Fail the job.
        let opts = store::FailureOptions {
            message: "boom".into(),
            error_type: Some("RuntimeError".into()),
            backtrace: None,
            retry_at: None,
            kill: false,
        };
        state
            .store
            .record_failure(now, &job.id, opts)
            .await
            .unwrap();

        let req = empty_request("GET", &format!("/jobs/{}/errors", job.id));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let errors = body["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["attempt"], 1);
        assert_eq!(errors[0]["message"], "boom");
        assert_eq!(errors[0]["error_type"], "RuntimeError");
    }

    #[tokio::test]
    async fn list_errors_paginates_with_next_prev() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();

        // Enqueue and take.
        state
            .store
            .enqueue(
                now,
                store::EnqueueOptions::new("test", "q", serde_json::json!("x")),
            )
            .await
            .unwrap();
        let job = state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();

        // Fail, promote, retake, fail again to get 2 error records.
        let fail_opts = || store::FailureOptions {
            message: "err".into(),
            error_type: None,
            backtrace: None,
            retry_at: None,
            kill: false,
        };
        state
            .store
            .record_failure(now, &job.id, fail_opts())
            .await
            .unwrap();

        let scheduled = state.store.get_job(now, &job.id).await.unwrap().unwrap();
        state.store.promote_scheduled(&scheduled).await.unwrap();
        let retaken = state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap()
            .unwrap();
        state
            .store
            .record_failure(now, &retaken.id, fail_opts())
            .await
            .unwrap();

        // Page 1: limit=1, should get one error and a next link.
        let req = empty_request("GET", &format!("/jobs/{}/errors?limit=1", job.id));
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["errors"].as_array().unwrap().len(), 1);
        assert!(body["pages"]["next"].is_string());
        assert!(body["pages"]["prev"].is_null());

        // Page 2: follow the next link.
        let next_url = body["pages"]["next"].as_str().unwrap();
        let req = empty_request("GET", next_url);
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["errors"].as_array().unwrap().len(), 1);
        assert!(body["pages"]["next"].is_null());
        // prev should exist since we used a cursor.
        assert!(body["pages"]["prev"].is_string());
    }

    #[tokio::test]
    async fn list_errors_returns_404_for_unknown_job() {
        let app = test_app();
        let req = empty_request("GET", "/jobs/nonexistent/errors");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "job not found");
    }

    // --- get_error tests ---

    #[tokio::test]
    async fn get_error_returns_single_error_record() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();

        let job = state
            .store
            .enqueue(
                now,
                store::EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();
        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();
        state
            .store
            .record_failure(
                now,
                &job.id,
                store::FailureOptions {
                    message: "boom".into(),
                    error_type: Some("RuntimeError".into()),
                    backtrace: None,
                    retry_at: None,
                    kill: false,
                },
            )
            .await
            .unwrap();

        let req = empty_request("GET", &format!("/jobs/{}/errors/1", job.id));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["attempt"], 1);
        assert_eq!(body["message"], "boom");
        assert_eq!(body["error_type"], "RuntimeError");
    }

    #[tokio::test]
    async fn get_error_returns_404_for_unknown_job() {
        let app = test_app();
        let req = empty_request("GET", "/jobs/nonexistent/errors/1");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "job not found");
    }

    #[tokio::test]
    async fn get_error_returns_404_for_missing_attempt() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();

        let job = state
            .store
            .enqueue(
                now,
                store::EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = empty_request("GET", &format!("/jobs/{}/errors/1", job.id));
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["error"], "error record not found");
    }

    // --- Bulk enqueue tests ---

    #[tokio::test]
    async fn bulk_enqueue_returns_201() {
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "test", "queue": "default", "payload": "a"},
                    {"type": "test", "queue": "default", "payload": "b"},
                ]
            }),
        );
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn bulk_enqueue_returns_jobs_in_order() {
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "alpha", "queue": "q1", "payload": 1},
                    {"type": "beta",  "queue": "q2", "payload": 2},
                    {"type": "gamma", "queue": "q3", "payload": 3},
                ]
            }),
        );
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0]["type"], "alpha");
        assert_eq!(jobs[0]["queue"], "q1");
        assert_eq!(jobs[1]["type"], "beta");
        assert_eq!(jobs[1]["queue"], "q2");
        assert_eq!(jobs[2]["type"], "gamma");
        assert_eq!(jobs[2]["queue"], "q3");

        // IDs should be monotonically increasing (SCRU128 order).
        let id0 = jobs[0]["id"].as_str().unwrap();
        let id1 = jobs[1]["id"].as_str().unwrap();
        let id2 = jobs[2]["id"].as_str().unwrap();
        assert!(id0 < id1);
        assert!(id1 < id2);
    }

    #[tokio::test]
    async fn bulk_enqueue_is_atomic() {
        let (state, app) = test_state_and_app();

        // Second job has an invalid queue name — entire batch should fail.
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "test", "queue": "good", "payload": "a"},
                    {"type": "test", "queue": "", "payload": "b"},
                ]
            }),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // No jobs should have been created.
        let list = state
            .store
            .list_jobs(store::ListJobsOptions::new())
            .await
            .unwrap();
        assert!(list.jobs.is_empty());
    }

    #[tokio::test]
    async fn bulk_enqueue_validates_all_jobs() {
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "good", "queue": "default", "payload": "a"},
                    {"type": "",     "queue": "default", "payload": "b"},
                ]
            }),
        );
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let error = body["error"].as_str().unwrap();
        assert!(
            error.contains("jobs[1]"),
            "error should identify job index: {error}"
        );
    }

    #[tokio::test]
    async fn bulk_enqueue_empty_array_returns_201() {
        let req = json_request("POST", "/jobs/bulk", &serde_json::json!({"jobs": []}));
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["jobs"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn bulk_enqueue_mixed_ready_and_scheduled() {
        let (clock, state) = test_app_state();
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let app = app(state.clone());

        let future_ts = now + 60_000;
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "test", "queue": "default", "payload": "ready"},
                    {"type": "test", "queue": "default", "payload": "scheduled", "ready_at": future_ts},
                ]
            }),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs[0]["status"], "ready");
        assert_eq!(jobs[1]["status"], "scheduled");
        assert_eq!(jobs[1]["ready_at"], future_ts);
    }

    #[tokio::test]
    async fn bulk_enqueue_with_options() {
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {
                        "type": "test",
                        "queue": "default",
                        "payload": "opts",
                        "priority": 5,
                        "retry_limit": 10,
                        "backoff": {"exponent": 2.0, "base_ms": 1000, "jitter_ms": 500},
                        "retention": {"completed_ms": 3600000, "dead_ms": 7200000},
                    }
                ]
            }),
        );
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job = &body["jobs"][0];
        assert_eq!(job["priority"], 5);
        assert_eq!(job["retry_limit"], 10);
        assert_eq!(job["backoff"]["exponent"], 2.0);
        assert_eq!(job["backoff"]["base_ms"], 1000);
        assert_eq!(job["backoff"]["jitter_ms"], 500);
        assert_eq!(job["retention"]["completed_ms"], 3600000);
        assert_eq!(job["retention"]["dead_ms"], 7200000);
    }

    #[tokio::test]
    async fn bulk_enqueue_supports_msgpack() {
        #[derive(Serialize)]
        struct MsgPackBulk {
            jobs: Vec<MsgPackJob>,
        }
        #[derive(Serialize)]
        struct MsgPackJob {
            #[serde(rename = "type")]
            job_type: &'static str,
            queue: &'static str,
            payload: &'static str,
        }

        let body = rmp_serde::to_vec_named(&MsgPackBulk {
            jobs: vec![
                MsgPackJob {
                    job_type: "test",
                    queue: "q1",
                    payload: "a",
                },
                MsgPackJob {
                    job_type: "test",
                    queue: "q2",
                    payload: "b",
                },
            ],
        })
        .unwrap();

        let req = Request::builder()
            .method("POST")
            .uri("/jobs/bulk")
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
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["queue"], "q1");
        assert_eq!(jobs[1]["queue"], "q2");
    }

    // --- POST /jobs/success (bulk) tests ---

    #[tokio::test]
    async fn bulk_success_returns_204_when_all_found() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        // Enqueue 3 jobs.
        let mut ids = Vec::new();
        for i in 0..3 {
            let req = json_request(
                "POST",
                "/jobs",
                &serde_json::json!({"type": "test", "queue": "default", "payload": format!("j{i}")}),
            );
            let res = app.clone().oneshot(req).await.unwrap();
            let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
            ids.push(body["id"].as_str().unwrap().to_string());
        }

        // Take all 3 so they're in the in-flight set.
        state
            .store
            .take_next_n_jobs(now, &HashSet::new(), 3)
            .await
            .unwrap();

        // Bulk complete all 3.
        let req = json_request("POST", "/jobs/success", &serde_json::json!({"ids": ids}));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn bulk_success_returns_422_with_not_found() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        // Enqueue and take 2 jobs.
        let mut ids = Vec::new();
        for i in 0..2 {
            let req = json_request(
                "POST",
                "/jobs",
                &serde_json::json!({"type": "test", "queue": "default", "payload": format!("j{i}")}),
            );
            let res = app.clone().oneshot(req).await.unwrap();
            let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
            ids.push(body["id"].as_str().unwrap().to_string());
        }

        state
            .store
            .take_next_n_jobs(now, &HashSet::new(), 2)
            .await
            .unwrap();

        // Bulk complete with 1 valid + 1 bogus ID.
        let req = json_request(
            "POST",
            "/jobs/success",
            &serde_json::json!({"ids": [ids[0], "bogus_id"]}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let not_found = body["not_found"].as_array().unwrap();
        assert_eq!(not_found.len(), 1);
        assert_eq!(not_found[0], "bogus_id");
    }

    #[tokio::test]
    async fn bulk_success_valid_jobs_committed_despite_not_found() {
        // Use non-zero retention so the completed job remains visible via GET.
        let mut config = store::StorageConfig::default();
        config.default_completed_retention_ms = 3_600_000;
        let (clock, state) = test_app_state_with_config(config);
        let now = clock.load(Ordering::Relaxed);
        let state = Arc::new(state);
        let app = app(state.clone());

        // Enqueue and take a job.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "x"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let job_id = body["id"].as_str().unwrap().to_string();

        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Bulk complete with the valid job + a bogus one.
        let req = json_request(
            "POST",
            "/jobs/success",
            &serde_json::json!({"ids": [job_id, "bogus"]}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);

        // Verify the valid job is now Completed.
        let req = empty_request("GET", &format!("/jobs/{job_id}"));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["status"], "completed");
    }

    #[tokio::test]
    async fn bulk_success_returns_400_for_empty_ids() {
        let req = json_request("POST", "/jobs/success", &serde_json::json!({"ids": []}));
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn bulk_success_returns_422_for_pending_jobs() {
        let (_, app) = test_state_and_app();

        // Enqueue 2 jobs but don't take them.
        let mut ids = Vec::new();
        for i in 0..2 {
            let req = json_request(
                "POST",
                "/jobs",
                &serde_json::json!({"type": "test", "queue": "default", "payload": format!("j{i}")}),
            );
            let res = app.clone().oneshot(req).await.unwrap();
            let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
            ids.push(body["id"].as_str().unwrap().to_string());
        }

        // Bulk complete — should report all as not_found (they're pending, not in-flight).
        let req = json_request("POST", "/jobs/success", &serde_json::json!({"ids": ids}));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let not_found = body["not_found"].as_array().unwrap();
        assert_eq!(not_found.len(), 2);
    }

    #[tokio::test]
    async fn bulk_success_msgpack_round_trip() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        // Enqueue and take 2 jobs.
        let mut ids = Vec::new();
        for i in 0..2 {
            let req = json_request(
                "POST",
                "/jobs",
                &serde_json::json!({"type": "test", "queue": "default", "payload": format!("j{i}")}),
            );
            let res = app.clone().oneshot(req).await.unwrap();
            let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
            ids.push(body["id"].as_str().unwrap().to_string());
        }

        state
            .store
            .take_next_n_jobs(now, &HashSet::new(), 2)
            .await
            .unwrap();

        // Send bulk success via MsgPack.
        #[derive(Serialize)]
        struct MsgPackBulkSuccess {
            ids: Vec<String>,
        }

        let body = rmp_serde::to_vec_named(&MsgPackBulkSuccess { ids: ids.clone() }).unwrap();
        let req = Request::builder()
            .method("POST")
            .uri("/jobs/success")
            .header("content-type", "application/msgpack")
            .body(Body::from(body))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn bulk_success_deduplicates_ids() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        // Enqueue and take 1 job.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({"type": "test", "queue": "default", "payload": "dup"}),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let id = body["id"].as_str().unwrap().to_string();

        state
            .store
            .take_next_job(now, &HashSet::new())
            .await
            .unwrap();

        // Send the same ID twice — should succeed, not infinite-loop.
        let req = json_request(
            "POST",
            "/jobs/success",
            &serde_json::json!({"ids": [id, id]}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    // --- Unique jobs ---

    fn pro_app() -> Router {
        let (_, state) = test_app_state();
        *state.license.write().unwrap() = License::Licensed {
            licensee_id: "test".into(),
            licensee_name: "Test".into(),
            tier: crate::license::Tier::Pro,
            expires_at: u64::MAX,
        };
        app(Arc::new(state))
    }

    #[tokio::test]
    async fn unique_enqueue_returns_201_for_new_job() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "task", "queue": "q", "payload": {},
                "unique_key": "k1"
            }),
        );
        let res = pro_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["duplicate"], false);
        assert_eq!(body["unique_key"], "k1");
        assert_eq!(body["unique_while"], "queued");
    }

    #[tokio::test]
    async fn unique_enqueue_returns_200_for_duplicate() {
        let app = pro_app();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "task", "queue": "q", "payload": {},
                "unique_key": "k1"
            }),
        );
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let first: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();

        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "task", "queue": "q", "payload": {},
                "unique_key": "k1"
            }),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["duplicate"], true);
        assert_eq!(body["id"], first["id"]);
    }

    #[tokio::test]
    async fn unique_enqueue_returns_403_on_free_tier() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "task", "queue": "q", "payload": {},
                "unique_key": "k1"
            }),
        );
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(
            body["error"].as_str().unwrap().contains("pro license"),
            "error should mention pro license, got: {}",
            body["error"]
        );
    }

    #[tokio::test]
    async fn unique_while_without_key_returns_400() {
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "task", "queue": "q", "payload": {},
                "unique_while": "active"
            }),
        );
        let res = pro_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unique_bulk_returns_200_when_all_duplicates() {
        let app = pro_app();

        // First: create a job.
        let req = json_request(
            "POST",
            "/jobs",
            &serde_json::json!({
                "type": "task", "queue": "q", "payload": {},
                "unique_key": "k1"
            }),
        );
        app.clone().oneshot(req).await.unwrap();

        // Bulk: all duplicates of the same key.
        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "task", "queue": "q", "payload": {}, "unique_key": "k1"},
                    {"type": "task", "queue": "q", "payload": {}, "unique_key": "k1"},
                ]
            }),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert!(jobs[0]["duplicate"].as_bool().unwrap());
        assert!(jobs[1]["duplicate"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn unique_bulk_returns_201_when_mixed() {
        let app = pro_app();

        let req = json_request(
            "POST",
            "/jobs/bulk",
            &serde_json::json!({
                "jobs": [
                    {"type": "task", "queue": "q", "payload": {}, "unique_key": "k1"},
                    {"type": "task", "queue": "q", "payload": {}, "unique_key": "k1"},
                    {"type": "task", "queue": "q", "payload": {}, "unique_key": "k2"},
                ]
            }),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert!(!jobs[0]["duplicate"].as_bool().unwrap());
        assert!(jobs[1]["duplicate"].as_bool().unwrap());
        assert!(!jobs[2]["duplicate"].as_bool().unwrap());
    }

    // --- DELETE /jobs/{id} ---

    #[tokio::test]
    async fn delete_job_returns_204() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = empty_request("DELETE", &format!("/jobs/{}", job.id));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn delete_job_returns_404_for_missing_job() {
        let req = empty_request("DELETE", "/jobs/nonexistent");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_job_actually_removes_the_job() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("test", "default", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = empty_request("DELETE", &format!("/jobs/{}", job.id));
        app.clone().oneshot(req).await.unwrap();

        // GET should now return 404.
        let req = empty_request("GET", &format!("/jobs/{}", job.id));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    // --- GET /jobs?id= ---

    #[tokio::test]
    async fn list_jobs_with_id_filter() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        let j1 = state
            .store
            .enqueue(now, EnqueueOptions::new("a", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        let _j2 = state
            .store
            .enqueue(now, EnqueueOptions::new("b", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();

        let req = empty_request("GET", &format!("/jobs?id={}", j1.id));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        let jobs = body["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["id"], j1.id);
    }

    #[tokio::test]
    async fn list_jobs_with_invalid_id_returns_400() {
        let req = empty_request("GET", "/jobs?id=not-a-valid-id");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_jobs_with_nonexistent_id_returns_empty() {
        let req = empty_request("GET", "/jobs?id=0000000000000000000000000");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["jobs"].as_array().unwrap().is_empty());
    }

    // --- DELETE /jobs (bulk) ---

    #[tokio::test]
    async fn bulk_delete_returns_count() {
        let (state, app) = test_state_and_app();

        state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let req = empty_request("DELETE", "/jobs?queue=q");
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["deleted"], 1);
    }

    #[tokio::test]
    async fn bulk_delete_by_id_returns_count() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = empty_request("DELETE", &format!("/jobs?id={}", job.id));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["deleted"], 1);
    }

    #[tokio::test]
    async fn bulk_delete_with_invalid_filter_returns_400() {
        let req = empty_request("DELETE", "/jobs?filter=.x%20====");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn bulk_delete_with_invalid_id_returns_400() {
        let req = empty_request("DELETE", "/jobs?id=not-valid");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn bulk_delete_actually_removes_jobs() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        state
            .store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap();
        state
            .store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap();

        let req = empty_request("DELETE", "/jobs");
        app.clone().oneshot(req).await.unwrap();

        // GET should return empty.
        let req = empty_request("GET", "/jobs");
        let res = app.oneshot(req).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert!(body["jobs"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bulk_delete_no_matches_returns_zero() {
        let req = empty_request("DELETE", "/jobs?queue=nonexistent");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["deleted"], 0);
    }

    // --- GET /jobs/count ---

    #[tokio::test]
    async fn count_jobs_returns_zero_for_empty_store() {
        let req = empty_request("GET", "/jobs/count");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["count"], 0);
    }

    #[tokio::test]
    async fn count_jobs_returns_total_count() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        for i in 0..5 {
            state
                .store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(i)))
                .await
                .unwrap();
        }

        let req = empty_request("GET", "/jobs/count");
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["count"], 5);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_status() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        // Enqueue 3 ready jobs.
        for i in 0..3 {
            state
                .store
                .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(i)))
                .await
                .unwrap();
        }

        // Take one (moves to in_flight).
        state
            .store
            .take_next_job(now, &std::collections::HashSet::new())
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/count?status=ready");
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["count"], 2);
    }

    #[tokio::test]
    async fn count_jobs_filters_by_queue() {
        let (state, app) = test_state_and_app();
        let now = crate::time::now_millis();

        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("t", "emails", serde_json::json!(1)),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("t", "emails", serde_json::json!(2)),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("t", "reports", serde_json::json!(3)),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/jobs/count?queue=emails");
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["count"], 2);
    }

    #[tokio::test]
    async fn count_jobs_rejects_unknown_params() {
        let req = empty_request("GET", "/jobs/count?bogus=true");
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    // --- PATCH /jobs/{id} ---

    #[tokio::test]
    async fn patch_job_returns_200_with_updated_job() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"retry_limit": 10}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body["retry_limit"], 10);
        assert_eq!(body["id"], job.id);
    }

    #[tokio::test]
    async fn patch_job_returns_404_for_missing_job() {
        let req = json_request("PATCH", "/jobs/nonexistent", &serde_json::json!({}));
        let res = test_app().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn patch_job_returns_422_for_completed_job() {
        let (_, state) = test_app_state_with_config({
            let mut c = store::StorageConfig::default();
            c.default_completed_retention_ms = 60_000;
            c
        });
        state.store.rebuild_indexes().await.unwrap();
        let state = Arc::new(state);
        let app = app(state.clone());
        let now = crate::time::now_millis();

        let job = state
            .store
            .enqueue(now, EnqueueOptions::new("t", "q", serde_json::json!(null)))
            .await
            .unwrap()
            .into_job();
        state
            .store
            .take_next_job(now, &std::collections::HashSet::new())
            .await
            .unwrap();
        state.store.mark_completed(now, &job.id).await.unwrap();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"retry_limit": 10}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn patch_job_clears_field_with_null() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)).retry_limit(5),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"retry_limit": null}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let text = response_body(res).await;
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        // retry_limit should be absent (skip_serializing_if = None) or null.
        assert!(
            !body.as_object().unwrap().contains_key("retry_limit") || body["retry_limit"].is_null(),
            "expected retry_limit to be absent or null, got: {text}"
        );
    }

    #[tokio::test]
    async fn patch_job_updates_queue_and_priority() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q1", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"queue": "q2", "priority": 42}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let text = response_body(res).await;
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["queue"], "q2");
        assert_eq!(body["priority"], 42);
    }

    #[tokio::test]
    async fn patch_job_rejects_empty_queue() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"queue": ""}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn patch_job_rejects_queue_with_comma() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"queue": "a,b"}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn patch_job_rejects_null_queue() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"queue": null}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn patch_job_rejects_null_priority() {
        let (state, app) = test_state_and_app();

        let job = state
            .store
            .enqueue(
                crate::time::now_millis(),
                EnqueueOptions::new("t", "q", serde_json::json!(null)),
            )
            .await
            .unwrap()
            .into_job();

        let req = json_request(
            "PATCH",
            &format!("/jobs/{}", job.id),
            &serde_json::json!({"priority": null}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // --- PATCH /jobs (bulk) tests ---

    #[tokio::test]
    async fn bulk_patch_jobs_updates_matching() {
        let (state, app) = test_state_and_app();

        for _ in 0..3 {
            state
                .store
                .enqueue(
                    crate::time::now_millis(),
                    EnqueueOptions::new("t", "q1", serde_json::json!(null)),
                )
                .await
                .unwrap();
        }

        let req = json_request(
            "PATCH",
            "/jobs?queue=q1",
            &serde_json::json!({"queue": "q2"}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let text = response_body(res).await;
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["patched"], 3);
    }

    #[tokio::test]
    async fn bulk_patch_jobs_rejects_terminal_status_filter() {
        let (_, app) = test_state_and_app();

        let req = json_request(
            "PATCH",
            "/jobs?status=completed",
            &serde_json::json!({"priority": 1}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn bulk_patch_jobs_rejects_null_queue() {
        let (_, app) = test_state_and_app();

        let req = json_request("PATCH", "/jobs", &serde_json::json!({"queue": null}));
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn bulk_patch_jobs_returns_zero_for_no_matches() {
        let (_, app) = test_state_and_app();

        let req = json_request(
            "PATCH",
            "/jobs?queue=nonexistent",
            &serde_json::json!({"priority": 1}),
        );
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let text = response_body(res).await;
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["patched"], 0);
    }

    #[tokio::test]
    async fn list_queues_empty_store() {
        let req = empty_request("GET", "/queues");
        let res = test_app().oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(body, serde_json::json!({"queues": []}));
    }

    #[tokio::test]
    async fn list_queues_returns_distinct_queues() {
        let (state, app) = test_state_and_app();
        let now = (state.clock)();

        use crate::store::EnqueueOptions;
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "payments/invoices", serde_json::json!(null)),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "payments/refunds", serde_json::json!(null)),
            )
            .await
            .unwrap();
        state
            .store
            .enqueue(
                now,
                EnqueueOptions::new("test", "webhooks", serde_json::json!(null)),
            )
            .await
            .unwrap();

        let req = empty_request("GET", "/queues");
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_body(res).await).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"queues": ["payments/invoices", "payments/refunds", "webhooks"]})
        );
    }
}
