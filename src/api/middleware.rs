// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Shared middleware for the API layer.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Generate a request logging middleware with a compile-time target.
macro_rules! request_logger {
    ($name:ident, $target:expr) => {
        pub async fn $name(req: Request, next: Next) -> Response {
            let method = req.method().clone();
            let uri = req.uri().clone();
            let start = std::time::Instant::now();

            let res = next.run(req).await;

            let elapsed = start.elapsed();
            tracing::info!(
                target: $target,
                method = %method,
                path = %uri,
                status = res.status().as_u16(),
                latency_ms = format_args!("{:.3}", elapsed.as_secs_f64() * 1000.0),
                "request"
            );

            res
        }
    };
}

request_logger!(primary_request_logging, "zizq::api::primary");
request_logger!(admin_request_logging, "zizq::api::admin");
