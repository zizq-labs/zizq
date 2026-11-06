// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Protection for the admin API against requests made by web pages.
//!
//! The admin API has no authentication of its own, so anything that can
//! reach its port can use it. Browsers complicate that: any page the
//! operator happens to visit can ask the browser to open a WebSocket to
//! `127.0.0.1:8901`, or submit a form to it. CORS does not apply to
//! either, so without a check here such a page could stream job payloads
//! out of the server or delete jobs.
//!
//! Browsers always send an `Origin` header on WebSocket upgrades and on
//! `POST` requests, while non-browser clients (`zizq top`, `zizq backup`,
//! curl) do not. Requests without one are therefore let through. Requests
//! with one must satisfy two rules:
//!
//! 1. The origin must match the host the request was sent to. This stops
//!    a page on another site using the admin API.
//! 2. That host must be `localhost`, an IP address, or a name listed with
//!    `--admin-allowed-hosts`. This stops DNS rebinding, where a page on
//!    the attacker's own domain re-points that domain at `127.0.0.1` so
//!    that its requests appear to come from the same origin.

use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Host names, in addition to `localhost` and IP addresses, that web
/// pages may use to reach the admin API.
#[derive(Debug, Clone, Default)]
pub struct AllowedHosts(Arc<[String]>);

impl AllowedHosts {
    /// Build the list from host names, e.g. `zizq-admin.example.com`.
    pub fn new<I, S>(hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self(
            hosts
                .into_iter()
                .map(|h| h.as_ref().trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .collect(),
        )
    }

    fn permits(&self, hostname: &str) -> bool {
        hostname.eq_ignore_ascii_case("localhost")
            || hostname.parse::<IpAddr>().is_ok()
            || self.0.iter().any(|h| h.eq_ignore_ascii_case(hostname))
    }
}

/// Middleware rejecting admin API requests from web pages that are not
/// permitted to use it. See the module docs for the rules.
pub async fn check_origin(
    State(allowed_hosts): State<AllowedHosts>,
    req: Request,
    next: Next,
) -> Response {
    let Some(origin) = req.headers().get(header::ORIGIN) else {
        return next.run(req).await;
    };

    let origin = origin.to_str().unwrap_or("");
    let authority = request_authority(req.uri(), req.headers()).unwrap_or("");

    if is_permitted(origin, authority, &allowed_hosts) {
        return next.run(req).await;
    }

    tracing::warn!(
        target: "zizq::api::admin",
        origin,
        host = authority,
        "rejected a request from a web page; \
         list the host in --admin-allowed-hosts to permit it"
    );

    (
        StatusCode::FORBIDDEN,
        "Requests from web pages are not permitted from this origin or host.\n",
    )
        .into_response()
}

/// The host and port the request was sent to. HTTP/2 carries it in the
/// `:authority` pseudo-header, which hyper exposes through the URI;
/// HTTP/1.1 carries it in `Host`.
fn request_authority<'a>(uri: &'a Uri, headers: &'a HeaderMap) -> Option<&'a str> {
    uri.authority()
        .map(|a| a.as_str())
        .or_else(|| headers.get(header::HOST)?.to_str().ok())
}

/// Whether a request carrying `origin`, sent to `authority`, may proceed.
fn is_permitted(origin: &str, authority: &str, allowed_hosts: &AllowedHosts) -> bool {
    // Origins are `scheme://host[:port]`, with the port omitted when it is
    // the scheme's default, just as it is in `Host`. Anything else, such as
    // the `null` origin of a sandboxed frame, is rejected.
    let Some(origin_authority) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };

    origin_authority.eq_ignore_ascii_case(authority) && allowed_hosts.permits(hostname(authority))
}

/// The host part of `host[:port]`, without the brackets of an IPv6
/// address.
fn hostname(authority: &str) -> &str {
    if let Some(rest) = authority.strip_prefix('[') {
        rest.split_once(']').map_or(rest, |(host, _)| host)
    } else {
        authority
            .split_once(':')
            .map_or(authority, |(host, _)| host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none() -> AllowedHosts {
        AllowedHosts::default()
    }

    #[test]
    fn same_origin_on_localhost_is_permitted() {
        assert!(is_permitted(
            "http://localhost:8901",
            "localhost:8901",
            &none()
        ));
        assert!(is_permitted(
            "http://LocalHost:8901",
            "localhost:8901",
            &none()
        ));
    }

    #[test]
    fn same_origin_on_an_ip_address_is_permitted() {
        assert!(is_permitted(
            "http://127.0.0.1:8901",
            "127.0.0.1:8901",
            &none()
        ));
        assert!(is_permitted(
            "http://10.1.2.3:8901",
            "10.1.2.3:8901",
            &none()
        ));
        assert!(is_permitted("http://[::1]:8901", "[::1]:8901", &none()));
    }

    #[test]
    fn same_origin_over_https_is_permitted() {
        assert!(is_permitted(
            "https://localhost:8901",
            "localhost:8901",
            &none()
        ));
    }

    #[test]
    fn same_origin_on_a_default_port_is_permitted() {
        assert!(is_permitted("http://127.0.0.1", "127.0.0.1", &none()));
    }

    #[test]
    fn another_site_is_rejected() {
        assert!(!is_permitted(
            "https://evil.example",
            "127.0.0.1:8901",
            &none()
        ));
        assert!(!is_permitted(
            "http://localhost:3000",
            "localhost:8901",
            &none()
        ));
    }

    #[test]
    fn a_null_origin_is_rejected() {
        assert!(!is_permitted("null", "127.0.0.1:8901", &none()));
    }

    #[test]
    fn a_missing_host_is_rejected() {
        assert!(!is_permitted("http://127.0.0.1:8901", "", &none()));
    }

    #[test]
    fn an_unlisted_host_name_is_rejected_even_from_the_same_origin() {
        // DNS rebinding: the attacker's own domain, re-pointed at the
        // server, makes the request look same-origin.
        assert!(!is_permitted(
            "http://evil.example:8901",
            "evil.example:8901",
            &none()
        ));
    }

    #[test]
    fn a_listed_host_name_is_permitted() {
        let allowed = AllowedHosts::new(["Zizq-Admin.example.com"]);
        assert!(is_permitted(
            "https://zizq-admin.example.com",
            "zizq-admin.example.com",
            &allowed
        ));
        assert!(!is_permitted(
            "https://other.example.com",
            "other.example.com",
            &allowed
        ));
    }

    #[test]
    fn a_listed_host_name_must_still_match_the_origin() {
        let allowed = AllowedHosts::new(["zizq-admin.example.com"]);
        assert!(!is_permitted(
            "https://evil.example",
            "zizq-admin.example.com",
            &allowed
        ));
    }

    #[test]
    fn hostname_strips_the_port() {
        assert_eq!(hostname("localhost:8901"), "localhost");
        assert_eq!(hostname("localhost"), "localhost");
        assert_eq!(hostname("[::1]:8901"), "::1");
        assert_eq!(hostname("[::1]"), "::1");
    }

    #[test]
    fn allowed_hosts_ignores_blank_entries() {
        let allowed = AllowedHosts::new(["", " "]);
        assert!(allowed.0.is_empty());
    }
}
