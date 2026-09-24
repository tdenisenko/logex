//! Browser-origin admission shared by normal and maintenance HTTP routes.
use std::{fmt, str::FromStr};

use axum::http::{
    HeaderMap, Uri,
    header::{HOST, ORIGIN},
};
use serde::{Deserialize, Deserializer};
use url::Url;

// Bounds parsing independently of the transport's aggregate header limit.
const MAX_ORIGIN_BYTES: usize = 2048;

/// A normalized HTTP(S) browser origin, including its effective port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowserOrigin(String);

impl FromStr for BrowserOrigin {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid =
            || "expected an HTTP(S) origin without credentials, path, query or fragment".to_owned();
        if value.len() > MAX_ORIGIN_BYTES
            || value
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            || value.contains(['\\', '?', '#'])
        {
            return Err(invalid());
        }
        let (scheme, rest) = value.split_once("://").ok_or_else(invalid)?;
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(invalid());
        }
        // Check the unnormalized shape first: URL parsing may otherwise turn
        // /a/.., backslashes or other non-origin URLs into a root URL.
        let authority = rest.strip_suffix('/').unwrap_or(rest);
        if authority.is_empty() || authority.contains(['/', '@']) || authority.ends_with(':') {
            return Err(invalid());
        }
        let url = Url::parse(value).map_err(|_| invalid())?;
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid());
        }
        Ok(Self(url.origin().ascii_serialization()))
    }
}

impl fmt::Display for BrowserOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BrowserOrigin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

fn direct_http_origin(headers: &HeaderMap, uri: &Uri) -> Option<BrowserOrigin> {
    // This listener speaks HTTP. TLS termination must use the explicit allowlist;
    // neither request-supplied URI schemes nor forwarding headers establish TLS.
    if uri.scheme_str().is_some_and(|scheme| scheme != "http") {
        return None;
    }
    let from_authority = |authority: &str| -> Option<BrowserOrigin> {
        if authority.len() > MAX_ORIGIN_BYTES || authority.contains('/') {
            return None;
        }
        format!("http://{authority}").parse().ok()
    };
    let mut hosts = headers.get_all(HOST).iter();
    let host = hosts.next();
    if hosts.next().is_some() {
        return None;
    }
    let host = match host {
        Some(host) => Some(from_authority(host.to_str().ok()?)?),
        None => None,
    };
    let authority = match uri.authority() {
        Some(authority) => Some(from_authority(authority.as_str())?),
        None => None,
    };
    match (host, authority) {
        (Some(host), Some(authority)) if host == authority => Some(host),
        (Some(_), Some(_)) => None,
        (Some(origin), None) | (None, Some(origin)) => Some(origin),
        (None, None) => None,
    }
}

pub(crate) fn request_origin_is_allowed(
    headers: &HeaderMap,
    uri: &Uri,
    allowed: &[BrowserOrigin],
) -> bool {
    let mut origins = headers.get_all(ORIGIN).iter();
    let Some(value) = origins.next() else {
        // Native clients are authenticated separately and need not send Origin.
        return true;
    };
    if origins.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(origin) = value.parse::<BrowserOrigin>() else {
        return false;
    };
    if allowed.is_empty() {
        direct_http_origin(headers, uri).is_some_and(|direct| direct == origin)
    } else {
        // Explicit origins replace the direct default and remain meaningful when
        // a trusted deployment's reverse proxy rewrites the backend Host.
        allowed.contains(&origin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn origins_normalize_host_scheme_ipv6_and_effective_port() {
        for (input, expected) in [
            ("HTTP://EXAMPLE.COM:80/", "http://example.com"),
            ("https://example.com:443", "https://example.com"),
            ("http://[0:0:0:0:0:0:0:1]:80", "http://[::1]"),
            ("https://example.com:8443", "https://example.com:8443"),
            ("https://bücher.example", "https://xn--bcher-kva.example"),
        ] {
            let origin: BrowserOrigin = input.parse().unwrap();
            assert_eq!(origin.to_string(), expected);
            assert_eq!(origin, expected.parse().unwrap());
        }
        assert_ne!(
            "http://example.com".parse::<BrowserOrigin>().unwrap(),
            "https://example.com".parse().unwrap()
        );
        assert_ne!(
            "http://example.com".parse::<BrowserOrigin>().unwrap(),
            "http://example.com:81".parse().unwrap()
        );
    }

    #[test]
    fn origin_parser_rejects_non_origin_urls_and_opaque_values() {
        for input in [
            "",
            "null",
            "*",
            "file:///tmp",
            "data:text/plain,x",
            "ws://example.com",
            "http:example.com",
            "http:///example.com",
            "http://",
            "http://example.com:",
            "http://user@example.com",
            "http://:password@example.com",
            "http://@example.com",
            "http://example.com/a",
            "http://example.com/a/..",
            "http://example.com//",
            "http://example.com?",
            "http://example.com#",
            "http://example.com\\",
            " http://example.com",
            "http://example.com\t",
            "http://exa\nmple.com",
            "http://example.com:65536",
            "http://[::1",
            "http://one http://two",
            "http://one,http://two",
        ] {
            assert!(input.parse::<BrowserOrigin>().is_err(), "{input:?}");
        }
        assert!(
            format!("http://{}", "a".repeat(MAX_ORIGIN_BYTES))
                .parse::<BrowserOrigin>()
                .is_err()
        );
        assert!(serde_json::from_str::<BrowserOrigin>("\"null\"").is_err());
        assert_eq!(
            serde_json::from_str::<BrowserOrigin>("\"https://EXAMPLE.com:443\"").unwrap(),
            "https://example.com".parse().unwrap()
        );
    }

    fn headers(origin: &str, host: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, origin.parse().unwrap());
        if let Some(host) = host {
            headers.insert(HOST, host.parse().unwrap());
        }
        headers
    }

    #[test]
    fn direct_origin_requires_unambiguous_http_authority() {
        let relative: Uri = "/query/cancel".parse().unwrap();
        for (origin, host, expected) in [
            ("http://example.com", "EXAMPLE.com:80", true),
            ("http://[::1]:8577", "[::1]:8577", true),
            ("https://example.com", "example.com", false),
            ("http://example.com:81", "example.com", false),
            ("http://other.com", "example.com", false),
            ("http://example.com", "user@example.com", false),
            ("http://example.com", "example.com/", false),
        ] {
            assert_eq!(
                request_origin_is_allowed(&headers(origin, Some(host)), &relative, &[]),
                expected
            );
        }
        assert!(!request_origin_is_allowed(
            &headers("http://example.com", None),
            &relative,
            &[]
        ));
        let absolute: Uri = "http://example.com/query".parse().unwrap();
        assert!(request_origin_is_allowed(
            &headers("http://example.com", None),
            &absolute,
            &[]
        ));
        assert!(request_origin_is_allowed(
            &headers("http://example.com", Some("EXAMPLE.com:80")),
            &absolute,
            &[]
        ));
        assert!(!request_origin_is_allowed(
            &headers("http://example.com", Some("other.com")),
            &absolute,
            &[]
        ));
        let secure: Uri = "https://example.com/query".parse().unwrap();
        assert!(!request_origin_is_allowed(
            &headers("https://example.com", Some("example.com")),
            &secure,
            &[]
        ));
        let mut duplicate = headers("http://example.com", Some("example.com"));
        duplicate.append(HOST, HeaderValue::from_static("example.com"));
        assert!(!request_origin_is_allowed(&duplicate, &relative, &[]));
    }

    #[test]
    fn allowlist_replaces_default_and_ignores_forwarding_headers() {
        let uri: Uri = "/ws".parse().unwrap();
        let allowed = vec![
            "https://dashboard.example".parse().unwrap(),
            "https://other.example:8443".parse().unwrap(),
        ];
        for origin in ["https://dashboard.example", "https://other.example:8443"] {
            assert!(request_origin_is_allowed(
                &headers(origin, Some("backend:8577")),
                &uri,
                &allowed
            ));
        }
        assert!(!request_origin_is_allowed(
            &headers("http://backend:8577", Some("backend:8577")),
            &uri,
            &allowed
        ));
        let mut forwarded = headers("https://dashboard.example", Some("backend:8577"));
        forwarded.insert(
            "forwarded",
            "host=dashboard.example;proto=https".parse().unwrap(),
        );
        forwarded.insert("x-forwarded-host", "dashboard.example".parse().unwrap());
        forwarded.insert("x-forwarded-proto", "https".parse().unwrap());
        assert!(!request_origin_is_allowed(&forwarded, &uri, &[]));
        forwarded.append(ORIGIN, "https://dashboard.example".parse().unwrap());
        assert!(!request_origin_is_allowed(&forwarded, &uri, &allowed));
        assert!(request_origin_is_allowed(&HeaderMap::new(), &uri, &allowed));
    }

    fn state() -> (tempfile::TempDir, std::sync::Arc<crate::AppState>) {
        let temp = tempfile::tempdir().unwrap();
        let storage =
            logex_storage::PartitionManager::open(logex_storage::PartitionManagerConfig {
                data_dir: temp.path().to_owned(),
                ..Default::default()
            })
            .unwrap();
        let state = std::sync::Arc::new(crate::AppState::new(
            storage,
            None,
            logex_types::SyncStatus::default(),
        ));
        (temp, state)
    }

    #[tokio::test]
    async fn normal_and_maintenance_routes_share_auth_first_origin_policy() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let (_temp, state) = state();
        let config = crate::HttpServerConfig {
            dashboard_password: Some("secret".to_owned()),
            allowed_origins: vec![
                "https://dashboard.example".parse().unwrap(),
                "https://other.example:8443".parse().unwrap(),
            ],
            ..Default::default()
        };
        let maintenance =
            std::sync::Arc::new(crate::MaintenanceState::new(crate::RepairPhase::Inspecting));
        for (router, accepted) in [
            (
                crate::build_router_with_config(state.clone(), config.clone()),
                StatusCode::OK,
            ),
            (
                crate::build_maintenance_router(maintenance, config),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            for (origin, credentials, expected) in [
                ("https://foreign.example", None, StatusCode::UNAUTHORIZED),
                (
                    "https://foreign.example",
                    Some("Basic invalid"),
                    StatusCode::UNAUTHORIZED,
                ),
                (
                    "https://foreign.example",
                    Some("Basic bG9nZXg6c2VjcmV0"),
                    StatusCode::FORBIDDEN,
                ),
                (
                    "http://backend:8577",
                    Some("Basic bG9nZXg6c2VjcmV0"),
                    StatusCode::FORBIDDEN,
                ),
                (
                    "null",
                    Some("Basic bG9nZXg6c2VjcmV0"),
                    StatusCode::FORBIDDEN,
                ),
                (
                    "https://dashboard.example",
                    Some("Basic bG9nZXg6c2VjcmV0"),
                    accepted,
                ),
                (
                    "https://other.example:8443",
                    Some("Basic bG9nZXg6c2VjcmV0"),
                    accepted,
                ),
            ] {
                let mut request = Request::builder()
                    .method("POST")
                    .uri("/query/cancel")
                    .header(HOST, "backend:8577")
                    .header(ORIGIN, origin);
                if let Some(credentials) = credentials {
                    request = request.header("authorization", credentials);
                }
                let response = router
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    expected,
                    "{origin}, credentials={credentials:?}"
                );
            }
            let response = router
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .header(ORIGIN, "null")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), accepted);
        }
        assert_eq!(state.query_memory.used(), 0);
        assert!(state.storage_failure().is_none());
    }

    #[tokio::test]
    async fn explicit_proxy_origin_can_cancel_while_direct_origin_cannot() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let (_temp, state) = state();
        let router = crate::build_router_with_config(
            state.clone(),
            crate::HttpServerConfig {
                dashboard_enabled: false,
                allowed_origins: vec!["https://dashboard.example".parse().unwrap()],
                ..Default::default()
            },
        );
        let active = state.query_control.start().unwrap();
        for (origin, expected, canceled) in [
            ("http://backend:8577", StatusCode::FORBIDDEN, false),
            ("https://dashboard.example", StatusCode::OK, true),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/query/cancel")
                        .header(HOST, "backend:8577")
                        .header(ORIGIN, origin)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            assert_eq!(active.was_canceled(), canceled);
        }
    }

    #[tokio::test]
    async fn complete_router_checks_origin_before_real_websocket_upgrade() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let (_temp, state) = state();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = crate::build_router_with_config(
            state,
            crate::HttpServerConfig {
                dashboard_password: Some("secret".to_owned()),
                ..Default::default()
            },
        );
        struct Server(tokio::task::JoinHandle<()>);
        impl Drop for Server {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _server = Server(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
        let same_origin = format!("http://{address}");
        for (origin, authenticated, expected) in [
            (Some("https://foreign.example"), false, "401"),
            (Some("https://foreign.example"), true, "403"),
            (Some(same_origin.as_str()), true, "101"),
            (None, true, "101"),
        ] {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            let mut request = format!(
                "GET /ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            );
            if let Some(origin) = origin {
                request.push_str(&format!("Origin: {origin}\r\n"));
            }
            if authenticated {
                request.push_str("Authorization: Basic bG9nZXg6c2VjcmV0\r\n");
            }
            request.push_str("\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                tokio::io::BufReader::new(stream).read_line(&mut response),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                response.split_whitespace().nth(1),
                Some(expected),
                "{response}"
            );
        }
    }
}
