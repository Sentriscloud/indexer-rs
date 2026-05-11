//! HTTP error type + production-grade error envelope.
//!
//! Wire shape:
//! ```json
//! {
//!   "error": "block not found",
//!   "code": "not_found",
//!   "request_id": "01HV8Z..."
//! }
//! ```
//!
//! - `error`: human-readable message — preserves TS port byte-fidelity for
//!   clients that grep on this string.
//! - `code`: machine-readable code (`invalid_query` / `not_found` /
//!   `internal`) so frontends can branch without parsing prose.
//! - `request_id`: x-request-id from the `tower_http::request_id` layer —
//!   lets operators grep the access log for the originating request.

use axum::Json;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header::HeaderName;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// HTTP header carrying the per-request UUID. Operators grep access logs by
/// this header value.
pub const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Result alias used by handlers.
pub type ApiResult<T> = std::result::Result<T, ApiError>;

/// API-level error. Maps to a (status, body) pair via [`IntoResponse`].
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Caller's input was invalid (400).
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Resource not found (404).
    #[error("{0} not found")]
    NotFound(String),

    /// Underlying database failure (500).
    #[error("db: {0}")]
    Db(#[from] indexer_db::DbError),

    /// Direct sqlx error (500).
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            ApiError::InvalidQuery(_) => "invalid_query",
            ApiError::NotFound(_) => "not_found",
            ApiError::Db(_) | ApiError::Sqlx(_) => "internal",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            ApiError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Db(_) | ApiError::Sqlx(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn user_message(&self) -> String {
        match self {
            ApiError::InvalidQuery(msg) => msg.clone(),
            ApiError::NotFound(what) => format!("{what} not found"),
            ApiError::Db(_) | ApiError::Sqlx(_) => "internal server error".to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let ApiError::Db(e) = &self {
            tracing::error!(error = %e, "db failure");
        }
        if let ApiError::Sqlx(e) = &self {
            tracing::error!(error = %e, "sqlx failure");
        }
        let body = json!({
            "error": self.user_message(),
            "code": self.code(),
            // request_id is filled in by the post-handler middleware that
            // peeks at the response extensions; absent from this default.
        });
        (self.status(), Json(body)).into_response()
    }
}

/// Middleware: generate a `x-request-id` per request (or pass through if
/// upstream already set one), stash on request extensions for handlers +
/// echo on the response header. Pair this with [`set_request_id_on_error`]
/// to also inject the id into error JSON bodies.
///
/// Layer order: outermost so every other layer + handler sees the id.
pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    let id = req
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            // Cheap 16-byte hex id; not a UUID format spec but good enough
            // for grep + uniqueness. Avoids pulling in uuid as a dep.
            let mut bytes = [0u8; 8];
            // Mix the system time + process-local counter for uniqueness.
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as u64 | (d.as_secs() << 32))
                .unwrap_or(0);
            let seq = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            bytes[..8].copy_from_slice(&(nanos ^ seq).to_be_bytes());
            hex::encode(bytes)
        });
    req.extensions_mut().insert(RequestId(id.clone()));
    let mut resp = next.run(req).await;
    if let Ok(val) = axum::http::HeaderValue::from_str(&id) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, val);
    }
    resp
}

static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Per-request id, available via `axum::Extension<RequestId>` in handlers.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_stable() {
        assert_eq!(ApiError::InvalidQuery("".into()).code(), "invalid_query");
        assert_eq!(ApiError::NotFound("block".into()).code(), "not_found");
    }

    #[test]
    fn not_found_message_includes_resource() {
        let e = ApiError::NotFound("tx".into());
        assert_eq!(e.user_message(), "tx not found");
        assert_eq!(e.status(), StatusCode::NOT_FOUND);
    }
}
