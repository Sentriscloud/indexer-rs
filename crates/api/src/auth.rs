//! Optional bearer-token auth middleware.
//!
//! When `INDEXER_API_BEARER_TOKEN` env is set on the API binary, every
//! route except `/health` requires `Authorization: Bearer <token>`. When
//! unset (default), all routes are open — the operator runs Caddy out front
//! and gates at the edge instead.
//!
//! Comparison is constant-time via `subtle::ConstantTimeEq` to defeat
//! timing attacks on token shape.

use axum::Json;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Per-process auth state. Cloned cheaply via Arc.
#[derive(Clone)]
pub struct AuthState {
    /// None => auth disabled (open API).
    expected: Option<Arc<Vec<u8>>>,
}

impl AuthState {
    /// Construct from an optional bearer token. None disables auth entirely.
    pub fn new(token: Option<String>) -> Self {
        Self {
            expected: token.map(|t| Arc::new(t.into_bytes())),
        }
    }

    /// True when the API is running open (no token configured).
    pub fn is_open(&self) -> bool {
        self.expected.is_none()
    }
}

/// Axum middleware function. Pass via `from_fn_with_state(state, require_bearer)`.
pub async fn require_bearer(
    axum::extract::State(state): axum::extract::State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    // Always allow /health — k8s/docker probes shouldn't need creds.
    if req.uri().path() == "/health" {
        return next.run(req).await;
    }
    let Some(expected) = &state.expected else {
        return next.run(req).await;
    };
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let presented = header.and_then(|h| h.strip_prefix("Bearer ").map(str::trim));
    match presented {
        Some(token) if token.as_bytes().ct_eq(expected).into() => next.run(req).await,
        _ => unauthorized(),
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            axum::http::header::WWW_AUTHENTICATE,
            "Bearer realm=\"sentrix-indexer\"",
        )],
        Json(json!({ "error": "unauthorized" })),
    )
        .into_response()
}
