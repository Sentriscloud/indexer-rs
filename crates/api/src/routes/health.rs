//! `GET /health` — liveness check. TS port returned `{ ok: true }`; we
//! mirror exactly so any dashboard hitting the endpoint sees the same
//! shape across the dual-run cutover window.

use crate::SharedState;
use axum::{Json, Router, routing::get};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Health {
    ok: bool,
}

async fn handler() -> Json<Health> {
    Json(Health { ok: true })
}

/// Router for `/health`.
pub fn router() -> Router<SharedState> {
    Router::new().route("/health", get(handler))
}
