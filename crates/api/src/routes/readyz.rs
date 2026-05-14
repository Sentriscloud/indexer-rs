//! `GET /readyz` — readiness check (PG + cache reachable). Distinct from
//! `/health` which is liveness-only (process is up). Operators wire `/readyz`
//! into the orchestrator's traffic-routing decision (k8s readinessProbe,
//! HAProxy / Caddy active health-check). When this returns 503, the LB
//! drains the instance.
//!
//! Checks:
//!  - PG: `SELECT 1` with 1s timeout
//!  - Cache: `PING`-equivalent via a no-op GET; only checked when configured
//!
//! Response shape:
//! ```json
//! { "ok": true,  "checks": { "pg": "ok", "cache": "ok" } }
//! { "ok": false, "checks": { "pg": "ok", "cache": "down: redis: ..." } }
//! ```
//! 200 when all critical checks pass; 503 otherwise.

use crate::SharedState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::time::Duration;

async fn handler(State(state): State<SharedState>) -> (StatusCode, Json<Value>) {
    // Static strings on the wire — raw sqlx/redis errors leak schema or
    // connection details (audit 2026-05-13). Detail goes to tracing.
    let pg = match tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&state.pool),
    )
    .await
    {
        Ok(Ok(_)) => "ok".to_string(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "readyz: pg probe failed");
            "down: pg error".to_string()
        }
        Err(_) => "down: timeout".to_string(),
    };

    let cache = match &state.cache {
        None => "disabled".to_string(),
        Some(c) => match c.get::<Value>("__readyz_probe__").await {
            // Open / connection error / etc. → down. Cache miss is fine.
            Ok(_) => "ok".to_string(),
            Err(indexer_cache::CacheError::Open) => "down: circuit breaker open".to_string(),
            Err(e) => {
                tracing::error!(error = %e, "readyz: cache probe failed");
                "down: cache error".to_string()
            }
        },
    };

    let pg_ok = pg == "ok";
    // Cache is non-critical: down doesn't fail readiness, just degrades cost.
    let body = json!({
        "ok": pg_ok,
        "checks": { "pg": pg, "cache": cache }
    });
    let code = if pg_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(body))
}

/// Router for `/readyz`.
pub fn router() -> Router<SharedState> {
    Router::new().route("/readyz", get(handler))
}
