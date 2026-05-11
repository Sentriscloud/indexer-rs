//! `GET /metrics` — Prometheus exposition. Aggregates every `metrics::counter!`
//! / `histogram!` / `gauge!` call across the API + middleware stack.
//!
//! Built once at app boot via `PrometheusBuilder::install_recorder()`; the
//! handler just renders the global registry. No per-request work.

use axum::Router;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;

async fn handler(State(handle): State<PrometheusHandle>) -> impl IntoResponse {
    let body = handle.render();
    (
        StatusCode::OK,
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        body,
    )
}

/// Router for `/metrics`. Caller passes the `PrometheusHandle` returned by
/// the recorder install (kept alive in `AppState` would be cleaner; kept
/// as router state here so /metrics can be merged into the main router
/// without bloating `SharedState`).
pub fn router(handle: PrometheusHandle) -> Router {
    Router::new()
        .route("/metrics", get(handler))
        .with_state(handle)
}
