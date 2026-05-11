//! Observability bootstrap — installs the Prometheus recorder + per-request
//! latency histogram. Returns the `PrometheusHandle` the operator passes to
//! [`crate::routes::metrics::router`] so `/metrics` can render the registry.
//!
//! Latency histogram bucket choice mirrors what the chain repo uses for
//! its own RPC traces: 1ms / 5ms / 25ms / 100ms / 500ms / 2.5s. Anything
//! over 2.5s lands in the +Inf bucket (rare for indexer reads).

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::time::Instant;

const LATENCY_BUCKETS: &[f64] = &[0.001, 0.005, 0.025, 0.1, 0.5, 2.5];

/// Install the Prometheus recorder once at boot. Returns the handle for
/// `/metrics` rendering. Panics if called twice (the global recorder is a
/// once-only install).
pub fn install_recorder() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("indexer_api_request_seconds".into()),
            LATENCY_BUCKETS,
        )
        .expect("valid bucket spec")
        .install_recorder()
        .expect("only one recorder per process")
}

/// Per-request latency + status middleware. Slot via
/// `axum::middleware::from_fn(track_request)` after auth so probes don't
/// pollute the metric. Labels: `method`, `path`, `status`.
pub async fn track_request(req: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let resp = next.run(req).await;
    let elapsed = started.elapsed().as_secs_f64();
    let status = resp.status().as_u16().to_string();
    metrics::counter!(
        "indexer_api_requests_total",
        "method" => method.clone(),
        "path" => path.clone(),
        "status" => status.clone(),
    )
    .increment(1);
    metrics::histogram!(
        "indexer_api_request_seconds",
        "method" => method,
        "path" => path,
        "status" => status,
    )
    .record(elapsed);
    resp
}
