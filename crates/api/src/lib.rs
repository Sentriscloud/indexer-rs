//! indexer-api
//!
//! Axum REST + GraphQL surface mirroring the TS Fastify port byte-for-byte
//! on REST shape. Field shapes (snake_case keys, bigint-as-string values,
//! etc.) match the existing `apps/api` so the Rust port can drop in behind
//! the same Caddy upstream during dual-run cutover.
//!
//! Production-readiness layers (post-Phase 9):
//!  - Optional bearer-token auth (`INDEXER_API_BEARER_TOKEN`)
//!  - Per-IP rate limit via `tower_governor` (default 50 r/s sustained,
//!    50 r/s burst headroom)
//!  - Cache-aside through `indexer_cache` for hot reads (`/blocks`,
//!    `/stats/daily`, `/accounts/active`, `/coinblast/tokens`) when
//!    `AppState::cache` is `Some`
//!  - Prometheus metrics: `/metrics` + per-request latency histogram
//!  - Readiness probe: `/readyz` checks PG (+ cache when configured)

#![cfg_attr(not(test), warn(missing_docs))]

pub mod auth;
pub mod cached;
pub mod error;
pub mod graphql;
pub mod observability;
pub mod routes;
pub mod serialise;

use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use indexer_cache::CacheClient;
use indexer_db::PgPool;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tower_governor::GovernorLayer;
use tower_governor::governor::GovernorConfigBuilder;

/// Shared state passed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// Read pool. Single shared instance across all routes.
    pub pool: PgPool,
    /// Optional Redis cache. None = cache layer disabled (every request
    /// hits PG). Hot routes use cache-aside via this client when present;
    /// on a cache failure (Redis down, circuit-breaker open) they fall
    /// back to PG — analytics drop, never correctness.
    pub cache: Option<CacheClient>,
}

/// Convenience type alias for handlers that take the shared state via
/// `axum::extract::State`.
pub type SharedState = Arc<AppState>;

/// Optional config switches for [`make_router`]. Sane defaults for prod.
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// Bearer token. None = open API (Caddy out front).
    pub auth_token: Option<String>,
    /// Per-IP sustained rate (req/sec). Default 50.
    pub rate_per_sec: u64,
    /// Per-IP burst headroom (extra requests above sustained). Default 50.
    pub rate_burst: u32,
    /// Prometheus handle returned by [`observability::install_recorder`].
    /// Pass `None` if metrics already wired by an earlier call site.
    pub metrics_handle: Option<PrometheusHandle>,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            auth_token: None,
            rate_per_sec: 50,
            rate_burst: 50,
            metrics_handle: None,
        }
    }
}

/// Build the axum router with REST + GraphQL + observability + auth +
/// rate-limit. Caller adds CORS + tracing + timeout layers before binding.
pub fn make_router(state: AppState, cfg: RouterConfig) -> Router {
    let shared = Arc::new(state);
    let schema = graphql::build_schema(shared.clone());
    let auth_state = auth::AuthState::new(cfg.auth_token);
    if !auth_state.is_open() {
        tracing::info!("api: bearer-token auth ENABLED");
    } else {
        tracing::info!(
            "api: running OPEN (no INDEXER_API_BEARER_TOKEN); rely on Caddy/edge for auth"
        );
    }
    if shared.cache.is_some() {
        tracing::info!("api: Redis cache wired (cache-aside on hot reads)");
    } else {
        tracing::info!("api: cache DISABLED (every request hits PG)");
    }

    let governor = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(cfg.rate_per_sec)
            .burst_size(cfg.rate_burst)
            .finish()
            .expect("governor config valid"),
    );

    let rest = Router::new()
        .merge(routes::health::router())
        .merge(routes::readyz::router())
        .merge(routes::openapi::router())
        .merge(routes::blocks::router())
        .merge(routes::tx::router())
        .merge(routes::address::router())
        .merge(routes::leaderboards::router())
        .merge(routes::coinblast::router())
        .merge(routes::stats::router())
        .merge(routes::contracts::router())
        .merge(routes::etherscan::router())
        .with_state(shared.clone());
    let gql = graphql::router(schema).with_state(shared);

    let app = Router::new().merge(rest).merge(gql);

    // /metrics is intentionally NOT merged here. It serves on a separate
    // internal listener (see `metrics_router`) bound to 127.0.0.1 by the
    // bin so the public Caddy proxy can never expose it (audit 2026-05-13).
    // Old call sites that still pass metrics_handle get a one-shot warn so
    // the silent-drop is visible in logs; field is kept for ABI compat
    // until next breaking release.
    if cfg.metrics_handle.is_some() {
        tracing::warn!(
            "RouterConfig.metrics_handle is set but the public router no longer mounts /metrics. \
             Spawn the internal listener via metrics_router() instead — see bin/api.rs. \
             The handle is being ignored; this field will be removed in a future release."
        );
    }

    app.layer(from_fn(observability::track_request))
        .layer(GovernorLayer::new(governor))
        .layer(from_fn_with_state(auth_state, auth::require_bearer))
        // request_id is outermost so EVERY layer + handler observes the same
        // value, and the response header carries it back even on 4xx/5xx
        // before the auth or governor layers reject.
        .layer(from_fn(error::request_id_middleware))
}

/// Build the internal /metrics router. Bind this on 127.0.0.1:9080 (or
/// equivalent loopback-only address) — never expose through the public
/// proxy (audit 2026-05-13: previously merged into the public router with
/// no auth gating).
pub fn metrics_router(handle: PrometheusHandle) -> Router {
    routes::metrics::router(handle)
}

/// Time-to-live tier hints exported for route handlers calling
/// [`cached::get_or_load`]. Re-export from `indexer_cache` so handlers
/// don't need a direct dep on the cache crate.
pub use indexer_cache::Tier as CacheTier;
