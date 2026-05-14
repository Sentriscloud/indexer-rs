//! sentrix-indexer-rs — HTTP API server.
//!
//! Reads `DATABASE_URL` + `INDEXER_API_BIND` from env (latter defaults to
//! `0.0.0.0:8080`), opens a PG pool, mounts the axum router from
//! `indexer_api::make_router`, layers CORS + tracing + 30s timeout, and
//! serves until Ctrl-C / SIGTERM.

use std::time::Duration;

use axum::http::StatusCode;
use figment::Figment;
use figment::providers::Env;
use indexer_api::{AppState, RouterConfig, make_router, metrics_router, observability};
use indexer_cache::{CacheClient, CacheConfig};
use indexer_db::{PoolConfig, connect};
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

#[derive(Debug, Deserialize)]
struct ApiConfig {
    database_url: String,
    #[serde(default = "default_bind")]
    indexer_api_bind: String,
    #[serde(default = "default_max_connections")]
    indexer_api_max_connections: u32,
    /// Optional bearer token. When set, every route except /health requires
    /// `Authorization: Bearer <this>`. Unset = open API (Caddy out front).
    #[serde(default)]
    indexer_api_bearer_token: Option<String>,
    /// Optional Redis URL (`redis://`/`rediss://`). When set, hot routes
    /// use cache-aside via the `indexer_cache` client.
    #[serde(default)]
    redis_url: Option<String>,
    /// Cache key namespace. Default `sentrix:idx:<network>`; falls back to
    /// `sentrix:idx:api` if `indexer_network` isn't passed.
    #[serde(default)]
    indexer_cache_namespace: Option<String>,
    /// Per-IP sustained rate. Default 50 r/s.
    #[serde(default = "default_rate")]
    indexer_api_rate_per_sec: u64,
    /// Per-IP burst. Default 50.
    #[serde(default = "default_burst")]
    indexer_api_rate_burst: u32,
    /// Loopback address for the internal Prometheus `/metrics` listener.
    /// Default 127.0.0.1:9080; set to empty to disable (audit 2026-05-13:
    /// previously merged into the public router with no auth gating).
    #[serde(default = "default_metrics_bind")]
    indexer_api_metrics_bind: String,
}
fn default_metrics_bind() -> String {
    "127.0.0.1:9080".to_string()
}
fn default_rate() -> u64 {
    50
}
fn default_burst() -> u32 {
    50
}

fn default_bind() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_max_connections() -> u32 {
    20
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cfg: ApiConfig = Figment::new().merge(Env::raw()).extract()?;
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = %cfg.indexer_api_bind,
        max_connections = cfg.indexer_api_max_connections,
        "api: booting",
    );

    let mut pool_cfg = PoolConfig::from_url(&cfg.database_url);
    pool_cfg.max_connections = cfg.indexer_api_max_connections;
    let pool = connect(&pool_cfg).await?;

    let cache = match cfg.redis_url.as_deref() {
        Some(url) => {
            let ns = cfg
                .indexer_cache_namespace
                .clone()
                .unwrap_or_else(|| "sentrix:idx:api".to_string());
            tracing::info!(redis_url = %url, namespace = %ns, "api: connecting Redis cache");
            let cc = CacheConfig::new(url, ns);
            match CacheClient::connect(cc).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(error = %e, "api: Redis connect failed; running cache-disabled");
                    None
                }
            }
        }
        None => {
            tracing::info!("api: REDIS_URL unset; cache disabled");
            None
        }
    };

    let metrics_handle = observability::install_recorder();
    let router_cfg = RouterConfig {
        auth_token: cfg.indexer_api_bearer_token.clone(),
        rate_per_sec: cfg.indexer_api_rate_per_sec,
        rate_burst: cfg.indexer_api_rate_burst,
        metrics_handle: None,
    };

    // Bind /metrics on a loopback-only listener so Caddy/edge proxy can
    // never expose it. Operator scrapes via host-local Prometheus.
    // Loopback-only is enforced at startup — `0.0.0.0` / public IPs would
    // re-create the unauth-public-metrics exposure this PR closes.
    if !cfg.indexer_api_metrics_bind.is_empty() {
        let bind = cfg.indexer_api_metrics_bind.clone();
        let parsed: std::net::SocketAddr = bind
            .parse()
            .map_err(|e| anyhow::anyhow!("INDEXER_API_METRICS_BIND parse: {e}"))?;
        if !parsed.ip().is_loopback() {
            anyhow::bail!(
                "INDEXER_API_METRICS_BIND={bind} must be loopback (127.0.0.1/::1). \
                 /metrics is unauthenticated; binding to a non-loopback address \
                 would re-expose it via the edge proxy."
            );
        }
        let mh = metrics_handle.clone();
        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(&bind).await {
                Ok(listener) => {
                    tracing::info!(addr = %bind, "api: metrics listener up (loopback)");
                    if let Err(e) =
                        axum::serve(listener, metrics_router(mh).into_make_service()).await
                    {
                        tracing::error!(error = %e, "api: metrics listener exited");
                    }
                }
                Err(e) => {
                    tracing::error!(addr = %bind, error = %e, "api: metrics bind failed");
                }
            }
        });
    } else {
        tracing::info!("api: INDEXER_API_METRICS_BIND empty; metrics endpoint disabled");
    }

    let app = make_router(AppState { pool, cache }, router_cfg)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        // Permissive CORS is intentional: this is a no-auth public read API.
        // Any browser origin should be able to fetch /blocks, /tx/<hash>,
        // etc. for explorers + dashboards. If/when bearer auth becomes
        // mandatory (not opt-in via INDEXER_API_BEARER_TOKEN), revisit and
        // tighten to an allowlist (audit 2026-05-13).
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&cfg.indexer_api_bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "api: listening");

    // `into_make_service_with_connect_info` injects ConnectInfo<SocketAddr>
    // which the per-IP rate-limit middleware (tower_governor's default
    // PeerIpKeyExtractor) reads. Without this every request returns 500
    // "Unable To Extract Key!" before the route handler runs.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    tracing::info!("api: shutdown complete");
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl_c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("api: shutdown signal received");
}
