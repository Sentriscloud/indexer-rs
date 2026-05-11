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
use indexer_api::{AppState, RouterConfig, make_router, observability};
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
        metrics_handle: Some(metrics_handle),
    };

    let app = make_router(AppState { pool, cache }, router_cfg)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
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
