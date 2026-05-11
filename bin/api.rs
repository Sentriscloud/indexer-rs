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
use indexer_api::{AppState, make_router};
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

    let app = make_router(AppState { pool }, cfg.indexer_api_bearer_token.clone())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&cfg.indexer_api_bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "api: listening");

    axum::serve(listener, app)
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
