//! indexer-api
//!
//! Axum REST surface mirroring the TS Fastify port byte-for-byte. Field
//! shapes (snake_case keys, bigint-as-string values, etc.) match the
//! existing `apps/api` so the Rust port can drop in behind the same Caddy
//! upstream during dual-run cutover.
//!
//! Phase 5 ships the load-bearing read paths:
//!  - `GET /health`
//!  - `GET /blocks?limit&before`
//!  - `GET /blocks/:height`
//!  - `GET /tx/:hash`
//!
//! Etherscan-compat (`/api?module=...`), CoinBlast (`/coinblast/*`), and
//! the long-tail native routes (`/address/:addr/*`, `/stats/*`,
//! `/whale/*`) ship in the next iteration.

#![cfg_attr(not(test), warn(missing_docs))]

pub mod error;
pub mod routes;
pub mod serialise;

use axum::Router;
use indexer_db::PgPool;
use std::sync::Arc;

/// Shared state passed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// Read pool. Single shared instance across all routes.
    pub pool: PgPool,
}

/// Build the axum router with all Phase 5 routes mounted at the root.
/// Caller adds middleware (CORS, tracing, timeouts) before binding.
pub fn make_router(state: AppState) -> Router {
    Router::new()
        .merge(routes::health::router())
        .merge(routes::blocks::router())
        .merge(routes::tx::router())
        .with_state(Arc::new(state))
}

/// Convenience type alias for handlers that take the shared state via
/// `axum::extract::State`.
pub type SharedState = Arc<AppState>;
