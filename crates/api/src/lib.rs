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

pub mod auth;
pub mod error;
pub mod graphql;
pub mod routes;
pub mod serialise;

use axum::Router;
use axum::middleware::from_fn_with_state;
use indexer_db::PgPool;
use std::sync::Arc;

/// Shared state passed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// Read pool. Single shared instance across all routes.
    pub pool: PgPool,
}

/// Build the axum router with all routes (REST + GraphQL) mounted at the
/// root. `auth_token` (optional) gates every route except `/health` behind
/// a `Authorization: Bearer <token>` header. Pass None to leave the API
/// open (Caddy/nginx out front handles auth at the edge — recommended for
/// production per `INDEXER_RS_CREDS_AUTH.md`).
///
/// Caller adds CORS + tracing + timeout layers before binding.
pub fn make_router(state: AppState, auth_token: Option<String>) -> Router {
    let shared = Arc::new(state);
    let schema = graphql::build_schema(shared.clone());
    let auth_state = auth::AuthState::new(auth_token);
    if !auth_state.is_open() {
        tracing::info!("api: bearer-token auth ENABLED");
    } else {
        tracing::info!(
            "api: running OPEN (no INDEXER_API_BEARER_TOKEN); rely on Caddy/edge for auth"
        );
    }
    let rest = Router::new()
        .merge(routes::health::router())
        .merge(routes::blocks::router())
        .merge(routes::tx::router())
        .merge(routes::address::router())
        .merge(routes::leaderboards::router())
        .merge(routes::coinblast::router())
        .merge(routes::stats::router())
        .merge(routes::etherscan::router())
        .with_state(shared.clone());
    let gql = graphql::router(schema).with_state(shared);
    Router::new()
        .merge(rest)
        .merge(gql)
        .layer(from_fn_with_state(auth_state, auth::require_bearer))
}

/// Convenience type alias for handlers that take the shared state via
/// `axum::extract::State`.
pub type SharedState = Arc<AppState>;
