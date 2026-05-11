//! indexer-db
//!
//! Postgres connection management + migrations + per-table query helpers.
//! Phase 1 ships the schema (mirrors the TS drizzle schema exactly) and a
//! thin wrapper around `sqlx::PgPool`. The compile-time `query!` macros
//! land in Phase 2 once the `.sqlx/` cache is wired up in CI.

#![cfg_attr(not(test), warn(missing_docs))]

pub mod blocks;
pub mod cb_queries;
pub mod cb_tokens;
pub mod cb_trades;
pub mod logs;
pub mod meta;
pub mod stats;
pub mod token_transfers;
pub mod transactions;

pub use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

/// Embed the migration SQL files at compile time so the binary can run
/// `sqlx::migrate!()` without shipping the SQL alongside it.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Errors surfaced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// Underlying sqlx error.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    /// Migration application failed.
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

/// Result alias.
pub type DbResult<T> = std::result::Result<T, DbError>;

/// Pool-construction config. Defaults match production single-process sizing
/// per spec §18.9.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Postgres connection string.
    pub url: String,
    /// Maximum pooled connections. Default 20.
    pub max_connections: u32,
    /// Minimum idle connections. Default 2.
    pub min_connections: u32,
    /// Per-acquire timeout. Default 5s.
    pub acquire_timeout: Duration,
}

impl PoolConfig {
    /// Construct from a database URL with default sizing.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: 20,
            min_connections: 2,
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

/// Open a Postgres pool. Caller decides whether to also call [`migrate`].
pub async fn connect(cfg: &PoolConfig) -> DbResult<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .min_connections(cfg.min_connections)
        .acquire_timeout(cfg.acquire_timeout)
        .connect(&cfg.url)
        .await?;
    Ok(pool)
}

/// Apply all pending migrations.
pub async fn migrate(pool: &PgPool) -> DbResult<()> {
    MIGRATOR.run(pool).await?;
    Ok(())
}
