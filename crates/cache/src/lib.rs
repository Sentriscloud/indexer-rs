//! indexer-cache
//!
//! Redis-backed cache for the API surface. Three TTL tiers per spec §9:
//!   - **Tier 1** (60s): chain-wide aggregates that change every block
//!     (`stats`, latest-blocks index).
//!   - **Tier 2** (5min): per-address rollups that change but are
//!     paginated (address tx counts, balance snapshots).
//!   - **Tier 3** (1h): per-block / per-tx detail that's immutable once
//!     written (specific block-by-height, specific tx-by-hash).
//!
//! Failure mode: if Redis is unreachable / slow, the API falls back to
//! direct PG reads — the [`CircuitBreaker`] tracks consecutive failures
//! and short-circuits to the PG path for `open_for_ms` after a threshold
//! is breached. The breaker is fully in-process; multi-replica deployments
//! each have their own breaker state, which is fine because Redis is the
//! shared dependency, not the breaker.

#![cfg_attr(not(test), warn(missing_docs))]

mod breaker;
mod client;

pub use breaker::CircuitBreaker;
pub use client::{CacheClient, CacheConfig, Tier};

/// Errors surfaced by the cache layer. The API should treat these as
/// non-fatal — fall back to the underlying source of truth (PG).
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// fred (Redis) error.
    #[error("redis: {0}")]
    Redis(String),
    /// JSON encode / decode error.
    #[error("encode: {0}")]
    Encode(#[from] serde_json::Error),
    /// Circuit breaker is open; caller should fall back.
    #[error("circuit open: skipping redis call")]
    Open,
}

impl From<fred::error::Error> for CacheError {
    fn from(e: fred::error::Error) -> Self {
        CacheError::Redis(e.to_string())
    }
}

/// Result alias.
pub type CacheResult<T> = std::result::Result<T, CacheError>;
