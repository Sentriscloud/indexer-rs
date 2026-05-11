//! indexer-coinblast
//!
//! CoinBlast launchpad worker — separate cursor (`last_synced_coinblast_height`),
//! topic-filtered scan over the chain RPC, populates `cb_tokens` + `cb_trades`.
//! Runs in parallel with the chain-wide block-by-block sync because the
//! factory deployed at h≈1.18M (mainnet) and waiting for the chain-wide
//! cursor to catch up would cost weeks of latency.
//!
//! Idempotency:
//!  - `cb_trades` has a unique `(tx_hash, log_index)` index, so re-running
//!    any chunk is safe.
//!  - `cb_tokens` uses `curve_address` as PK with `ON CONFLICT DO NOTHING`.
//!  - Aggregate updates (`total_volume_srx`, `trade_count`, `last_price_srx`)
//!    live in the same SQL transaction as the trade insert and are gated
//!    by the conflict — a re-run skips both insert and update.
//!
//! Orphan adoption (CoinBlast Genesis = CBLAST direct-deployed pre-factory)
//! is deferred — needs `eth_call` infrastructure on the chain client. The
//! production TS indexer adopts orphans on first event sighting via on-chain
//! `token() / curveSupply() / graduationSrxThreshold()` reads. Until then,
//! events from non-factory curves are silently skipped (logged at WARN).

#![cfg_attr(not(test), warn(missing_docs))]

pub mod events;
pub mod handlers;
pub mod orphan;
pub mod worker;

pub use events::{COINBLAST_DEPLOY_BLOCK, COINBLAST_FACTORY_ADDRESS, Network};
pub use worker::{WorkerConfig, run_coinblast_worker};

/// `_meta` key holding the worker's cursor (highest block fully scanned for
/// CoinBlast events).
pub const META_KEY_COINBLAST_CURSOR: &str = "last_synced_coinblast_height";

/// Errors surfaced by the worker.
#[derive(Debug, thiserror::Error)]
pub enum CoinblastError {
    /// Chain RPC failure.
    #[error("chain: {0}")]
    Chain(#[from] indexer_chain::ChainError),
    /// Postgres failure.
    #[error("db: {0}")]
    Db(#[from] indexer_db::DbError),
    /// Direct sqlx error from a place we couldn't funnel through `indexer_db`.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// ABI decode failed on a CoinBlast-shaped log.
    #[error("decode: {0}")]
    Decode(String),
    /// Caller asked for something invalid.
    #[error("invalid: {0}")]
    Invalid(String),
}

/// Result alias.
pub type CoinblastResult<T> = std::result::Result<T, CoinblastError>;
