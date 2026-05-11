//! indexer-sync
//!
//! Sync core for the Sentrix indexer. Glues together the chain client
//! ([`indexer_chain`]) and the Postgres helpers ([`indexer_db`]) into a
//! backfill loop, a tail loop, and a reorg checker.
//!
//! Invariants enforced (per spec §5):
//!  1. **Idempotent writes** — every insert uses `ON CONFLICT DO NOTHING`.
//!  2. **Cursor atomic with data** — block + txs + logs + cursor advance
//!     all happen inside one `sqlx::Transaction`; commit is the durable
//!     boundary.
//!  3. **Reorg rewind clears all downstream data** — `delete_from(blocks, h)`
//!     rides FK CASCADE down to txs and logs.
//!  4. **SAFE_LAG enforced** — backfill never advances closer to tip than
//!     [`SyncConfig::safe_lag`] blocks.
//!  5. **Backfill cursor monotonic** — cursor only ever increases inside
//!     the backfill loop; the only way to move it backwards is the
//!     reorg path.
//! 10. **Single writer per height** — the tail loop's [`SingleFlight`]
//!     gate ensures we never have two `index_block` chains for the same
//!     height in flight at once.

#![cfg_attr(not(test), warn(missing_docs))]

pub mod backfill;
pub mod block_writer;
pub mod cursor;
pub mod reorg;
pub mod single_flight;
pub mod tail;

mod convert;

pub use backfill::run_backfill;
pub use cursor::{LAST_SYNCED_HEIGHT_KEY, read_cursor, write_cursor};
pub use single_flight::SingleFlight;

use indexer_domain::BlockHeight;
use std::time::Duration;

/// Sync-side configuration. Fields with no obvious default are required;
/// the rest match the values discussed in spec §5 / §7.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Minimum gap to keep between our cursor and the chain tip. Buffer for
    /// short reorgs that haven't BFT-finalized yet. Spec §5 invariant 4.
    pub safe_lag: u64,
    /// Backfill stops if the chain provider says we'd advance past this
    /// height (None = no manual cap).
    pub max_backfill_height: Option<BlockHeight>,
    /// Reorg checker tick. Spec §7 default 60s.
    pub reorg_check_interval: Duration,
    /// How many blocks back from tip the reorg checker probes.
    pub reorg_probe_depth: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            safe_lag: 8,
            max_backfill_height: None,
            reorg_check_interval: Duration::from_secs(60),
            reorg_probe_depth: 16,
        }
    }
}

/// Errors surfaced by sync operations. Wraps the three downstream layers.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// Chain RPC / gRPC / REST failure.
    #[error("chain: {0}")]
    Chain(#[from] indexer_chain::ChainError),

    /// Postgres error.
    #[error("db: {0}")]
    Db(#[from] indexer_db::DbError),

    /// Direct sqlx error from a place we couldn't funnel through `indexer_db`
    /// (e.g. mid-transaction).
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// Caller asked the sync loop to do something it can't.
    #[error("invalid: {0}")]
    Invalid(String),
}

/// Result alias.
pub type SyncResult<T> = std::result::Result<T, SyncError>;
