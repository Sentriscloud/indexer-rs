//! indexer-analytics
//!
//! ClickHouse sink for raw tx rows + a periodic flusher. Spec §10.
//!
//! Pipeline:
//!  - Sync layer pushes a `RawTxRow` per finalized tx via [`AnalyticsHandle::push`]
//!    (mpsc unbounded — buffer growth is bounded by the chain's own tx
//!    volume).
//!  - The flusher task drains the buffer every 15s OR on graceful
//!    shutdown, batches into one INSERT per drain, retries on transient
//!    failure with bounded attempts.
//!
//! Buffer durability: in-memory only (spec §10.5). On crash the unsent
//! batch is lost, but it's analytics-only data — the canonical source of
//! truth (`transactions` in PG) is unchanged. A future iteration can
//! upgrade this to a small on-disk WAL (sled) if the analytics gap
//! becomes operationally painful.

#![cfg_attr(not(test), warn(missing_docs))]

mod flusher;
mod row;

pub use flusher::{AnalyticsHandle, run_flusher};
pub use row::RawTxRow;

/// Errors surfaced by the analytics layer. The sync layer treats these as
/// non-fatal — analytics is observability, not correctness.
#[derive(Debug, thiserror::Error)]
pub enum AnalyticsError {
    /// ClickHouse client / network failure.
    #[error("clickhouse: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
    /// Caller pushed to a closed channel (flusher already exited).
    #[error("buffer closed")]
    Closed,
}

/// Result alias.
pub type AnalyticsResult<T> = std::result::Result<T, AnalyticsError>;
