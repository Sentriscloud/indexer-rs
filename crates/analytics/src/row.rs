//! Wire shape of a raw-tx analytics row. ClickHouse table contract:
//!
//! ```sql
//! CREATE TABLE raw_tx (
//!     block_height UInt64,
//!     timestamp    UInt64,
//!     tx_hash      String,
//!     from_addr    String,
//!     to_addr      Nullable(String),
//!     value_str    String,    -- numeric(78,0) decimal
//!     fee_str      String,
//!     gas_used     UInt64,
//!     status       UInt8,
//!     tx_type      LowCardinality(String)
//! ) ENGINE = MergeTree() ORDER BY (block_height, tx_hash);
//! ```
//!
//! `value_str` / `fee_str` carry decimal strings instead of u256 because
//! ClickHouse's `Decimal256` is still flagged experimental and our query
//! patterns (sum, count, group-by) use the raw aggregates that work fine
//! over String comparison + cast-on-read.

use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// One row per indexed tx, written to ClickHouse for query-friendly
/// observability + bench analytics.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
#[allow(missing_docs)]
pub struct RawTxRow {
    pub block_height: u64,
    pub timestamp: u64,
    pub tx_hash: String,
    pub from_addr: String,
    pub to_addr: Option<String>,
    pub value_str: String,
    pub fee_str: String,
    pub gas_used: u64,
    pub status: u8,
    pub tx_type: String,
}
