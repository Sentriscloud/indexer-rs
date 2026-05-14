//! Newtype wrappers around chain identifiers.
//!
//! The TS indexer mixed `bigint` heights with `number` log indexes freely;
//! a function expecting `(BlockHeight, LogIndex)` couldn't reject a swapped
//! call site. Rust newtypes pin the intent at compile time.

use serde::{Deserialize, Serialize};

/// Block height. `bigint` in PG, `u64` on the wire.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct BlockHeight(pub i64);

impl BlockHeight {
    /// Returns the height as `u64` (chain-side native), or None when the
    /// underlying value is negative.
    ///
    /// The sync layer uses `BlockHeight(-1)` as a "no cursor written yet"
    /// sentinel (see `crates/sync/src/cursor.rs`); a future caller hitting
    /// `as_u64()` on that sentinel previously panicked + killed the worker.
    /// Returning Option forces the caller to acknowledge the case (audit
    /// 2026-05-13).
    pub fn as_u64(self) -> Option<u64> {
        u64::try_from(self.0).ok()
    }
}

impl From<u64> for BlockHeight {
    fn from(v: u64) -> Self {
        Self(v as i64)
    }
}

/// Position of a transaction within its block.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct TxIndex(pub i32);

/// Position of a log within its block (chain-wide, not per-tx).
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct LogIndex(pub i32);

/// DPoS epoch number.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct EpochNumber(pub i64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_height_roundtrip_json() {
        let h = BlockHeight(123_456);
        let s = serde_json::to_string(&h).unwrap();
        assert_eq!(s, "123456");
        let back: BlockHeight = serde_json::from_str(&s).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn block_height_from_u64() {
        let h: BlockHeight = 42u64.into();
        assert_eq!(h.0, 42);
        assert_eq!(h.as_u64(), Some(42));
    }

    #[test]
    fn block_height_sentinel_no_panic() {
        // Cursor sentinel — must not panic, must surface as None so callers
        // skip rather than coerce.
        assert_eq!(BlockHeight(-1).as_u64(), None);
    }

    #[test]
    fn tx_index_and_log_index_distinct_types() {
        // Compile-time guarantee — TxIndex and LogIndex don't unify.
        let _t = TxIndex(0);
        let _l = LogIndex(0);
        // Cross-assignment would not compile:
        //   let _: TxIndex = LogIndex(0);
    }
}
