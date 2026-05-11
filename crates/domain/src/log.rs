//! Log — EVM event log emitted during tx execution.

use crate::{Address, BlockHeight, Hash, LogIndex};
use serde::{Deserialize, Serialize};

/// EVM event log. Composite PK on `(block_height, log_index)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Log {
    /// Block this log was emitted in.
    pub block_height: BlockHeight,
    /// Tx that emitted the log.
    pub tx_hash: Hash,
    /// Block-wide log index (not per-tx).
    pub log_index: LogIndex,
    /// Emitting contract address.
    pub address: Address,
    /// First indexed topic (typically the event signature hash).
    pub topic0: Option<Hash>,
    /// Second indexed topic.
    pub topic1: Option<Hash>,
    /// Third indexed topic.
    pub topic2: Option<Hash>,
    /// Fourth indexed topic.
    pub topic3: Option<Hash>,
    /// Non-indexed event payload (hex string).
    pub data: Option<String>,
}
