//! Block — primary chain entity. One row per finalized block.

use crate::{BlockHeight, Hash, Wei};
use serde::{Deserialize, Serialize};

/// Finalized chain block. Row in `blocks`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    /// Height (PG primary key, monotonic).
    pub height: BlockHeight,
    /// Block hash (lowercase 0x-prefixed, 66 chars).
    pub hash: Hash,
    /// Parent block hash.
    pub parent_hash: Hash,
    /// Unix timestamp seconds (chain time).
    pub timestamp: i64,
    /// Validator address that proposed this block (lowercase 0x-prefixed).
    pub validator: String,
    /// Total gas consumed by all txs in this block.
    pub gas_used: i64,
    /// Block gas limit.
    pub gas_limit: i64,
    /// EIP-1559 base fee. Optional pre-fork.
    pub base_fee: Option<Wei>,
    /// Number of txs in this block.
    pub tx_count: i32,
    /// Post-execution state root (Sparse Merkle Trie). Optional pre-state-root-fork.
    pub state_root: Option<Hash>,
    /// BFT round at which this block reached supermajority.
    pub round: i32,
    /// Justification signer addresses (the validators that precommitted).
    pub justification_signers: Vec<String>,
}
