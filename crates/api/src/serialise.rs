//! JSON serialisers — produce the exact field shapes the TS Fastify port
//! emits. Verified against `apps/api/src/routes/native.ts` serialiseBlock /
//! serialiseTx / serialiseLog. Keys are snake_case; bigint heights /
//! timestamps / gas / nonce render as strings (so they survive JSON's f64
//! precision limit on the wire).

use indexer_domain::{Block, Log, TokenTransfer, Transaction, Wei};
use serde::{Deserialize, Serialize};

/// Wire shape of a Block. Matches `serialiseBlock` in the TS port.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireBlock {
    /// `bigint` height as decimal string.
    pub height: String,
    /// 0x-prefixed hash.
    pub hash: String,
    /// 0x-prefixed parent hash.
    pub parent_hash: String,
    /// Unix seconds, decimal string.
    pub timestamp: String,
    /// 0x-prefixed validator (proposer) address.
    pub validator: String,
    /// `bigint` gas used as decimal string.
    pub gas_used: String,
    /// `bigint` gas limit as decimal string.
    pub gas_limit: String,
    /// Optional `numeric(78, 0)` base fee — already string-shaped via Wei.
    pub base_fee: Option<Wei>,
    /// Tx count (i32 fits in JSON number).
    pub tx_count: i32,
    /// Optional 0x-prefixed state root (None pre state-root-fork).
    pub state_root: Option<String>,
    /// BFT round (i32 fits in JSON number).
    pub round: i32,
}

impl From<&Block> for WireBlock {
    fn from(b: &Block) -> Self {
        Self {
            height: b.height.0.to_string(),
            hash: b.hash.clone(),
            parent_hash: b.parent_hash.clone(),
            timestamp: b.timestamp.to_string(),
            validator: b.validator.clone(),
            gas_used: b.gas_used.to_string(),
            gas_limit: b.gas_limit.to_string(),
            base_fee: b.base_fee,
            tx_count: b.tx_count,
            state_root: b.state_root.clone(),
            round: b.round,
        }
    }
}

/// Block + nested transactions — the `/blocks/:height` response shape.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireBlockWithTxs {
    /// Inlined block fields.
    #[serde(flatten)]
    pub block: WireBlock,
    /// Block's transactions, ordered by `tx_index`.
    pub transactions: Vec<WireTransaction>,
}

/// Wire shape of a Transaction. Field renames + bigint stringification
/// match the TS port (note `from_addr` → `from`, `to_addr` → `to`).
#[derive(Debug, Serialize, Deserialize)]
pub struct WireTransaction {
    /// 0x-prefixed tx hash.
    pub hash: String,
    /// `bigint` block height as decimal string.
    pub block_height: String,
    /// Position within block (i32 fits in JSON number).
    pub tx_index: i32,
    /// 0x-prefixed sender.
    pub from: String,
    /// 0x-prefixed receiver. None for contract creation / system tx.
    pub to: Option<String>,
    /// `numeric(78, 0)` value — already string-shaped via Wei.
    pub value: Wei,
    /// `bigint` gas limit as decimal string.
    pub gas_limit: String,
    /// `bigint` gas used as decimal string. None until receipt observed.
    pub gas_used: Option<String>,
    /// `numeric(78, 0)` gas price.
    pub gas_price: Option<Wei>,
    /// `numeric(78, 0)` fee paid.
    pub fee: Wei,
    /// `bigint` nonce as decimal string.
    pub nonce: String,
    /// Hex-encoded calldata.
    pub data: Option<String>,
    /// 0 = failed, 1 = success.
    pub status: i16,
    /// Address of contract created by this tx (CREATE / CREATE2).
    pub contract_address: Option<String>,
    /// Tx kind: native / evm / system / coinbase.
    pub tx_type: String,
}

impl From<&Transaction> for WireTransaction {
    fn from(t: &Transaction) -> Self {
        Self {
            hash: t.hash.clone(),
            block_height: t.block_height.0.to_string(),
            tx_index: t.tx_index.0,
            from: t.from_addr.clone(),
            to: t.to_addr.clone(),
            value: t.value,
            gas_limit: t.gas_limit.to_string(),
            gas_used: t.gas_used.map(|n| n.to_string()),
            gas_price: t.gas_price,
            fee: t.fee,
            nonce: t.nonce.to_string(),
            data: t.data.clone(),
            status: t.status,
            contract_address: t.contract_address.clone(),
            tx_type: t.tx_type.as_str().to_string(),
        }
    }
}

/// Wire shape of a Log. Topics flattened to a single array (TS port filters
/// nulls + flattens `topic0..topic3`).
#[derive(Debug, Serialize, Deserialize)]
pub struct WireLog {
    /// `bigint` block height as decimal string.
    pub block_height: String,
    /// 0x-prefixed tx hash.
    pub tx_hash: String,
    /// Block-wide log index (i32).
    pub log_index: i32,
    /// 0x-prefixed emitting contract.
    pub address: String,
    /// All non-null topics, in `topic0..topic3` order.
    pub topics: Vec<String>,
    /// Hex-encoded payload.
    pub data: Option<String>,
}

impl From<&Log> for WireLog {
    fn from(l: &Log) -> Self {
        let topics = [&l.topic0, &l.topic1, &l.topic2, &l.topic3]
            .iter()
            .filter_map(|t| t.as_ref().cloned())
            .collect();
        Self {
            block_height: l.block_height.0.to_string(),
            tx_hash: l.tx_hash.clone(),
            log_index: l.log_index.0,
            address: l.address.clone(),
            topics,
            data: l.data.clone(),
        }
    }
}

/// Wire shape of a TokenTransfer. Matches the TS port's row shape — `id` is
/// the PG identity, lowercase addresses, decimal-string amounts.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireTransfer {
    /// PG-assigned surrogate.
    pub id: Option<i64>,
    /// `bigint` block height as decimal string.
    pub block_height: String,
    /// 0x-prefixed tx hash that emitted the transfer.
    pub tx_hash: String,
    /// Block-wide log index.
    pub log_index: i32,
    /// Token contract.
    pub contract: String,
    /// erc20 / erc721 / erc1155.
    pub standard: String,
    /// 0x-prefixed sender.
    pub from_addr: String,
    /// 0x-prefixed receiver.
    pub to_addr: String,
    /// `numeric(78, 0)` token id; null for erc20.
    pub token_id: Option<Wei>,
    /// `numeric(78, 0)` amount.
    pub amount: Wei,
}

impl From<&TokenTransfer> for WireTransfer {
    fn from(t: &TokenTransfer) -> Self {
        Self {
            id: t.id,
            block_height: t.block_height.0.to_string(),
            tx_hash: t.tx_hash.clone(),
            log_index: t.log_index.0,
            contract: t.contract.clone(),
            standard: t.standard.as_str().to_string(),
            from_addr: t.from_addr.clone(),
            to_addr: t.to_addr.clone(),
            token_id: t.token_id,
            amount: t.amount,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_domain::{BlockHeight, LogIndex, TxIndex, TxType};

    #[test]
    fn block_renders_height_as_string() {
        let b = Block {
            height: BlockHeight(12345),
            hash: "0xabc".into(),
            parent_hash: "0xdef".into(),
            timestamp: 1_700_000_000,
            validator: "0x1111111111111111111111111111111111111111".into(),
            gas_used: 21_000,
            gas_limit: 8_000_000,
            base_fee: None,
            tx_count: 1,
            state_root: Some("0xfff".into()),
            round: 0,
            justification_signers: vec![],
        };
        let wire: WireBlock = (&b).into();
        let v = serde_json::to_value(&wire).unwrap();
        assert_eq!(v["height"], "12345");
        assert_eq!(v["timestamp"], "1700000000");
        assert_eq!(v["gas_used"], "21000");
        assert_eq!(v["tx_count"], 1);
        assert!(v["base_fee"].is_null());
        assert_eq!(v["state_root"], "0xfff");
    }

    #[test]
    fn tx_renames_from_addr_and_to_addr() {
        let t = Transaction {
            hash: "0xabc".into(),
            block_height: BlockHeight(1),
            tx_index: TxIndex(0),
            from_addr: "0xaaa".into(),
            to_addr: Some("0xbbb".into()),
            value: Wei::from(1_000u64),
            gas_limit: 21_000,
            gas_used: Some(21_000),
            gas_price: None,
            fee: Wei::ZERO,
            nonce: 5,
            data: None,
            status: 1,
            contract_address: None,
            tx_type: TxType::Evm,
        };
        let wire: WireTransaction = (&t).into();
        let v = serde_json::to_value(&wire).unwrap();
        assert_eq!(v["from"], "0xaaa");
        assert_eq!(v["to"], "0xbbb");
        assert!(v.get("from_addr").is_none());
        assert!(v.get("to_addr").is_none());
        assert_eq!(v["nonce"], "5");
        assert_eq!(v["gas_used"], "21000");
        assert_eq!(v["value"], "1000");
        assert_eq!(v["tx_type"], "evm");
    }

    #[test]
    fn log_flattens_topics_and_drops_nulls() {
        let l = Log {
            block_height: BlockHeight(1),
            tx_hash: "0xabc".into(),
            log_index: LogIndex(0),
            address: "0xaddr".into(),
            topic0: Some("0xt0".into()),
            topic1: None,
            topic2: Some("0xt2".into()),
            topic3: None,
            data: Some("0x".into()),
        };
        let wire: WireLog = (&l).into();
        let v = serde_json::to_value(&wire).unwrap();
        assert_eq!(v["topics"], serde_json::json!(["0xt0", "0xt2"]));
    }
}
