//! Native REST client for the chain's `/chain/blocks/<n>` + `/tx/<hash>`
//! endpoints.
//!
//! Sentrix's EVM JSON-RPC view is incomplete: `eth_getBlockByNumber` ignores
//! the `full=true` flag (always returns hash arrays), and
//! `eth_getTransactionByHash` returns a native envelope instead of the EVM
//! shape alloy expects. So for block + tx ingest we go straight to the
//! native REST endpoints — same host, different paths, canonical shape.
//!
//! The provider's alloy-backed `block_with_txs` is unusable until the chain
//! ships proper EVM JSON-RPC compat; this client is the working path.

use crate::error::{ChainError, ChainResult};
use indexer_domain::{BlockHeight, Hash};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Subset of the native `/tx/<hash>` response shape that the indexer needs.
/// Extra fields the chain may emit are ignored on decode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NativeTxResponse {
    /// Tx hash (echoed back).
    pub txid: String,
    /// Sender address.
    pub from_address: String,
    /// Receiver address. None for contract creation / system tx.
    pub to_address: Option<String>,
    /// Sentri amount (chain-native unit).
    pub amount: u64,
    /// Sentri fee.
    pub fee: u64,
    /// Sender nonce.
    pub nonce: u64,
    /// Block timestamp seconds.
    pub timestamp: u64,
    /// Block height the tx is included in.
    pub block_height: u64,
    /// Tx kind: native | evm | system | coinbase.
    pub tx_type: String,
    /// Hex-encoded data field (calldata for EVM, payload for native ops).
    pub data: Option<String>,
}

/// Native REST client.
#[derive(Debug, Clone)]
pub struct RestClient {
    base: String,
    http: reqwest::Client,
}

impl RestClient {
    /// Build a client pointing at the chain's HTTP base URL (no trailing
    /// `/tx/...`). Default 10s request timeout.
    pub fn new(base_url: impl Into<String>) -> ChainResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            base: base_url.into().trim_end_matches('/').to_owned(),
            http,
        })
    }

    /// Fetch a tx by hash. Returns None on 404 so the caller can decide
    /// whether to retry (chain hasn't seen it yet) vs surface as missing.
    pub async fn tx(&self, hash: &Hash) -> ChainResult<Option<NativeTxResponse>> {
        let url = format!("{}/tx/{}", self.base, hash);
        let resp = self.http.get(&url).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ChainError::Rpc(format!("native rest {status}: {body}")));
        }
        let body = resp.bytes().await?;
        let parsed: NativeTxResponse = serde_json::from_slice(&body)?;
        Ok(Some(parsed))
    }

    /// Fetch `/chain/info` — chain tip + pruning window + supply summary.
    /// Used by the backfill orchestrator to detect when an asked-for height
    /// has fallen out of the chain's block-body retention window so it can
    /// jump the cursor straight to `window_start_block` rather than walking
    /// 404s one-by-one.
    pub async fn chain_info(&self) -> ChainResult<ChainInfoResponse> {
        let url = format!("{}/chain/info", self.base);
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ChainError::Rpc(format!("native rest {status}: {body}")));
        }
        let body = resp.bytes().await?;
        let parsed: ChainInfoResponse = serde_json::from_slice(&body)?;
        Ok(parsed)
    }

    /// Fetch a block by height with full txs inlined. Returns None on 404
    /// (chain doesn't have this height yet, or asking past pruning window).
    pub async fn block(&self, h: BlockHeight) -> ChainResult<Option<NativeBlockResponse>> {
        let url = format!("{}/chain/blocks/{}", self.base, h.0);
        let resp = self.http.get(&url).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ChainError::Rpc(format!("native rest {status}: {body}")));
        }
        let body = resp.bytes().await?;
        let parsed: NativeBlockResponse = serde_json::from_slice(&body)?;
        Ok(Some(parsed))
    }
}

/// Subset of `/chain/info` — chain tip + the rolling block-body retention
/// window the chain advertises (`window_start_block` is the lowest height
/// whose body is still queryable via `/chain/blocks/<n>`; older heights
/// 404).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainInfoResponse {
    /// Current chain tip height.
    pub height: i64,
    /// Lowest height with a queryable block body. Heights below this have
    /// been pruned from the chain's storage. Asking for them returns 404.
    /// May be absent on archive-mode nodes that retain full history.
    #[serde(default)]
    pub window_start_block: Option<i64>,
    /// True if `height - window_start_block < total_blocks` (i.e. chain
    /// has pruned old bodies). Absent on archive-mode nodes.
    #[serde(default)]
    pub window_is_partial: Option<bool>,
}

/// Subset of the native `/chain/blocks/<n>` response shape that the indexer
/// needs. Mirrors the chain's `Block` serialization (lowercase + snake_case
/// fields, hashes are bare hex without `0x` prefix, `state_root` is a raw
/// 32-byte array, `transactions[]` is a vec of inline native tx objects).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeBlockResponse {
    /// Block height (monotonic, PK in PG).
    pub index: i64,
    /// Block hash, bare hex (no `0x` prefix).
    pub hash: String,
    /// Parent block hash, bare hex.
    pub previous_hash: String,
    /// Unix timestamp seconds (chain time).
    pub timestamp: i64,
    /// Validator address that proposed this block, lowercase `0x...`.
    pub validator: String,
    /// BFT round at which this block reached supermajority.
    pub round: i32,
    /// 32-byte state root, serialized as a JSON array of u8.
    #[serde(default)]
    pub state_root: Option<Vec<u8>>,
    /// Inline tx envelopes for every tx in this block.
    #[serde(default)]
    pub transactions: Vec<NativeBlockTx>,
}

/// Inline tx object as it appears inside a block's `transactions[]`.
/// Differs from [`NativeTxResponse`] (which is the standalone tx envelope
/// from `/tx/<hash>`): block-inlined txs don't carry the wrapping `block_*`
/// fields; the height is implicit in the parent block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeBlockTx {
    /// Tx hash, bare hex (no `0x` prefix).
    pub txid: String,
    /// Sender address. The literal string `"COINBASE"` for block-reward txs.
    pub from_address: String,
    /// `None` for contract creation. Sentrix's native tx serializer omits
    /// the field rather than emitting `null`, so default to None on missing.
    #[serde(default)]
    pub to_address: Option<String>,
    /// Sentri amount transferred.
    pub amount: u64,
    /// Sentri fee paid.
    pub fee: u64,
    /// Sender nonce.
    pub nonce: u64,
    /// Tx timestamp seconds (usually equal to block timestamp).
    pub timestamp: i64,
    /// Chain ID. 0 for COINBASE / system txs, 7119/7120 for user EVM txs.
    #[serde(default)]
    pub chain_id: i64,
    /// Hex-encoded calldata for EVM txs, plain string for system tags.
    #[serde(default)]
    pub data: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_minimal_response() {
        let raw = r#"{
            "txid": "0xabc",
            "from_address": "0x1111111111111111111111111111111111111111",
            "to_address": "0x2222222222222222222222222222222222222222",
            "amount": 100000000,
            "fee": 10000,
            "nonce": 1,
            "timestamp": 1700000000,
            "block_height": 12345,
            "tx_type": "native",
            "data": null,
            "extra_field_we_ignore": "ok"
        }"#;
        let parsed: NativeTxResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.txid, "0xabc");
        assert_eq!(parsed.amount, 100_000_000);
        assert_eq!(parsed.tx_type, "native");
        assert!(parsed.data.is_none());
    }

    #[test]
    fn rejects_missing_required_field() {
        let raw = r#"{ "txid": "0xabc" }"#;
        assert!(serde_json::from_str::<NativeTxResponse>(raw).is_err());
    }
}
