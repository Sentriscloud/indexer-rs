//! Convert alloy `Block` (rpc-types) into our `indexer_domain::Block` +
//! the underlying `Transaction` rows.
//!
//! Implementation note: alloy 0.7's `rpc_types::Transaction` hides its
//! interesting fields inside `inner: Recovered<TxEnvelope>` and surfaces
//! them via traits (`alloy_consensus::Transaction`). The trait surface
//! churns between alloy minor versions; we'd be re-doing this every bump.
//!
//! Instead we lean on alloy's own JSON shape (which matches the wire
//! `eth_getBlockByNumber` format that our DTO `WireTx` mirrors). One
//! `serde_json` round-trip + decode is cheap at our request rates and
//! decouples us from alloy's internal API.

use alloy_rpc_types::Block as RpcBlock;
use indexer_domain::{
    Block as DomBlock, BlockHeight, Hash, Log as DomLog, Transaction as DomTx, TxIndex, TxType, Wei,
};
use serde::Deserialize;
use std::str::FromStr;

/// Header-level Block (no txs).
pub fn to_domain_block(rpc: &RpcBlock) -> Result<DomBlock, ConvertError> {
    let header = &rpc.header;
    let h = BlockHeight::from(header.number);
    Ok(DomBlock {
        height: h,
        hash: hex_hash(header.hash.as_slice()),
        parent_hash: hex_hash(header.parent_hash.as_slice()),
        timestamp: i64::try_from(header.timestamp)
            .map_err(|_| ConvertError::OutOfRange("timestamp".into()))?,
        validator: hex_addr(header.beneficiary.as_slice()),
        gas_used: i64::try_from(header.gas_used)
            .map_err(|_| ConvertError::OutOfRange("gas_used".into()))?,
        gas_limit: i64::try_from(header.gas_limit)
            .map_err(|_| ConvertError::OutOfRange("gas_limit".into()))?,
        base_fee: header.base_fee_per_gas.map(Wei::from),
        tx_count: i32::try_from(rpc.transactions.len())
            .map_err(|_| ConvertError::OutOfRange("tx_count".into()))?,
        state_root: Some(hex_hash(header.state_root.as_slice())),
        // Sentrix-native fields the EVM RPC view doesn't expose. Filled in
        // by the native REST follow-up when the sync layer needs them; for
        // now we default to safe zeros / empties.
        round: 0,
        justification_signers: Vec::new(),
    })
}

/// Convert each tx in the RPC block to our domain shape. Returns empty when
/// the block was fetched without `transactions=full`.
pub fn to_domain_txs(rpc: &RpcBlock) -> Result<Vec<DomTx>, ConvertError> {
    // Round-trip the alloy Block through JSON to read tx fields by name.
    let v =
        serde_json::to_value(rpc).map_err(|e| ConvertError::Decode(format!("rpc->json: {e}")))?;
    let wire: WireBlock = serde_json::from_value(v)
        .map_err(|e| ConvertError::Decode(format!("json->WireBlock: {e}")))?;
    let height = BlockHeight::from(rpc.header.number);
    wire.transactions
        .iter()
        .enumerate()
        .map(|(idx, t)| convert_wire_tx(height, idx, t))
        .collect()
}

fn convert_wire_tx(height: BlockHeight, idx: usize, t: &WireTx) -> Result<DomTx, ConvertError> {
    let tx_index =
        TxIndex(i32::try_from(idx).map_err(|_| ConvertError::OutOfRange("tx_index".into()))?);
    Ok(DomTx {
        hash: t.hash.clone(),
        block_height: height,
        tx_index,
        from_addr: t.from.clone(),
        to_addr: t.to.clone(),
        value: parse_wei("value", &t.value)?,
        gas_limit: parse_hex_i64("gas", &t.gas)?,
        gas_used: None,
        gas_price: t
            .gas_price
            .as_deref()
            .map(|s| parse_wei("gas_price", s))
            .transpose()?,
        fee: Wei::ZERO,
        nonce: parse_hex_i64("nonce", &t.nonce)?,
        data: Some(t.input.clone()),
        status: 1,
        contract_address: None,
        // Pre-Voyager / pre-EVM blocks carry native-only txs; this hint gets
        // refined by the native REST follow-up. EVM is the safe default for
        // EVM-shaped reads at this layer.
        tx_type: TxType::Evm,
    })
}

/// Convert an alloy `Log` into our domain `Log`. The block's logs come from
/// `eth_getLogs` (separate call from `eth_getBlockByNumber`).
pub fn to_domain_log(rpc: &alloy_rpc_types::Log) -> Result<DomLog, ConvertError> {
    let height = BlockHeight::from(
        rpc.block_number
            .ok_or_else(|| ConvertError::Missing("log.block_number".into()))?,
    );
    let log_index = i32::try_from(
        rpc.log_index
            .ok_or_else(|| ConvertError::Missing("log.log_index".into()))?,
    )
    .map_err(|_| ConvertError::OutOfRange("log_index".into()))?;
    let tx_hash = rpc
        .transaction_hash
        .map(|h| hex_hash(h.as_slice()))
        .ok_or_else(|| ConvertError::Missing("log.transaction_hash".into()))?;
    let topics = rpc.topics();
    let topic = |i: usize| topics.get(i).map(|t| hex_hash(t.as_slice()));
    Ok(DomLog {
        block_height: height,
        tx_hash,
        log_index: indexer_domain::LogIndex(log_index),
        address: hex_addr(rpc.address().as_slice()),
        topic0: topic(0),
        topic1: topic(1),
        topic2: topic(2),
        topic3: topic(3),
        data: Some(format!("0x{}", hex::encode(rpc.data().data.as_ref()))),
    })
}

fn hex_hash(bytes: &[u8]) -> Hash {
    format!("0x{}", hex::encode(bytes))
}

fn hex_addr(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn parse_hex_i64(field: &str, s: &str) -> Result<i64, ConvertError> {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    i64::from_str_radix(stripped, 16)
        .map_err(|e| ConvertError::Decode(format!("{field} '{s}': {e}")))
}

fn parse_wei(field: &str, s: &str) -> Result<Wei, ConvertError> {
    use alloy_primitives::U256;
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    let u = U256::from_str_radix(stripped, 16)
        .map_err(|e| ConvertError::Decode(format!("{field} '{s}': {e}")))?;
    let _ = Wei::from_str; // keep import alive (used in tests).
    Ok(Wei::from(u))
}

#[derive(Deserialize)]
struct WireBlock {
    transactions: Vec<WireTx>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireTx {
    hash: String,
    from: String,
    to: Option<String>,
    value: String,
    gas: String,
    gas_price: Option<String>,
    nonce: String,
    input: String,
}

/// Conversion failed mid-flight. These are bugs (chain returned something
/// out of expected range), so the sync layer surfaces them as `SyncError`.
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// A field that should have been present was None.
    #[error("missing: {0}")]
    Missing(String),
    /// Numeric value didn't fit our storage type (e.g. block timestamp > i64).
    #[error("out of range: {0}")]
    OutOfRange(String),
    /// JSON / hex decode failure.
    #[error("decode: {0}")]
    Decode(String),
}
