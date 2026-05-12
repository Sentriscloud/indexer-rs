//! Convert chain responses into our `indexer_domain` row types.
//!
//! Two paths live here:
//!
//! - **Native REST** ([`to_domain_block_from_native`], [`to_domain_txs_from_native`])
//!   pulls `/chain/blocks/<n>` and maps the Sentrix-native shape directly.
//!   This is the working backfill path — Sentrix's `eth_getBlockByNumber`
//!   ignores the `full=true` flag (always returns hash arrays), so the
//!   alloy `Block` route can't decode the tx vec.
//! - **alloy Log** ([`to_domain_log`]) is still used for `eth_getLogs`,
//!   which Sentrix implements correctly.

use indexer_chain::{NativeBlockResponse, NativeBlockTx};
use indexer_domain::{
    Block as DomBlock, BlockHeight, Hash, Log as DomLog, Transaction as DomTx, TxIndex, TxType, Wei,
};

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

/// Add a `0x` prefix if missing. Sentrix's native serializer drops the
/// prefix on hashes/addresses; the domain layer wants it.
fn with_0x(s: &str) -> String {
    if s.starts_with("0x") || s.starts_with("0X") {
        s.to_string()
    } else {
        format!("0x{s}")
    }
}

/// Map a native `/chain/blocks/<n>` block (header view) into a domain
/// `Block`. Gas accounting fields aren't in the native shape (Sentrix
/// doesn't model gas at the block level the EVM way), so they're zeroed —
/// the receipts backfill, when wired, can fill them per-tx.
pub fn to_domain_block_from_native(b: &NativeBlockResponse) -> Result<DomBlock, ConvertError> {
    if b.index < 0 {
        return Err(ConvertError::OutOfRange("block.index".into()));
    }
    let tx_count = i32::try_from(b.transactions.len())
        .map_err(|_| ConvertError::OutOfRange("tx_count".into()))?;
    let state_root = b.state_root.as_ref().filter(|r| !r.is_empty()).map(|r| hex_hash(r));
    Ok(DomBlock {
        height: BlockHeight(b.index),
        hash: with_0x(&b.hash),
        parent_hash: with_0x(&b.previous_hash),
        timestamp: b.timestamp,
        validator: with_0x(&b.validator),
        gas_used: 0,
        gas_limit: 0,
        base_fee: None,
        tx_count,
        state_root,
        round: b.round,
        // Justification signer list is exposed via /chain/blocks/<n> as a
        // separate field (`justification`) that we don't decode yet; the
        // staking layer doesn't read it during backfill.
        justification_signers: Vec::new(),
    })
}

/// Map a native block's `transactions[]` into domain `Transaction` rows.
/// Lossy on EVM-only fields (gas_limit, gas_price) — they're not in the
/// native shape; rely on EVM JSON-RPC compat once the chain ships it.
pub fn to_domain_txs_from_native(b: &NativeBlockResponse) -> Result<Vec<DomTx>, ConvertError> {
    let height = BlockHeight(b.index);
    b.transactions
        .iter()
        .enumerate()
        .map(|(idx, t)| convert_native_tx(height, idx, t))
        .collect()
}

fn convert_native_tx(
    height: BlockHeight,
    idx: usize,
    t: &NativeBlockTx,
) -> Result<DomTx, ConvertError> {
    let tx_index =
        TxIndex(i32::try_from(idx).map_err(|_| ConvertError::OutOfRange("tx_index".into()))?);
    let nonce =
        i64::try_from(t.nonce).map_err(|_| ConvertError::OutOfRange("tx.nonce".into()))?;
    let from_addr = if t.from_address == "COINBASE" {
        // The TS schema uses a lowercase sentinel; the historical row is
        // `from_addr = '0x0000...0000'`, `tx_type = 'coinbase'`. Match that
        // exactly so parity-comparison reads stay aligned.
        "0x0000000000000000000000000000000000000000".to_string()
    } else {
        with_0x(&t.from_address)
    };
    let to_addr = t.to_address.as_ref().map(|a| with_0x(a));
    let tx_type = if t.from_address == "COINBASE" {
        TxType::Coinbase
    } else if t.chain_id == 0 {
        TxType::System
    } else {
        // chain_id == 7119 / 7120 indicates the EVM tx flow; we can't tell
        // pure native from EVM precisely without the tx_type field, so EVM
        // is the conservative bucket. Refine later if signal arrives.
        TxType::Evm
    };
    Ok(DomTx {
        hash: with_0x(&t.txid),
        block_height: height,
        tx_index,
        from_addr,
        to_addr,
        value: Wei::from(alloy_primitives::U256::from(t.amount)),
        gas_limit: 0,
        gas_used: None,
        gas_price: None,
        fee: Wei::from(alloy_primitives::U256::from(t.fee)),
        nonce,
        data: t.data.clone(),
        status: 1,
        contract_address: None,
        tx_type,
    })
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
}
