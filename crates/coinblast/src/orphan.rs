//! Orphan-curve adoption — handles direct-deployed CoinBlast curves that
//! never went through the factory (e.g. CBLAST Genesis pre-factory). On
//! first sighting of a Buy/Sell/Graduated event from an unknown emitter
//! we probe the contract via `eth_call` for `token()`, `curveSupply()`,
//! `graduationSrxThreshold()` — if all three return cleanly, we adopt
//! the curve into `cb_tokens` so subsequent events are tracked normally.
//!
//! Topic-collision defence: an unrelated contract that happens to emit
//! the same event signature won't have all three view functions; the
//! `eth_call` reverts and adoption fails. The worker memoises adoption
//! attempts (curve / not-curve) so each address is probed at most once
//! per worker lifetime.

use crate::{CoinblastError, CoinblastResult};
use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolCall, sol};
use indexer_chain::ChainProvider;
use indexer_db::{PgPool, cb_tokens};
use indexer_domain::Wei;

sol! {
    /// Minimal view ABI present on every real CoinBlastCurve.
    interface CoinBlastCurveViews {
        function token() external view returns (address);
        function curveSupply() external view returns (uint256);
        function graduationSrxThreshold() external view returns (uint256);
    }

    /// ERC-20 metadata. Best-effort — fall back to placeholders if either
    /// reverts.
    interface Erc20Meta {
        function name() external view returns (string);
        function symbol() external view returns (string);
    }
}

/// Probe an unknown emitter; if it answers as a CoinBlastCurve, adopt it
/// into `cb_tokens` (idempotent on PK conflict). Returns true on adopt.
pub async fn try_adopt(
    pool: &PgPool,
    provider: &ChainProvider,
    candidate: Address,
    created_block: i64,
    created_tx_hash: &str,
) -> CoinblastResult<bool> {
    // 1. Probe the curve view surface. Three sequential calls keep the
    //    code linear; a topic-collision contract reverts on the first one
    //    and we bail without paying for the rest.
    let token = match call_token(provider, candidate).await {
        Ok(a) => a,
        Err(_) => return Ok(false),
    };
    let curve_supply = match call_uint256(
        provider,
        candidate,
        CoinBlastCurveViews::curveSupplyCall {}.abi_encode(),
    )
    .await
    {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };
    let graduation_threshold = match call_uint256(
        provider,
        candidate,
        CoinBlastCurveViews::graduationSrxThresholdCall {}.abi_encode(),
    )
    .await
    {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };

    // 2. Best-effort metadata reads on the underlying token.
    let name = call_string(provider, token, Erc20Meta::nameCall {}.abi_encode())
        .await
        .unwrap_or_else(|_| "Unknown".to_string());
    let symbol = call_string(provider, token, Erc20Meta::symbolCall {}.abi_encode())
        .await
        .unwrap_or_else(|_| "???".to_string());

    // 3. Insert with zero owner — we don't have CurveCreated to give us one,
    //    and chasing the contract-creation tx isn't worth the round trip.
    let row = cb_tokens::InsertCbToken {
        curve_address: hex_addr(candidate),
        token_address: hex_addr(token),
        owner_address: ZERO_ADDR.to_string(),
        name,
        symbol,
        curve_supply: Wei::from(curve_supply),
        graduation_threshold: Wei::from(graduation_threshold),
        created_block,
        created_tx_hash: created_tx_hash.to_string(),
    };
    cb_tokens::insert(pool, &row).await?;
    tracing::info!(
        curve = %hex_addr(candidate),
        symbol = %row.symbol,
        "coinblast: adopted orphan curve (direct-deploy, no factory event)",
    );
    Ok(true)
}

const ZERO_ADDR: &str = "0x0000000000000000000000000000000000000000";

async fn call_token(provider: &ChainProvider, addr: Address) -> CoinblastResult<Address> {
    let data = CoinBlastCurveViews::tokenCall {}.abi_encode();
    let raw = provider.call(addr, data.into()).await?;
    let decoded = CoinBlastCurveViews::tokenCall::abi_decode_returns(&raw, true)
        .map_err(|e| CoinblastError::Decode(e.to_string()))?;
    Ok(decoded._0)
}

async fn call_uint256(
    provider: &ChainProvider,
    addr: Address,
    data: Vec<u8>,
) -> CoinblastResult<U256> {
    let raw = provider.call(addr, data.into()).await?;
    if raw.len() < 32 {
        return Err(CoinblastError::Decode(format!(
            "uint256 return too short: {} bytes",
            raw.len()
        )));
    }
    Ok(U256::from_be_slice(&raw[..32]))
}

async fn call_string(
    provider: &ChainProvider,
    addr: Address,
    data: Vec<u8>,
) -> CoinblastResult<String> {
    let raw = provider.call(addr, data.into()).await?;
    let decoded = Erc20Meta::nameCall::abi_decode_returns(&raw, true)
        .map_err(|e| CoinblastError::Decode(e.to_string()))?;
    Ok(decoded._0)
}

fn hex_addr(a: Address) -> String {
    format!("0x{}", hex::encode(a.as_slice()))
}
