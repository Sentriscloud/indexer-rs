//! Per-event handlers. Each takes a `&mut sqlx::Transaction<'_, Postgres>`
//! so the worker can wrap a trade insert + aggregate update in one
//! atomic SQL transaction (idempotent under chunk re-run).

use crate::events::{Buy, CurveCreated, Graduated, Sell};
use crate::{CoinblastError, CoinblastResult};
use alloy_primitives::Address;
use alloy_rpc_types::Log;
use alloy_sol_types::SolEvent;
use indexer_db::{cb_tokens, cb_trades};
use indexer_domain::Wei;

/// Apply a `CurveCreated` log: insert into `cb_tokens` (no-op on conflict).
pub async fn apply_curve_created<'e, E>(executor: E, l: &Log) -> CoinblastResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    let block = require_block_number(l)?;
    let tx_hash = require_tx_hash(l)?;
    let event = decode::<CurveCreated>(l)?;
    let row = cb_tokens::InsertCbToken {
        curve_address: hex_addr(event.curve),
        token_address: hex_addr(event.token),
        owner_address: hex_addr(event.owner),
        name: event.name,
        symbol: event.symbol,
        curve_supply: Wei::from(event.curveSupply),
        graduation_threshold: Wei::from(event.graduationSrxThreshold),
        created_block: block,
        created_tx_hash: tx_hash,
    };
    cb_tokens::insert(executor, &row).await?;
    Ok(())
}

/// Apply a `Buy` log: insert trade + bump aggregates atomically. Returns
/// true if the trade was new (inserted), false if it was a re-run.
pub async fn apply_buy(pool: &indexer_db::PgPool, l: &Log) -> CoinblastResult<bool> {
    let block = require_block_number(l)?;
    let log_index = require_log_index(l)?;
    let tx_hash = require_tx_hash(l)?;
    let curve = require_emitter(l)?;
    let event = decode::<Buy>(l)?;
    let srx = Wei::from(event.srxIn);
    let tokens = Wei::from(event.tokensOut);
    let last_price = price_per_token(srx, tokens);
    write_trade(
        pool,
        cb_trades::InsertCbTrade {
            curve_address: curve.clone(),
            token_address: None,
            kind: cb_trades::TradeKind::Buy,
            trader_address: hex_addr(event.buyer),
            srx_amount: srx,
            token_amount: tokens,
            fee: Wei::from(event.fee),
            block_number: block,
            tx_hash,
            log_index,
        },
        srx,
        last_price,
        true,
    )
    .await
}

/// Apply a `Sell` log: insert trade + bump aggregates atomically.
pub async fn apply_sell(pool: &indexer_db::PgPool, l: &Log) -> CoinblastResult<bool> {
    let block = require_block_number(l)?;
    let log_index = require_log_index(l)?;
    let tx_hash = require_tx_hash(l)?;
    let curve = require_emitter(l)?;
    let event = decode::<Sell>(l)?;
    let srx = Wei::from(event.srxOut);
    let tokens = Wei::from(event.tokensIn);
    let last_price = price_per_token(srx, tokens);
    write_trade(
        pool,
        cb_trades::InsertCbTrade {
            curve_address: curve.clone(),
            token_address: None,
            kind: cb_trades::TradeKind::Sell,
            trader_address: hex_addr(event.seller),
            srx_amount: srx,
            token_amount: tokens,
            fee: Wei::from(event.fee),
            block_number: block,
            tx_hash,
            log_index,
        },
        srx,
        last_price,
        true,
    )
    .await
}

/// Apply a `Graduated` log: insert trade row (kind = graduated) + flip
/// `cb_tokens.is_graduated`.
pub async fn apply_graduated(pool: &indexer_db::PgPool, l: &Log) -> CoinblastResult<bool> {
    let block = require_block_number(l)?;
    let log_index = require_log_index(l)?;
    let tx_hash = require_tx_hash(l)?;
    let curve = require_emitter(l)?;
    let event = decode::<Graduated>(l)?;
    let srx = Wei::from(event.srxLiquidity);
    let tokens = Wei::from(event.tokenLiquidity);
    let mut tx = pool.begin().await?;
    let inserted = cb_trades::insert(
        &mut *tx,
        &cb_trades::InsertCbTrade {
            curve_address: curve.clone(),
            token_address: None,
            kind: cb_trades::TradeKind::Graduated,
            trader_address: hex_addr(event.pair),
            srx_amount: srx,
            token_amount: tokens,
            fee: Wei::ZERO,
            block_number: block,
            tx_hash,
            log_index,
        },
    )
    .await?;
    if inserted {
        cb_tokens::mark_graduated(&mut *tx, &curve).await?;
    }
    tx.commit().await?;
    Ok(inserted)
}

// ── helpers ────────────────────────────────────────────────────────────

async fn write_trade(
    pool: &indexer_db::PgPool,
    row: cb_trades::InsertCbTrade,
    srx_amount: Wei,
    last_price: Wei,
    bump: bool,
) -> CoinblastResult<bool> {
    let mut tx = pool.begin().await?;
    let inserted = cb_trades::insert(&mut *tx, &row).await?;
    if inserted && bump {
        cb_tokens::bump_trade_aggregate(&mut *tx, &row.curve_address, srx_amount, last_price)
            .await?;
    }
    tx.commit().await?;
    Ok(inserted)
}

fn decode<E: SolEvent>(l: &Log) -> CoinblastResult<E> {
    let topics: Vec<_> = l.topics().to_vec();
    let data = l.data().data.as_ref();
    let log_data = alloy_primitives::LogData::new_unchecked(topics, data.to_vec().into());
    E::decode_log_data(&log_data, true).map_err(|e| CoinblastError::Decode(e.to_string()))
}

fn require_block_number(l: &Log) -> CoinblastResult<i64> {
    let n = l
        .block_number
        .ok_or_else(|| CoinblastError::Invalid("log.block_number missing".into()))?;
    i64::try_from(n).map_err(|_| CoinblastError::Invalid(format!("block_number {n} > i64::MAX")))
}

fn require_log_index(l: &Log) -> CoinblastResult<i32> {
    let n = l
        .log_index
        .ok_or_else(|| CoinblastError::Invalid("log.log_index missing".into()))?;
    i32::try_from(n).map_err(|_| CoinblastError::Invalid(format!("log_index {n} > i32::MAX")))
}

fn require_tx_hash(l: &Log) -> CoinblastResult<String> {
    l.transaction_hash
        .map(|h| format!("0x{}", hex::encode(h.as_slice())))
        .ok_or_else(|| CoinblastError::Invalid("log.transaction_hash missing".into()))
}

fn require_emitter(l: &Log) -> CoinblastResult<String> {
    Ok(hex_addr(l.address()))
}

fn hex_addr(a: Address) -> String {
    format!("0x{}", hex::encode(a.as_slice()))
}

/// SRX per token (last-trade price). Uses i64-safe division — if `tokens`
/// is zero we return zero (defensive: a zero-token trade would be a contract
/// bug, but we don't want to panic the worker over it).
fn price_per_token(srx: Wei, tokens: Wei) -> Wei {
    use alloy_primitives::U256;
    let t = tokens.0;
    if t.is_zero() {
        Wei::ZERO
    } else {
        Wei::from(srx.0.checked_div(t).unwrap_or(U256::ZERO))
    }
}
