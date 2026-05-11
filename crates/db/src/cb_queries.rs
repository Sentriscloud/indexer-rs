//! Read-side query helpers for the CoinBlast tables. Insert + aggregate
//! mutations live in `cb_tokens` / `cb_trades`; this module is the read
//! surface for the API routes.

use crate::{DbResult, PgPool};
use sqlx::Row;

/// One row of `/coinblast/tokens` — list of curves with their key fields.
#[derive(Debug, Clone)]
pub struct CbTokenRow {
    /// Curve contract.
    pub curve_address: String,
    /// Underlying ERC-20.
    pub token_address: String,
    /// Curve owner / launcher (zero addr for orphan-adopted curves).
    pub owner_address: String,
    /// Display name.
    pub name: String,
    /// Display symbol.
    pub symbol: String,
    /// Decimal-string total supply.
    pub curve_supply: String,
    /// Decimal-string SRX threshold.
    pub graduation_threshold: String,
    /// Did this curve raise the threshold + seed an AMM pair?
    pub is_graduated: bool,
    /// Block of the CurveCreated event.
    pub created_block: i64,
    /// Tx that created the curve.
    pub created_tx_hash: String,
    /// Decimal-string lifetime SRX volume.
    pub total_volume_srx: String,
    /// Buy + Sell count (Graduated excluded).
    pub trade_count: i32,
    /// Decimal-string price of the latest Buy/Sell.
    pub last_price_srx: String,
}

/// List curves, newest-first by `created_block`. `limit` clamps already done
/// at the route layer.
pub async fn list_tokens(pool: &PgPool, limit: i64) -> DbResult<Vec<CbTokenRow>> {
    let rows = sqlx::query(
        "SELECT curve_address, token_address, owner_address, name, symbol, \
                curve_supply::text, graduation_threshold::text, is_graduated, \
                created_block, created_tx_hash, \
                total_volume_srx::text, trade_count, last_price_srx::text \
         FROM cb_tokens ORDER BY created_block DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(row_to_token)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// Single curve detail by `curve_address` (lowercased on entry by the caller).
pub async fn get_token(pool: &PgPool, curve_address: &str) -> DbResult<Option<CbTokenRow>> {
    let row = sqlx::query(
        "SELECT curve_address, token_address, owner_address, name, symbol, \
                curve_supply::text, graduation_threshold::text, is_graduated, \
                created_block, created_tx_hash, \
                total_volume_srx::text, trade_count, last_price_srx::text \
         FROM cb_tokens WHERE curve_address = $1",
    )
    .bind(curve_address)
    .fetch_optional(pool)
    .await?;
    row.map(row_to_token).transpose().map_err(Into::into)
}

/// Compact trade row for `/coinblast/trades`. `srx_amount` and `token_amount`
/// carried as decimal strings.
#[derive(Debug, Clone)]
pub struct CbTradeRow {
    /// PG-assigned identity.
    pub id: i64,
    /// Curve this trade hit.
    pub curve_address: String,
    /// buy / sell / graduated.
    pub kind: String,
    /// Trader address (or AMM pair address for graduated rows).
    pub trader_address: String,
    /// Decimal-string SRX amount.
    pub srx_amount: String,
    /// Decimal-string token amount.
    pub token_amount: String,
    /// Decimal-string fee (zero for graduated).
    pub fee: String,
    /// Block number of the event.
    pub block_number: i64,
    /// Tx hash.
    pub tx_hash: String,
    /// Block-wide log index.
    pub log_index: i32,
}

/// Recent trades, optionally narrowed to one curve. Newest-first by block.
pub async fn list_trades(
    pool: &PgPool,
    curve: Option<&str>,
    limit: i64,
) -> DbResult<Vec<CbTradeRow>> {
    let rows = match curve {
        Some(c) => {
            sqlx::query(
                "SELECT id, curve_address, type, trader_address, \
                    srx_amount::text, token_amount::text, fee::text, \
                    block_number, tx_hash, log_index \
             FROM cb_trades WHERE curve_address = $1 \
             ORDER BY block_number DESC, log_index DESC LIMIT $2",
            )
            .bind(c)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query(
                "SELECT id, curve_address, type, trader_address, \
                    srx_amount::text, token_amount::text, fee::text, \
                    block_number, tx_hash, log_index \
             FROM cb_trades \
             ORDER BY block_number DESC, log_index DESC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    rows.into_iter()
        .map(row_to_trade)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

fn row_to_token(row: sqlx::postgres::PgRow) -> Result<CbTokenRow, sqlx::Error> {
    Ok(CbTokenRow {
        curve_address: row.try_get("curve_address")?,
        token_address: row.try_get("token_address")?,
        owner_address: row.try_get("owner_address")?,
        name: row.try_get("name")?,
        symbol: row.try_get("symbol")?,
        curve_supply: row.try_get("curve_supply")?,
        graduation_threshold: row.try_get("graduation_threshold")?,
        is_graduated: row.try_get("is_graduated")?,
        created_block: row.try_get("created_block")?,
        created_tx_hash: row.try_get("created_tx_hash")?,
        total_volume_srx: row.try_get("total_volume_srx")?,
        trade_count: row.try_get("trade_count")?,
        last_price_srx: row.try_get("last_price_srx")?,
    })
}

fn row_to_trade(row: sqlx::postgres::PgRow) -> Result<CbTradeRow, sqlx::Error> {
    Ok(CbTradeRow {
        id: row.try_get("id")?,
        curve_address: row.try_get("curve_address")?,
        kind: row.try_get("type")?,
        trader_address: row.try_get("trader_address")?,
        srx_amount: row.try_get("srx_amount")?,
        token_amount: row.try_get("token_amount")?,
        fee: row.try_get("fee")?,
        block_number: row.try_get("block_number")?,
        tx_hash: row.try_get("tx_hash")?,
        log_index: row.try_get("log_index")?,
    })
}
