//! Query helpers for `cb_tokens` — one row per CoinBlast curve launch.

use crate::{DbResult, PgPool};
use indexer_domain::Wei;
use sqlx::Row;

/// Row to insert. Aggregate fields default to zero; the worker bumps them
/// inside the same SQL transaction as the trade insert.
#[derive(Debug, Clone)]
pub struct InsertCbToken {
    /// Curve contract address (PK).
    pub curve_address: String,
    /// Underlying ERC-20 token address.
    pub token_address: String,
    /// Curve owner / launcher.
    pub owner_address: String,
    /// Token name.
    pub name: String,
    /// Token symbol.
    pub symbol: String,
    /// Total tokens sold by the curve before graduation.
    pub curve_supply: Wei,
    /// SRX raised threshold that triggers graduation to AMM LP.
    pub graduation_threshold: Wei,
    /// Block of the CurveCreated event.
    pub created_block: i64,
    /// Tx that emitted CurveCreated.
    pub created_tx_hash: String,
}

/// Insert a curve. ON CONFLICT (curve_address) DO NOTHING — re-running the
/// chunk that emitted CurveCreated is a no-op.
pub async fn insert<'e, E>(executor: E, t: &InsertCbToken) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO cb_tokens (curve_address, token_address, owner_address, name, symbol, \
            curve_supply, graduation_threshold, is_graduated, created_block, created_tx_hash, \
            total_volume_srx, trade_count, last_price_srx) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, false, $8, $9, 0, 0, 0) \
         ON CONFLICT (curve_address) DO NOTHING",
    )
    .bind(&t.curve_address)
    .bind(&t.token_address)
    .bind(&t.owner_address)
    .bind(&t.name)
    .bind(&t.symbol)
    .bind(t.curve_supply)
    .bind(t.graduation_threshold)
    .bind(t.created_block)
    .bind(&t.created_tx_hash)
    .execute(executor)
    .await?;
    Ok(())
}

/// Bump aggregates after a Buy or Sell trade. `srx_amount` adds to volume,
/// `last_price` overwrites (last-trade-wins). Trade count bumps by 1.
pub async fn bump_trade_aggregate<'e, E>(
    executor: E,
    curve_address: &str,
    srx_amount: Wei,
    last_price: Wei,
) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "UPDATE cb_tokens \
         SET total_volume_srx = total_volume_srx + $2, \
             trade_count = trade_count + 1, \
             last_price_srx = $3 \
         WHERE curve_address = $1",
    )
    .bind(curve_address)
    .bind(srx_amount)
    .bind(last_price)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mark a curve as graduated.
pub async fn mark_graduated<'e, E>(executor: E, curve_address: &str) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query("UPDATE cb_tokens SET is_graduated = true WHERE curve_address = $1")
        .bind(curve_address)
        .execute(executor)
        .await?;
    Ok(())
}

/// Hydrate the worker's known-curves set on boot. Returns lowercase
/// addresses.
pub async fn known_curve_addresses(pool: &PgPool) -> DbResult<Vec<String>> {
    let rows = sqlx::query("SELECT curve_address FROM cb_tokens")
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|r| r.try_get::<String, _>("curve_address"))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}
