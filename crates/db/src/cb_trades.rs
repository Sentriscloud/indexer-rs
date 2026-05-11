//! Query helpers for `cb_trades` — one row per Buy/Sell/Graduated event.

use crate::DbResult;
use indexer_domain::Wei;

/// CoinBlast trade kind. Mirrors the `cb_trades.type` column values.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TradeKind {
    /// Buy: trader pays SRX, receives tokens.
    Buy,
    /// Sell: trader burns tokens, receives SRX.
    Sell,
    /// Graduation: curve raised threshold, LP seeded into the AMM. One-shot
    /// per curve.
    Graduated,
}

impl TradeKind {
    /// String form stored in `cb_trades.type` (varchar(12)).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
            Self::Graduated => "graduated",
        }
    }
}

/// Row to insert. `id` is PG-assigned identity.
#[derive(Debug, Clone)]
pub struct InsertCbTrade {
    /// Curve contract address.
    pub curve_address: String,
    /// Underlying token (optional — graduation rows often leave it null in
    /// the TS port; preserved for parity).
    pub token_address: Option<String>,
    /// Trade kind.
    pub kind: TradeKind,
    /// Trader address (buyer/seller; for graduation the value is the
    /// AMM pair address).
    pub trader_address: String,
    /// SRX amount (in for buy, out for sell, total liquidity for graduation).
    pub srx_amount: Wei,
    /// Token amount (out for buy, in for sell, total liquidity for graduation).
    pub token_amount: Wei,
    /// Fee paid (Buy/Sell only; 0 for graduation).
    pub fee: Wei,
    /// Block number of the event.
    pub block_number: i64,
    /// Tx hash that emitted the event.
    pub tx_hash: String,
    /// Block-wide log index of the event.
    pub log_index: i32,
}

/// Insert a trade row. ON CONFLICT (tx_hash, log_index) DO NOTHING — the
/// idempotency key for re-runs. Returns true on insert, false on conflict.
pub async fn insert<'e, E>(executor: E, t: &InsertCbTrade) -> DbResult<bool>
where
    E: sqlx::PgExecutor<'e>,
{
    let res = sqlx::query(
        "INSERT INTO cb_trades (curve_address, token_address, type, trader_address, \
            srx_amount, token_amount, fee, block_number, tx_hash, log_index) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         ON CONFLICT (tx_hash, log_index) DO NOTHING",
    )
    .bind(&t.curve_address)
    .bind(&t.token_address)
    .bind(t.kind.as_str())
    .bind(&t.trader_address)
    .bind(t.srx_amount)
    .bind(t.token_amount)
    .bind(t.fee)
    .bind(t.block_number)
    .bind(&t.tx_hash)
    .bind(t.log_index)
    .execute(executor)
    .await?;
    Ok(res.rows_affected() > 0)
}
