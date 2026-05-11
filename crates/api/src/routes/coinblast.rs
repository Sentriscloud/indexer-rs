//! `/coinblast/*` — launchpad-facing read API. Mirrors the surface the
//! coinblast frontend hits today (token cards, curve detail, recent
//! trades feed). Same wire conventions as the chain-wide routes.

use crate::SharedState;
use crate::error::{ApiError, ApiResult};
use crate::routes::clamp_limit;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::cb_queries::{self, CbTokenRow, CbTradeRow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TradeQuery {
    limit: Option<String>,
    /// Optional curve filter — restrict to one launch.
    curve: Option<String>,
}

#[derive(Debug, Serialize)]
struct WireToken {
    curve_address: String,
    token_address: String,
    owner_address: String,
    name: String,
    symbol: String,
    /// Decimal-string `numeric(78, 0)` total supply.
    curve_supply: String,
    /// Decimal-string SRX threshold for graduation.
    graduation_threshold: String,
    is_graduated: bool,
    created_block: i64,
    created_tx_hash: String,
    /// Decimal-string lifetime SRX volume.
    total_volume_srx: String,
    /// Buy + Sell count.
    trade_count: i32,
    /// Decimal-string price of the latest Buy/Sell.
    last_price_srx: String,
}

impl From<CbTokenRow> for WireToken {
    fn from(t: CbTokenRow) -> Self {
        Self {
            curve_address: t.curve_address,
            token_address: t.token_address,
            owner_address: t.owner_address,
            name: t.name,
            symbol: t.symbol,
            curve_supply: t.curve_supply,
            graduation_threshold: t.graduation_threshold,
            is_graduated: t.is_graduated,
            created_block: t.created_block,
            created_tx_hash: t.created_tx_hash,
            total_volume_srx: t.total_volume_srx,
            trade_count: t.trade_count,
            last_price_srx: t.last_price_srx,
        }
    }
}

#[derive(Debug, Serialize)]
struct WireTrade {
    id: i64,
    curve_address: String,
    /// "buy" | "sell" | "graduated".
    #[serde(rename = "type")]
    kind: String,
    trader_address: String,
    /// Decimal-string SRX amount.
    srx_amount: String,
    /// Decimal-string token amount.
    token_amount: String,
    /// Decimal-string fee (zero for graduated).
    fee: String,
    block_number: i64,
    tx_hash: String,
    log_index: i32,
}

impl From<CbTradeRow> for WireTrade {
    fn from(t: CbTradeRow) -> Self {
        Self {
            id: t.id,
            curve_address: t.curve_address,
            kind: t.kind,
            trader_address: t.trader_address,
            srx_amount: t.srx_amount,
            token_amount: t.token_amount,
            fee: t.fee,
            block_number: t.block_number,
            tx_hash: t.tx_hash,
            log_index: t.log_index,
        }
    }
}

#[derive(Debug, Serialize)]
struct TokensResponse {
    tokens: Vec<WireToken>,
}

#[derive(Debug, Serialize)]
struct TokenDetailResponse {
    token: WireToken,
}

#[derive(Debug, Serialize)]
struct TradesResponse {
    trades: Vec<WireTrade>,
}

async fn list_tokens(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<TokensResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    let rows = cb_queries::list_tokens(&state.pool, limit).await?;
    Ok(Json(TokensResponse {
        tokens: rows.into_iter().map(WireToken::from).collect(),
    }))
}

async fn token_detail(
    State(state): State<SharedState>,
    Path(curve): Path<String>,
) -> ApiResult<Json<TokenDetailResponse>> {
    let curve = curve.to_lowercase();
    let row = cb_queries::get_token(&state.pool, &curve)
        .await?
        .ok_or_else(|| ApiError::NotFound("curve".into()))?;
    Ok(Json(TokenDetailResponse { token: row.into() }))
}

async fn list_trades(
    State(state): State<SharedState>,
    Query(q): Query<TradeQuery>,
) -> ApiResult<Json<TradesResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    let curve = q.curve.as_deref().map(|s| s.to_lowercase());
    let rows = cb_queries::list_trades(&state.pool, curve.as_deref(), limit).await?;
    Ok(Json(TradesResponse {
        trades: rows.into_iter().map(WireTrade::from).collect(),
    }))
}

/// Router for `/coinblast/{tokens,tokens/:curve,trades}`.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/coinblast/tokens", get(list_tokens))
        .route("/coinblast/tokens/{curve}", get(token_detail))
        .route("/coinblast/trades", get(list_trades))
}
