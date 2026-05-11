//! `/stats/daily` — chain-wide aggregates per day_bucket from the
//! `stats_daily_mv` materialised view (db migration 0002).

use crate::SharedState;
use crate::error::ApiResult;
use crate::routes::clamp_limit;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::stats;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
}

#[derive(Debug, Serialize)]
struct DailyRow {
    /// Decimal-string `floor(timestamp / 86400)`.
    day_bucket: String,
    /// Decimal-string block count for the bucket.
    block_count: String,
    /// Decimal-string sum of tx_count.
    tx_count: String,
    /// Decimal-string sum of gas_used.
    gas_used: String,
    /// Decimal-string lowest height in the bucket.
    first_height: String,
    /// Decimal-string highest height in the bucket.
    last_height: String,
}

#[derive(Debug, Serialize)]
struct DailyResponse {
    daily: Vec<DailyRow>,
}

async fn daily(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<DailyResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    let rows = stats::daily(&state.pool, limit).await?;
    Ok(Json(DailyResponse {
        daily: rows
            .into_iter()
            .map(|r| DailyRow {
                day_bucket: r.day_bucket.to_string(),
                block_count: r.block_count.to_string(),
                tx_count: r.tx_count.to_string(),
                gas_used: r.gas_used.to_string(),
                first_height: r.first_height.to_string(),
                last_height: r.last_height.to_string(),
            })
            .collect(),
    }))
}

/// Router for `/stats/daily`.
pub fn router() -> Router<SharedState> {
    Router::new().route("/stats/daily", get(daily))
}
