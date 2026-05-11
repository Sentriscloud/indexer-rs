//! `/accounts/active` + `/whale/transfers` — chain-wide leaderboards.
//! Aggregates over the existing `transactions` table; no extra schema.
//!
//! Both routes are cache-aside (chain tier, 60s TTL): aggregates are
//! expensive (`GROUP BY` over full `transactions`) but eventually-consistent,
//! so a 60s staleness ceiling trades off honestly.

use crate::error::{ApiError, ApiResult};
use crate::routes::clamp_limit;
use crate::{CacheTier, SharedState, cached};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::transactions;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ActiveAccount {
    /// 1-indexed rank within this response.
    rank: i64,
    address: String,
    /// Number renders fine here — sender activity caps well below i32 / f64
    /// loss; if a single sender ever crosses 2^53 we'll switch to string.
    tx_count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ActiveResponse {
    accounts: Vec<ActiveAccount>,
}

async fn accounts_active(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ActiveResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    let key = format!("accounts:active:{limit}");
    let resp: ActiveResponse = cached::get_or_load(&state, &key, CacheTier::Chain, || async {
        let rows = transactions::top_senders(&state.pool, limit).await?;
        Ok::<_, ApiError>(ActiveResponse {
            accounts: rows
                .into_iter()
                .enumerate()
                .map(|(i, (address, tx_count))| ActiveAccount {
                    rank: (i as i64) + 1,
                    address,
                    tx_count,
                })
                .collect(),
        })
    })
    .await?;
    Ok(Json(resp))
}

#[derive(Debug, Serialize, Deserialize)]
struct WhaleTransfer {
    hash: String,
    from: String,
    to: Option<String>,
    /// `numeric(78, 0)` value as decimal string.
    value: String,
    block_height: i64,
    timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WhaleResponse {
    transfers: Vec<WhaleTransfer>,
}

async fn whale_transfers(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<WhaleResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    let key = format!("whale:transfers:{limit}");
    let resp: WhaleResponse = cached::get_or_load(&state, &key, CacheTier::Chain, || async {
        let rows = transactions::top_by_value(&state.pool, limit).await?;
        Ok::<_, ApiError>(WhaleResponse {
            transfers: rows
                .into_iter()
                .map(|r| WhaleTransfer {
                    hash: r.hash,
                    from: r.from_addr,
                    to: r.to_addr,
                    value: r.value,
                    block_height: r.block_height,
                    timestamp: r.timestamp,
                })
                .collect(),
        })
    })
    .await?;
    Ok(Json(resp))
}

/// Router for `/accounts/active` + `/whale/transfers`.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/accounts/active", get(accounts_active))
        .route("/whale/transfers", get(whale_transfers))
}
