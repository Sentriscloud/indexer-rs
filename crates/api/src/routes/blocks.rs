//! `/blocks` + `/blocks/:height` — list-newest-first + single block detail
//! with nested transactions. TS port at `apps/api/src/routes/native.ts`.

use crate::error::{ApiError, ApiResult};
use crate::routes::clamp_limit;
use crate::serialise::{WireBlock, WireBlockWithTxs, WireTransaction};
use crate::{CacheTier, SharedState, cached};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::{blocks, transactions};
use indexer_domain::BlockHeight;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
    before: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    blocks: Vec<WireBlock>,
}

async fn list(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ListResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    let before = match q.before {
        None => None,
        Some(s) => Some(BlockHeight(s.parse::<i64>().map_err(|_| {
            ApiError::InvalidQuery("invalid before: must be a non-negative integer".into())
        })?)),
    };
    // Cache-aside: chain-tier (60s TTL) — list is invalidated by every new
    // block, but at >1 req/s the 60s TTL still saves >>50 PG reads/min.
    let key = format!(
        "blocks:list:{limit}:{}",
        before
            .map(|b| b.0.to_string())
            .unwrap_or_else(|| "tip".into())
    );
    let response: ListResponse = cached::get_or_load(&state, &key, CacheTier::Chain, || async {
        let rows = blocks::list_before(&state.pool, before, limit).await?;
        Ok::<_, ApiError>(ListResponse {
            blocks: rows.iter().map(WireBlock::from).collect(),
        })
    })
    .await?;
    Ok(Json(response))
}

#[derive(Debug, Serialize)]
struct DetailResponse {
    block: WireBlockWithTxs,
}

async fn detail(
    State(state): State<SharedState>,
    Path(height_str): Path<String>,
) -> ApiResult<Json<DetailResponse>> {
    let h = BlockHeight(height_str.parse::<i64>().map_err(|_| {
        ApiError::InvalidQuery("invalid height: must be a non-negative integer".into())
    })?);
    let block = blocks::get_by_height(&state.pool, h)
        .await?
        .ok_or_else(|| ApiError::NotFound("block".into()))?;
    let txs = transactions::for_block(&state.pool, h).await?;
    Ok(Json(DetailResponse {
        block: WireBlockWithTxs {
            block: (&block).into(),
            transactions: txs.iter().map(WireTransaction::from).collect(),
        },
    }))
}

/// Router for `/blocks` + `/blocks/:height`.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/blocks", get(list))
        .route("/blocks/{height}", get(detail))
}
