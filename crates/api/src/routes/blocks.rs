//! `/blocks` + `/blocks/:height` — list-newest-first + single block detail
//! with nested transactions. TS port at `apps/api/src/routes/native.ts`.

use crate::SharedState;
use crate::error::{ApiError, ApiResult};
use crate::routes::clamp_limit;
use crate::serialise::{WireBlock, WireBlockWithTxs, WireTransaction};
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

#[derive(Debug, Serialize)]
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
    let rows = blocks::list_before(&state.pool, before, limit).await?;
    Ok(Json(ListResponse {
        blocks: rows.iter().map(WireBlock::from).collect(),
    }))
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
