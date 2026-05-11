//! `/address/:addr/{txs,transfers}` — paginated activity for an address.
//! Mirrors the TS Fastify port's two address-page endpoints. Address is
//! lowercased to match how the indexer writes it (storage layer normalises
//! on insert).

use crate::SharedState;
use crate::error::ApiResult;
use crate::routes::clamp_limit;
use crate::serialise::{WireTransaction, WireTransfer};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::{token_transfers, transactions};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TransferQuery {
    limit: Option<String>,
    /// Optional standard narrow: erc20 | erc721 | erc1155.
    standard: Option<String>,
}

#[derive(Debug, Serialize)]
struct TxsResponse {
    transactions: Vec<WireTransaction>,
}

#[derive(Debug, Serialize)]
struct TransfersResponse {
    transfers: Vec<WireTransfer>,
}

async fn txs(
    State(state): State<SharedState>,
    Path(addr): Path<String>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<TxsResponse>> {
    let addr = addr.to_lowercase();
    let limit = clamp_limit(q.limit.as_deref());
    let rows = transactions::for_address(&state.pool, &addr, limit).await?;
    Ok(Json(TxsResponse {
        transactions: rows.iter().map(WireTransaction::from).collect(),
    }))
}

async fn transfers(
    State(state): State<SharedState>,
    Path(addr): Path<String>,
    Query(q): Query<TransferQuery>,
) -> ApiResult<Json<TransfersResponse>> {
    let addr = addr.to_lowercase();
    let limit = clamp_limit(q.limit.as_deref());
    let rows =
        token_transfers::for_address(&state.pool, &addr, q.standard.as_deref(), limit).await?;
    Ok(Json(TransfersResponse {
        transfers: rows.iter().map(WireTransfer::from).collect(),
    }))
}

/// Router for `/address/:addr/{txs,transfers}`.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/address/{addr}/txs", get(txs))
        .route("/address/{addr}/transfers", get(transfers))
}
