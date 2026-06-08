//! `/tx/:hash` — single tx detail with all logs ordered by `log_index`.
//! TS port: `apps/api/src/routes/native.ts`. Hash is lowercased to mirror
//! the TS port's case-insensitive lookup.

use crate::SharedState;
use crate::error::{ApiError, ApiResult};
use crate::serialise::{WireLog, WireTransaction};
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::{blocks, logs, transactions};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct TxResponse {
    tx: WireTransaction,
    logs: Vec<WireLog>,
    /// Unix-seconds chain time of the tx's block. The per-tx row carries no
    /// timestamp (it lives on `blocks`), so the detail view joins it here —
    /// otherwise the explorer can't show when an indexed tx happened. 0 if the
    /// block row is somehow missing (shouldn't occur for an indexed tx).
    block_timestamp: i64,
}

async fn detail(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
) -> ApiResult<Json<TxResponse>> {
    let hash = hash.to_lowercase();
    let tx = transactions::get_by_hash(&state.pool, &hash)
        .await?
        .ok_or_else(|| ApiError::NotFound("tx".into()))?;
    let log_rows = logs::for_tx(&state.pool, &hash).await?;
    let block_timestamp = blocks::get_by_height(&state.pool, tx.block_height)
        .await?
        .map(|b| b.timestamp)
        .unwrap_or(0);
    Ok(Json(TxResponse {
        tx: (&tx).into(),
        logs: log_rows.iter().map(WireLog::from).collect(),
        block_timestamp,
    }))
}

/// Router for `/tx/:hash`.
pub fn router() -> Router<SharedState> {
    Router::new().route("/tx/{hash}", get(detail))
}
