//! `/tx/:hash` — single tx detail with all logs ordered by `log_index`.
//! TS port: `apps/api/src/routes/native.ts`. Hash is lowercased to mirror
//! the TS port's case-insensitive lookup.

use crate::SharedState;
use crate::error::{ApiError, ApiResult};
use crate::serialise::{WireLog, WireTransaction};
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::{logs, transactions};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct TxResponse {
    tx: WireTransaction,
    logs: Vec<WireLog>,
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
    Ok(Json(TxResponse {
        tx: (&tx).into(),
        logs: log_rows.iter().map(WireLog::from).collect(),
    }))
}

/// Router for `/tx/:hash`.
pub fn router() -> Router<SharedState> {
    Router::new().route("/tx/{hash}", get(detail))
}
