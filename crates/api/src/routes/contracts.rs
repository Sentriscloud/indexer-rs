//! `/contracts/recent|pioneers|stats` — contract leaderboards from the
//! `contracts` table (migration 0004). Response shape
//! `{"contracts":[{rank, address, first_seen_block, last_seen_block, code_hash}]}`,
//! matching the legacy indexer / the explorer's expected contract.

use crate::error::{ApiError, ApiResult};
use crate::routes::clamp_limit;
use crate::{CacheTier, SharedState, cached};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::contracts;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContractEntry {
    rank: i64,
    address: String,
    first_seen_block: i64,
    last_seen_block: i64,
    /// NULL until an eth_getCode pass lands; the frontend renders it as "—".
    code_hash: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContractsResponse {
    contracts: Vec<ContractEntry>,
}

async fn load(
    state: &SharedState,
    limit: i64,
    ascending: bool,
    key: &str,
) -> ApiResult<Json<ContractsResponse>> {
    let resp: ContractsResponse = cached::get_or_load(state, key, CacheTier::Chain, || async {
        let rows = contracts::list(&state.pool, limit, ascending).await?;
        Ok::<_, ApiError>(ContractsResponse {
            contracts: rows
                .into_iter()
                .enumerate()
                .map(|(i, r)| ContractEntry {
                    rank: i as i64 + 1,
                    address: r.address,
                    first_seen_block: r.first_seen_block,
                    last_seen_block: r.last_seen_block,
                    code_hash: r.code_hash,
                })
                .collect(),
        })
    })
    .await?;
    Ok(Json(resp))
}

/// `/contracts/recent` — newest contracts first.
async fn recent(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ContractsResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    load(&state, limit, false, &format!("contracts:recent:{limit}")).await
}

/// `/contracts/pioneers` — earliest contracts first.
async fn pioneers(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ContractsResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    load(&state, limit, true, &format!("contracts:pioneers:{limit}")).await
}

/// `/contracts/stats` — the explorer's sortable contracts list; defaults to
/// newest-created (same as recent), kept on the shared `{contracts:[…]}` shape.
async fn stats(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ContractsResponse>> {
    let limit = clamp_limit(q.limit.as_deref());
    load(&state, limit, false, &format!("contracts:stats:{limit}")).await
}

/// Router for `/contracts/{recent,pioneers,stats}`.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/contracts/recent", get(recent))
        .route("/contracts/pioneers", get(pioneers))
        .route("/contracts/stats", get(stats))
}

#[cfg(test)]
mod tests {
    use super::{ContractEntry, ContractsResponse};

    #[test]
    fn contracts_response_shape() {
        let resp = ContractsResponse {
            contracts: vec![ContractEntry {
                rank: 1,
                address: "0xc0ffee".into(),
                first_seen_block: 100,
                last_seen_block: 200,
                code_hash: None,
            }],
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v["contracts"].is_array());
        let e = &v["contracts"][0];
        assert_eq!(e["rank"], 1);
        assert_eq!(e["address"], "0xc0ffee");
        assert_eq!(e["first_seen_block"], 100);
        assert_eq!(e["last_seen_block"], 200);
        assert!(e["code_hash"].is_null(), "null code_hash → frontend '—'");
    }
}
