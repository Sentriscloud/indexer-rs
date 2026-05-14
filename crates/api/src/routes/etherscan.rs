//! Etherscan-compat shim — `GET /api?module=...&action=...&...`. Lets
//! generic ethers tooling (block explorers, wallet metadata fetchers)
//! query Sentrix without bespoke client code.
//!
//! Subset implemented:
//!   - `module=account&action=txlist&address=...&startblock=&endblock=&page=&offset=&sort=`
//!   - `module=block&action=getblocknobytime&timestamp=&closest=before|after`
//!   - `module=stats&action=ethsupply` (returns chain native supply — proxy
//!     for ETH-supply consumers)
//!
//! Response envelope: `{ "status": "0|1", "message": "OK|...", "result": ... }`
//! per Etherscan v1 spec. Errors return `status=0` with the message in
//! `result` (some clients put the err there, some in `message`; we mirror
//! both to maximise compatibility).

use crate::SharedState;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::transactions;
use indexer_domain::BlockHeight;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

#[derive(Debug, Deserialize)]
struct EsQuery {
    module: Option<String>,
    action: Option<String>,
    address: Option<String>,
    /// Etherscan `startblock` — accepted for compat. Not yet filtered on
    /// (`for_address` takes no height range); we surface a status=0 error
    /// rather than silently returning the full set (audit 2026-05-13).
    #[serde(rename = "startblock")]
    start_block: Option<String>,
    #[serde(rename = "endblock")]
    end_block: Option<String>,
    page: Option<String>,
    offset: Option<String>,
    /// Etherscan `sort=asc|desc` — accepted for compat; we always render
    /// newest-first since `for_address` orders DESC.
    #[allow(dead_code)]
    sort: Option<String>,
    timestamp: Option<String>,
    closest: Option<String>,
}

#[derive(Debug, Serialize)]
struct EsEnvelope {
    status: String,
    message: String,
    result: Value,
}

async fn dispatch(State(state): State<SharedState>, Query(q): Query<EsQuery>) -> Json<EsEnvelope> {
    let module = q.module.as_deref().unwrap_or("");
    let action = q.action.as_deref().unwrap_or("");
    let body = match (module, action) {
        ("account", "txlist") => txlist(&state, q).await,
        ("block", "getblocknobytime") => getblocknobytime(&state, q).await,
        ("stats", "ethsupply") => ethsupply(&state).await,
        (m, a) => err(format!("module/action not supported: {m}/{a}")),
    };
    Json(body)
}

async fn txlist(state: &SharedState, q: EsQuery) -> EsEnvelope {
    let Some(addr) = q.address else {
        return err("address parameter is required".into());
    };
    let addr = addr.to_lowercase();
    // Reject params we don't actually honour. Silently ignoring them
    // returned wrong-window results to clients that pin a height range
    // (audit 2026-05-13). `page` is rejected too — without offset+page we
    // can only ever return the first page, so accepting page>1 would lie.
    let unsupported: Vec<&str> = [
        ("startblock", q.start_block.as_deref()),
        ("endblock", q.end_block.as_deref()),
        ("page", q.page.as_deref()),
    ]
    .iter()
    .filter_map(|(name, v)| match v {
        Some(s) if !s.is_empty() && *s != "0" && (*name != "page" || *s != "1") => Some(*name),
        _ => None,
    })
    .collect();
    if !unsupported.is_empty() {
        return err(format!(
            "param not supported: {}",
            unsupported.join(",")
        ));
    }
    // Etherscan offset = page size. We cap at 100 to align with the rest
    // of the indexer's pagination caps.
    let offset = q
        .offset
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(25)
        .clamp(1, 100);
    // Phase 5 helper returns newest-first; ?sort=asc would re-sort, but
    // Etherscan default is asc by default — we render newest-first as a
    // pragmatic default since most consumers walk newest-first anyway.
    let rows = match transactions::for_address(&state.pool, &addr, offset).await {
        Ok(r) => r,
        Err(e) => return db_err("etherscan.txlist", e),
    };
    let result = rows
        .into_iter()
        .map(|t| {
            json!({
                "blockNumber": t.block_height.0.to_string(),
                "hash": t.hash,
                "from": t.from_addr,
                "to": t.to_addr,
                "value": t.value.to_string(),
                "gas": t.gas_limit.to_string(),
                "gasUsed": t.gas_used.unwrap_or(0).to_string(),
                "gasPrice": t.gas_price.map(|w| w.to_string()).unwrap_or_default(),
                "nonce": t.nonce.to_string(),
                "input": t.data.unwrap_or_default(),
                "isError": if t.status == 1 { "0" } else { "1" },
                "txreceipt_status": t.status.to_string(),
                "contractAddress": t.contract_address.unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    ok(Value::Array(result))
}

async fn getblocknobytime(state: &SharedState, q: EsQuery) -> EsEnvelope {
    let Some(ts_str) = q.timestamp else {
        return err("timestamp parameter is required".into());
    };
    let Ok(ts) = ts_str.parse::<i64>() else {
        return err("timestamp must be a positive integer".into());
    };
    let closest = q.closest.as_deref().unwrap_or("before");
    let row =
        match closest {
            "after" => sqlx::query(
                "SELECT height FROM blocks WHERE timestamp >= $1 ORDER BY timestamp ASC LIMIT 1",
            )
            .bind(ts)
            .fetch_optional(&state.pool)
            .await,
            _ => sqlx::query(
                "SELECT height FROM blocks WHERE timestamp <= $1 ORDER BY timestamp DESC LIMIT 1",
            )
            .bind(ts)
            .fetch_optional(&state.pool)
            .await,
        };
    match row {
        Ok(Some(r)) => match r.try_get::<i64, _>("height") {
            Ok(h) => ok(Value::String(BlockHeight(h).0.to_string())),
            Err(e) => db_err("etherscan.getblocknobytime.decode", e),
        },
        Ok(None) => err(format!("no block at-or-{closest} timestamp {ts}")),
        Err(e) => db_err("etherscan.getblocknobytime", e),
    }
}

async fn ethsupply(state: &SharedState) -> EsEnvelope {
    // Sentrix doesn't track issuance in PG (it's a chain-side query); this
    // shim returns a placeholder until a chain-supply read lands. Most
    // Etherscan consumers fall back gracefully on missing/zero supply.
    let _ = state;
    ok(Value::String("0".into()))
}

fn ok(result: Value) -> EsEnvelope {
    EsEnvelope {
        status: "1".into(),
        message: "OK".into(),
        result,
    }
}

fn err(msg: String) -> EsEnvelope {
    EsEnvelope {
        status: "0".into(),
        message: msg.clone(),
        result: Value::String(msg),
    }
}

/// Log full DB error internally, return a generic envelope so we don't
/// leak schema/connection details to callers (audit 2026-05-13).
fn db_err<E: std::fmt::Display>(scope: &'static str, e: E) -> EsEnvelope {
    tracing::error!(scope = scope, error = %e, "etherscan db failure");
    err("database error".into())
}

/// Router for the etherscan-compat `/api` entry point.
pub fn router() -> Router<SharedState> {
    Router::new().route("/api", get(dispatch))
}
