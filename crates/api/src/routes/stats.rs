//! `/stats/daily` — chain-wide aggregates per day from the `stats_daily_mv`
//! materialised view (db migration 0002). The response is a bare array
//! `[{date, blocks, transactions}]`, matching the legacy TS indexer so the
//! explorer frontend can consume either indexer interchangeably.

use crate::error::{ApiError, ApiResult};
use crate::routes::clamp_limit;
use crate::{CacheTier, SharedState, cached};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::stats;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DailyRow {
    /// `YYYY-MM-DD` (UTC).
    date: String,
    /// Block count for the day.
    blocks: i64,
    /// Transaction count for the day.
    transactions: i64,
}

async fn daily(
    State(state): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<Vec<DailyRow>>> {
    let limit = clamp_limit(q.limit.as_deref());
    // Cache-aside: chain tier (60s TTL). MV refresh cadence is 5 min;
    // even short cache TTL collapses 60s of bursts into 1 PG read.
    let key = format!("stats:daily:{limit}");
    let rows: Vec<DailyRow> = cached::get_or_load(&state, &key, CacheTier::Chain, || async {
        let rows = stats::daily(&state.pool, limit).await?;
        Ok::<_, ApiError>(
            rows.into_iter()
                .map(|r| DailyRow {
                    date: r.date,
                    blocks: r.blocks,
                    transactions: r.transactions,
                })
                .collect(),
        )
    })
    .await?;
    Ok(Json(rows))
}

/// Router for `/stats/daily`.
pub fn router() -> Router<SharedState> {
    Router::new().route("/stats/daily", get(daily))
}

#[cfg(test)]
mod tests {
    use super::DailyRow;

    #[test]
    fn daily_row_is_flat_legacy_shape() {
        // Each element must be a flat {date, blocks, transactions} object with
        // numeric counts — the legacy TS indexer shape the explorer consumes.
        // No `daily` wrapper, no `day_bucket`/`block_count` field names.
        let row = DailyRow {
            date: "2026-04-24".into(),
            blocks: 102108,
            transactions: 102110,
        };
        let v = serde_json::to_value(&row).unwrap();
        assert_eq!(v["date"], "2026-04-24");
        assert_eq!(v["blocks"], 102108);
        assert_eq!(v["transactions"], 102110);
        assert!(v.get("day_bucket").is_none());
        assert!(v.get("block_count").is_none());
    }

    #[test]
    fn daily_response_serialises_as_bare_array() {
        let rows = vec![DailyRow {
            date: "2026-04-24".into(),
            blocks: 1,
            transactions: 2,
        }];
        let v = serde_json::to_value(&rows).unwrap();
        assert!(v.is_array(), "response must be a bare array, not an object");
        assert!(v.get("daily").is_none());
    }
}
