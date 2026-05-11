//! Read helpers backed by `stats_daily_mv` (migration 0002).

use crate::{DbResult, PgPool};
use sqlx::Row;

/// One row of `/stats/daily` — pre-aggregated per day_bucket.
#[derive(Debug, Clone)]
pub struct StatsDailyRow {
    /// `floor(timestamp / 86400)` — chain-day bucket.
    pub day_bucket: i64,
    /// Blocks that landed in this bucket.
    pub block_count: i64,
    /// Sum of `blocks.tx_count` over the bucket.
    pub tx_count: i64,
    /// Sum of `blocks.gas_used` over the bucket.
    pub gas_used: i64,
    /// First (lowest) block height in the bucket.
    pub first_height: i64,
    /// Last (highest) block height in the bucket.
    pub last_height: i64,
}

/// Read the last `limit` daily buckets, newest-first.
pub async fn daily(pool: &PgPool, limit: i64) -> DbResult<Vec<StatsDailyRow>> {
    let rows = sqlx::query(
        "SELECT day_bucket, block_count, tx_count, gas_used, first_height, last_height \
         FROM stats_daily_mv ORDER BY day_bucket DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            Ok(StatsDailyRow {
                day_bucket: r.try_get("day_bucket")?,
                block_count: r.try_get("block_count")?,
                tx_count: r.try_get("tx_count")?,
                gas_used: r.try_get("gas_used")?,
                first_height: r.try_get("first_height")?,
                last_height: r.try_get("last_height")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}

/// Trigger a CONCURRENTLY refresh of the MV. Called by the route on cache
/// miss for the most-recent bucket, OR by the operator on demand.
pub async fn refresh(pool: &PgPool) -> DbResult<()> {
    sqlx::query("REFRESH MATERIALIZED VIEW CONCURRENTLY stats_daily_mv")
        .execute(pool)
        .await?;
    Ok(())
}
