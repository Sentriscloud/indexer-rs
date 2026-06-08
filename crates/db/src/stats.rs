//! Read helpers backed by `stats_daily_mv` (migration 0002).

use crate::{DbResult, PgPool};
use sqlx::Row;

/// One row of `/stats/daily`. Field names + types mirror the legacy TS
/// indexer's response (`date` ISO-8601 day, numeric `blocks`/`transactions`)
/// so the explorer frontend consumes either indexer interchangeably. The
/// calendar date is derived from the `day_bucket` (epoch-day) in SQL via
/// `to_timestamp(day_bucket * 86400)` at UTC.
#[derive(Debug, Clone)]
pub struct StatsDailyRow {
    /// `YYYY-MM-DD` (UTC) for the bucket.
    pub date: String,
    /// Blocks that landed in this bucket.
    pub blocks: i64,
    /// Sum of `blocks.tx_count` over the bucket.
    pub transactions: i64,
}

/// Read the last `limit` daily buckets, newest-first.
pub async fn daily(pool: &PgPool, limit: i64) -> DbResult<Vec<StatsDailyRow>> {
    let rows = sqlx::query(
        "SELECT to_char(to_timestamp(day_bucket * 86400) AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS date, \
                block_count AS blocks, \
                tx_count AS transactions \
         FROM stats_daily_mv ORDER BY day_bucket DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            Ok(StatsDailyRow {
                date: r.try_get("date")?,
                blocks: r.try_get("blocks")?,
                transactions: r.try_get("transactions")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}

/// CONCURRENTLY refresh — does not lock out reads, but Postgres rejects it on
/// a never-populated MV. Use for the periodic refresh once populated.
pub async fn refresh(pool: &PgPool) -> DbResult<()> {
    sqlx::query("REFRESH MATERIALIZED VIEW CONCURRENTLY stats_daily_mv")
        .execute(pool)
        .await?;
    Ok(())
}

/// Plain (blocking) refresh — the only form that works on a never-populated
/// MV. Run once at startup before switching to `refresh` on the interval.
pub async fn refresh_full(pool: &PgPool) -> DbResult<()> {
    sqlx::query("REFRESH MATERIALIZED VIEW stats_daily_mv")
        .execute(pool)
        .await?;
    Ok(())
}
