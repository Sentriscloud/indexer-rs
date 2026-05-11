//! Query helpers for `logs`.

use crate::{DbResult, PgPool};
use indexer_domain::{BlockHeight, Hash, Log, LogIndex};
use sqlx::Row;

/// Insert a single log. ON CONFLICT (block_height, log_index) DO NOTHING.
pub async fn insert<'e, E>(executor: E, l: &Log) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO logs (block_height, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (block_height, log_index) DO NOTHING",
    )
    .bind(l.block_height)
    .bind(&l.tx_hash)
    .bind(l.log_index)
    .bind(&l.address)
    .bind(&l.topic0)
    .bind(&l.topic1)
    .bind(&l.topic2)
    .bind(&l.topic3)
    .bind(&l.data)
    .execute(executor)
    .await?;
    Ok(())
}

/// All logs for a given tx, ordered by `log_index`.
pub async fn for_tx(pool: &PgPool, tx_hash: &Hash) -> DbResult<Vec<Log>> {
    let rows = sqlx::query(
        "SELECT block_height, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data \
         FROM logs WHERE tx_hash = $1 ORDER BY log_index ASC",
    )
    .bind(tx_hash)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(row_to_log)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

fn row_to_log(row: sqlx::postgres::PgRow) -> Result<Log, sqlx::Error> {
    Ok(Log {
        block_height: row.try_get::<i64, _>("block_height").map(BlockHeight)?,
        tx_hash: row.try_get("tx_hash")?,
        log_index: row.try_get::<i32, _>("log_index").map(LogIndex)?,
        address: row.try_get("address")?,
        topic0: row.try_get("topic0")?,
        topic1: row.try_get("topic1")?,
        topic2: row.try_get("topic2")?,
        topic3: row.try_get("topic3")?,
        data: row.try_get("data")?,
    })
}
