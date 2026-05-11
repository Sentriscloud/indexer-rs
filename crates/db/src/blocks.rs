//! Query helpers for `blocks`.
//!
//! Helpers are generic over `sqlx::PgExecutor` so callers can pass either a
//! `&PgPool` (one-shot statements) or a `&mut sqlx::Transaction<'_, Postgres>`
//! (atomic multi-statement writes — see `crates/sync/block_writer.rs`).
//! Phase 1 used dynamic queries; the compile-time `query!` swap lands in
//! Phase 2/3 once the `.sqlx/` cache is wired into CI.

use crate::{DbResult, PgPool};
use indexer_domain::{Block, BlockHeight, Hash, Wei};
use sqlx::Row;

/// Insert a single block. ON CONFLICT (height) DO NOTHING — idempotent for
/// at-least-once delivery from the sync layer (per spec §5 invariant 1).
pub async fn insert<'e, E>(executor: E, b: &Block) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    let signers = serde_json::Value::Array(
        b.justification_signers
            .iter()
            .map(|s| serde_json::Value::String(s.clone()))
            .collect(),
    );
    sqlx::query(
        "INSERT INTO blocks (height, hash, parent_hash, timestamp, validator, \
            gas_used, gas_limit, base_fee, tx_count, state_root, round, justification_signers) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT (height) DO NOTHING",
    )
    .bind(b.height)
    .bind(&b.hash)
    .bind(&b.parent_hash)
    .bind(b.timestamp)
    .bind(&b.validator)
    .bind(b.gas_used)
    .bind(b.gas_limit)
    .bind(b.base_fee)
    .bind(b.tx_count)
    .bind(&b.state_root)
    .bind(b.round)
    .bind(signers)
    .execute(executor)
    .await?;
    Ok(())
}

/// Look up a block by height.
pub async fn get_by_height(pool: &PgPool, h: BlockHeight) -> DbResult<Option<Block>> {
    let row_opt = sqlx::query(
        "SELECT height, hash, parent_hash, timestamp, validator, gas_used, gas_limit, \
                base_fee, tx_count, state_root, round, justification_signers \
         FROM blocks WHERE height = $1",
    )
    .bind(h)
    .fetch_optional(pool)
    .await?;
    Ok(row_opt.map(row_to_block).transpose()?)
}

/// Look up a block by hash.
pub async fn get_by_hash(pool: &PgPool, hash: &Hash) -> DbResult<Option<Block>> {
    let row_opt = sqlx::query(
        "SELECT height, hash, parent_hash, timestamp, validator, gas_used, gas_limit, \
                base_fee, tx_count, state_root, round, justification_signers \
         FROM blocks WHERE hash = $1",
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?;
    Ok(row_opt.map(row_to_block).transpose()?)
}

/// Latest indexed block height. Returns None on empty table.
pub async fn latest_height(pool: &PgPool) -> DbResult<Option<BlockHeight>> {
    let row_opt = sqlx::query("SELECT MAX(height) AS h FROM blocks")
        .fetch_optional(pool)
        .await?;
    let h: Option<i64> = row_opt.and_then(|r| r.try_get::<Option<i64>, _>("h").ok().flatten());
    Ok(h.map(BlockHeight))
}

/// Delete a block (and dependent txs/logs via FK CASCADE) at a height. Used
/// by the reorg rewind path.
pub async fn delete_at<'e, E>(executor: E, h: BlockHeight) -> DbResult<u64>
where
    E: sqlx::PgExecutor<'e>,
{
    let result = sqlx::query("DELETE FROM blocks WHERE height = $1")
        .bind(h)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

/// Delete every block at or above `h` (inclusive). Reorg helper used when
/// the divergence point is `h`.
pub async fn delete_from<'e, E>(executor: E, h: BlockHeight) -> DbResult<u64>
where
    E: sqlx::PgExecutor<'e>,
{
    let result = sqlx::query("DELETE FROM blocks WHERE height >= $1")
        .bind(h)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

fn row_to_block(row: sqlx::postgres::PgRow) -> Result<Block, sqlx::Error> {
    let signers_json: serde_json::Value = row.try_get("justification_signers")?;
    let signers = match signers_json {
        serde_json::Value::Array(arr) => arr
            .into_iter()
            .filter_map(|v| match v {
                serde_json::Value::String(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Ok(Block {
        height: row.try_get::<i64, _>("height").map(BlockHeight)?,
        hash: row.try_get("hash")?,
        parent_hash: row.try_get("parent_hash")?,
        timestamp: row.try_get("timestamp")?,
        validator: row.try_get("validator")?,
        gas_used: row.try_get("gas_used")?,
        gas_limit: row.try_get("gas_limit")?,
        base_fee: row.try_get::<Option<Wei>, _>("base_fee")?,
        tx_count: row.try_get("tx_count")?,
        state_root: row.try_get("state_root")?,
        round: row.try_get("round")?,
        justification_signers: signers,
    })
}
