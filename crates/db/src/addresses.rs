//! `addresses` registry (migration 0005) — every from/to address seen in a tx,
//! lazily classified as contract/EOA by the detector worker. Powers
//! `/contracts/recent|pioneers|stats` (`WHERE is_contract = true`).

use crate::{DbResult, PgPool};
use sqlx::Row;

/// One contract row for the `/contracts` leaderboards.
#[derive(Debug, Clone)]
pub struct ContractRow {
    /// Contract address (lowercase 0x-hex).
    pub address: String,
    /// Block the address was first seen.
    pub first_seen_block: i64,
    /// Block the address was most recently seen.
    pub last_seen_block: i64,
    /// `keccak(code)` for the contract; never NULL once classified.
    pub code_hash: Option<String>,
}

/// Upsert a batch of `(address, block)` sightings. Keeps the earliest
/// `first_seen_block` (ON CONFLICT leaves it) and advances `last_seen_block`.
/// New rows default to `is_contract=false, code_hash=NULL` (unclassified) so
/// the detector picks them up. Idempotent — safe on reorg replay.
pub async fn upsert_batch<'e, E>(executor: E, seen: &[(String, i64)]) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    if seen.is_empty() {
        return Ok(());
    }
    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO addresses (address, first_seen_block, last_seen_block) ",
    );
    qb.push_values(seen.iter(), |mut row, (addr, block)| {
        row.push_bind(addr).push_bind(*block).push_bind(*block);
    });
    qb.push(
        " ON CONFLICT (address) DO UPDATE SET \
          last_seen_block = GREATEST(addresses.last_seen_block, EXCLUDED.last_seen_block)",
    );
    qb.build().execute(executor).await?;
    Ok(())
}

/// Up to `limit` not-yet-classified addresses (code_hash IS NULL) for the
/// detector to run eth_getCode against. Uses the partial unclassified index.
pub async fn unclassified_batch(pool: &PgPool, limit: i64) -> DbResult<Vec<String>> {
    let rows = sqlx::query("SELECT address FROM addresses WHERE code_hash IS NULL LIMIT $1")
        .bind(limit)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|r| r.try_get::<String, _>("address"))
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}

/// Record a detector classification. `code_hash` is always set after a probe
/// ("0x" for an EOA, keccak(code) for a contract) so the row leaves the
/// unclassified set.
pub async fn classify<'e, E>(
    executor: E,
    address: &str,
    is_contract: bool,
    code_hash: &str,
) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query("UPDATE addresses SET is_contract = $2, code_hash = $3 WHERE address = $1")
        .bind(address)
        .bind(is_contract)
        .bind(code_hash)
        .execute(executor)
        .await?;
    Ok(())
}

/// List contracts by first-seen height. `ascending` → pioneers (oldest first);
/// otherwise recent (newest first).
pub async fn list_contracts(
    pool: &PgPool,
    limit: i64,
    ascending: bool,
) -> DbResult<Vec<ContractRow>> {
    let sql = if ascending {
        "SELECT address, first_seen_block, last_seen_block, code_hash \
         FROM addresses WHERE is_contract = true \
         ORDER BY first_seen_block ASC, address ASC LIMIT $1"
    } else {
        "SELECT address, first_seen_block, last_seen_block, code_hash \
         FROM addresses WHERE is_contract = true \
         ORDER BY first_seen_block DESC, address ASC LIMIT $1"
    };
    let rows = sqlx::query(sql).bind(limit).fetch_all(pool).await?;
    rows.into_iter()
        .map(|r| {
            Ok(ContractRow {
                address: r.try_get("address")?,
                first_seen_block: r.try_get("first_seen_block")?,
                last_seen_block: r.try_get("last_seen_block")?,
                code_hash: r.try_get("code_hash")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}

/// Total address rows — gates the one-time history backfill.
pub async fn count(pool: &PgPool) -> DbResult<i64> {
    let row = sqlx::query("SELECT COUNT(*) AS n FROM addresses")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get("n")?)
}

/// One-time history backfill: seed `addresses` from every from/to address
/// already in `transactions`, with min/max block as first/last seen. Rows land
/// unclassified (code_hash NULL) for the detector to process. Returns rows
/// inserted. No-op-safe via ON CONFLICT.
pub async fn backfill_from_transactions(pool: &PgPool) -> DbResult<u64> {
    let res = sqlx::query(
        "INSERT INTO addresses (address, first_seen_block, last_seen_block) \
         SELECT addr, MIN(block_height), MAX(block_height) FROM ( \
             SELECT from_addr AS addr, block_height FROM transactions WHERE from_addr IS NOT NULL \
             UNION ALL \
             SELECT to_addr AS addr, block_height FROM transactions WHERE to_addr IS NOT NULL \
         ) u GROUP BY addr \
         ON CONFLICT (address) DO NOTHING",
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}
