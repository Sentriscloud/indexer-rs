//! `/contracts/recent|pioneers|stats` — contract leaderboards backed by the
//! `contracts` table (migration 0004). Rows come from contract-creation txs
//! (`transactions.to_address IS NULL`); the created address is computed by the
//! sync layer (Postgres can't keccak) and upserted here.

use crate::{DbResult, PgPool};
use sqlx::Row;

/// One contract row for the leaderboard responses. Field names mirror the
/// legacy indexer / the explorer's expected shape.
#[derive(Debug, Clone)]
pub struct ContractRow {
    /// Contract address (lowercase 0x-hex).
    pub address: String,
    /// Block the contract was created in.
    pub first_seen_block: i64,
    /// Most recent block the contract was seen in.
    pub last_seen_block: i64,
    /// Reserved for a later eth_getCode pass; NULL renders as "—" in the UI.
    pub code_hash: Option<String>,
}

/// Upsert a contract creation. Keeps the earliest `first_seen_block` /
/// `created_tx_hash` and advances `last_seen_block` on replays — idempotent, so
/// reorg/backfill re-runs are safe.
pub async fn upsert_creation<'e, E>(
    executor: E,
    address: &str,
    block: i64,
    tx_hash: &str,
) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO contracts (address, first_seen_block, last_seen_block, created_tx_hash) \
         VALUES ($1, $2, $2, $3) \
         ON CONFLICT (address) DO UPDATE SET \
             first_seen_block = LEAST(contracts.first_seen_block, EXCLUDED.first_seen_block), \
             last_seen_block  = GREATEST(contracts.last_seen_block, EXCLUDED.last_seen_block)",
    )
    .bind(address)
    .bind(block)
    .bind(tx_hash)
    .execute(executor)
    .await?;
    Ok(())
}

/// List contracts by creation height. `ascending` → pioneers (oldest first);
/// otherwise recent (newest first).
pub async fn list(pool: &PgPool, limit: i64, ascending: bool) -> DbResult<Vec<ContractRow>> {
    let sql = if ascending {
        "SELECT address, first_seen_block, last_seen_block, code_hash \
         FROM contracts ORDER BY first_seen_block ASC, address ASC LIMIT $1"
    } else {
        "SELECT address, first_seen_block, last_seen_block, code_hash \
         FROM contracts ORDER BY first_seen_block DESC, address ASC LIMIT $1"
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

/// Total contract count — gates the one-time history backfill.
pub async fn count(pool: &PgPool) -> DbResult<i64> {
    let row = sqlx::query("SELECT COUNT(*) AS n FROM contracts")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get("n")?)
}

/// Stream creation txs already in `transactions` (to_address IS NULL) so the
/// sync layer can compute their addresses + backfill `contracts` once.
pub struct CreationTx {
    /// Creator (sender) address.
    pub from_addr: String,
    /// Sender nonce at creation time.
    pub nonce: i64,
    /// Block the creation tx landed in.
    pub block_height: i64,
    /// Creation tx hash.
    pub hash: String,
}

/// Read all historical contract-creation txs ordered by height — used by the
/// one-time backfill. Limited columns keep the scan light.
pub async fn creation_txs(pool: &PgPool) -> DbResult<Vec<CreationTx>> {
    let rows = sqlx::query(
        "SELECT from_addr, nonce, block_height, hash \
         FROM transactions WHERE to_addr IS NULL ORDER BY block_height ASC",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            Ok(CreationTx {
                from_addr: r.try_get("from_addr")?,
                nonce: r.try_get("nonce")?,
                block_height: r.try_get("block_height")?,
                hash: r.try_get("hash")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}
