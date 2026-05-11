//! Query helpers for `token_transfers`.

use crate::{DbResult, PgPool};
use indexer_domain::{BlockHeight, LogIndex, TokenStandard, TokenTransfer, Wei};
use sqlx::Row;

/// Insert a single decoded transfer. The `(tx_hash, log_index)` pair is
/// effectively unique for the source log, but the table doesn't pin a unique
/// constraint on it (drizzle didn't either) — the worker dedupes upstream.
pub async fn insert<'e, E>(executor: E, t: &TokenTransfer) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO token_transfers (block_height, tx_hash, log_index, contract, standard, \
            from_addr, to_addr, token_id, amount) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(t.block_height)
    .bind(&t.tx_hash)
    .bind(t.log_index)
    .bind(&t.contract)
    .bind(t.standard.as_str())
    .bind(&t.from_addr)
    .bind(&t.to_addr)
    .bind(t.token_id)
    .bind(t.amount)
    .execute(executor)
    .await?;
    Ok(())
}

/// Paginated token-transfer history for an address — transfers where the
/// address is sender OR receiver, newest-first by block height. Optional
/// `standard` narrows to a specific token kind ("erc20" / "erc721" /
/// "erc1155"). Mirrors the TS `/address/:addr/transfers` route.
pub async fn for_address(
    pool: &PgPool,
    addr: &str,
    standard: Option<&str>,
    limit: i64,
) -> DbResult<Vec<TokenTransfer>> {
    let rows = match standard {
        Some(s) => sqlx::query(
            "SELECT id, block_height, tx_hash, log_index, contract, standard, from_addr, to_addr, \
                    token_id, amount \
             FROM token_transfers \
             WHERE (from_addr = $1 OR to_addr = $1) AND standard = $2 \
             ORDER BY block_height DESC LIMIT $3",
        )
        .bind(addr)
        .bind(s)
        .bind(limit)
        .fetch_all(pool)
        .await?,
        None => sqlx::query(
            "SELECT id, block_height, tx_hash, log_index, contract, standard, from_addr, to_addr, \
                    token_id, amount \
             FROM token_transfers \
             WHERE from_addr = $1 OR to_addr = $1 \
             ORDER BY block_height DESC LIMIT $2",
        )
        .bind(addr)
        .bind(limit)
        .fetch_all(pool)
        .await?,
    };
    rows.into_iter()
        .map(row_to_transfer)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// All transfers for a given block, ordered by `log_index`.
pub async fn for_block(pool: &PgPool, h: BlockHeight) -> DbResult<Vec<TokenTransfer>> {
    let rows = sqlx::query(
        "SELECT id, block_height, tx_hash, log_index, contract, standard, from_addr, to_addr, \
                token_id, amount \
         FROM token_transfers WHERE block_height = $1 ORDER BY log_index ASC",
    )
    .bind(h)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(row_to_transfer)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

fn row_to_transfer(row: sqlx::postgres::PgRow) -> Result<TokenTransfer, sqlx::Error> {
    let standard_str: String = row.try_get("standard")?;
    let standard = match standard_str.as_str() {
        "erc20" => TokenStandard::Erc20,
        "erc721" => TokenStandard::Erc721,
        "erc1155" => TokenStandard::Erc1155,
        other => {
            return Err(sqlx::Error::Decode(
                format!("unknown token standard: {other}").into(),
            ));
        }
    };
    Ok(TokenTransfer {
        id: Some(row.try_get("id")?),
        block_height: row.try_get::<i64, _>("block_height").map(BlockHeight)?,
        tx_hash: row.try_get("tx_hash")?,
        log_index: row.try_get::<i32, _>("log_index").map(LogIndex)?,
        contract: row.try_get("contract")?,
        standard,
        from_addr: row.try_get("from_addr")?,
        to_addr: row.try_get("to_addr")?,
        token_id: row.try_get::<Option<Wei>, _>("token_id")?,
        amount: row.try_get::<Wei, _>("amount")?,
    })
}
