//! Query helpers for `transactions`.

use crate::{DbResult, PgPool};
use indexer_domain::{BlockHeight, Hash, Transaction, TxIndex, TxType, Wei};
use sqlx::Row;

/// Insert a single tx. ON CONFLICT (hash) DO NOTHING for idempotency.
pub async fn insert<'e, E>(executor: E, t: &Transaction) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO transactions (hash, block_height, tx_index, from_addr, to_addr, value, \
            gas_limit, gas_used, gas_price, fee, nonce, data, status, contract_address, tx_type) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
         ON CONFLICT (hash) DO NOTHING",
    )
    .bind(&t.hash)
    .bind(t.block_height)
    .bind(t.tx_index)
    .bind(&t.from_addr)
    .bind(&t.to_addr)
    .bind(t.value)
    .bind(t.gas_limit)
    .bind(t.gas_used)
    .bind(t.gas_price)
    .bind(t.fee)
    .bind(t.nonce)
    .bind(&t.data)
    .bind(t.status)
    .bind(&t.contract_address)
    .bind(t.tx_type.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// Multi-row batch insert. ON CONFLICT (hash) DO NOTHING per row, so a
/// crash mid-batch on retry hits the existing rows and silently skips them.
pub async fn insert_batch<'e, E>(executor: E, txs: &[Transaction]) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    if txs.is_empty() {
        return Ok(());
    }
    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO transactions (hash, block_height, tx_index, from_addr, to_addr, value, \
            gas_limit, gas_used, gas_price, fee, nonce, data, status, contract_address, tx_type) ",
    );
    qb.push_values(txs.iter(), |mut row, t| {
        row.push_bind(&t.hash)
            .push_bind(t.block_height)
            .push_bind(t.tx_index)
            .push_bind(&t.from_addr)
            .push_bind(&t.to_addr)
            .push_bind(t.value)
            .push_bind(t.gas_limit)
            .push_bind(t.gas_used)
            .push_bind(t.gas_price)
            .push_bind(t.fee)
            .push_bind(t.nonce)
            .push_bind(&t.data)
            .push_bind(t.status)
            .push_bind(&t.contract_address)
            .push_bind(t.tx_type.as_str());
    });
    qb.push(" ON CONFLICT (hash) DO NOTHING");
    qb.build().execute(executor).await?;
    Ok(())
}

/// All txs in a block, ordered by `tx_index`.
pub async fn for_block(pool: &PgPool, h: BlockHeight) -> DbResult<Vec<Transaction>> {
    let rows = sqlx::query(
        "SELECT hash, block_height, tx_index, from_addr, to_addr, value, gas_limit, gas_used, \
                gas_price, fee, nonce, data, status, contract_address, tx_type \
         FROM transactions WHERE block_height = $1 ORDER BY tx_index ASC",
    )
    .bind(h)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(row_to_tx)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// Look up a tx by hash.
pub async fn get_by_hash(pool: &PgPool, hash: &Hash) -> DbResult<Option<Transaction>> {
    let row_opt = sqlx::query(
        "SELECT hash, block_height, tx_index, from_addr, to_addr, value, gas_limit, gas_used, \
                gas_price, fee, nonce, data, status, contract_address, tx_type \
         FROM transactions WHERE hash = $1",
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?;
    row_opt.map(row_to_tx).transpose().map_err(Into::into)
}

/// Paginated tx history for an address — txs where the address is sender
/// OR receiver, newest-first by block height. Mirrors the TS port's
/// `/address/:addr/txs` route. Address is matched lowercase (caller
/// normalises).
pub async fn for_address(pool: &PgPool, addr: &str, limit: i64) -> DbResult<Vec<Transaction>> {
    let rows = sqlx::query(
        "SELECT hash, block_height, tx_index, from_addr, to_addr, value, gas_limit, gas_used, \
                gas_price, fee, nonce, data, status, contract_address, tx_type \
         FROM transactions WHERE from_addr = $1 OR to_addr = $1 \
         ORDER BY block_height DESC LIMIT $2",
    )
    .bind(addr)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(row_to_tx)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// Top senders by tx count (global). Mirrors `/accounts/active` in TS.
/// Returns `(address, tx_count)` newest-rank-first.
pub async fn top_senders(pool: &PgPool, limit: i64) -> DbResult<Vec<(String, i64)>> {
    let rows = sqlx::query(
        "SELECT from_addr AS address, COUNT(*) AS tx_count \
         FROM transactions GROUP BY from_addr \
         ORDER BY COUNT(*) DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            Ok((
                r.try_get::<String, _>("address")?,
                r.try_get::<i64, _>("tx_count")?,
            ))
        })
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}

/// Top transactions by `value` DESC, with the block timestamp joined in
/// so callers can render "X SRX moved at T". Mirrors `/whale/transfers`.
pub async fn top_by_value(pool: &PgPool, limit: i64) -> DbResult<Vec<TopTxRow>> {
    let rows = sqlx::query(
        "SELECT t.hash, t.from_addr, t.to_addr, t.value::text AS value_str, \
                t.block_height, b.timestamp \
         FROM transactions t JOIN blocks b ON b.height = t.block_height \
         ORDER BY t.value DESC, t.block_height DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            Ok(TopTxRow {
                hash: r.try_get("hash")?,
                from_addr: r.try_get("from_addr")?,
                to_addr: r.try_get("to_addr")?,
                value: r.try_get("value_str")?,
                block_height: r.try_get("block_height")?,
                timestamp: r.try_get("timestamp")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()
        .map_err(Into::into)
}

/// Compact row for `/whale/transfers` — value carried as decimal string
/// (preserving numeric(78,0) precision over the wire).
#[derive(Debug, Clone)]
pub struct TopTxRow {
    /// Tx hash.
    pub hash: String,
    /// Sender.
    pub from_addr: String,
    /// Receiver.
    pub to_addr: Option<String>,
    /// Decimal-string value.
    pub value: String,
    /// Block height.
    pub block_height: i64,
    /// Block timestamp (chain-time seconds).
    pub timestamp: i64,
}

/// Cascade-delete txs for a given block (reorg rewind).
///
/// FK ON DELETE CASCADE handles dependent `logs` rows; the caller drops the
/// `blocks` row separately. With the per-block FK chain
/// (logs -> transactions -> blocks), a `delete_from(blocks, h)` is enough on
/// its own; this helper exists for callers that want to wipe txs without
/// touching the block row (rare).
pub async fn delete_for_block<'e, E>(executor: E, h: BlockHeight) -> DbResult<u64>
where
    E: sqlx::PgExecutor<'e>,
{
    let result = sqlx::query("DELETE FROM transactions WHERE block_height = $1")
        .bind(h)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

fn row_to_tx(row: sqlx::postgres::PgRow) -> Result<Transaction, sqlx::Error> {
    let tx_type_str: String = row.try_get("tx_type")?;
    let tx_type = match tx_type_str.as_str() {
        "native" => TxType::Native,
        "evm" => TxType::Evm,
        "system" => TxType::System,
        "coinbase" => TxType::Coinbase,
        other => {
            return Err(sqlx::Error::Decode(
                format!("unknown tx_type: {other}").into(),
            ));
        }
    };
    Ok(Transaction {
        hash: row.try_get("hash")?,
        block_height: row.try_get::<i64, _>("block_height").map(BlockHeight)?,
        tx_index: row.try_get::<i32, _>("tx_index").map(TxIndex)?,
        from_addr: row.try_get("from_addr")?,
        to_addr: row.try_get("to_addr")?,
        value: row.try_get::<Wei, _>("value")?,
        gas_limit: row.try_get("gas_limit")?,
        gas_used: row.try_get("gas_used")?,
        gas_price: row.try_get::<Option<Wei>, _>("gas_price")?,
        fee: row.try_get::<Wei, _>("fee")?,
        nonce: row.try_get("nonce")?,
        data: row.try_get("data")?,
        status: row.try_get("status")?,
        contract_address: row.try_get("contract_address")?,
        tx_type,
    })
}
