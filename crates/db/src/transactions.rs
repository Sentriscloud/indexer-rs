//! Query helpers for `transactions`.

use crate::{DbResult, PgPool};
use indexer_domain::{BlockHeight, Hash, Transaction, TxIndex, TxType, Wei};
use sqlx::Row;

/// Insert a single tx. ON CONFLICT (hash) DO NOTHING for idempotency.
pub async fn insert(pool: &PgPool, t: &Transaction) -> DbResult<()> {
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
    .execute(pool)
    .await?;
    Ok(())
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

/// Cascade-delete txs for a given block (reorg rewind).
///
/// FK ON DELETE CASCADE handles dependent `logs` rows; the caller drops the
/// `blocks` row separately.
pub async fn delete_for_block(pool: &PgPool, h: BlockHeight) -> DbResult<u64> {
    let result = sqlx::query("DELETE FROM transactions WHERE block_height = $1")
        .bind(h)
        .execute(pool)
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
