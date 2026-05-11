//! Atomic block-write transaction.
//!
//! Wraps block + tx + log inserts + cursor advance in a single
//! `sqlx::Transaction`. Either all of it commits (cursor reflects the new
//! state) or none of it does (a crash mid-write leaves the cursor pointing
//! at the previous height + the partial rows are rolled back). Spec §5
//! invariants 1, 2, 10.

use crate::cursor::write_cursor;
use crate::{SyncError, SyncResult};
use indexer_db::{PgPool, blocks, logs, transactions};
use indexer_domain::{Block, Log, Transaction};

/// Bundle of rows to write atomically. Built by the sync loop before calling
/// [`write_block`].
pub struct BlockBundle {
    /// The block header row.
    pub block: Block,
    /// All txs in the block, ordered by `tx_index`.
    pub txs: Vec<Transaction>,
    /// All logs emitted during the block's txs, ordered by `log_index`.
    pub logs: Vec<Log>,
}

/// Write a block bundle + advance the chain-wide cursor in one transaction.
///
/// Returns Ok on commit. Returns Err with the underlying sqlx/db error on
/// rollback — the cursor stays at its previous value, the writer can retry
/// the same height.
pub async fn write_block(pool: &PgPool, b: BlockBundle) -> SyncResult<()> {
    let mut tx = pool.begin().await.map_err(SyncError::from)?;

    // Order matters: blocks first (FK target), then transactions (FK target
    // for logs), then logs.
    blocks::insert(&mut *tx, &b.block).await?;
    for t in &b.txs {
        transactions::insert(&mut *tx, t).await?;
    }
    for l in &b.logs {
        logs::insert(&mut *tx, l).await?;
    }

    // Cursor advance shares the transaction so it lands or rolls back with
    // the data. `now_ts` = the block's chain timestamp so cursor staleness
    // is comparable to chain time.
    write_cursor(&mut *tx, b.block.height, b.block.timestamp).await?;

    tx.commit().await.map_err(SyncError::from)?;
    Ok(())
}
