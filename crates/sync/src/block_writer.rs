//! Atomic block-write transaction.
//!
//! Wraps block + tx + log inserts + cursor advance in a single
//! `sqlx::Transaction`. Either all of it commits (cursor reflects the new
//! state) or none of it does (a crash mid-write leaves the cursor pointing
//! at the previous height + the partial rows are rolled back). Spec §5
//! invariants 1, 2, 10.
//!
//! After commit, optionally pushes one [`indexer_analytics::RawTxRow`] per
//! tx into the analytics buffer. The push is fire-and-forget — analytics is
//! observability, not correctness, so a closed channel logs a warning but
//! doesn't fail the write.

use crate::cursor::write_cursor;
use crate::{SyncError, SyncResult};
use indexer_analytics::{AnalyticsHandle, RawTxRow};
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
/// `analytics` is optional — when wired, each tx in the bundle gets pushed
/// to the analytics buffer after the SQL commit.
///
/// Returns Ok on commit. Returns Err with the underlying sqlx/db error on
/// rollback — the cursor stays at its previous value, the writer can retry
/// the same height.
pub async fn write_block(
    pool: &PgPool,
    b: BlockBundle,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<()> {
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

    // Best-effort analytics push, after the SQL boundary so a failed flusher
    // can't roll back our data.
    if let Some(handle) = analytics {
        // Should never hit (writer only runs on heights coming back from
        // resolved blocks, never the -1 sentinel from the cursor) but keep
        // analytics non-fatal: warn + skip the row, don't panic the loop.
        let block_height = match b.block.height.as_u64() {
            Some(h) => h,
            None => {
                tracing::warn!(
                    height = ?b.block.height,
                    "analytics: skipping row — block height not convertible to u64 \
                     (cursor sentinel reached writer; this should not happen)"
                );
                return Ok(());
            }
        };
        for t in &b.txs {
            let row = RawTxRow {
                block_height,
                timestamp: b.block.timestamp as u64,
                tx_hash: t.hash.clone(),
                from_addr: t.from_addr.clone(),
                to_addr: t.to_addr.clone(),
                value_str: t.value.to_string(),
                fee_str: t.fee.to_string(),
                gas_used: t.gas_used.unwrap_or(0) as u64,
                status: t.status as u8,
                tx_type: t.tx_type.as_str().to_string(),
            };
            if let Err(e) = handle.push(row) {
                tracing::warn!(error = %e, "analytics push failed; flusher closed?");
                break;
            }
        }
    }

    Ok(())
}
