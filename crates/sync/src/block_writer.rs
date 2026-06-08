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
use alloy_primitives::Address;
use indexer_analytics::{AnalyticsHandle, RawTxRow};
use indexer_db::{PgPool, blocks, contracts, logs, token_transfers, transactions};
use indexer_domain::{Block, Log, TokenTransfer, Transaction};

/// CREATE-address of a contract-creation tx (`to_addr IS NULL`):
/// `keccak(rlp(sender, nonce))[12:]`. Lowercase 0x-hex to match the indexer's
/// address casing. `None` if the sender doesn't parse or the nonce is negative.
fn created_contract_address(from: &str, nonce: i64) -> Option<String> {
    let sender: Address = from.parse().ok()?;
    let nonce = u64::try_from(nonce).ok()?;
    Some(sender.create(nonce).to_string().to_lowercase())
}

/// One-time history backfill for `/contracts`: scan creation txs already in
/// `transactions` (`to_addr IS NULL`), compute their CREATE addresses, and
/// populate the `contracts` table. No-op once the table has rows — the
/// steady-state block writer maintains it thereafter. Safe to call on every
/// boot (the count guard makes repeats cheap).
pub async fn backfill_contracts(pool: &PgPool) -> SyncResult<()> {
    if contracts::count(pool).await? > 0 {
        return Ok(());
    }
    let creations = contracts::creation_txs(pool).await?;
    if creations.is_empty() {
        return Ok(());
    }
    let total = creations.len();
    let mut tx = pool.begin().await.map_err(SyncError::from)?;
    let mut written = 0usize;
    for c in &creations {
        if let Some(addr) = created_contract_address(&c.from_addr, c.nonce) {
            contracts::upsert_creation(&mut *tx, &addr, c.block_height, &c.hash).await?;
            written += 1;
        }
    }
    tx.commit().await.map_err(SyncError::from)?;
    tracing::info!(total, written, "contracts: history backfill complete");
    Ok(())
}

/// Page size for batch inserts. Postgres protocol caps bind params at
/// ~65k per query; the widest table (transactions, 15 cols) tops out at
/// ~4300 rows per statement. 500 leaves comfortable headroom and keeps
/// one batch's write+commit latency reasonable.
const BATCH_INSERT_CHUNK: usize = 500;

/// Bundle of rows to write atomically. Built by the sync loop before calling
/// [`write_block`].
pub struct BlockBundle {
    /// The block header row.
    pub block: Block,
    /// All txs in the block, ordered by `tx_index`.
    pub txs: Vec<Transaction>,
    /// All logs emitted during the block's txs, ordered by `log_index`.
    pub logs: Vec<Log>,
    /// Decoded ERC-20 / ERC-721 transfers from this block's logs. Sync
    /// layer fills via `token_decode::decode_transfer`; empty for blocks
    /// with no qualifying events.
    pub token_transfers: Vec<TokenTransfer>,
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
    // for logs), then logs, then derived token_transfers.
    blocks::insert(&mut *tx, &b.block).await?;
    for t in &b.txs {
        transactions::insert(&mut *tx, t).await?;
        // Contract creation (no recipient) → record for /contracts leaderboards.
        if t.to_addr.is_none()
            && let Some(addr) = created_contract_address(&t.from_addr, t.nonce)
        {
            contracts::upsert_creation(&mut *tx, &addr, t.block_height.0, &t.hash).await?;
        }
    }
    for l in &b.logs {
        logs::insert(&mut *tx, l).await?;
    }
    for tt in &b.token_transfers {
        token_transfers::insert(&mut *tx, tt).await?;
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

/// Batched variant of [`write_block`] — writes N bundles in a single
/// transaction with multi-row INSERTs. Amortises commit/fsync cost across
/// the batch (one commit per chunk vs one per block). Returns Ok on commit.
///
/// On crash mid-batch the transaction rolls back; on retry the per-row
/// `ON CONFLICT (...) DO NOTHING` keeps inserts idempotent. Cursor advances
/// to the highest height in the batch only after commit, so a partial
/// batch is replayed safely.
pub async fn batch_write_blocks(
    pool: &PgPool,
    bundles: Vec<BlockBundle>,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<()> {
    if bundles.is_empty() {
        return Ok(());
    }

    // Highest block in the chunk drives the cursor advance + the analytics
    // timestamp (we use each row's own block timestamp).
    let max_height = bundles
        .iter()
        .map(|b| b.block.height)
        .max()
        .expect("non-empty bundles");
    let cursor_ts = bundles
        .iter()
        .find(|b| b.block.height == max_height)
        .map(|b| b.block.timestamp)
        .unwrap_or(0);

    // Flatten + sort by primary key so the multi-row INSERTs execute against
    // index pages in roughly sequential order — measurable PG win on large
    // batches. Order matters per spec §5 invariant 2 (FK order) but the
    // sorts here are within tables; we still insert tables in dependency
    // order: blocks → transactions → logs.
    let mut all_blocks: Vec<Block> = bundles.iter().map(|b| b.block.clone()).collect();
    all_blocks.sort_by_key(|b| b.height);
    let mut all_txs: Vec<Transaction> = bundles.iter().flat_map(|b| b.txs.clone()).collect();
    all_txs.sort_by_key(|t| (t.block_height, t.tx_index));
    let mut all_logs: Vec<Log> = bundles.iter().flat_map(|b| b.logs.clone()).collect();
    all_logs.sort_by_key(|l| (l.block_height, l.log_index));
    let mut all_transfers: Vec<TokenTransfer> = bundles
        .iter()
        .flat_map(|b| b.token_transfers.clone())
        .collect();
    all_transfers.sort_by_key(|t| (t.block_height, t.log_index));

    let mut tx = pool.begin().await.map_err(SyncError::from)?;
    for chunk in all_blocks.chunks(BATCH_INSERT_CHUNK) {
        blocks::insert_batch(&mut *tx, chunk).await?;
    }
    for chunk in all_txs.chunks(BATCH_INSERT_CHUNK) {
        transactions::insert_batch(&mut *tx, chunk).await?;
    }
    // Contract creations (no recipient) → /contracts leaderboards.
    for t in &all_txs {
        if t.to_addr.is_none()
            && let Some(addr) = created_contract_address(&t.from_addr, t.nonce)
        {
            contracts::upsert_creation(&mut *tx, &addr, t.block_height.0, &t.hash).await?;
        }
    }
    for chunk in all_logs.chunks(BATCH_INSERT_CHUNK) {
        logs::insert_batch(&mut *tx, chunk).await?;
    }
    for chunk in all_transfers.chunks(BATCH_INSERT_CHUNK) {
        token_transfers::insert_batch(&mut *tx, chunk).await?;
    }
    write_cursor(&mut *tx, max_height, cursor_ts).await?;
    tx.commit().await.map_err(SyncError::from)?;

    // Analytics — same pattern as write_block, post-commit.
    if let Some(handle) = analytics {
        for bundle in &bundles {
            let bh = match bundle.block.height.as_u64() {
                Some(h) => h,
                None => continue,
            };
            for t in &bundle.txs {
                let row = RawTxRow {
                    block_height: bh,
                    timestamp: bundle.block.timestamp as u64,
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
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::created_contract_address;

    #[test]
    fn create_address_matches_known_vector() {
        // Canonical CREATE example (sender, nonce 0) → fixed contract address.
        let got = created_contract_address("0x6ac7ea33f8831ea9dcc53393aaa88b25a785dbf0", 0);
        assert_eq!(
            got.as_deref(),
            Some("0xcd234a471b72ba2f1ccf0a70fcaba648a5eecd8d")
        );
    }

    #[test]
    fn create_address_rejects_bad_sender_and_nonce() {
        assert_eq!(created_contract_address("not-an-address", 0), None);
        assert_eq!(
            created_contract_address("0x6ac7ea33f8831ea9dcc53393aaa88b25a785dbf0", -1),
            None
        );
    }
}
