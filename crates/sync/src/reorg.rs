//! Reorg detection + rewind.
//!
//! Approach: pick a probe height a few blocks back from our cursor, fetch
//! the chain's hash at that height, compare to what we have stored. Match
//! → no reorg, return. Mismatch → walk backwards block-by-block to find
//! the divergence point, then `delete_from(blocks, divergence_h)` (cascade
//! drops txs + logs) + reset the cursor to `divergence_h - 1`. Backfill
//! re-ingests on the next tick. Spec §5 invariant 3.
//!
//! BFT-finalized chains rarely reorg, but the indexer must still handle
//! the case where the chain rewinds during a recovery (e.g. operator-side
//! chain.db rsync to canonical) — see `state-divergence-recovery.md`
//! operator runbook.

use crate::cursor::write_cursor;
use crate::{SyncConfig, SyncError, SyncResult};
use indexer_chain::ChainProvider;
use indexer_db::{PgPool, blocks};
use indexer_domain::BlockHeight;

/// Run one reorg-check pass. Returns the new cursor height after rewind, or
/// the existing cursor if nothing to do.
pub async fn check_once(
    pool: &PgPool,
    provider: &ChainProvider,
    cfg: &SyncConfig,
    cursor: BlockHeight,
) -> SyncResult<BlockHeight> {
    if cursor.0 < 0 {
        return Ok(cursor);
    }
    let probe_h = BlockHeight(cursor.0.saturating_sub(cfg.reorg_probe_depth as i64).max(0));
    let local = match blocks::get_by_height(pool, probe_h).await? {
        Some(b) => b,
        None => return Ok(cursor),
    };
    let remote = match provider.block_with_txs(probe_h).await? {
        Some(b) => b,
        None => {
            // Chain hasn't restored this height yet — rare but possible
            // during a recovery snapshot rsync. Hold position; next tick
            // re-probes.
            return Ok(cursor);
        }
    };
    let remote_hash = format!("0x{}", hex::encode(remote.header.hash.as_slice()));
    if remote_hash == local.hash {
        return Ok(cursor);
    }
    tracing::warn!(
        height = probe_h.0,
        local = %local.hash,
        remote = %remote_hash,
        "reorg detected at probe height; walking back to divergence",
    );
    let divergence = walk_back_to_divergence(pool, provider, probe_h).await?;
    rewind_to(pool, divergence).await?;
    let new_cursor = BlockHeight(divergence.0.saturating_sub(1));
    tracing::warn!(
        rewind_from = cursor.0,
        rewind_to = new_cursor.0,
        divergence = divergence.0,
        "reorg rewind complete; backfill will re-ingest from cursor+1",
    );
    Ok(new_cursor)
}

/// Walk backward from `probe_h` until local hash matches remote. Returns
/// the lowest height `d` where local diverged (i.e. `d-1` is still good).
async fn walk_back_to_divergence(
    pool: &PgPool,
    provider: &ChainProvider,
    probe_h: BlockHeight,
) -> SyncResult<BlockHeight> {
    let mut cursor = probe_h;
    loop {
        if cursor.0 == 0 {
            return Ok(BlockHeight(0));
        }
        let parent = BlockHeight(cursor.0 - 1);
        let local = blocks::get_by_height(pool, parent).await?;
        let remote = provider.block_with_txs(parent).await?;
        match (local, remote) {
            (Some(l), Some(r)) => {
                let r_hash = format!("0x{}", hex::encode(r.header.hash.as_slice()));
                if r_hash == l.hash {
                    return Ok(cursor);
                }
                cursor = parent;
            }
            (None, _) => {
                // We have a gap below `cursor` — divergence is here.
                return Ok(cursor);
            }
            (_, None) => {
                // Remote shorter than us, very rare. Treat parent as
                // divergence.
                return Ok(cursor);
            }
        }
    }
}

/// Atomic rewind: delete every block at or above `h` (FK CASCADE drops
/// txs + logs), then reset the cursor to `h - 1`.
async fn rewind_to(pool: &PgPool, h: BlockHeight) -> SyncResult<()> {
    let mut tx = pool.begin().await.map_err(SyncError::from)?;
    let dropped = blocks::delete_from(&mut *tx, h).await?;
    let new_cursor = BlockHeight(h.0.saturating_sub(1));
    // We don't have a fresh chain timestamp here; reuse 0 — the next
    // backfill pass overwrites this with the next finalized block's ts.
    write_cursor(&mut *tx, new_cursor, 0).await?;
    tx.commit().await.map_err(SyncError::from)?;
    tracing::warn!(
        rewind_from = h.0,
        rows_dropped = dropped,
        "reorg rewind committed",
    );
    Ok(())
}
