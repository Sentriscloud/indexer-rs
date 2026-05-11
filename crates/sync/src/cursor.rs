//! Sync cursor — last-synced-height + chain-id + reorg checkpoint.
//!
//! Stored in `_meta`. The cursor is written inside the same `sqlx::Transaction`
//! as the block / tx / log inserts so a crash between data write and cursor
//! write can't desync the two (spec §5 invariant 2).

use crate::SyncResult;
use indexer_db::{PgPool, meta};
use indexer_domain::BlockHeight;

/// `_meta` key that holds the highest height the indexer has fully written.
/// On boot, `read_cursor` returns `BlockHeight(-1)` if unset so the
/// backfill walks from genesis.
pub const LAST_SYNCED_HEIGHT_KEY: &str = "last_synced_height";

/// Read the chain-wide sync cursor. Returns `Some(h)` if the indexer has
/// successfully written through height `h`; `None` means no rows yet.
pub async fn read_cursor(pool: &PgPool) -> SyncResult<Option<BlockHeight>> {
    let v = meta::get_i64(pool, LAST_SYNCED_HEIGHT_KEY).await?;
    Ok(v.map(BlockHeight))
}

/// Write the cursor inside an existing transaction. Caller commits.
///
/// `now_ts` is the chain-time second associated with the bump (we put the
/// finalized block's timestamp here so cursor staleness is comparable to
/// chain time, not wall-clock).
pub async fn write_cursor<'e, E>(executor: E, h: BlockHeight, now_ts: i64) -> SyncResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    meta::set_i64(executor, LAST_SYNCED_HEIGHT_KEY, h.0, now_ts).await?;
    Ok(())
}
