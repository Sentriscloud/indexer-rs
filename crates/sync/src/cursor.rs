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
/// Monotonic — the on-disk value only updates if `h` is strictly greater
/// than the current value. Two parallel writers (the batched backfill loop
/// and the per-block tail catchup) can race and commit out-of-order;
/// without this guard, the slower writer's older cursor would clobber the
/// faster writer's newer one. The GREATEST cast keeps the row at the
/// highest committed height regardless of commit order.
///
/// `now_ts` is the chain-time second associated with the bump (we put the
/// finalized block's timestamp here so cursor staleness is comparable to
/// chain time, not wall-clock). updated_at always refreshes so callers can
/// audit recency, even when the value itself is unchanged.
pub async fn write_cursor<'e, E>(executor: E, h: BlockHeight, now_ts: i64) -> SyncResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO _meta (key, value, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (key) DO UPDATE SET \
            value = GREATEST(_meta.value::int8, EXCLUDED.value::int8)::text, \
            updated_at = EXCLUDED.updated_at",
    )
    .bind(LAST_SYNCED_HEIGHT_KEY)
    .bind(h.0.to_string())
    .bind(now_ts)
    .execute(executor)
    .await?;
    Ok(())
}
