//! Backfill loop — sequential block-by-block ingest.
//!
//! Strategy: read cursor → ask chain for tip → walk from cursor+1 to
//! min(tip - safe_lag, max_backfill). For each height: fetch block (with
//! txs) + logs → convert → write atomically (block + txs + logs + cursor)
//! → continue. Retries via [`indexer_chain::retry_with_backoff`]; permanent
//! failures bubble to the orchestrator.
//!
//! Cancellation: caller passes a [`CancellationToken`]; the loop checks
//! between blocks and exits cleanly. In-flight commits run to completion
//! (the cursor never lands ahead of the data — spec §5 invariant 2).

use crate::block_writer::{BlockBundle, write_block};
use crate::convert::{to_domain_block, to_domain_log, to_domain_txs};
use crate::cursor::read_cursor;
use crate::{SyncConfig, SyncError, SyncResult};
use indexer_chain::{BackoffConfig, ChainProvider, retry_with_backoff};
use indexer_db::PgPool;
use indexer_domain::BlockHeight;
use tokio_util::sync::CancellationToken;

/// Run the backfill loop until cancellation OR until we reach
/// `tip - safe_lag` (then return; caller decides whether to sleep + recheck
/// or hand off to the tail loop).
pub async fn run_backfill(
    pool: &PgPool,
    provider: &ChainProvider,
    cfg: &SyncConfig,
    cancel: CancellationToken,
) -> SyncResult<BlockHeight> {
    let mut cursor = read_cursor(pool).await?.unwrap_or(BlockHeight(-1));
    let backoff = BackoffConfig::default();

    let tip = retry_with_backoff(backoff, || async { provider.block_number().await }).await?;
    let cap = match cfg.max_backfill_height {
        Some(m) => m.0.min(tip.0.saturating_sub(cfg.safe_lag as i64)),
        None => tip.0.saturating_sub(cfg.safe_lag as i64),
    };
    if cap <= cursor.0 {
        tracing::debug!(cursor = cursor.0, tip = tip.0, "backfill: nothing to do");
        return Ok(cursor);
    }

    tracing::info!(
        from = cursor.0 + 1,
        to = cap,
        tip = tip.0,
        safe_lag = cfg.safe_lag,
        "backfill: starting walk",
    );

    while cursor.0 < cap {
        if cancel.is_cancelled() {
            tracing::info!(cursor = cursor.0, "backfill: cancelled");
            return Ok(cursor);
        }
        let next = BlockHeight(cursor.0 + 1);
        ingest_one(pool, provider, next, backoff).await?;
        cursor = next;
    }
    tracing::info!(
        cursor = cursor.0,
        "backfill: caught up to safe-lag boundary"
    );
    Ok(cursor)
}

/// Fetch + write one block. Pulled out for the tail loop to reuse.
pub async fn ingest_one(
    pool: &PgPool,
    provider: &ChainProvider,
    h: BlockHeight,
    backoff: BackoffConfig,
) -> SyncResult<()> {
    let block_opt =
        retry_with_backoff(backoff, || async { provider.block_with_txs(h).await }).await?;
    let block = block_opt.ok_or_else(|| {
        SyncError::Invalid(format!(
            "backfill: provider returned None for height {}",
            h.0
        ))
    })?;

    let logs = retry_with_backoff(backoff, || async {
        provider.logs_in_range(h, h, None).await
    })
    .await?;

    let dom_block = to_domain_block(&block).map_err(|e| SyncError::Invalid(e.to_string()))?;
    let dom_txs = to_domain_txs(&block).map_err(|e| SyncError::Invalid(e.to_string()))?;
    let dom_logs = logs
        .iter()
        .map(to_domain_log)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SyncError::Invalid(e.to_string()))?;

    write_block(
        pool,
        BlockBundle {
            block: dom_block,
            txs: dom_txs,
            logs: dom_logs,
        },
    )
    .await
}
