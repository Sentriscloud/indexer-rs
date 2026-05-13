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
use crate::convert::{to_domain_block_from_native, to_domain_log, to_domain_txs_from_native};
use crate::cursor::{read_cursor, write_cursor};
use crate::{SyncConfig, SyncError, SyncResult};
use indexer_analytics::AnalyticsHandle;
use indexer_chain::{BackoffConfig, ChainProvider, RestClient, retry_with_backoff};
use indexer_db::PgPool;
use indexer_domain::BlockHeight;
use tokio_util::sync::CancellationToken;

/// Run the backfill loop until cancellation OR until we reach
/// `tip - safe_lag` (then return; caller decides whether to sleep + recheck
/// or hand off to the tail loop).
pub async fn run_backfill(
    pool: &PgPool,
    provider: &ChainProvider,
    rest: &RestClient,
    cfg: &SyncConfig,
    cancel: CancellationToken,
    analytics: Option<&AnalyticsHandle>,
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
        ingest_one(pool, provider, rest, next, backoff, analytics).await?;
        cursor = next;
    }
    tracing::info!(
        cursor = cursor.0,
        "backfill: caught up to safe-lag boundary"
    );
    Ok(cursor)
}

/// Fetch + write one block. Pulled out for the tail loop to reuse.
///
/// Blocks + their tx envelopes come from the native REST endpoint
/// (`/chain/blocks/<n>`) — Sentrix's `eth_getBlockByNumber(full=true)`
/// ignores the `full` flag, so the alloy path can't decode the tx vec.
/// Logs still go through alloy / `eth_getLogs`, which works correctly.
pub async fn ingest_one(
    pool: &PgPool,
    provider: &ChainProvider,
    rest: &RestClient,
    h: BlockHeight,
    backoff: BackoffConfig,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<()> {
    let block_opt = retry_with_backoff(backoff, || async { rest.block(h).await }).await?;
    let Some(block) = block_opt else {
        // Chain returned 404 — `h` is before the rolling block-body
        // retention window (Sentrix prunes block bodies outside
        // `window_start_block`). The block existed + was canonical; we
        // just can't fetch its txs anymore.
        //
        // Walking the gap one-block-at-a-time is wasteful — at ~1 skip
        // per RTT and ~1.7M pruned blocks on a months-old chain, that's
        // weeks of network round-trips for zero indexed rows. Instead,
        // ask the chain where its retention window starts and jump the
        // cursor straight there. The historical gap is documented as a
        // followup: spin up an archive-mode chain node + re-point
        // INDEXER_NETWORK at it to fill the gap in one fresh walk.
        match rest.chain_info().await {
            Ok(info) if info.window_start_block.is_some_and(|w| w > h.0) => {
                let target = BlockHeight(info.window_start_block.unwrap() - 1);
                tracing::warn!(
                    from = h.0,
                    to = target.0 + 1,
                    pruned = target.0 + 1 - h.0,
                    "backfill: chain has pruned this block body (404); jumping cursor to \
                     window_start_block. Historical gap can be filled later by repointing \
                     INDEXER_NETWORK at an archive-mode node that retains all block bodies."
                );
                write_cursor(pool, target, 0).await?;
                return Ok(());
            }
            Ok(_) => {
                // Chain didn't advertise a retention window (archive-mode
                // or pre-feature); the 404 is genuinely a one-off gap.
                // Fall back to single-block skip + log.
                tracing::warn!(
                    height = h.0,
                    "backfill: 404 from chain but no window_start_block \
                     advertised; skipping single block."
                );
                write_cursor(pool, h, 0).await?;
                return Ok(());
            }
            Err(e) => {
                // chain_info failed; surface as transient error so the
                // orchestrator retries the whole iteration rather than
                // burning the cursor on a transient network blip.
                return Err(SyncError::Chain(e));
            }
        }
    };

    let logs = retry_with_backoff(backoff, || async {
        provider.logs_in_range(h, h, None).await
    })
    .await?;

    let dom_block =
        to_domain_block_from_native(&block).map_err(|e| SyncError::Invalid(e.to_string()))?;
    let dom_txs =
        to_domain_txs_from_native(&block).map_err(|e| SyncError::Invalid(e.to_string()))?;
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
        analytics,
    )
    .await
}
