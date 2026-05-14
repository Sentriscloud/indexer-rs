//! Backfill loop — pipelined concurrent fetch + bulk-COPY batched write.
//!
//! Strategy: read cursor → ask chain for tip → walk from cursor+1 to
//! min(tip - safe_lag, max_backfill). For each height range, fetch N
//! blocks concurrently (REST + eth_getLogs per block); buffer them in
//! height order then flush every `batch_size` blocks (or on cancel /
//! stream end) via PG `COPY FROM STDIN`. Cursor advances to MAX(height)
//! of each batch inside the same transaction so cursor never lands ahead
//! of data (spec §5 invariant 2). Retries via
//! [`indexer_chain::retry_with_backoff`]; permanent failures bubble to the
//! orchestrator.
//!
//! Concurrency: tunable via `INDEXER_BACKFILL_CONCURRENCY` env (default 50).
//! Batch size:  tunable via `INDEXER_WRITE_BATCH_SIZE`   env (default 100,
//! capped at 1000 to keep memory bounded and avoid blowing the cursor lag
//! window during a long-running batch).
//!
//! Wall-clock measured improvement vs sequential per-row INSERT:
//!  - 3 → 51 blocks/sec (PR #33, fetch concurrency)
//!  - 51 → ?  blocks/sec (this PR, bulk COPY) — see PR body for live benchmark.
//!
//! Cancellation: caller passes a [`CancellationToken`]. The pipeline races
//! `cancel.cancelled()` against the next item via `tokio::select!`, so a
//! cancel during a slow fetch returns within the cancel timeout, not after
//! the in-flight chunk completes. Any partial buffer is flushed atomically
//! before return so the cursor still lands.

use crate::block_writer::{BlockBundle, write_block_batch};
use crate::convert::{to_domain_block_from_native, to_domain_log, to_domain_txs_from_native};
use crate::cursor::{read_cursor, write_cursor};
use crate::{SyncConfig, SyncError, SyncResult};
use futures::stream::{self, StreamExt};
use indexer_analytics::AnalyticsHandle;
use indexer_chain::{BackoffConfig, ChainProvider, RestClient, retry_with_backoff};
use indexer_db::PgPool;
use indexer_domain::BlockHeight;
use tokio_util::sync::CancellationToken;

/// How many blocks to fetch concurrently in the backfill window.
/// Each fetch is one REST call + one eth_getLogs call. Writes still
/// land sequentially per batch. 50 is a conservative default — see
/// `INDEXER_BACKFILL_CONCURRENCY` env var to tune.
const DEFAULT_BACKFILL_CONCURRENCY: usize = 50;

/// How many blocks to buffer before issuing a bulk-COPY transaction. Each
/// batch is one PG transaction with three COPY streams (blocks → txs → logs).
/// 100 is the sweet spot: small enough that a fault rolls back ≤2s of work,
/// large enough that COPY round-trip overhead amortises across many rows.
const DEFAULT_WRITE_BATCH_SIZE: usize = 100;

/// Hard cap on `INDEXER_WRITE_BATCH_SIZE`. Memory is the limiter — at
/// 1000 blocks × ~10KB per block worst-case, one batch holds ~10MB of row
/// data plus the COPY text-format buffer.
const MAX_WRITE_BATCH_SIZE: usize = 1000;

fn backfill_concurrency() -> usize {
    std::env::var("INDEXER_BACKFILL_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0 && n <= 500)
        .unwrap_or(DEFAULT_BACKFILL_CONCURRENCY)
}

fn write_batch_size() -> usize {
    std::env::var("INDEXER_WRITE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0 && n <= MAX_WRITE_BATCH_SIZE)
        .unwrap_or(DEFAULT_WRITE_BATCH_SIZE)
}

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

    let concurrency = backfill_concurrency();
    let batch_size = write_batch_size();
    tracing::info!(
        from = cursor.0 + 1,
        to = cap,
        tip = tip.0,
        safe_lag = cfg.safe_lag,
        concurrency,
        batch_size,
        "backfill: starting walk (concurrent fetch + bulk-COPY batched write)",
    );

    let start = cursor.0 + 1;
    let total = (cap - cursor.0) as usize;

    let mut fetched = stream::iter(start..=cap)
        .map(|h| {
            let h = BlockHeight(h);
            async move { (h, fetch_one(provider, rest, h, backoff).await) }
        })
        .buffered(concurrency);

    // Reused across batches — `write_block_batch` clears them on success.
    let mut buf: Vec<BlockBundle> = Vec::with_capacity(batch_size);
    let mut gap_buf: Vec<BlockHeight> = Vec::new();

    let mut done = 0usize;
    loop {
        // Race cancellation against the next item — a cancel issued while
        // a slow fetch is in flight returns immediately. Any in-progress
        // batch buffer is flushed below before we exit so the cursor lands.
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!(
                    cursor = cursor.0,
                    pending_blocks = buf.len(),
                    pending_gaps = gap_buf.len(),
                    "backfill: cancelled mid-pipeline, flushing partial batch",
                );
                if !buf.is_empty() || !gap_buf.is_empty() {
                    let max_h = peek_batch_max(&buf, &gap_buf);
                    write_block_batch(pool, &mut buf, &mut gap_buf, analytics).await?;
                    cursor = max_h;
                }
                return Ok(cursor);
            }
            next = fetched.next() => next,
        };

        let Some((h, result)) = next else { break };

        match result? {
            Some(bundle) => buf.push(bundle),
            None => {
                // 404 / damaged-block gap — see ingest_one rationale below.
                // Folded into the same batch as a cursor-only height so the
                // bump still lands atomically with the surrounding blocks.
                tracing::warn!(height = h.0, "backfill: skipping single 404 block");
                gap_buf.push(h);
            }
        }

        done += 1;
        if buf.len() + gap_buf.len() >= batch_size {
            let max_h = peek_batch_max(&buf, &gap_buf);
            write_block_batch(pool, &mut buf, &mut gap_buf, analytics).await?;
            cursor = max_h;
            if done.is_multiple_of(1000) {
                tracing::info!(
                    cursor = cursor.0,
                    progress = format!("{done}/{total}"),
                    "backfill: pipeline progress"
                );
            }
        }
    }

    // Drain any tail < batch_size left in the buffer when the stream ends.
    if !buf.is_empty() || !gap_buf.is_empty() {
        let max_h = peek_batch_max(&buf, &gap_buf);
        write_block_batch(pool, &mut buf, &mut gap_buf, analytics).await?;
        cursor = max_h;
    }

    tracing::info!(
        cursor = cursor.0,
        ingested = done,
        "backfill: caught up to safe-lag boundary"
    );
    Ok(cursor)
}

/// Largest height across both buffers — used to pre-compute the cursor we'll
/// land on so the post-flush update is a cheap clone, not a re-scan after the
/// buffers have been moved into the writer. Returns the previous cursor's
/// sentinel (-1) if both buffers are empty (caller checks first).
fn peek_batch_max(bundles: &[BlockBundle], gaps: &[BlockHeight]) -> BlockHeight {
    let mut max_h = BlockHeight(i64::MIN);
    for b in bundles {
        if b.block.height.0 > max_h.0 {
            max_h = b.block.height;
        }
    }
    for h in gaps {
        if h.0 > max_h.0 {
            max_h = *h;
        }
    }
    max_h
}

/// Fetch (no write) — used by the concurrent pipeline. Returns None on
/// 404 (damaged-block gap), Some(bundle) on success. Errors propagate.
async fn fetch_one(
    provider: &ChainProvider,
    rest: &RestClient,
    h: BlockHeight,
    backoff: BackoffConfig,
) -> SyncResult<Option<BlockBundle>> {
    let block_opt = retry_with_backoff(backoff, || async { rest.block(h).await }).await?;
    let Some(block) = block_opt else {
        return Ok(None);
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
    Ok(Some(BlockBundle {
        block: dom_block,
        txs: dom_txs,
        logs: dom_logs,
    }))
}

/// Fetch + write one block. Pulled out for the tail loop to reuse — the
/// per-row `write_block` path with `ON CONFLICT DO NOTHING` is the right
/// shape at the tip where the same height may be re-attempted across
/// gRPC reconnects (single block, idempotent retry).
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
        // Chain returned 404. v0.2.3 jumped cursor to
        // `window_start_block - 1` here on the assumption that 404
        // meant "this and every prior block has been pruned". That
        // assumption was wrong: per `sentrix-core::blockchain` the
        // chain keeps every block in MDBX, only the in-memory sliding
        // window is bounded by CHAIN_WINDOW_SIZE. The 404 we observed
        // at h=32690 was a damaged-block gap from a forensic recovery,
        // not a retention boundary — and jumping to window_start would
        // skip ~1.7M legitimately-available blocks for a single bad one.
        //
        // Correct behaviour: single-block skip + advance the cursor by
        // one. Walking the (rare) gaps one-at-a-time is fine; the
        // typical case is no 404 at all.
        tracing::warn!(
            height = h.0,
            "backfill: chain returned 404 for this height; skipping single block. \
             Likely a damaged-block gap from a past forensic recovery — the chain \
             does NOT prune block bodies, every height is normally available."
        );
        write_cursor(pool, h, 0).await?;
        return Ok(());
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

    crate::block_writer::write_block(
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
