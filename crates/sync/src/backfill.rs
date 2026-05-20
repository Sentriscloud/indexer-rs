//! Backfill loop — pipelined concurrent fetch + serial write.
//!
//! Strategy: read cursor → ask chain for tip → walk from cursor+1 to
//! min(tip - safe_lag, max_backfill). For each height range, fetch N
//! blocks concurrently (REST + eth_getLogs per block); commit them
//! sequentially in height order so cursor never lands ahead of data
//! (spec §5 invariant 2). Retries via [`indexer_chain::retry_with_backoff`];
//! permanent failures bubble to the orchestrator.
//!
//! Concurrency: tunable via `INDEXER_BACKFILL_CONCURRENCY` env (default 50).
//! Wall-clock measured improvement vs sequential: 3 → 51 blocks/sec on
//! mainnet (2026-05-14, 1.5M-block backfill 138h → ~8h).
//!
//! Cancellation: caller passes a [`CancellationToken`]. The pipeline races
//! `cancel.cancelled()` against the next item via `tokio::select!`, so a
//! cancel during a slow fetch returns within the cancel timeout, not after
//! the in-flight chunk completes.

use crate::block_writer::{BlockBundle, batch_write_blocks, write_block};
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
/// Each fetch is one REST call + one eth_getLogs call. Writes land in
/// batches of `INDEXER_BACKFILL_BATCH` (default 100). 50 is a
/// conservative default — see `INDEXER_BACKFILL_CONCURRENCY` env var to tune.
const DEFAULT_BACKFILL_CONCURRENCY: usize = 50;

/// How many fetched bundles to accumulate before flushing as one batched
/// transaction. Bigger batch = fewer commits + lower fsync overhead;
/// smaller batch = lower memory + more frequent cursor advance. 100 is a
/// good middle: ~5MB peak buffer for typical mainnet blocks, single-digit
/// commits/sec at 500+ b/s.
const DEFAULT_BACKFILL_BATCH: usize = 100;

fn backfill_concurrency() -> usize {
    std::env::var("INDEXER_BACKFILL_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0 && n <= 500)
        .unwrap_or(DEFAULT_BACKFILL_CONCURRENCY)
}

fn backfill_batch_size() -> usize {
    std::env::var("INDEXER_BACKFILL_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0 && n <= 1000)
        .unwrap_or(DEFAULT_BACKFILL_BATCH)
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

    tracing::info!(
        from = cursor.0 + 1,
        to = cap,
        tip = tip.0,
        safe_lag = cfg.safe_lag,
        "backfill: starting walk",
    );

    // Pipeline: fetch N blocks concurrently (each = REST + eth_getLogs),
    // write them sequentially in height order so the cursor never lands
    // ahead of the data. Per-block latency was the bottleneck (3 b/s);
    // concurrent fetch + serial write keeps invariants while saturating
    // the network. INDEXER_BACKFILL_CONCURRENCY env var tunes the pool.
    let concurrency = backfill_concurrency();
    let batch_size = backfill_batch_size();
    tracing::info!(
        concurrency,
        batch_size,
        "backfill: pipelined fetch + batched write enabled"
    );

    let start = cursor.0 + 1;
    let total = (cap - cursor.0) as usize;

    let mut fetched = stream::iter(start..=cap)
        .map(|h| {
            let h = BlockHeight(h);
            async move { (h, fetch_one(provider, rest, h, backoff).await) }
        })
        .buffered(concurrency);

    // Accumulator: bundles wait here until we hit batch_size or stream ends,
    // then flush as one transaction. 404-gap heights advance the cursor
    // out-of-band (separate write) so the batch invariant (all heights
    // present) holds for the bundles that flush together.
    let mut buf: Vec<BlockBundle> = Vec::with_capacity(batch_size);
    let mut done = 0usize;
    loop {
        // Race cancellation against the next item — a cancel issued while
        // a slow fetch is in flight returns immediately rather than after
        // the chunk completes.
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Flush whatever we've accumulated so the cursor reflects
                // it before we exit; otherwise the next start re-fetches
                // already-buffered bundles.
                if !buf.is_empty() {
                    let highest = buf.iter().map(|b| b.block.height).max().unwrap();
                    batch_write_blocks(pool, std::mem::take(&mut buf), analytics).await?;
                    cursor = highest;
                }
                tracing::info!(cursor = cursor.0, "backfill: cancelled mid-pipeline");
                return Ok(cursor);
            }
            next = fetched.next() => next,
        };
        let (h, result) = match next {
            Some(item) => item,
            None => break,
        };
        match result? {
            Some(bundle) => buf.push(bundle),
            None => {
                // 404 / damaged-block gap. Flush in-flight buffer first so
                // the gap-cursor advance lands AFTER the data we already
                // fetched, preserving the spec §5 invariant 2 (cursor never
                // lands ahead of data).
                if !buf.is_empty() {
                    batch_write_blocks(pool, std::mem::take(&mut buf), analytics).await?;
                    // cursor is overwritten to the gap height below;
                    // batch_write already advanced the persisted cursor.
                }
                tracing::warn!(height = h.0, "backfill: skipping single 404 block");
                write_cursor(pool, h, 0).await?;
                cursor = h;
            }
        }
        done += 1;
        // Flush at batch boundary.
        if buf.len() >= batch_size {
            let highest = buf.iter().map(|b| b.block.height).max().unwrap();
            batch_write_blocks(pool, std::mem::take(&mut buf), analytics).await?;
            cursor = highest;
        }
        if done.is_multiple_of(1000) {
            tracing::info!(
                cursor = cursor.0,
                progress = format!("{done}/{total}"),
                "backfill: pipeline progress"
            );
        }
    }
    // Tail flush — anything still buffered after the stream closes.
    if !buf.is_empty() {
        let highest = buf.iter().map(|b| b.block.height).max().unwrap();
        batch_write_blocks(pool, std::mem::take(&mut buf), analytics).await?;
        cursor = highest;
    }
    tracing::info!(
        cursor = cursor.0,
        ingested = done,
        "backfill: caught up to safe-lag boundary"
    );
    Ok(cursor)
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

    // 2026-05-20: Sentrix's native /chain/blocks/<n> and eth_getLogs can
    // disagree — a tx whose effects reverted gets stripped from the block
    // tx vec but its log envelopes still come back from eth_getLogs. The
    // `logs.tx_hash` FK then blows the whole batch. Drop logs whose
    // tx_hash isn't backed by a tx row in this same bundle; they'd be
    // orphaned anyway.
    use std::collections::HashSet;
    let tx_hash_set: HashSet<_> = dom_txs.iter().map(|t| t.hash.clone()).collect();
    let logs_total = dom_logs.len();
    let dom_logs: Vec<_> = dom_logs
        .into_iter()
        .filter(|l| tx_hash_set.contains(&l.tx_hash))
        .collect();
    if dom_logs.len() < logs_total {
        tracing::debug!(
            block = h.0,
            dropped = logs_total - dom_logs.len(),
            "backfill: dropped orphan logs (tx_hash not in block.txs)"
        );
    }

    let dom_token_transfers: Vec<_> = dom_logs
        .iter()
        .filter_map(crate::token_decode::decode_transfer)
        .collect();

    Ok(Some(BlockBundle {
        block: dom_block,
        txs: dom_txs,
        logs: dom_logs,
        token_transfers: dom_token_transfers,
    }))
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

    // Same orphan-log filter as fetch_one — keep the FK invariant on the
    // tail path so the live chain doesn't stall the indexer the way the
    // backfill batch did.
    use std::collections::HashSet;
    let tx_hash_set: HashSet<_> = dom_txs.iter().map(|t| t.hash.clone()).collect();
    let dom_logs: Vec<_> = dom_logs
        .into_iter()
        .filter(|l| tx_hash_set.contains(&l.tx_hash))
        .collect();

    let dom_token_transfers: Vec<_> = dom_logs
        .iter()
        .filter_map(crate::token_decode::decode_transfer)
        .collect();

    write_block(
        pool,
        BlockBundle {
            block: dom_block,
            txs: dom_txs,
            logs: dom_logs,
            token_transfers: dom_token_transfers,
        },
        analytics,
    )
    .await
}
