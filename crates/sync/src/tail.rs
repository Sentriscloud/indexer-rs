//! Tail loop — consume the chain's gRPC `StreamEvents` push and walk the
//! cursor forward as new blocks finalize.
//!
//! Flow per event:
//!   1. Receive a [`pb::ChainEvent`].
//!   2. If it's a [`pb::BlockFinalized`], extract the height + offer it
//!      to the [`SingleFlight`] gate.
//!   3. If we acquired the gate, walk from `cursor + 1` to the offered
//!      tip via `backfill::ingest_one` (per-block writes are atomic).
//!   4. After each chain, drain any stashed pending tip via
//!      `take_pending_and_release` and continue if more arrived.
//!
//! Spec §5 invariant 6 (gRPC `StreamLagged` → trigger backfill) is
//! handled by the orchestrator: on a Lagged sentinel we exit the tail
//! loop with `TailExit::Lagged`, and the caller re-runs `run_backfill`
//! to re-sync via JSON-RPC before re-subscribing.

use crate::cursor::read_cursor;
use crate::single_flight::{Offer, SingleFlight};
use crate::{SyncConfig, SyncError, SyncResult, backfill};
use indexer_analytics::AnalyticsHandle;
use indexer_chain::pb;
use indexer_chain::{BackoffConfig, ChainProvider, GrpcClient};
use indexer_db::PgPool;
use indexer_domain::BlockHeight;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Reason the tail loop exited. The orchestrator decides what to do next.
#[derive(Debug, PartialEq, Eq)]
pub enum TailExit {
    /// The caller cancelled us cleanly.
    Cancelled,
    /// The chain stream emitted [`pb::StreamLagged`]; the caller should
    /// re-run a JSON-RPC backfill before re-subscribing.
    Lagged,
    /// The stream ended (server-side close or transport drop). Caller
    /// reconnects.
    StreamEnded,
}

/// Run the tail loop. Returns when the stream ends, the cancellation token
/// fires, or a Lagged sentinel arrives.
pub async fn run_tail(
    pool: &PgPool,
    provider: &ChainProvider,
    grpc: &mut GrpcClient,
    cfg: &SyncConfig,
    gate: Arc<SingleFlight>,
    cancel: CancellationToken,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<TailExit> {
    let mut stream = grpc
        .stream_events(pb::StreamEventsRequest {
            filters: Vec::new(),
            from_sequence: 0,
        })
        .await?;

    let backoff = BackoffConfig::default();

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => return Ok(TailExit::Cancelled),

            maybe_event = stream.message() => match maybe_event {
                Err(status) => return Err(SyncError::Chain(status.into())),
                Ok(None) => return Ok(TailExit::StreamEnded),
                Ok(Some(ev)) => {
                    use pb::chain_event::Event::*;
                    match ev.event {
                        Some(BlockFinalized(b)) => {
                            let Some(tip) = b.block.map(|blk| BlockHeight::from(blk.index)) else {
                                continue;
                            };
                            handle_finalized(pool, provider, cfg, &gate, backoff, tip, analytics).await?;
                        }
                        Some(Lagged(_)) => return Ok(TailExit::Lagged),
                        Some(PendingTx(_) | ValidatorSetChange(_) | Log(_)) | None => {
                            // Phase 3 only consumes BlockFinalized for cursor
                            // advance. Pending-tx / validator-set / per-log
                            // pushes get handled by later phases (mempool
                            // viewer, validator dashboard, decoded
                            // log-driven workers).
                        }
                    }
                }
            }
        }
    }
}

async fn handle_finalized(
    pool: &PgPool,
    provider: &ChainProvider,
    cfg: &SyncConfig,
    gate: &SingleFlight,
    backoff: BackoffConfig,
    tip: BlockHeight,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<()> {
    let mut current = match gate.offer(tip) {
        Offer::Stashed => return Ok(()),
        Offer::Acquired(h) => h,
    };
    loop {
        let cursor = read_cursor(pool).await?.unwrap_or(BlockHeight(-1));
        let cap = current.0.saturating_sub(cfg.safe_lag as i64);
        if cap > cursor.0 {
            let mut h = BlockHeight(cursor.0 + 1);
            while h.0 <= cap {
                backfill::ingest_one(pool, provider, h, backoff, analytics).await?;
                h = BlockHeight(h.0 + 1);
            }
        }
        match gate.take_pending_and_release() {
            None => return Ok(()),
            Some(next) => {
                current = next;
            }
        }
    }
}
