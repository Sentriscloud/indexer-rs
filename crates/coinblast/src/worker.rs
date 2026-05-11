//! Worker — chunked backfill loop. Walks `[from, to]` ranges of the chain,
//! filters logs by topic, dispatches to `handlers`. Cursor advances after
//! each chunk write.
//!
//! `chunk_size` defaults to 4000 (Sentrix RPC caps `eth_getLogs` at 5000
//! blocks per call; 4000 leaves headroom). `safe_lag` mirrors the chain-wide
//! sync layer.

use crate::events::{
    Buy, COINBLAST_DEPLOY_BLOCK, COINBLAST_FACTORY_ADDRESS, CurveCreated, Graduated, Network, Sell,
};
use crate::{CoinblastError, CoinblastResult, META_KEY_COINBLAST_CURSOR, handlers};
use alloy_primitives::B256;
use alloy_sol_types::SolEvent;
use indexer_chain::{BackoffConfig, ChainProvider, retry_with_backoff};
use indexer_db::{PgPool, meta};
use indexer_domain::BlockHeight;
use std::collections::HashSet;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Worker config. Defaults match the TS production indexer.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Network to bind to.
    pub network: Network,
    /// How many blocks per `eth_getLogs` chunk.
    pub chunk_size: u64,
    /// Stay this many blocks short of tip.
    pub safe_lag: u64,
    /// Pause this long between chunks (avoids burning CPU on empty chunks).
    pub tick: Duration,
    /// Sleep this long after error / when caught up to tip - safe_lag.
    pub idle_sleep: Duration,
}

impl WorkerConfig {
    /// Sensible default for the given network.
    pub fn for_network(network: Network) -> Self {
        Self {
            network,
            chunk_size: 4000,
            safe_lag: 5,
            tick: Duration::from_millis(500),
            idle_sleep: Duration::from_secs(2),
        }
    }
}

/// Run the CoinBlast worker until cancellation.
pub async fn run_coinblast_worker(
    pool: &PgPool,
    provider: &ChainProvider,
    cfg: &WorkerConfig,
    cancel: CancellationToken,
) -> CoinblastResult<()> {
    let factory = COINBLAST_FACTORY_ADDRESS(cfg.network);
    let mut known_curves = hydrate_known_curves(pool).await?;
    tracing::info!(
        network = ?cfg.network,
        factory = %format!("0x{}", hex::encode(factory.as_slice())),
        curves = known_curves.len(),
        "coinblast: worker booting",
    );

    let backoff = BackoffConfig::default();
    let topic_curve_created = CurveCreated::SIGNATURE_HASH;
    let topic_buy = Buy::SIGNATURE_HASH;
    let topic_sell = Sell::SIGNATURE_HASH;
    let topic_graduated = Graduated::SIGNATURE_HASH;

    loop {
        if cancel.is_cancelled() {
            tracing::info!("coinblast: cancelled");
            return Ok(());
        }

        let cursor = read_cursor(pool, cfg.network).await?;
        let tip = retry_with_backoff(backoff, || async { provider.block_number().await }).await?;
        let target = tip.0.saturating_sub(cfg.safe_lag as i64);
        if cursor >= target {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(cfg.idle_sleep) => continue,
            }
        }

        let from = BlockHeight(cursor + 1);
        let to_proposed = cursor.saturating_add(cfg.chunk_size as i64);
        let to = BlockHeight(to_proposed.min(target));

        if let Err(e) = run_chunk(
            pool,
            provider,
            from,
            to,
            factory,
            &mut known_curves,
            backoff,
            topic_curve_created,
            topic_buy,
            topic_sell,
            topic_graduated,
        )
        .await
        {
            tracing::warn!(
                from = from.0,
                to = to.0,
                error = %e,
                "coinblast: chunk failed; will retry next tick",
            );
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(cfg.idle_sleep) => continue,
            }
        }

        write_cursor(pool, cfg.network, to).await?;
        tracing::info!(
            from = from.0,
            to = to.0,
            curves = known_curves.len(),
            "coinblast: chunk done",
        );

        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(cfg.tick) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_chunk(
    pool: &PgPool,
    provider: &ChainProvider,
    from: BlockHeight,
    to: BlockHeight,
    factory: alloy_primitives::Address,
    known_curves: &mut HashSet<String>,
    backoff: BackoffConfig,
    topic_curve_created: B256,
    topic_buy: B256,
    topic_sell: B256,
    topic_graduated: B256,
) -> CoinblastResult<()> {
    // Pass 1: factory CurveCreated logs — scoped by emitter to avoid topic
    // collision with unrelated contracts that might emit a same-topic event.
    let factory_logs = retry_with_backoff(backoff, || async {
        provider.logs_in_range(from, to, Some(factory)).await
    })
    .await?;

    for l in &factory_logs {
        if l.topics().first().copied() == Some(topic_curve_created) {
            handlers::apply_curve_created(pool, l).await?;
            known_curves.insert(format!("0x{}", hex::encode(l.address().as_slice())));
        }
    }

    // Pass 2: curve Buy/Sell/Graduated logs across the chunk. We pull
    // unfiltered + dispatch by (emitter, topic0). Emitter MUST be in
    // known_curves — orphan adoption deferred (CBLAST is the only known
    // pre-factory direct deploy; future workers run with eth_call wired).
    let curve_logs = retry_with_backoff(backoff, || async {
        provider.logs_in_range(from, to, None).await
    })
    .await?;

    for l in &curve_logs {
        let emitter = format!("0x{}", hex::encode(l.address().as_slice()));
        if !known_curves.contains(&emitter) {
            continue;
        }
        let Some(t0) = l.topics().first().copied() else {
            continue;
        };
        if t0 == topic_buy {
            handlers::apply_buy(pool, l).await?;
        } else if t0 == topic_sell {
            handlers::apply_sell(pool, l).await?;
        } else if t0 == topic_graduated {
            handlers::apply_graduated(pool, l).await?;
        }
    }
    Ok(())
}

async fn hydrate_known_curves(pool: &PgPool) -> CoinblastResult<HashSet<String>> {
    let curves = indexer_db::cb_tokens::known_curve_addresses(pool).await?;
    Ok(curves.into_iter().map(|s| s.to_lowercase()).collect())
}

async fn read_cursor(pool: &PgPool, network: Network) -> CoinblastResult<i64> {
    let v = meta::get_i64(pool, META_KEY_COINBLAST_CURSOR).await?;
    let floor = COINBLAST_DEPLOY_BLOCK(network) as i64 - 1;
    Ok(v.unwrap_or(floor).max(floor))
}

async fn write_cursor(pool: &PgPool, _network: Network, to: BlockHeight) -> CoinblastResult<()> {
    // Use 0 as the timestamp here — the worker doesn't read block times.
    // Operators still see the cursor row update via PG `xmin` / explicit
    // last-modified queries on `_meta.updated_at`.
    meta::set_i64(pool, META_KEY_COINBLAST_CURSOR, to.0, 0).await?;
    Ok(())
}

// Silence unused-import warning on platforms where the trait import is
// only used through method calls.
#[allow(dead_code)]
fn _force_used(_: &dyn std::fmt::Debug, _: CoinblastError) {}
