//! sentrix-indexer-rs — sync + CoinBlast worker daemon.
//!
//! Reads `DATABASE_URL`, `RPC_URL` (chain JSON-RPC), `GRPC_URL`
//! (chain gRPC, optional — disables tail loop when absent),
//! `INDEXER_NETWORK` (`mainnet` | `testnet`), and optional
//! `CLICKHOUSE_URL` (enables the analytics flusher) from env. Spawns:
//!  - chain-wide backfill loop (always)
//!  - chain-wide tail loop (if GRPC_URL set)
//!  - CoinBlast worker (always)
//!  - analytics flusher (if CLICKHOUSE_URL set)
//!
//! All workers share a `CancellationToken`; SIGTERM / Ctrl-C cancels and
//! the task waits for in-flight commits before exiting (spec §5 invariant 9).

use std::sync::Arc;
use std::time::Duration;

use clickhouse::Client as ChClient;
use figment::Figment;
use figment::providers::Env;
use indexer_analytics::run_flusher;
use indexer_chain::{ChainProvider, GrpcClient};
use indexer_coinblast::{Network, WorkerConfig as CbConfig, run_coinblast_worker};
use indexer_db::{PoolConfig, connect, migrate};
use indexer_sync::{SingleFlight, SyncConfig, TailExit, run_backfill, run_tail};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct IndexerConfig {
    database_url: String,
    rpc_url: String,
    #[serde(default)]
    grpc_url: Option<String>,
    #[serde(default)]
    clickhouse_url: Option<String>,
    #[serde(default = "default_clickhouse_table")]
    clickhouse_raw_tx_table: String,
    #[serde(default = "default_network")]
    indexer_network: String,
    #[serde(default = "default_max_connections")]
    indexer_pg_max_connections: u32,
    #[serde(default = "default_backfill_loop_interval_secs")]
    indexer_backfill_loop_secs: u64,
    #[serde(default = "default_analytics_flush_secs")]
    indexer_analytics_flush_secs: u64,
}

fn default_network() -> String {
    "testnet".to_string()
}
fn default_max_connections() -> u32 {
    10
}
fn default_backfill_loop_interval_secs() -> u64 {
    5
}
fn default_clickhouse_table() -> String {
    "raw_tx".to_string()
}
fn default_analytics_flush_secs() -> u64 {
    15
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cfg: IndexerConfig = Figment::new().merge(Env::raw()).extract()?;
    let network = match cfg.indexer_network.as_str() {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => anyhow::bail!("INDEXER_NETWORK must be 'mainnet' or 'testnet', got '{other}'"),
    };
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        network = ?network,
        rpc_url = %cfg.rpc_url,
        grpc_url = ?cfg.grpc_url,
        clickhouse_url = ?cfg.clickhouse_url,
        "indexer: booting",
    );

    let mut pool_cfg = PoolConfig::from_url(&cfg.database_url);
    pool_cfg.max_connections = cfg.indexer_pg_max_connections;
    let pool = connect(&pool_cfg).await?;
    migrate(&pool).await?;

    let provider = ChainProvider::http(&cfg.rpc_url)?;
    let cancel = CancellationToken::new();

    // Analytics flusher (optional). The handle threads into both the
    // backfill and tail loops so every committed tx pushes one RawTxRow.
    let (analytics_handle, analytics_join) = match cfg.clickhouse_url.as_deref() {
        Some(url) => {
            let ch = ChClient::default().with_url(url);
            let (handle, join) = run_flusher(
                ch,
                cfg.clickhouse_raw_tx_table.clone(),
                Duration::from_secs(cfg.indexer_analytics_flush_secs),
                cancel.clone(),
            );
            (Some(handle), Some(join))
        }
        None => {
            tracing::info!("CLICKHOUSE_URL unset; analytics flusher disabled");
            (None, None)
        }
    };

    // Backfill loop on a tokio interval. Each tick re-reads the cursor +
    // walks forward; bookkeeping idempotent.
    let backfill_handle = {
        let pool = pool.clone();
        let provider = provider.clone();
        let cancel = cancel.clone();
        let analytics = analytics_handle.clone();
        let interval = Duration::from_secs(cfg.indexer_backfill_loop_secs);
        tokio::spawn(async move {
            let cfg = SyncConfig::default();
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                    _ = tick.tick() => {
                        if let Err(e) = run_backfill(&pool, &provider, &cfg, cancel.clone(), analytics.as_ref()).await {
                            tracing::warn!(error = %e, "backfill loop iteration failed");
                        }
                    }
                }
            }
        })
    };

    // Tail loop (gRPC StreamEvents) — runs in parallel with backfill,
    // closes the cold-start gap then handles every new tip via SingleFlight.
    let gate = Arc::new(SingleFlight::new());
    let tail_handle = match cfg.grpc_url.clone() {
        Some(url) => {
            let pool = pool.clone();
            let provider = provider.clone();
            let cancel = cancel.clone();
            let gate = gate.clone();
            let analytics = analytics_handle.clone();
            Some(tokio::spawn(async move {
                let sync_cfg = SyncConfig::default();
                loop {
                    if cancel.is_cancelled() {
                        return Ok::<(), anyhow::Error>(());
                    }
                    let mut grpc = match GrpcClient::connect(url.clone()).await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!(error = %e, "tail: gRPC connect failed; retrying in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    };
                    match run_tail(
                        &pool,
                        &provider,
                        &mut grpc,
                        &sync_cfg,
                        gate.clone(),
                        cancel.clone(),
                        analytics.as_ref(),
                    )
                    .await
                    {
                        Ok(TailExit::Cancelled) => return Ok(()),
                        Ok(TailExit::Lagged) => {
                            // Backfill loop will re-sync the gap on its next
                            // tick; we just reconnect.
                            tracing::warn!(
                                "tail: stream Lagged; reconnecting after backfill catch-up"
                            );
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        Ok(TailExit::StreamEnded) => {
                            tracing::warn!("tail: stream ended; reconnecting in 2s");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "tail: failed; reconnecting in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }))
        }
        None => {
            tracing::info!("GRPC_URL unset; tail loop disabled (backfill carries the load)");
            None
        }
    };

    // CoinBlast worker has its own cursor + chunked scan loop.
    let coinblast_handle = {
        let pool = pool.clone();
        let provider = provider.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let cfg = CbConfig::for_network(network);
            run_coinblast_worker(&pool, &provider, &cfg, cancel)
                .await
                .map_err(anyhow::Error::from)
        })
    };

    shutdown_signal().await;
    tracing::info!("indexer: shutdown signal received; cancelling workers");
    cancel.cancel();

    let _ = backfill_handle.await?;
    let _ = coinblast_handle.await?;
    if let Some(t) = tail_handle {
        let _ = t.await?;
    }
    if let Some(a) = analytics_join
        && let Err(e) = a.await?
    {
        tracing::warn!(error = %e, "analytics flusher exited with error");
    }
    tracing::info!("indexer: shutdown complete");
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl_c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
