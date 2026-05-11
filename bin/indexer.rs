//! sentrix-indexer-rs — sync + CoinBlast worker daemon.
//!
//! Reads `DATABASE_URL`, `RPC_URL` (chain JSON-RPC), `GRPC_URL`
//! (chain gRPC, optional — disables tail loop when absent), and
//! `INDEXER_NETWORK` (`mainnet` | `testnet`) from env. Spawns:
//!  - chain-wide backfill loop (always)
//!  - chain-wide tail loop (if GRPC_URL set; deferred wiring)
//!  - CoinBlast worker (always)
//!
//! All workers share a `CancellationToken`; SIGTERM / Ctrl-C cancels and
//! the task waits for in-flight commits before exiting (spec §5 invariant 9).

use std::sync::Arc;
use std::time::Duration;

use figment::Figment;
use figment::providers::Env;
use indexer_chain::ChainProvider;
use indexer_coinblast::{Network, WorkerConfig as CbConfig, run_coinblast_worker};
use indexer_db::{PoolConfig, connect, migrate};
use indexer_sync::{SingleFlight, SyncConfig, run_backfill};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct IndexerConfig {
    database_url: String,
    rpc_url: String,
    #[serde(default)]
    grpc_url: Option<String>,
    #[serde(default = "default_network")]
    indexer_network: String,
    #[serde(default = "default_max_connections")]
    indexer_pg_max_connections: u32,
    #[serde(default = "default_backfill_loop_interval_secs")]
    indexer_backfill_loop_secs: u64,
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
        "indexer: booting",
    );

    let mut pool_cfg = PoolConfig::from_url(&cfg.database_url);
    pool_cfg.max_connections = cfg.indexer_pg_max_connections;
    let pool = connect(&pool_cfg).await?;
    migrate(&pool).await?;

    let provider = ChainProvider::http(&cfg.rpc_url)?;
    let cancel = CancellationToken::new();

    // Backfill loop on a tokio interval. Each tick re-reads the cursor +
    // walks forward; bookkeeping idempotent.
    let backfill_handle = {
        let pool = pool.clone();
        let provider = provider.clone();
        let cancel = cancel.clone();
        let interval = Duration::from_secs(cfg.indexer_backfill_loop_secs);
        tokio::spawn(async move {
            let cfg = SyncConfig::default();
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                    _ = tick.tick() => {
                        if let Err(e) = run_backfill(&pool, &provider, &cfg, cancel.clone()).await {
                            tracing::warn!(error = %e, "backfill loop iteration failed");
                        }
                    }
                }
            }
        })
    };

    // CoinBlast worker has its own cursor + chunked scan loop. Runs in
    // parallel; both share the cancellation token.
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

    // tail loop hook-up (gRPC StreamEvents) — wired only if GRPC_URL set.
    // Until then the backfill loop carries the load via its own re-tick
    // cadence. SingleFlight gate is constructed up-front so future tail
    // wiring slots in without rewiring backfill.
    let _gate = Arc::new(SingleFlight::new());
    if cfg.grpc_url.is_some() {
        tracing::warn!(
            "GRPC_URL set but tail loop wiring is deferred; backfill loop carries the load"
        );
    }

    shutdown_signal().await;
    tracing::info!("indexer: shutdown signal received; cancelling workers");
    cancel.cancel();

    let _ = backfill_handle.await?;
    let _ = coinblast_handle.await?;
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
