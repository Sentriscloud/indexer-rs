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
use indexer_chain::{ChainProvider, GrpcClient, RestClient};
use indexer_coinblast::{Network, WorkerConfig as CbConfig, run_coinblast_worker};
use indexer_db::{PoolConfig, connect, migrate};
use indexer_sync::{SingleFlight, SyncConfig, TailExit, run_backfill, run_tail};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct IndexerConfig {
    database_url: String,
    rpc_url: String,
    /// Optional separate base URL for the native REST endpoints
    /// (`/chain/blocks/<n>`, `/tx/<hash>`). Defaults to `rpc_url` when unset.
    /// Lets us point JSON-RPC at `<host>/rpc` and REST at `<host>` (root)
    /// when bypassing the Caddy edge that handles the path rewrite.
    #[serde(default)]
    rest_url: Option<String>,
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
    #[serde(default = "default_stats_refresh_secs")]
    indexer_stats_refresh_secs: u64,
    #[serde(default = "default_contract_detect_interval_secs")]
    indexer_contract_detect_interval_secs: u64,
    #[serde(default = "default_contract_detect_batch")]
    indexer_contract_detect_batch: i64,
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
fn default_stats_refresh_secs() -> u64 {
    300
}
fn default_contract_detect_interval_secs() -> u64 {
    4
}
fn default_contract_detect_batch() -> i64 {
    10
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

    // One-time address-history backfill: seed `addresses` from every from/to
    // address already in `transactions` so the contract detector can classify
    // historical addresses too. No-op once `addresses` is populated. Runs in the
    // background — the GROUP BY over all txs is heavy on a large chain.
    {
        let pool = pool.clone();
        tokio::spawn(async move {
            match indexer_db::addresses::count(&pool).await {
                Ok(0) => match indexer_db::addresses::backfill_from_transactions(&pool).await {
                    Ok(n) => tracing::info!(inserted = n, "addresses: history backfill complete"),
                    Err(e) => tracing::warn!(error = %e, "addresses history backfill failed"),
                },
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "addresses backfill: count failed"),
            }
        });
    }

    let provider = ChainProvider::http(&cfg.rpc_url)?;
    // Native REST client for `/chain/blocks/<n>` + `/tx/<hash>` — Sentrix's
    // EVM JSON-RPC ignores `full=true` on getBlockByNumber, so block + tx
    // ingest goes via the native REST path. REST_URL falls back to RPC_URL;
    // override when JSON-RPC needs `/rpc` suffix (direct fullnode bypass).
    let rest_base = cfg.rest_url.as_deref().unwrap_or(&cfg.rpc_url);
    let rest = RestClient::new(rest_base)?;
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
        let rest = rest.clone();
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
                        if let Err(e) = run_backfill(&pool, &provider, &rest, &cfg, cancel.clone(), analytics.as_ref()).await {
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
            let rest = rest.clone();
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
                        &rest,
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

    // Stats MV refresh loop. `stats_daily_mv` (migration 0002) has no
    // auto-refresh; without this it stays empty and `/stats/daily` returns
    // nothing. The first tick fires immediately and does a plain (blocking)
    // refresh — Postgres rejects `REFRESH ... CONCURRENTLY` on a
    // never-populated MV — then every subsequent tick uses CONCURRENTLY so
    // reads are never locked out.
    let stats_refresh_handle = {
        let pool = pool.clone();
        let cancel = cancel.clone();
        let interval = Duration::from_secs(cfg.indexer_stats_refresh_secs);
        tokio::spawn(async move {
            let mut populated = false;
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                    _ = tick.tick() => {
                        let res = if populated {
                            indexer_db::stats::refresh(&pool).await
                        } else {
                            indexer_db::stats::refresh_full(&pool).await
                        };
                        match res {
                            Ok(()) => populated = true,
                            Err(e) => {
                                tracing::warn!(error = %e, "stats_daily_mv refresh failed");
                            }
                        }
                    }
                }
            }
        })
    };

    // Contract detector: lazily classify `addresses` (is_contract + code_hash)
    // via eth_getCode, rate-limited, so /contracts/* fills over time.
    let detector_handle = {
        let pool = pool.clone();
        let provider = provider.clone();
        let cancel = cancel.clone();
        let interval = Duration::from_secs(cfg.indexer_contract_detect_interval_secs);
        let batch = cfg.indexer_contract_detect_batch;
        tokio::spawn(async move {
            indexer_sync::run_contract_detector(&pool, &provider, interval, batch, cancel)
                .await
                .map_err(anyhow::Error::from)
        })
    };

    shutdown_signal().await;
    tracing::info!("indexer: shutdown signal received; cancelling workers");
    cancel.cancel();

    let _ = stats_refresh_handle.await?;
    let _ = detector_handle.await?;
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
