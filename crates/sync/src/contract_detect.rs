//! Lazy contract detector — classifies `addresses` rows (`is_contract` +
//! `code_hash`) by running `eth_getCode`, rate-limited so a cold start doesn't
//! flood the RPC. Mirrors the legacy TS `contract-detect.ts` worker.
//! `/contracts/*` then serves `WHERE is_contract = true`.

use crate::SyncResult;
use alloy_primitives::{Address, keccak256};
use indexer_chain::ChainProvider;
use indexer_db::{PgPool, addresses};
use std::str::FromStr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// What `eth_getCode` returns for an account with no code (an EOA).
const NO_CODE: &str = "0x";

/// Run the detector until cancelled. Each tick classifies up to `batch`
/// not-yet-classified addresses, then waits `interval` before the next sweep.
/// A `getCode` failure leaves the row unclassified (retried next sweep); an
/// unparseable address is marked EOA so it never blocks the queue.
pub async fn run_contract_detector(
    pool: &PgPool,
    provider: &ChainProvider,
    interval: Duration,
    batch: i64,
    cancel: CancellationToken,
) -> SyncResult<()> {
    let mut tick = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {
                let candidates = addresses::unclassified_batch(pool, batch).await?;
                for addr in candidates {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    if let Err(e) = classify_one(pool, provider, &addr).await {
                        tracing::warn!(
                            address = %addr, error = %e,
                            "contract detector: classify failed; will retry next sweep"
                        );
                    }
                }
            }
        }
    }
}

/// Probe one address with `eth_getCode` and record the result.
async fn classify_one(pool: &PgPool, provider: &ChainProvider, addr: &str) -> SyncResult<()> {
    let Ok(parsed) = Address::from_str(addr) else {
        // Unparseable (shouldn't happen — addresses come from indexed txs).
        // Mark EOA so it leaves the candidate set permanently.
        addresses::classify(pool, addr, false, NO_CODE).await?;
        return Ok(());
    };
    let code = provider.get_code(parsed).await?;
    let is_contract = !code.is_empty();
    let code_hash = if is_contract {
        format!("0x{:x}", keccak256(&code))
    } else {
        NO_CODE.to_string()
    };
    addresses::classify(pool, addr, is_contract, &code_hash).await?;
    Ok(())
}
