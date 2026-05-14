//! alloy JSON-RPC provider wrapper for EVM-shaped reads.
//!
//! Surface kept narrow to the methods the sync layer (Phase 3) actually
//! calls: `eth_blockNumber`, `eth_getBlockByNumber` (with full txs),
//! `eth_getLogs`, `eth_chainId`. Receipt fetching arrives when the
//! gas-used backfill lands; out of scope for Phase 2.
//!
//! Underlying transport: `alloy_provider::ProviderBuilder` with the default
//! HTTP transport. Caller can hand a `RootProvider<Http<reqwest::Client>>`
//! to share a connection pool with the REST client if desired.

use crate::error::{ChainError, ChainResult, rpc_err};
use alloy_primitives::{Address, Bytes};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Block, BlockNumberOrTag, Filter, Log, TransactionInput, TransactionRequest};
use indexer_domain::BlockHeight;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Concrete provider type — alloy 2.0's default HTTP transport (reqwest).
/// Hidden from callers behind [`ChainProvider`]; exposed via
/// [`ChainProvider::raw`] for advanced use.
pub type HttpProvider = RootProvider;

/// Thin wrapper around one or more alloy `RootProvider`s. Single-URL is
/// the common case; multi-URL spreads JSON-RPC load round-robin across
/// fullnodes for higher backfill throughput.
#[derive(Clone)]
pub struct ChainProvider {
    inners: Vec<HttpProvider>,
    cursor: Arc<AtomicUsize>,
}

impl ChainProvider {
    /// Build a provider from a comma-separated URL list (or a single URL).
    /// Returns Err if no valid URL parses.
    pub fn http(url: &str) -> ChainResult<Self> {
        let mut inners = Vec::new();
        for raw in url.split(',') {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                continue;
            }
            let parsed = trimmed.parse::<reqwest::Url>().map_err(|e| {
                ChainError::InvalidArgument(format!("bad rpc url '{trimmed}': {e}"))
            })?;
            let inner = ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(parsed);
            inners.push(inner);
        }
        if inners.is_empty() {
            return Err(ChainError::InvalidArgument("rpc url is empty".to_string()));
        }
        Ok(Self {
            inners,
            cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Round-robin pick. Atomic FetchAdd is lock-free across concurrent
    /// fetch tasks in the backfill pipeline.
    fn next(&self) -> &HttpProvider {
        let i = self.cursor.fetch_add(1, Ordering::Relaxed);
        &self.inners[i % self.inners.len()]
    }

    /// First underlying provider, exposed for advanced use (custom RPC calls).
    /// Multi-URL callers should prefer the wrapped methods which round-robin.
    pub fn raw(&self) -> &HttpProvider {
        &self.inners[0]
    }

    /// `eth_chainId`.
    pub async fn chain_id(&self) -> ChainResult<u64> {
        self.next().get_chain_id().await.map_err(rpc_err)
    }

    /// Latest finalized block height per the node we're talking to.
    pub async fn block_number(&self) -> ChainResult<BlockHeight> {
        let n = self.next().get_block_number().await.map_err(rpc_err)?;
        Ok(BlockHeight::from(n))
    }

    /// `eth_getBlockByNumber(h, full=true)`. Returns None if the node hasn't
    /// seen this height yet.
    pub async fn block_with_txs(&self, h: BlockHeight) -> ChainResult<Option<Block>> {
        let n = h.as_u64().ok_or_else(|| {
            ChainError::InvalidArgument(format!("block_with_txs: negative height {h:?}"))
        })?;
        let tag = BlockNumberOrTag::Number(n);
        self.next()
            .get_block_by_number(tag)
            .full()
            .await
            .map_err(rpc_err)
    }

    /// `eth_getLogs` over an inclusive height range, optionally narrowed by
    /// emitter address. Returns the raw alloy `Log` shape; the handlers crate
    /// (Phase 3+) decodes individual event topics.
    pub async fn logs_in_range(
        &self,
        from: BlockHeight,
        to: BlockHeight,
        address: Option<Address>,
    ) -> ChainResult<Vec<Log>> {
        if to < from {
            return Err(ChainError::InvalidArgument(format!(
                "logs_in_range: to ({to:?}) < from ({from:?})"
            )));
        }
        let from_n = from.as_u64().ok_or_else(|| {
            ChainError::InvalidArgument(format!("logs_in_range: negative from {from:?}"))
        })?;
        let to_n = to.as_u64().ok_or_else(|| {
            ChainError::InvalidArgument(format!("logs_in_range: negative to {to:?}"))
        })?;
        let mut filter = Filter::new().from_block(from_n).to_block(to_n);
        if let Some(addr) = address {
            filter = filter.address(addr);
        }
        self.next().get_logs(&filter).await.map_err(rpc_err)
    }

    /// `eth_call` against `to` with abi-encoded `data`. Returns the raw
    /// return bytes; caller decodes via `alloy_sol_types`. Used by the
    /// CoinBlast worker to validate orphan curves (probe `token()` etc).
    /// Latest block tag.
    pub async fn call(&self, to: Address, data: Bytes) -> ChainResult<Bytes> {
        let req = TransactionRequest::default()
            .to(to)
            .input(TransactionInput::new(data));
        self.next().call(req).await.map_err(rpc_err)
    }
}
