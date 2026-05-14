//! indexer-chain
//!
//! Sentrix chain client surface: alloy JSON-RPC provider for EVM-shaped reads,
//! tonic gRPC client for the chain's `sentrix.v1` service (currently
//! `StreamEvents`), and a `reqwest`-backed REST client for the native
//! `/tx/<hash>` endpoint that exposes pre-EVM tx fields the JSON-RPC view
//! omits.
//!
//! Phase 2 ships the wire types + retry helper. Integration tests against a
//! live testnet RPC are gated behind `#[ignore]` so CI doesn't depend on
//! network reachability — run with `cargo test -p indexer-chain -- --ignored`.

#![cfg_attr(not(test), warn(missing_docs))]

pub mod error;
pub mod grpc;
pub mod provider;
pub mod rest;
pub mod retry;

/// Re-export of the canonical `sentrix.v1` protobuf types from the
/// `sentrix-proto` crate (single source of truth, published on crates.io).
/// Existing `crate::pb::Foo` call sites keep working unchanged.
pub use sentrix_proto as pb;

pub use error::{ChainError, ChainResult};
pub use grpc::GrpcClient;
pub use provider::{ChainProvider, HttpProvider};
pub use rest::{
    ChainInfoResponse, NativeBlockResponse, NativeBlockTx, NativeTxResponse, RestClient,
};
pub use retry::{BackoffConfig, retry_with_backoff};
