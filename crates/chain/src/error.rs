//! Errors surfaced by the chain client.
//!
//! Three transports = three concrete underlying error types. The unified
//! `ChainError` lets the sync layer treat them uniformly while preserving
//! the source for diagnostics. `is_transient` decides whether the retry
//! helper should back off + try again or surface the failure.

use std::fmt;

/// Result alias.
pub type ChainResult<T> = std::result::Result<T, ChainError>;

/// Chain client error.
#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    /// alloy JSON-RPC transport error.
    #[error("rpc: {0}")]
    Rpc(String),

    /// gRPC transport / status error.
    #[error("grpc: {0}")]
    Grpc(#[from] tonic::Status),

    /// gRPC connect-time error (channel build).
    #[error("grpc transport: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    /// Native REST HTTP error.
    #[error("rest: {0}")]
    Rest(#[from] reqwest::Error),

    /// JSON deserialisation failed.
    #[error("decode: {0}")]
    Decode(#[from] serde_json::Error),

    /// Block / tx not found at the requested coordinate.
    #[error("not found: {0}")]
    NotFound(String),

    /// Caller supplied an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl ChainError {
    /// Hint for the retry helper. True = transient (network blip, 5xx,
    /// gRPC `Unavailable`); false = caller bug or permanent miss.
    pub fn is_transient(&self) -> bool {
        match self {
            ChainError::Rpc(_) => true,
            ChainError::Grpc(s) => matches!(
                s.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::ResourceExhausted
                    | tonic::Code::Aborted
                    | tonic::Code::Internal
            ),
            ChainError::GrpcTransport(_) => true,
            ChainError::Rest(e) => {
                e.is_timeout() || e.is_connect() || e.status().is_some_and(|s| s.is_server_error())
            }
            ChainError::Decode(_) => false,
            ChainError::NotFound(_) => false,
            ChainError::InvalidArgument(_) => false,
        }
    }
}

/// Wrap a stringly-typed alloy provider error into [`ChainError::Rpc`].
pub(crate) fn rpc_err<E: fmt::Display>(e: E) -> ChainError {
    ChainError::Rpc(e.to_string())
}
