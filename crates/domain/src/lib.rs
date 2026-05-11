//! indexer-domain
//!
//! Type-safe domain model for the Sentrix indexer. Wraps chain primitives
//! (heights, indexes, U256 wei amounts) in newtypes so the type system catches
//! a class of mix-up bugs the TypeScript port couldn't.

#![cfg_attr(not(test), warn(missing_docs))]

mod block;
mod ids;
mod log;
mod token_transfer;
mod transaction;
mod wei;

pub use block::Block;
pub use ids::{BlockHeight, EpochNumber, LogIndex, TxIndex};
pub use log::Log;
pub use token_transfer::{TokenStandard, TokenTransfer};
pub use transaction::{Transaction, TxType};
pub use wei::Wei;

/// Hex-encoded address (lowercase, 0x-prefixed, 42 chars). Stored as
/// `varchar(42)` in PG to mirror the TS schema.
pub type Address = String;

/// Hex-encoded 32-byte hash (lowercase, 0x-prefixed, 66 chars). Stored as
/// `varchar(66)` in PG to mirror the TS schema.
pub type Hash = String;
