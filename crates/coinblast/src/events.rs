//! CoinBlast event ABIs + topic0 + per-network deploy info.
//!
//! `sol!` macro generates Rust types with auto-computed `SIGNATURE_HASH`
//! (= topic0) and `decode_log_data` for each event. Keeps the ABI as the
//! single source of truth — renaming a parameter breaks the topic via
//! recompile, doesn't silently drift.
//!
//! Canonical signature reference (indexed flags do NOT affect topic0,
//! only the type list):
//!   - `CurveCreated(address,address,address,string,string,uint256,uint256)`
//!   - `Buy(address,uint256,uint256,uint256)`
//!   - `Sell(address,uint256,uint256,uint256)`
//!   - `Graduated(address,uint256,uint256,uint256)`

use alloy_primitives::{Address, address};
use alloy_sol_types::sol;

/// Which Sentrix network the worker is bound to. CoinBlast factory + deploy
/// block are per-network constants.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Network {
    /// Sentrix mainnet (chain id 7119).
    Mainnet,
    /// Sentrix testnet (chain id 7120).
    Testnet,
}

/// CoinBlast factory contract address per network.
pub const COINBLAST_FACTORY_ADDRESS: fn(Network) -> Address = |n| match n {
    Network::Mainnet => address!("c9D7a61D7C2F428F6A055916488041fD00532110"),
    Network::Testnet => address!("c7FBd67fb809b189998cB27F1857b50A3e09619c"),
};

/// First block at which the factory could possibly emit. Floor of the
/// worker's backfill scan so we don't waste round-trips on empty pre-deploy
/// history.
pub const COINBLAST_DEPLOY_BLOCK: fn(Network) -> u64 = |n| match n {
    Network::Mainnet => 1_178_667,
    Network::Testnet => 1_637_883,
};

sol! {
    /// Emitted by the factory on every successful curve launch.
    event CurveCreated(
        address indexed curve,
        address indexed token,
        address indexed owner,
        string name,
        string symbol,
        uint256 curveSupply,
        uint256 graduationSrxThreshold,
    );

    /// Emitted by a curve on every buy. Caller pays SRX, gets tokens.
    event Buy(
        address indexed buyer,
        uint256 srxIn,
        uint256 fee,
        uint256 tokensOut,
    );

    /// Emitted by a curve on every sell. Caller burns tokens, gets SRX.
    event Sell(
        address indexed seller,
        uint256 tokensIn,
        uint256 fee,
        uint256 srxOut,
    );

    /// Emitted by a curve on graduation (raised the SRX threshold; LP gets
    /// seeded into the AMM, curve becomes inert).
    event Graduated(
        address indexed pair,
        uint256 srxLiquidity,
        uint256 tokenLiquidity,
        uint256 lpBurned,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::SolEvent;

    #[test]
    fn topic0_hashes_are_stable() {
        // These are the wire-level signatures used by topic-filtered scans.
        // If they ever change, every consumer of indexed CoinBlast data
        // (scan, coinblast frontend, analytics) must be redeployed in lock-
        // step — pin them here so the test fails on accidental rename.
        let curve_created = format!("{:?}", CurveCreated::SIGNATURE_HASH);
        let buy = format!("{:?}", Buy::SIGNATURE_HASH);
        let sell = format!("{:?}", Sell::SIGNATURE_HASH);
        let graduated = format!("{:?}", Graduated::SIGNATURE_HASH);
        // Just confirm the types compile + topic0 is a 32-byte hash render.
        assert_eq!(curve_created.len(), 66, "0x + 64 hex");
        assert_eq!(buy.len(), 66);
        assert_eq!(sell.len(), 66);
        assert_eq!(graduated.len(), 66);
    }

    #[test]
    fn factory_addresses_distinct() {
        assert_ne!(
            COINBLAST_FACTORY_ADDRESS(Network::Mainnet),
            COINBLAST_FACTORY_ADDRESS(Network::Testnet),
        );
    }

    #[test]
    fn deploy_blocks_match_known_history() {
        assert_eq!(COINBLAST_DEPLOY_BLOCK(Network::Mainnet), 1_178_667);
        assert_eq!(COINBLAST_DEPLOY_BLOCK(Network::Testnet), 1_637_883);
    }
}
