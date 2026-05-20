//! ERC-20 / ERC-721 Transfer event decoder.
//!
//! Sentrix indexer-handlers crate is still a placeholder (Phase 0), so the
//! `token_transfers` table sat empty even though raw logs were captured.
//! Until the declarative-handler framework lands, decode the well-known
//! Transfer signatures inline here so scan UIs can resolve token balances.
//!
//! ERC-1155 has a different topic0 (`TransferSingle` / `TransferBatch`) and
//! a richer encoding; out of scope for this pass.

use alloy_primitives::U256;
use indexer_domain::{Log, TokenStandard, TokenTransfer, Wei};

/// `keccak256("Transfer(address,address,uint256)")` — same selector for
/// ERC-20 amount transfers and ERC-721 token-id transfers. The two are
/// distinguished by topic count + data shape.
const TRANSFER_TOPIC0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Try to decode a log as an ERC-20 or ERC-721 Transfer event. Returns
/// `None` for anything else — caller drops it.
///
/// Decoding rules:
/// - topic0 must equal the canonical Transfer selector.
/// - topic1, topic2 are 32-byte-padded from/to addresses (indexed).
/// - ERC-20: topic3 absent, data = 32-byte amount.
/// - ERC-721: topic3 present (indexed token_id), data empty, amount = 1.
pub fn decode_transfer(log: &Log) -> Option<TokenTransfer> {
    if log.topic0.as_deref() != Some(TRANSFER_TOPIC0) {
        return None;
    }
    let from_topic = log.topic1.as_deref()?;
    let to_topic = log.topic2.as_deref()?;
    let from_addr = topic_to_address(from_topic)?;
    let to_addr = topic_to_address(to_topic)?;

    let (standard, token_id, amount) = match log.topic3.as_deref() {
        Some(id_topic) => {
            // ERC-721: token_id in topic3, data empty (or padding only).
            let token_id = topic_to_u256(id_topic)?;
            (TokenStandard::Erc721, Some(Wei(token_id)), Wei(U256::from(1u64)))
        }
        None => {
            // ERC-20: amount in data (must be exactly 32 bytes).
            let data_str = log.data.as_deref()?;
            let amount = data_to_u256(data_str)?;
            (TokenStandard::Erc20, None, Wei(amount))
        }
    };

    Some(TokenTransfer {
        id: None,
        block_height: log.block_height,
        tx_hash: log.tx_hash.clone(),
        log_index: log.log_index,
        contract: log.address.clone(),
        standard,
        from_addr,
        to_addr,
        token_id,
        amount,
    })
}

/// Last 20 bytes of a 32-byte topic → `0x`-prefixed lowercase address.
fn topic_to_address(topic: &str) -> Option<String> {
    let hex = topic.trim_start_matches("0x");
    if hex.len() != 64 {
        return None;
    }
    Some(format!("0x{}", &hex[24..]))
}

/// 32-byte topic → U256.
fn topic_to_u256(topic: &str) -> Option<U256> {
    let hex = topic.trim_start_matches("0x");
    if hex.len() != 64 {
        return None;
    }
    let bytes = hex::decode(hex).ok()?;
    Some(U256::from_be_slice(&bytes))
}

/// 32-byte data field → U256. ERC-20 Transfer always has data length 32.
fn data_to_u256(data: &str) -> Option<U256> {
    let hex = data.trim_start_matches("0x");
    if hex.len() != 64 {
        return None;
    }
    let bytes = hex::decode(hex).ok()?;
    Some(U256::from_be_slice(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_domain::{BlockHeight, LogIndex};

    fn base_log() -> Log {
        Log {
            block_height: BlockHeight(1),
            tx_hash: "0xabc".into(),
            log_index: LogIndex(0),
            address: "0xcontract".into(),
            topic0: Some(TRANSFER_TOPIC0.into()),
            topic1: Some(
                "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            ),
            topic2: Some(
                "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            ),
            topic3: None,
            data: Some(
                "0x0000000000000000000000000000000000000000000000000000000000000064".into(),
            ),
        }
    }

    #[test]
    fn decodes_erc20_transfer() {
        let t = decode_transfer(&base_log()).expect("erc20 should decode");
        assert_eq!(t.standard, TokenStandard::Erc20);
        assert_eq!(t.from_addr, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(t.to_addr, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_eq!(t.amount.0, U256::from(100u64));
        assert!(t.token_id.is_none());
    }

    #[test]
    fn decodes_erc721_transfer() {
        let mut log = base_log();
        log.topic3 = Some(
            "0x0000000000000000000000000000000000000000000000000000000000000007".into(),
        );
        log.data = Some("0x".into());
        let t = decode_transfer(&log).expect("erc721 should decode");
        assert_eq!(t.standard, TokenStandard::Erc721);
        assert_eq!(t.token_id.unwrap().0, U256::from(7u64));
        assert_eq!(t.amount.0, U256::from(1u64));
    }

    #[test]
    fn skips_non_transfer() {
        let mut log = base_log();
        log.topic0 = Some("0xdeadbeef".into());
        assert!(decode_transfer(&log).is_none());
    }

    #[test]
    fn skips_malformed_topic() {
        let mut log = base_log();
        log.topic1 = Some("0xshort".into());
        assert!(decode_transfer(&log).is_none());
    }
}
