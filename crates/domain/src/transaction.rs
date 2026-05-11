//! Transaction — one row per chain tx (native or EVM).

use crate::{Address, BlockHeight, Hash, TxIndex, Wei};
use serde::{Deserialize, Serialize};

/// Transaction kind. Mirrors the TS schema's `tx_type` column values.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    /// Native Sentrix tx (transfer, staking op, registry op).
    Native,
    /// EVM tx executed through revm.
    Evm,
    /// System tx synthesised by the runtime (epoch boundary, slash, etc).
    System,
    /// Coinbase reward distribution.
    Coinbase,
}

impl TxType {
    /// String representation as stored in `transactions.tx_type` (varchar(24)).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Evm => "evm",
            Self::System => "system",
            Self::Coinbase => "coinbase",
        }
    }
}

/// Chain transaction. Row in `transactions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    /// Tx hash (PG primary key).
    pub hash: Hash,
    /// Block this tx was included in.
    pub block_height: BlockHeight,
    /// Position within the block.
    pub tx_index: TxIndex,
    /// Sender address.
    pub from_addr: Address,
    /// Receiver address. None for contract creation.
    pub to_addr: Option<Address>,
    /// Native value transferred (in chain's smallest unit).
    pub value: Wei,
    /// Gas limit.
    pub gas_limit: i64,
    /// Gas actually consumed. None until receipt is observed.
    pub gas_used: Option<i64>,
    /// Gas price (legacy) or effective gas price (1559).
    pub gas_price: Option<Wei>,
    /// Total fee paid (gas_used × gas_price).
    pub fee: Wei,
    /// Sender nonce.
    pub nonce: i64,
    /// Calldata (hex string).
    pub data: Option<String>,
    /// 1 = success, 0 = failed.
    pub status: i16,
    /// Address of the contract created by this tx (CREATE / CREATE2).
    pub contract_address: Option<Address>,
    /// Tx kind.
    pub tx_type: TxType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_type_serializes_lowercase() {
        let s = serde_json::to_string(&TxType::Native).unwrap();
        assert_eq!(s, "\"native\"");
        let s = serde_json::to_string(&TxType::Evm).unwrap();
        assert_eq!(s, "\"evm\"");
    }

    #[test]
    fn tx_type_as_str_matches_serde() {
        for ty in [
            TxType::Native,
            TxType::Evm,
            TxType::System,
            TxType::Coinbase,
        ] {
            let serde_form: String =
                serde_json::from_value(serde_json::json!(ty.as_str())).unwrap();
            let serde_back: TxType =
                serde_json::from_str(&serde_json::to_string(&ty).unwrap()).unwrap();
            assert_eq!(serde_back.as_str(), serde_form);
        }
    }
}
