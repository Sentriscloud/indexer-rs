//! `Wei` — newtype wrapper around `alloy_primitives::U256` for u256-scale
//! amounts (token values, balances, fees, base fee).
//!
//! Wire format: decimal string. Surface JSON ALWAYS string-typed (matches
//! the existing TS shape; survives JSON's f64 precision limit). PG storage:
//! `numeric(78, 0)` — encoded via `BigDecimal` since sqlx 0.8 has native
//! support, decoded back through the same path.

use alloy_primitives::U256;
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use sqlx::types::BigDecimal;
use std::fmt;
use std::str::FromStr;

/// Unsigned 256-bit integer mapped to PG `numeric(78, 0)` and JSON string.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Wei(pub U256);

impl Wei {
    /// Zero.
    pub const ZERO: Self = Self(U256::ZERO);

    /// Underlying U256.
    pub fn into_inner(self) -> U256 {
        self.0
    }
}

impl From<U256> for Wei {
    fn from(u: U256) -> Self {
        Self(u)
    }
}

impl From<u64> for Wei {
    fn from(v: u64) -> Self {
        Self(U256::from(v))
    }
}

impl FromStr for Wei {
    type Err = WeiParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        U256::from_str_radix(s, 10)
            .map(Self)
            .map_err(|e| WeiParseError(e.to_string()))
    }
}

impl fmt::Display for Wei {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Failed to parse a decimal string as `Wei` / `U256`.
#[derive(Debug, thiserror::Error)]
#[error("Wei parse failed: {0}")]
pub struct WeiParseError(String);

// JSON: always decimal string.

impl Serialize for Wei {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Wei {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// sqlx: numeric(78, 0) round-tripped through BigDecimal.

impl sqlx::Type<Postgres> for Wei {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <BigDecimal as sqlx::Type<Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <BigDecimal as sqlx::Type<Postgres>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for Wei {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let bd = BigDecimal::from_str(&self.0.to_string())?;
        <BigDecimal as sqlx::Encode<'q, Postgres>>::encode_by_ref(&bd, buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for Wei {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let bd = <BigDecimal as sqlx::Decode<'r, Postgres>>::decode(value)?;
        // BigDecimal `to_string()` for an integer can yield "123" or "1.23E+5";
        // normalize via the integer view first when scale is non-positive.
        let (digits, scale) = bd.as_bigint_and_exponent();
        if scale != 0 {
            return Err(format!("Wei expects integer numeric, got scale={scale}").into());
        }
        let s = digits.to_string();
        if let Some(stripped) = s.strip_prefix('-') {
            return Err(format!("Wei is unsigned; got negative {stripped}").into());
        }
        let u = U256::from_str_radix(&s, 10)?;
        Ok(Wei(u))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_small() {
        let w = Wei::from(42u64);
        let s = serde_json::to_string(&w).unwrap();
        assert_eq!(s, "\"42\"");
        let back: Wei = serde_json::from_str(&s).unwrap();
        assert_eq!(back, w);
    }

    #[test]
    fn json_roundtrip_large() {
        // 18-decimal token with billion-unit balance: 1e9 * 1e18.
        let w = Wei::from_str("1000000000000000000000000000").unwrap();
        let s = serde_json::to_string(&w).unwrap();
        assert_eq!(s, "\"1000000000000000000000000000\"");
        let back: Wei = serde_json::from_str(&s).unwrap();
        assert_eq!(back, w);
    }

    #[test]
    fn json_roundtrip_max_u256() {
        let max = Wei(U256::MAX);
        let s = serde_json::to_string(&max).unwrap();
        let back: Wei = serde_json::from_str(&s).unwrap();
        assert_eq!(back, max);
    }

    #[test]
    fn parse_decimal_string() {
        assert_eq!(Wei::from_str("0").unwrap(), Wei::ZERO);
        assert!(Wei::from_str("not a number").is_err());
        // Negative is rejected.
        assert!(Wei::from_str("-1").is_err());
    }

    #[test]
    fn display_matches_decimal() {
        assert_eq!(Wei::from(12345u64).to_string(), "12345");
    }
}
