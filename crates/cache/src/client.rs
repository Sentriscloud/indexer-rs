//! Redis client wrapper. Wraps `fred` so callers don't need to know the
//! pool / config story; exposes typed `get` / `set` over JSON-encodable
//! values; gates every call behind the [`CircuitBreaker`].

use crate::breaker::CircuitBreaker;
use crate::{CacheError, CacheResult};
use fred::clients::Pool;
use fred::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::Duration;

/// TTL tier per spec §9.
#[derive(Debug, Copy, Clone)]
pub enum Tier {
    /// Tier 1 — chain-wide aggregates (60s).
    Chain,
    /// Tier 2 — per-address rollups (5min).
    Address,
    /// Tier 3 — immutable detail (1h).
    Detail,
}

impl Tier {
    /// TTL in seconds.
    pub fn ttl_secs(self) -> i64 {
        match self {
            Self::Chain => 60,
            Self::Address => 300,
            Self::Detail => 3600,
        }
    }
}

/// Cache config.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Redis URL (`redis://host:port` / `rediss://...`).
    pub url: String,
    /// Pool size. Default 10.
    pub pool_size: usize,
    /// Key prefix per network — recommended `sentrix:idx:{network}`.
    pub key_namespace: String,
    /// Consecutive failures before the breaker opens. Default 5.
    pub breaker_threshold: u32,
    /// How long the breaker stays open. Default 30s.
    pub breaker_open_for: Duration,
}

impl CacheConfig {
    /// Construct with sensible defaults; URL + namespace required.
    pub fn new(url: impl Into<String>, key_namespace: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            pool_size: 10,
            key_namespace: key_namespace.into(),
            breaker_threshold: 5,
            breaker_open_for: Duration::from_secs(30),
        }
    }
}

/// High-level cache client. Cheap to clone (the pool + breaker are `Arc`-y).
#[derive(Clone)]
pub struct CacheClient {
    pool: Pool,
    namespace: String,
    breaker: Arc<CircuitBreaker>,
}

impl CacheClient {
    /// Connect a pooled Redis client + start its background tasks.
    pub async fn connect(cfg: CacheConfig) -> CacheResult<Self> {
        let mut fcfg = Config::from_url(&cfg.url)?;
        fcfg.fail_fast = true;
        let pool = Builder::from_config(fcfg).build_pool(cfg.pool_size)?;
        pool.init().await?;
        Ok(Self {
            pool,
            namespace: cfg.key_namespace,
            breaker: Arc::new(CircuitBreaker::new(
                cfg.breaker_threshold,
                cfg.breaker_open_for,
            )),
        })
    }

    /// Read a value, decoded as `T`. Returns Ok(None) for cache miss.
    /// Caller falls back to PG on Err (cache failure).
    pub async fn get<T>(&self, key: &str) -> CacheResult<Option<T>>
    where
        T: DeserializeOwned,
    {
        if self.breaker.is_open() {
            return Err(CacheError::Open);
        }
        let full = self.full_key(key);
        let raw: Option<String> = match self.pool.get(&full).await {
            Ok(v) => v,
            Err(e) => {
                self.breaker.record_failure();
                return Err(e.into());
            }
        };
        self.breaker.record_success();
        match raw {
            None => Ok(None),
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
        }
    }

    /// Write a value with the given tier's TTL.
    pub async fn set<T>(&self, key: &str, value: &T, tier: Tier) -> CacheResult<()>
    where
        T: Serialize,
    {
        if self.breaker.is_open() {
            return Err(CacheError::Open);
        }
        let body = serde_json::to_string(value)?;
        let full = self.full_key(key);
        let ttl = tier.ttl_secs();
        let result: Result<(), _> = self
            .pool
            .set(&full, body, Some(Expiration::EX(ttl)), None, false)
            .await;
        match result {
            Ok(()) => {
                self.breaker.record_success();
                Ok(())
            }
            Err(e) => {
                self.breaker.record_failure();
                Err(e.into())
            }
        }
    }

    /// Drop a key. Used when an upstream write invalidates a tier-1 entry.
    pub async fn invalidate(&self, key: &str) -> CacheResult<()> {
        if self.breaker.is_open() {
            return Err(CacheError::Open);
        }
        let full = self.full_key(key);
        let result: Result<(), _> = self.pool.del(&full).await;
        match result {
            Ok(()) => {
                self.breaker.record_success();
                Ok(())
            }
            Err(e) => {
                self.breaker.record_failure();
                Err(e.into())
            }
        }
    }

    fn full_key(&self, k: &str) -> String {
        format!("{}:{}", self.namespace, k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_per_tier() {
        assert_eq!(Tier::Chain.ttl_secs(), 60);
        assert_eq!(Tier::Address.ttl_secs(), 300);
        assert_eq!(Tier::Detail.ttl_secs(), 3600);
    }

    #[test]
    fn config_defaults() {
        let c = CacheConfig::new("redis://localhost:6379", "sentrix:idx:test");
        assert_eq!(c.pool_size, 10);
        assert_eq!(c.breaker_threshold, 5);
        assert_eq!(c.breaker_open_for, Duration::from_secs(30));
    }
}
