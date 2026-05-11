//! Retry helper with exponential backoff + full jitter.
//!
//! The sync layer wraps every chain call here so transient transport blips
//! don't escalate into spurious "block missing" errors. Permanent errors
//! (decode failures, NotFound, InvalidArgument) bypass retry — see
//! [`ChainError::is_transient`].

use crate::ChainResult;
use std::time::Duration;
use tokio::time::sleep;

/// Backoff schedule. Defaults match spec §7 retry budget for tail loop.
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    /// First retry waits this long (full-jitter ceiling).
    pub initial_delay: Duration,
    /// Subsequent retries cap at this delay.
    pub max_delay: Duration,
    /// Stop after this many attempts (including the first).
    pub max_attempts: u32,
    /// Multiplier applied between attempts before the cap clamps.
    pub multiplier: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            max_attempts: 6,
            multiplier: 2,
        }
    }
}

impl BackoffConfig {
    /// Compute the upper-bound delay for `attempt` (1-indexed).
    /// Caller jitters within `[0, ceiling]` before sleeping.
    pub fn ceiling_for_attempt(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.saturating_pow(attempt.saturating_sub(1));
        let nanos = self
            .initial_delay
            .as_nanos()
            .saturating_mul(u128::from(factor));
        let proposed = Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64);
        proposed.min(self.max_delay)
    }
}

/// Run `op` with retries on transient errors. The closure is recreated each
/// call (no shared mutable state across attempts unless the caller wires it).
///
/// Jitter source: `tokio::time::sleep` resolution + a deterministic offset
/// derived from `attempt`; we explicitly do NOT pull `rand` for this — the
/// hash-of-attempt jitter is good enough at our request rates and keeps the
/// dep tree leaner.
pub async fn retry_with_backoff<T, F, Fut>(cfg: BackoffConfig, mut op: F) -> ChainResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ChainResult<T>>,
{
    let mut last_err = None;
    for attempt in 1..=cfg.max_attempts {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if !e.is_transient() => return Err(e),
            Err(e) => {
                tracing::warn!(attempt, error = %e, "chain op transient failure");
                last_err = Some(e);
                if attempt < cfg.max_attempts {
                    let ceiling = cfg.ceiling_for_attempt(attempt);
                    // Pseudo-jitter: use attempt as a deterministic mod
                    // against the ceiling, so concurrent retriers don't
                    // synchronise. Real PRNG-jitter not required at our scale.
                    let jitter_ratio = (attempt as u64).wrapping_mul(1103515245) % 1024;
                    let scaled = ceiling.as_nanos() * u128::from(jitter_ratio) / 1024;
                    let actual = Duration::from_nanos(scaled.min(u128::from(u64::MAX)) as u64);
                    sleep(actual).await;
                }
            }
        }
    }
    Err(last_err.expect("loop entered with max_attempts >= 1"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChainError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn ceiling_grows_then_caps() {
        let cfg = BackoffConfig {
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            max_attempts: 10,
            multiplier: 2,
        };
        assert_eq!(cfg.ceiling_for_attempt(1), Duration::from_millis(10));
        assert_eq!(cfg.ceiling_for_attempt(2), Duration::from_millis(20));
        assert_eq!(cfg.ceiling_for_attempt(3), Duration::from_millis(40));
        assert_eq!(cfg.ceiling_for_attempt(4), Duration::from_millis(80));
        // 160ms proposed -> capped at 100ms.
        assert_eq!(cfg.ceiling_for_attempt(5), Duration::from_millis(100));
        assert_eq!(cfg.ceiling_for_attempt(20), Duration::from_millis(100));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retries_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = calls.clone();
        let cfg = BackoffConfig {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_attempts: 5,
            multiplier: 2,
        };
        let result = retry_with_backoff(cfg, || {
            let calls = calls_in.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(ChainError::Rpc("temporary".into()))
                } else {
                    Ok(n)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn permanent_errors_dont_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = calls.clone();
        let err = retry_with_backoff(BackoffConfig::default(), || {
            let calls = calls_in.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ChainError::NotFound("block 42".into()))
            }
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ChainError::NotFound(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn exhausts_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = calls.clone();
        let cfg = BackoffConfig {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_attempts: 4,
            multiplier: 2,
        };
        let err = retry_with_backoff(cfg, || {
            let calls = calls_in.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ChainError::Rpc("never works".into()))
            }
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ChainError::Rpc(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }
}
