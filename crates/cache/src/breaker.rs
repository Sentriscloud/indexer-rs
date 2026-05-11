//! Circuit breaker — counts consecutive Redis failures, opens for a
//! configurable window when the threshold trips, half-opens after the
//! window to probe recovery.
//!
//! Tracked entirely in-process via atomic counters + a parking_lot Mutex
//! around the open-until timestamp (microsecond critical section).

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// Thread-safe failure counter + open-state gate.
#[derive(Debug)]
pub struct CircuitBreaker {
    failures: AtomicU32,
    threshold: u32,
    open_for: Duration,
    open_until: Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    /// Construct. `threshold` consecutive failures trip; the breaker stays
    /// open for `open_for` before the next call is allowed through.
    pub fn new(threshold: u32, open_for: Duration) -> Self {
        Self {
            failures: AtomicU32::new(0),
            threshold,
            open_for,
            open_until: Mutex::new(None),
        }
    }

    /// True if the breaker is currently open (caller should fall back).
    pub fn is_open(&self) -> bool {
        let mut guard = self.open_until.lock();
        match *guard {
            Some(until) if Instant::now() < until => true,
            Some(_) => {
                // Window elapsed — half-open. Reset the latch so the next
                // call gets a probe attempt; failures count restarts at 0
                // so a single recovery doesn't immediately re-trip.
                *guard = None;
                self.failures.store(0, Ordering::Release);
                false
            }
            None => false,
        }
    }

    /// Record a successful call. Resets the failure counter.
    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Release);
    }

    /// Record a failed call. Trips the breaker if threshold reached.
    pub fn record_failure(&self) {
        let prev = self.failures.fetch_add(1, Ordering::AcqRel);
        if prev + 1 >= self.threshold {
            *self.open_until.lock() = Some(Instant::now() + self.open_for);
            tracing::warn!(
                consecutive_failures = prev + 1,
                open_for_ms = self.open_for.as_millis() as u64,
                "cache circuit breaker tripped",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_until_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        assert!(!cb.is_open());
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.is_open()); // 2 < threshold 3
        cb.record_failure();
        assert!(cb.is_open()); // 3 >= threshold
    }

    #[test]
    fn success_resets_count() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(30));
        cb.record_failure();
        cb.record_success();
        cb.record_failure(); // counter is back to 1, not 2 → still closed
        assert!(!cb.is_open());
    }

    #[test]
    fn open_window_elapses() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(0));
        cb.record_failure();
        // First check sees the just-opened breaker. Even with a 0ms window,
        // the elapsed-check transitions it to half-open and clears the latch.
        let _ = cb.is_open();
        // Second check must observe the closed state.
        std::thread::sleep(Duration::from_millis(1));
        assert!(!cb.is_open());
    }
}
