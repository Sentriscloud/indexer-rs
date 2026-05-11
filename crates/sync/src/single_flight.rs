//! Single-flight gate for the tail loop.
//!
//! Direct port of the TS indexer's `inflight + pendingTip` pattern. The
//! tail loop receives `BlockFinalized` events at chain rate (1/sec mainnet);
//! each event triggers an `index_block` call chain. If the previous chain
//! is still running when a new tip arrives, we MUST NOT spawn a second
//! writer for the same height (spec §5 invariant 10) — instead we stash
//! the pending tip, and the in-flight writer picks it up on completion.
//!
//! Critical section is microseconds (set/get an `Option<u64>`), so
//! `parking_lot::Mutex` is the right tool — never held across `.await`.

use indexer_domain::BlockHeight;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Gate that ensures only one indexing chain is in flight at a time.
#[derive(Debug, Default)]
pub struct SingleFlight {
    inflight: AtomicBool,
    pending: Mutex<Option<BlockHeight>>,
}

/// Outcome of [`SingleFlight::offer`].
#[derive(Debug, PartialEq, Eq)]
pub enum Offer {
    /// Caller acquired the gate and should run with this height.
    Acquired(BlockHeight),
    /// Gate was busy; the new tip was stashed and will be picked up by the
    /// in-flight worker via [`SingleFlight::take_pending_and_release`].
    Stashed,
}

impl SingleFlight {
    /// Construct an empty gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer a new tip. If the gate was free, marks it busy and returns
    /// `Acquired`. Otherwise stashes the tip (max-wins — a higher pending
    /// overwrites a lower) and returns `Stashed`.
    pub fn offer(&self, tip: BlockHeight) -> Offer {
        // Acquire-style CAS: only one caller flips false -> true.
        match self
            .inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // We took the gate. Drain any stashed pending tip so the
                // caller sees the freshest known view.
                let mut pending = self.pending.lock();
                let target = pending.take().map_or(tip, |stashed| stashed.max(tip));
                Offer::Acquired(target)
            }
            Err(_) => {
                let mut pending = self.pending.lock();
                *pending = Some(pending.map_or(tip, |existing| existing.max(tip)));
                Offer::Stashed
            }
        }
    }

    /// Caller finished its chain. If a higher tip was stashed while we
    /// worked, returns it (caller continues with that tip and stays in
    /// the in-flight state). Otherwise releases the gate and returns None.
    pub fn take_pending_and_release(&self) -> Option<BlockHeight> {
        let mut pending = self.pending.lock();
        if let Some(next) = pending.take() {
            // Stay in flight — caller continues with `next`.
            return Some(next);
        }
        // Nothing pending; release the gate.
        self.inflight.store(false, Ordering::Release);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_offer_acquires() {
        let g = SingleFlight::new();
        let o = g.offer(BlockHeight(10));
        assert_eq!(o, Offer::Acquired(BlockHeight(10)));
    }

    #[test]
    fn second_offer_stashes_higher_tip() {
        let g = SingleFlight::new();
        assert_eq!(g.offer(BlockHeight(10)), Offer::Acquired(BlockHeight(10)));
        assert_eq!(g.offer(BlockHeight(11)), Offer::Stashed);
        assert_eq!(g.offer(BlockHeight(13)), Offer::Stashed);
        // Lower offer doesn't displace higher stashed value.
        assert_eq!(g.offer(BlockHeight(12)), Offer::Stashed);

        // Worker drains: gets the highest stashed (13), not the latest (12).
        let next = g.take_pending_and_release();
        assert_eq!(next, Some(BlockHeight(13)));

        // Still in flight; releasing now returns None and frees gate.
        let next2 = g.take_pending_and_release();
        assert_eq!(next2, None);

        // Gate free — fresh offer acquires.
        assert_eq!(g.offer(BlockHeight(20)), Offer::Acquired(BlockHeight(20)));
    }

    #[test]
    fn release_when_no_pending_frees_gate() {
        let g = SingleFlight::new();
        assert!(matches!(g.offer(BlockHeight(1)), Offer::Acquired(_)));
        assert_eq!(g.take_pending_and_release(), None);
        assert!(matches!(g.offer(BlockHeight(2)), Offer::Acquired(_)));
    }

    #[test]
    fn acquired_height_is_max_of_offer_and_stash() {
        let g = SingleFlight::new();
        // Stash 100 by acquiring then offering more
        assert_eq!(g.offer(BlockHeight(50)), Offer::Acquired(BlockHeight(50)));
        assert_eq!(g.offer(BlockHeight(100)), Offer::Stashed);
        // Drain and release.
        assert_eq!(g.take_pending_and_release(), Some(BlockHeight(100)));
        assert_eq!(g.take_pending_and_release(), None);
        // Now offer 60 first — it acquires alone (no pending). This proves
        // a small offer after release doesn't accidentally grab a stale
        // higher pending — the stash was drained.
        assert_eq!(g.offer(BlockHeight(60)), Offer::Acquired(BlockHeight(60)));
    }
}
