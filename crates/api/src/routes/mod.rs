//! Route modules. Each file exports a `router()` returning a typed
//! `axum::Router<crate::SharedState>` so [`crate::make_router`] can compose
//! them with `.merge`.

pub mod blocks;
pub mod health;
pub mod tx;

const DEFAULT_LIMIT: i64 = 25;
const MAX_LIMIT: i64 = 100;

/// Parse `?limit=N` with the same clamping as the TS port: missing /
/// non-numeric / non-positive → 25; capped at 100.
pub(crate) fn clamp_limit(raw: Option<&str>) -> i64 {
    match raw.and_then(|s| s.parse::<i64>().ok()) {
        Some(n) if n > 0 => n.min(MAX_LIMIT),
        _ => DEFAULT_LIMIT,
    }
}
