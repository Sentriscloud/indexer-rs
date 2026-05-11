//! Cache-aside helper. Routes wrap their PG read in [`get_or_load`] —
//! cache hit → return cached. Miss / Redis down / breaker open → run the
//! loader, write result to cache (best-effort), return.
//!
//! Cache failures (CacheError::Open / Redis transport) **never fail the
//! request** — they fall back to the loader. Cache misses bump
//! `indexer_api_cache_miss_total`; hits bump `indexer_api_cache_hit_total`.

use crate::SharedState;
use indexer_cache::Tier;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Read-through cache helper. `key` is the cache key (will be namespaced
/// internally by `CacheClient`). `tier` controls TTL. `loader` is the
/// expensive PG read invoked only on miss.
///
/// Route handlers use this like:
/// ```ignore
/// let blocks = cached::get_or_load(&state, "blocks:list:25:none", Tier::Chain, || async {
///     blocks::list_before(&state.pool, None, 25).await
/// }).await?;
/// ```
pub async fn get_or_load<T, F, Fut, E>(
    state: &SharedState,
    key: &str,
    tier: Tier,
    loader: F,
) -> Result<T, E>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let route_label = key.split(':').next().unwrap_or("unknown").to_string();

    if let Some(cache) = &state.cache {
        match cache.get::<T>(key).await {
            Ok(Some(hit)) => {
                metrics::counter!("indexer_api_cache_hit_total", "route" => route_label)
                    .increment(1);
                return Ok(hit);
            }
            Ok(None) => {
                metrics::counter!("indexer_api_cache_miss_total", "route" => route_label.clone())
                    .increment(1);
            }
            Err(e) => {
                metrics::counter!(
                    "indexer_api_cache_error_total",
                    "route" => route_label.clone()
                )
                .increment(1);
                tracing::debug!(error = %e, key, "cache read failed; falling back to loader");
            }
        }
    }

    let value = loader().await?;

    if let Some(cache) = &state.cache
        && let Err(e) = cache.set(key, &value, tier).await
    {
        tracing::debug!(error = %e, key, "cache write failed; non-fatal");
    }

    Ok(value)
}
