//! Query helpers for `_meta` — indexer-internal key/value state.
//!
//! Used by the sync layer for cursors (`last_synced_height`,
//! `last_synced_coinblast_height`, etc.). Atomic with data writes when the
//! caller wraps both in a single `sqlx::Transaction` (spec §5 invariant 2).
//!
//! Values are stored as text — callers stringify what they put in. The
//! `updated_at` column is a chain-time second; callers pass it through.

use crate::{DbResult, PgPool};
use sqlx::Row;

/// Read a value, returning None if the key is unset.
pub async fn get(pool: &PgPool, key: &str) -> DbResult<Option<String>> {
    let row = sqlx::query("SELECT value FROM _meta WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(row.and_then(|r| r.try_get::<String, _>("value").ok()))
}

/// Upsert a value. Updates `updated_at` on every set so callers can audit
/// staleness via the cursor row directly.
pub async fn set<'e, E>(executor: E, key: &str, value: &str, updated_at: i64) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        "INSERT INTO _meta (key, value, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, \
                                          updated_at = EXCLUDED.updated_at",
    )
    .bind(key)
    .bind(value)
    .bind(updated_at)
    .execute(executor)
    .await?;
    Ok(())
}

/// Convenience: read an `i64`-typed cursor (height, etc.). Returns None if
/// unset; surfaces a parse error as `DbError::Sqlx` with a decode message.
pub async fn get_i64(pool: &PgPool, key: &str) -> DbResult<Option<i64>> {
    match get(pool, key).await? {
        None => Ok(None),
        Some(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|e| sqlx::Error::Decode(format!("_meta[{key}] not i64: {e}").into()).into()),
    }
}

/// Convenience: write an `i64`-typed cursor.
pub async fn set_i64<'e, E>(executor: E, key: &str, value: i64, updated_at: i64) -> DbResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    set(executor, key, &value.to_string(), updated_at).await
}
