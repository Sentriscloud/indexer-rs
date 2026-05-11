-- Materialised view for /stats/daily — pre-aggregates blocks by chain-day.
-- Mirrors apps/indexer (TS) migration 0005_stats_daily_mv.sql.
--
-- Refresh strategy: indexer worker calls
-- `REFRESH MATERIALIZED VIEW CONCURRENTLY stats_daily_mv` every 5 minutes
-- (Phase 8 cron will land in a follow-up — for now operator can refresh
-- via psql or the route layer can call REFRESH on cache miss).
--
-- Day bucket: floor(timestamp / 86400) — keeps the view chain-time aligned
-- regardless of the host's wall-clock TZ.

CREATE MATERIALIZED VIEW IF NOT EXISTS stats_daily_mv AS
SELECT
    (timestamp / 86400)::bigint                AS day_bucket,
    COUNT(*)::bigint                           AS block_count,
    COALESCE(SUM(tx_count), 0)::bigint         AS tx_count,
    COALESCE(SUM(gas_used), 0)::bigint         AS gas_used,
    MIN(height)::bigint                        AS first_height,
    MAX(height)::bigint                        AS last_height
FROM blocks
GROUP BY (timestamp / 86400);

-- Unique index is required for REFRESH ... CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS stats_daily_mv_day_idx
    ON stats_daily_mv (day_bucket);
