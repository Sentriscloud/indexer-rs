-- 0003_token_transfers_unique.sql — audit M-3 fix (2026-05-21)
--
-- token_transfers had only PRIMARY KEY (id) when shipped in PR #46. On reorg
-- recovery + replay the worker re-inserts the same (tx_hash, log_index) rows
-- without a uniqueness gate, producing duplicate transfer rows that double-
-- count balances downstream. The atomic cursor advance in the steady-state
-- writer prevented this in practice, but reorg / forensic-rerun paths bypass
-- that protection.
--
-- Add the unique constraint that should have been there from day one. The
-- corresponding `ON CONFLICT (tx_hash, log_index) DO NOTHING` is re-enabled
-- in `crates/db/src/token_transfers.rs::insert_batch` in the same change set.
--
-- Safe to run on a populated table: drops existing duplicates first
-- (keeping the lowest id per (tx_hash, log_index) pair) so the unique index
-- can be created. Production runs as of 2026-05-21 don't yet have duplicates
-- (no reorg has hit the patched indexer), so the DELETE is a no-op for now;
-- left in place to make this migration replay-safe on chains that did
-- accumulate duplicates before the upgrade.

DELETE FROM token_transfers t1
USING token_transfers t2
WHERE t1.id > t2.id
  AND t1.tx_hash = t2.tx_hash
  AND t1.log_index = t2.log_index;

CREATE UNIQUE INDEX IF NOT EXISTS transfers_tx_log_unique_idx
    ON token_transfers (tx_hash, log_index);
