-- 0004_contracts.sql — contract leaderboards (/contracts/recent|pioneers|stats)
--
-- Tracks contract-creation txs (transactions.to_address IS NULL). The created
-- address is the CREATE address keccak(rlp(sender, nonce))[12:], computed by the
-- indexer (Postgres can't keccak), so this migration only creates the table —
-- the indexer's block-writer populates it going forward, and a one-time startup
-- backfill fills history from existing `transactions WHERE to_address IS NULL`.
--
-- `code_hash` is reserved for a later eth_getCode pass (the frontend already
-- renders NULL as "—"); leaving it NULL keeps this MVP free of receipt/getCode
-- round-trips.

CREATE TABLE IF NOT EXISTS contracts (
    address           varchar(42) PRIMARY KEY,
    first_seen_block  bigint      NOT NULL,
    last_seen_block   bigint      NOT NULL,
    code_hash         varchar(66),
    tx_count          bigint      NOT NULL DEFAULT 1,
    created_tx_hash   varchar(66) NOT NULL
);

-- pioneers = earliest created; recent = newest created.
CREATE INDEX IF NOT EXISTS contracts_first_seen_idx ON contracts (first_seen_block);
CREATE INDEX IF NOT EXISTS contracts_last_seen_idx  ON contracts (last_seen_block DESC);
