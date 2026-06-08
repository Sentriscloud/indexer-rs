-- 0005_addresses.sql — address registry powering /contracts leaderboards.
--
-- Every `from`/`to` address seen in a tx is upserted here by the block writer
-- (is_contract=false, code_hash NULL = "not yet classified"). A background
-- detector lazily runs eth_getCode on unclassified rows and sets is_contract +
-- code_hash ("0x" for EOAs, keccak(code) for contracts). `/contracts/*` then
-- serves `WHERE is_contract = true ORDER BY first_seen_block`. Mirrors the
-- legacy TS indexer's `addresses` table + contract-detector worker.
--
-- Supersedes migration 0004's `contracts` table (which detected creations via
-- `to_addr IS NULL` — wrong for Sentrix, which records to_addr = the contract
-- address). The 0004 table is left in place (append-only migrations) but
-- unused; the API now reads `addresses`.

CREATE TABLE IF NOT EXISTS addresses (
    address          varchar(42) PRIMARY KEY,
    first_seen_block bigint      NOT NULL,
    last_seen_block  bigint      NOT NULL,
    is_contract      boolean     NOT NULL DEFAULT false,
    code_hash        varchar(66)
);

-- /contracts/recent (DESC) + /contracts/pioneers (ASC): is_contract narrows,
-- first_seen_block sorts within the narrowed slice.
CREATE INDEX IF NOT EXISTS addresses_contract_recent_idx
    ON addresses (is_contract, first_seen_block);

-- Detector candidate scan stays cheap: only unclassified rows.
CREATE INDEX IF NOT EXISTS addresses_unclassified_idx
    ON addresses (address) WHERE code_hash IS NULL;
