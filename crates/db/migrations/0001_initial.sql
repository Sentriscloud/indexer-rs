-- Initial Sentrix indexer schema. Mirrors the TS drizzle schema at
-- Sentriscloud/indexer/packages/db/src/schema.ts byte-for-byte at the table
-- + column + index level. Verified by the schema-parity test in
-- crates/db/tests/schema_parity.rs (deferred — runs against a throwaway PG).
--
-- Type choices match TS:
--   - varchar(42) for addresses, varchar(66) for hashes (NOT bytea — the
--     prior comment in schema.ts about bytea is aspirational and never landed).
--   - numeric(78, 0) for u256-scale wei amounts.
--   - bigint for heights / timestamps / nonces.
--   - jsonb for justification_signers + validator_set arrays.
-- Index naming + composite ordering preserved exactly so query plans stay
-- comparable across the TS / Rust ports during dual-run cutover.

-- ─── blocks ─────────────────────────────────────────────────────────────
CREATE TABLE blocks (
    height                bigint PRIMARY KEY,
    hash                  varchar(66) NOT NULL UNIQUE,
    parent_hash           varchar(66) NOT NULL,
    timestamp             bigint NOT NULL,
    validator             varchar(42) NOT NULL,
    gas_used              bigint NOT NULL DEFAULT 0,
    gas_limit             bigint NOT NULL DEFAULT 0,
    base_fee              numeric(78, 0),
    tx_count              integer NOT NULL DEFAULT 0,
    state_root            varchar(66),
    round                 integer NOT NULL DEFAULT 0,
    justification_signers jsonb DEFAULT '[]'::jsonb
);
CREATE INDEX blocks_validator_idx ON blocks (validator);
CREATE INDEX blocks_timestamp_idx ON blocks (timestamp);

-- ─── transactions ──────────────────────────────────────────────────────
CREATE TABLE transactions (
    hash             varchar(66) PRIMARY KEY,
    block_height     bigint NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
    tx_index         integer NOT NULL,
    from_addr        varchar(42) NOT NULL,
    to_addr          varchar(42),
    value            numeric(78, 0) NOT NULL DEFAULT 0,
    gas_limit        bigint NOT NULL DEFAULT 0,
    gas_used         bigint DEFAULT 0,
    gas_price        numeric(78, 0),
    fee              numeric(78, 0) NOT NULL DEFAULT 0,
    nonce            bigint NOT NULL DEFAULT 0,
    data             text,
    status           smallint NOT NULL DEFAULT 1,
    contract_address varchar(42),
    tx_type          varchar(24) NOT NULL DEFAULT 'native'
);
CREATE INDEX txs_block_height_idx ON transactions (block_height);
CREATE INDEX txs_from_idx         ON transactions (from_addr);
CREATE INDEX txs_to_idx           ON transactions (to_addr);
CREATE INDEX txs_contract_idx     ON transactions (contract_address);
CREATE INDEX txs_value_desc_idx   ON transactions (value);
CREATE INDEX txs_from_block_idx   ON transactions (from_addr, block_height);
CREATE INDEX txs_to_block_idx     ON transactions (to_addr,   block_height);

-- ─── logs ──────────────────────────────────────────────────────────────
CREATE TABLE logs (
    block_height bigint NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
    tx_hash      varchar(66) NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    log_index    integer NOT NULL,
    address      varchar(42) NOT NULL,
    topic0       varchar(66),
    topic1       varchar(66),
    topic2       varchar(66),
    topic3       varchar(66),
    data         text,
    PRIMARY KEY (block_height, log_index)
);
CREATE INDEX logs_address_idx ON logs (address);
CREATE INDEX logs_topic0_idx  ON logs (topic0);
CREATE INDEX logs_tx_idx      ON logs (tx_hash);

-- ─── token_transfers ───────────────────────────────────────────────────
CREATE TABLE token_transfers (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    block_height bigint NOT NULL,
    tx_hash      varchar(66) NOT NULL,
    log_index    integer NOT NULL,
    contract     varchar(42) NOT NULL,
    standard     varchar(12) NOT NULL,
    from_addr    varchar(42) NOT NULL,
    to_addr      varchar(42) NOT NULL,
    token_id     numeric(78, 0),
    amount       numeric(78, 0) NOT NULL
);
CREATE INDEX transfers_contract_idx   ON token_transfers (contract);
CREATE INDEX transfers_from_idx       ON token_transfers (from_addr);
CREATE INDEX transfers_to_idx         ON token_transfers (to_addr);
CREATE INDEX transfers_block_idx      ON token_transfers (block_height);
CREATE INDEX transfers_from_block_idx ON token_transfers (from_addr, block_height);
CREATE INDEX transfers_to_block_idx   ON token_transfers (to_addr,   block_height);

-- ─── addresses ─────────────────────────────────────────────────────────
CREATE TABLE addresses (
    address          varchar(42) PRIMARY KEY,
    first_seen_block bigint NOT NULL,
    last_seen_block  bigint NOT NULL,
    balance_cached   numeric(78, 0) DEFAULT 0,
    nonce            bigint DEFAULT 0,
    is_contract      boolean NOT NULL DEFAULT false,
    code_hash        varchar(66)
);
CREATE INDEX addresses_contract_recent_idx ON addresses (is_contract, first_seen_block);

-- ─── validators ────────────────────────────────────────────────────────
CREATE TABLE validators (
    address           varchar(42) PRIMARY KEY,
    moniker           varchar(64),
    commission_bp     integer,
    self_stake        numeric(78, 0) DEFAULT 0,
    total_delegated   numeric(78, 0) DEFAULT 0,
    blocks_proposed   bigint DEFAULT 0,
    last_active_block bigint,
    is_jailed         boolean NOT NULL DEFAULT false,
    jail_until        bigint
);

-- ─── epochs ────────────────────────────────────────────────────────────
CREATE TABLE epochs (
    epoch_number          bigint PRIMARY KEY,
    start_height          bigint NOT NULL,
    end_height            bigint NOT NULL,
    validator_set         jsonb NOT NULL,
    total_staked          numeric(78, 0) DEFAULT 0,
    total_blocks_produced bigint DEFAULT 0
);

-- ─── _meta ─────────────────────────────────────────────────────────────
CREATE TABLE _meta (
    key        varchar(64) PRIMARY KEY,
    value      text NOT NULL,
    updated_at bigint NOT NULL
);

-- ─── cb_tokens ─────────────────────────────────────────────────────────
CREATE TABLE cb_tokens (
    curve_address          varchar(42) PRIMARY KEY,
    token_address          varchar(42) NOT NULL UNIQUE,
    owner_address          varchar(42) NOT NULL,
    name                   text NOT NULL,
    symbol                 text NOT NULL,
    curve_supply           numeric(78, 0) NOT NULL,
    graduation_threshold   numeric(78, 0) NOT NULL,
    is_graduated           boolean NOT NULL DEFAULT false,
    created_block          bigint NOT NULL,
    created_tx_hash        varchar(66) NOT NULL,
    total_volume_srx       numeric(78, 0) NOT NULL DEFAULT 0,
    trade_count            integer NOT NULL DEFAULT 0,
    last_price_srx         numeric(78, 0) NOT NULL DEFAULT 0,
    image_url              text,
    description            text,
    twitter_url            text,
    telegram_url           text,
    website_url            text,
    metadata_updated_at    bigint
);
CREATE INDEX cb_tokens_owner_idx         ON cb_tokens (owner_address);
CREATE INDEX cb_tokens_graduated_idx     ON cb_tokens (is_graduated);
CREATE INDEX cb_tokens_created_block_idx ON cb_tokens (created_block);

-- ─── cb_trades ─────────────────────────────────────────────────────────
CREATE TABLE cb_trades (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    curve_address  varchar(42) NOT NULL,
    token_address  varchar(42),
    type           varchar(12) NOT NULL,
    trader_address varchar(42) NOT NULL,
    srx_amount     numeric(78, 0) NOT NULL DEFAULT 0,
    token_amount   numeric(78, 0) NOT NULL DEFAULT 0,
    fee            numeric(78, 0) NOT NULL DEFAULT 0,
    block_number   bigint NOT NULL,
    tx_hash        varchar(66) NOT NULL,
    log_index      integer NOT NULL
);
CREATE UNIQUE INDEX cb_trades_uniq_log           ON cb_trades (tx_hash, log_index);
CREATE INDEX        cb_trades_curve_idx          ON cb_trades (curve_address);
CREATE INDEX        cb_trades_trader_idx         ON cb_trades (trader_address);
CREATE INDEX        cb_trades_block_idx          ON cb_trades (block_number);
CREATE INDEX        cb_trades_srx_amount_desc_idx ON cb_trades (srx_amount);
