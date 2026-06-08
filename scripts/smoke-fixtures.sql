-- Seed data for the smoke test. Just enough to exercise every read path:
--   - 3 blocks (so /blocks pagination + /blocks/:height + /stats/daily aggregate work)
--   - 4 txs across the 3 blocks (so /tx/:hash + /address/:addr/txs + /whale/transfers + /accounts/active work)
--   - 2 logs on tx2 (so /tx/:hash returns logs + /coinblast topic dispatch can be verified)
--   - 1 token transfer (so /address/:addr/transfers works)
--   - 1 cb_token + 2 cb_trades (so /coinblast/* work)
--
-- Addresses + hashes are deterministic so the smoke runner can assert
-- on exact strings.

-- ── blocks ─────────────────────────────────────────────────────────────
INSERT INTO blocks (height, hash, parent_hash, timestamp, validator, gas_used, gas_limit, base_fee, tx_count, state_root, round, justification_signers) VALUES
    (1, '0x1111111111111111111111111111111111111111111111111111111111111111',
        '0x0000000000000000000000000000000000000000000000000000000000000000',
        1700000000, '0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1',
        21000, 8000000, NULL, 1,
        '0xaaaa000000000000000000000000000000000000000000000000000000000000', 0,
        '[]'::jsonb),
    (2, '0x2222222222222222222222222222222222222222222222222222222222222222',
        '0x1111111111111111111111111111111111111111111111111111111111111111',
        1700086400, '0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1',
        100000, 8000000, NULL, 2,
        '0xbbbb000000000000000000000000000000000000000000000000000000000000', 0,
        '[]'::jsonb),
    (3, '0x3333333333333333333333333333333333333333333333333333333333333333',
        '0x2222222222222222222222222222222222222222222222222222222222222222',
        1700172800, '0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2',
        50000, 8000000, NULL, 1,
        '0xcccc000000000000000000000000000000000000000000000000000000000000', 0,
        '[]'::jsonb);

-- ── transactions ───────────────────────────────────────────────────────
INSERT INTO transactions (hash, block_height, tx_index, from_addr, to_addr, value, gas_limit, gas_used, gas_price, fee, nonce, data, status, contract_address, tx_type) VALUES
    -- Whale tx (largest value).
    ('0xtxaaaa00000000000000000000000000000000000000000000000000000000aa',
     1, 0,
     '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
     '0xfeedfacefeedfacefeedfacefeedfacefeedface',
     1000000000000000000, 21000, 21000, 0, 0, 1, '0x', 1, NULL, 'evm'),
    -- Smaller tx, same sender (so /accounts/active ranks them).
    ('0xtxbbbb00000000000000000000000000000000000000000000000000000000bb',
     2, 0,
     '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
     '0xc0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
     500000000, 21000, 21000, 0, 0, 2, '0x', 1, NULL, 'evm'),
    -- Tx with logs (so /tx/:hash returns non-empty logs array).
    ('0xtxcccc00000000000000000000000000000000000000000000000000000000cc',
     2, 1,
     '0xfeedfacefeedfacefeedfacefeedfacefeedface',
     '0xc0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
     1, 100000, 50000, 0, 0, 0, '0xabcd', 1, NULL, 'evm'),
    -- Block 3 single tx.
    ('0xtxdddd00000000000000000000000000000000000000000000000000000000dd',
     3, 0,
     '0xc0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
     '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
     0, 21000, 21000, 0, 0, 0, '0x', 1, NULL, 'evm');

-- ── logs ───────────────────────────────────────────────────────────────
INSERT INTO logs (block_height, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data) VALUES
    (2, '0xtxcccc00000000000000000000000000000000000000000000000000000000cc',
     0, '0xc0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
     '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
     '0x000000000000000000000000feedfacefeedfacefeedfacefeedfacefeedface',
     '0x000000000000000000000000c0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
     NULL,
     '0x0000000000000000000000000000000000000000000000000000000000000001'),
    (2, '0xtxcccc00000000000000000000000000000000000000000000000000000000cc',
     1, '0xc0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
     '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
     NULL, NULL, NULL,
     '0x');

-- ── token_transfers ────────────────────────────────────────────────────
INSERT INTO token_transfers (block_height, tx_hash, log_index, contract, standard, from_addr, to_addr, token_id, amount) VALUES
    (2, '0xtxcccc00000000000000000000000000000000000000000000000000000000cc',
     0, '0xc0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff', 'erc20',
     '0xfeedfacefeedfacefeedfacefeedfacefeedface',
     '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
     NULL, 1000);

-- ── cb_tokens ──────────────────────────────────────────────────────────
INSERT INTO cb_tokens (curve_address, token_address, owner_address, name, symbol, curve_supply, graduation_threshold, is_graduated, created_block, created_tx_hash, total_volume_srx, trade_count, last_price_srx) VALUES
    ('0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb',
     '0xtokenfffffffffffffffffffffffffffffffffff',
     '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
     'Smoke Coin', 'SMOKE',
     1000000000000000000000000, 100000000000000000000, false,
     1, '0xtxaaaa00000000000000000000000000000000000000000000000000000000aa',
     1500000000, 2, 750000000);

-- ── cb_trades ──────────────────────────────────────────────────────────
INSERT INTO cb_trades (curve_address, token_address, type, trader_address, srx_amount, token_amount, fee, block_number, tx_hash, log_index) VALUES
    ('0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb',
     '0xtokenfffffffffffffffffffffffffffffffffff',
     'buy', '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
     1000000000, 1000000000000000000, 1000000,
     2, '0xtxbbbb00000000000000000000000000000000000000000000000000000000bb', 0),
    ('0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb',
     '0xtokenfffffffffffffffffffffffffffffffffff',
     'sell', '0xfeedfacefeedfacefeedfacefeedfacefeedface',
     500000000, 500000000000000000, 500000,
     3, '0xtxdddd00000000000000000000000000000000000000000000000000000000dd', 0);

-- ── contracts (Phase 2 leaderboards) ──────────────────────────────────
-- One fixture contract (code_hash NULL → frontend renders "—").
INSERT INTO contracts (address, first_seen_block, last_seen_block, code_hash, tx_count, created_tx_hash) VALUES
    ('0xc0ffee0000000000000000000000000000000001', 2, 3, NULL, 1,
     '0xtxcreate0000000000000000000000000000000000000000000000000000cc');

-- ── refresh stats_daily_mv ─────────────────────────────────────────────
-- Three blocks 86400s apart = three distinct day_buckets (19675, 19676, 19677).
REFRESH MATERIALIZED VIEW stats_daily_mv;
