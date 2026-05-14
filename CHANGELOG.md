# CHANGELOG

All notable changes to `indexer-rs`. Format: [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

- **CoinBlast orphan adoption** — direct-deployed curves (e.g. CBLAST Genesis pre-factory) are now probed via `eth_call` (`token()` / `curveSupply()` / `graduationSrxThreshold()`) on first sighting and adopted into `cb_tokens` automatically. Topic-collision contracts get cached in `known_non_curves` so each address is probed at most once per worker lifetime.
- `ChainProvider::call(to, data)` — `eth_call` wrapper returning raw bytes; callers decode via `alloy_sol_types`.
- `GET /address/:addr/txs?limit` and `GET /address/:addr/transfers?limit&standard` — paginated address activity, mirrors TS port.
- `bin/indexer.rs`: tail loop wired (gRPC `StreamEvents` parallel with backfill, SingleFlight gate, reconnect on Lagged / StreamEnded). Analytics flusher wired (CLICKHOUSE_URL env opt-in). Both honour shared `CancellationToken` for graceful shutdown.

## [0.1.0] — 2026-05-11

Initial code-complete release covering spec Phases 0 → 9.

### Added

- **Phase 0** — Cargo workspace, 9 placeholder crates + 2 binaries, CI pipeline.
- **Phase 1** — `domain` crate (`BlockHeight` / `TxIndex` / `LogIndex` / `EpochNumber` newtypes; `Wei` wrapping `U256` with `sqlx::Type` impls through `BigDecimal`; `Block` / `Transaction` / `Log` / `TokenTransfer` structs). `db` crate (`0001_initial.sql` mirroring the TS drizzle schema byte-for-byte across 10 tables; `PgPool` wrapper; per-table CRUD helpers generic over `sqlx::PgExecutor`).
- **Phase 2** — `chain` crate (`ChainProvider` wrapping alloy `RootProvider<Http<reqwest>>`; `GrpcClient` wrapping the generated tonic stub; `RestClient` for native `/tx/<hash>` JSON; `retry_with_backoff` with deterministic jitter, no rand dep). `build.rs` invokes `tonic-prost-build` on the canonical `sentrix.proto`.
- **Phase 3** — `sync` crate. `block_writer` writes block + txs + logs + cursor advance in one `sqlx::Transaction` (atomic, idempotent). `cursor` wraps `_meta[last_synced_height]`. `single_flight` gate (AtomicBool + Mutex stash) collapses tip bursts to one chain. `backfill` walks `cursor+1..=tip-safe_lag`. `reorg` probes at `cursor-reorg_probe_depth`, walks back to divergence on hash mismatch, cascade-deletes downstream + resets cursor. `tail` consumes tonic `StreamEvents`; on Lagged exits so the orchestrator re-runs JSON-RPC backfill before resubscribe.
- **Phase 4** — `coinblast` crate. `events.rs` declares `CurveCreated` / `Buy` / `Sell` / `Graduated` via `alloy-sol-types` `sol!` macro; topic0 + decoder generated from ABI. `worker.rs` runs chunked backfill (4000-block chunks, 5-block safe_lag). `handlers.rs` writes `cb_tokens` insert + `cb_trades` insert + aggregate bump in single SQL transactions; idempotent via `cb_trades` UNIQUE `(tx_hash, log_index)`.
- **Phase 5** — `api` crate. Axum routes mirror TS Fastify port byte-for-byte: snake_case keys, bigint stringified, `from_addr → from` rename, log topics flattened with nulls dropped. `GET /health`, `GET /blocks?limit&before`, `GET /blocks/:height`, `GET /tx/:hash`. `ApiError → IntoResponse` with `{ error: ... }` body.
- **Phase 6** — GraphQL via `async-graphql` + `async-graphql-axum`. `POST /graphql` and `GET /graphql/playground` (GraphiQL UI). Query: `block(height)`, `blocks(first, before)`, `transaction(hash)`. Same wire shape as REST.
- **Phase 7** — `cache` crate. `fred-9` `RedisPool` wrapper. `CacheClient::{get,set,invalidate}` with serde-JSON values, three TTL tiers (Chain=60s, Address=5min, Detail=1h), key namespace. `CircuitBreaker` (atomic counter + `parking_lot::Mutex<Option<Instant>>`) trips after threshold consecutive failures, half-opens after `open_for` window.
- **Phase 8** — `analytics` crate. ClickHouse `RawTxRow` (Decimal as String to avoid Decimal256 experimental flag). `AnalyticsHandle` (mpsc unbounded) + `run_flusher` (15s tick + drain on cancel; final flush before exit per spec §10.5).
- **Phase 9** — `bin/api.rs` + `bin/indexer.rs` wired with `figment` env config, graceful shutdown on SIGTERM/Ctrl-C. Multi-stage `Dockerfile` (rust:1.90-bookworm builder → debian-slim runtime, non-root uid 10001). `docker-compose.yml` (postgres:17-alpine + indexer + api with healthchecks).

### Toolchain

- Rust 1.90 (workspace `rust-version = "1.90"`, transitive deps require ≥1.86 via `ruint` / `icu` / `home`).
- Edition 2024.

### CI

- `cargo fmt --check` + `cargo build --workspace --locked` + `cargo nextest run --workspace --locked --no-tests=pass` + `cargo clippy --workspace --all-targets --locked -- -D warnings` + `cargo deny check` (advisories + bans + licenses + sources).

### Known follow-ups (non-blocking, lower priority)

- Push-on-commit hookup from sync layer to analytics handle (flusher infra is wired; sync side TBD).
- REST iter 2 long tail (`/stats/daily` materialised view, `/whale/*`, etherscan-compat `?module=`, coinblast-specific routes).
- Testcontainers wiring for golden-ingest + reorg-sim integration tests (currently `#[ignore]`'d).
