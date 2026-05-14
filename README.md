# Sentrix Indexer (Rust)

Rust rewrite of the [Sentriscloud/indexer](https://github.com/Sentriscloud/indexer) TypeScript indexer, with Redis cache and ClickHouse analytics layers added.

**Status:** Code-complete through spec Phase 9 (deploy artifacts shipped). Phases 10/11 are operator actions (dual-run cutover + TS decom). See **Development phases** below for what landed.

## Why

- Drop indexer-worker CPU footprint to <10 % (TS hits ~36 % on the same host).
- Replace Node v8 GC pauses with predictable Tokio scheduling.
- Single-binary deploy, no Node runtime.
- Redis cache + ClickHouse analytics from day one.
- Compile-time SQL checks via sqlx.

## Spec

Full design lives in the operator-private spec — 20 sections, 10-pass internal review, every phase has acceptance criteria.

## Workspace

```
crates/
  ├── domain/      shared types (Block, Tx, Log, BigInt wrappers)
  ├── db/          sqlx pool + migrations + query helpers
  ├── chain/       SentrixClient — alloy provider + tonic stream + REST tx fetch
  ├── handlers/    log decoders (ERC-20/721/1155) + dispatch registry
  ├── coinblast/   curve detection + applyTrade SQL transaction
  ├── sync/        backfill + tail + reorg + cursor + single-flight
  ├── cache/       Redis cache-aside + tiers + invalidation
  ├── analytics/   ClickHouse batch buffer + flusher
  └── api/         axum routes + GraphQL + Etherscan compat

bin/
  ├── indexer.rs   ingest worker entry point
  └── api.rs       HTTP server entry point

migrations/        sqlx migrations ported from drizzle
crates/chain/        gRPC client wrapping the sentrix-proto crate (crates.io); schema lives upstream
snapshots/         golden test corpora (api JSON, ingest pg_dump)
docker/            Dockerfiles + docker-compose
```

## Reference stack

| Concern | Crate |
| --- | --- |
| Async runtime | `tokio` (multi-thread) |
| HTTP server | `axum` 0.8 |
| JSON-RPC | `alloy-provider` 0.7 |
| gRPC | `tonic` 0.14 + `prost` 0.14 |
| Postgres | `sqlx` 0.8 with `query!` compile-time checks |
| Redis | `fred` 9 |
| ClickHouse | `clickhouse-rs` 0.13 |
| GraphQL | `async-graphql` 7 |
| Logging | `tracing` + `tracing-subscriber` (json) |
| Metrics | `metrics-exporter-prometheus` |

## Build

```bash
cargo build --release
cargo test --workspace                                      # 30+ unit tests
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check
```

## Run

Two binaries, both env-configured (`figment` reads from `Env::raw()`):

```bash
# API server — REST + GraphQL
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer \
INDEXER_API_BIND=0.0.0.0:8080 \
  ./target/release/api

# Sync + CoinBlast worker daemon
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer \
RPC_URL=https://rpc.sentrixchain.com \
GRPC_URL=https://grpc.sentrixchain.com:443 \
INDEXER_NETWORK=mainnet \
CLICKHOUSE_URL=http://localhost:8123 \
  ./target/release/indexer
```

Or via Docker compose (postgres + indexer + api wired):

```bash
cp compose.env.example compose.env  # add your secrets
docker compose up -d
curl http://127.0.0.1:8080/health   # {"ok":true}
```

## API surface

REST (snake_case keys, bigint stringified, mirrors TS Fastify port):

| Path | Description |
| --- | --- |
| `GET /health` | Liveness — `{"ok":true}` |
| `GET /blocks?limit&before` | Latest blocks, newest-first |
| `GET /blocks/:height` | Block detail with nested transactions |
| `GET /tx/:hash` | Transaction detail with logs |
| `GET /address/:addr/txs?limit` | Address tx history |
| `GET /address/:addr/transfers?limit&standard` | Token-transfer history |
| `GET /accounts/active?limit` | Top senders by tx count |
| `GET /whale/transfers?limit` | Top txs by `value` |

GraphQL:

| Path | Description |
| --- | --- |
| `POST /graphql` | Schema: `block(height)`, `blocks(first, before)`, `transaction(hash)` |
| `GET /graphql/playground` | GraphiQL UI |

## Development phases

- ✅ **Phase 0** — workspace bootstrap
- ✅ **Phase 1** — domain + db (newtypes, Wei, schema mirroring TS drizzle)
- ✅ **Phase 2** — chain client (alloy provider + tonic gRPC + native REST + retry)
- ✅ **Phase 3** — sync core (backfill + tail + reorg + cursor + single-flight)
- ✅ **Phase 4** — CoinBlast worker (factory + curve event handlers + orphan adoption)
- ✅ **Phase 5** — REST API (axum, byte-fidelity with TS Fastify port)
- ✅ **Phase 6** — GraphQL (async-graphql + GraphiQL playground)
- ✅ **Phase 7** — Redis cache (fred-9, 3-tier TTL, circuit breaker)
- ✅ **Phase 8** — ClickHouse analytics (RawTxRow + buffered flusher)
- ✅ **Phase 9** — Dockerfile + docker-compose + bin runtime wiring
- 🔜 **Phase 10** — dual-run with TS indexer for ≥1 week zero-diff (operator)
- 🔜 **Phase 11** — cutover + decommission TS (operator)

## License

BUSL-1.1, transitioning to Apache-2.0 on 2030-05-11. See `LICENSE`.
