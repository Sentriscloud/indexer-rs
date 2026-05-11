# Sentrix Indexer (Rust)

Rust rewrite of the [Sentriscloud/indexer](https://github.com/Sentriscloud/indexer) TypeScript indexer, with Redis cache and ClickHouse analytics layers added.

**Status:** Phase 0 — workspace scaffolded. No functional code yet. See spec for the full plan.

## Why

- Drop indexer-worker CPU footprint to <10 % (TS hits ~36 % on the same host).
- Replace Node v8 GC pauses with predictable Tokio scheduling.
- Single-binary deploy, no Node runtime.
- Redis cache + ClickHouse analytics from day one.
- Compile-time SQL checks via sqlx.

## Spec

Full design in `founder-private/SPEC_INDEXER_RUST.md` (operator-only). 20 sections, 10-pass internal review, every phase has acceptance criteria.

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
proto/             sentrix.proto (copy from chain repo)
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
cargo nextest run
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check
```

## Development phases

- **Phase 0** — workspace bootstrap (this commit)
- **Phase 0.5** — capture golden snapshots from TS production
- **Phase 1** — domain + db
- **Phase 2** — chain client
- **Phase 3** — sync core
- **Phase 4** — coinblast worker
- **Phase 5** — REST API (snapshot-test gated)
- **Phase 6** — GraphQL
- **Phase 7** — Redis cache
- **Phase 8** — ClickHouse analytics
- **Phase 9** — docker-compose + production-ready deploy
- **Phase 10** — dual-run with TS indexer for ≥1 week of zero-diff
- **Phase 11** — cutover + decommission TS

## License

BUSL-1.1, transitioning to Apache-2.0 on 2030-05-11. See `LICENSE`.
