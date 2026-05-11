# Multi-stage build: chef cache for cargo deps, then build the workspace,
# then ship a slim runtime with just the two binaries.

FROM rust:1.90-bookworm AS builder
WORKDIR /build

# protobuf-compiler for tonic-prost-build (sentrix-grpc proto gen).
# libclang-dev not needed here — only the chain repo's mdbx-sys needs it.
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Layer 1: copy manifests so cargo caches deps independent of source.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/domain/Cargo.toml crates/domain/Cargo.toml
COPY crates/db/Cargo.toml crates/db/Cargo.toml
COPY crates/chain/Cargo.toml crates/chain/Cargo.toml
COPY crates/handlers/Cargo.toml crates/handlers/Cargo.toml
COPY crates/coinblast/Cargo.toml crates/coinblast/Cargo.toml
COPY crates/sync/Cargo.toml crates/sync/Cargo.toml
COPY crates/cache/Cargo.toml crates/cache/Cargo.toml
COPY crates/analytics/Cargo.toml crates/analytics/Cargo.toml
COPY crates/api/Cargo.toml crates/api/Cargo.toml
COPY bin/Cargo.toml bin/Cargo.toml

# Stub source files so cargo can resolve + cache the dep graph without our
# code (gets overwritten in the next layer).
RUN mkdir -p crates/domain/src crates/db/src crates/chain/src crates/handlers/src \
    crates/coinblast/src crates/sync/src crates/cache/src crates/analytics/src \
    crates/api/src bin \
    && echo "" | tee crates/domain/src/lib.rs crates/db/src/lib.rs \
        crates/chain/src/lib.rs crates/handlers/src/lib.rs \
        crates/coinblast/src/lib.rs crates/sync/src/lib.rs \
        crates/cache/src/lib.rs crates/analytics/src/lib.rs \
        crates/api/src/lib.rs > /dev/null \
    && echo "fn main() {}" | tee bin/api.rs bin/indexer.rs > /dev/null \
    && cargo build --release --locked --bins || true

# Layer 2: bring in real sources + rebuild.
COPY crates crates
COPY bin bin
# Force cargo to rebuild every workspace crate. The stub-build layer above
# cached compiled artefacts for our empty-stub lib.rs files; without this
# `cargo clean -p ...`, cargo's fingerprint check sees no Cargo.toml change
# and reuses the stale stub artefacts — `indexer-domain` ends up exporting
# nothing, and `indexer-db` fails with `no BlockHeight in the root`.
RUN cargo clean -p indexer-domain -p indexer-db -p indexer-chain \
    -p indexer-handlers -p indexer-coinblast -p indexer-sync \
    -p indexer-cache -p indexer-analytics -p indexer-api -p indexer-bin
RUN cargo build --release --locked --bins

# Runtime image — only the binaries + their TLS / libc deps.
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/api /usr/local/bin/api
COPY --from=builder /build/target/release/indexer /usr/local/bin/indexer

# Non-root user for runtime safety.
RUN useradd -r -u 10001 -s /usr/sbin/nologin indexer
USER 10001

# Default to API; compose service overrides for the indexer worker.
ENV RUST_LOG=info
EXPOSE 8080
CMD ["/usr/local/bin/api"]
