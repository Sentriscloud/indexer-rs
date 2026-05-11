# Multi-stage build: compile the workspace, ship the two binaries on a
# minimal runtime. Dep caching is handled by the GitHub Actions buildx
# `cache-from / cache-to: type=gha` exporter — no stub-build trick.

FROM rust:1.90-bookworm AS builder
WORKDIR /build

# protobuf-compiler for tonic-prost-build (sentrix-grpc proto gen).
# libclang-dev not needed here — only the chain repo's mdbx-sys needs it.
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .

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
