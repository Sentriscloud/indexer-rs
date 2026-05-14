#!/usr/bin/env bash
# Smoke test — spin up postgres, apply migrations, seed fixtures, run the
# API binary, hit every endpoint, verify response shape, tear down.
#
# Usage:
#   ./scripts/smoke.sh                  # run full flow
#   SMOKE_KEEP=1 ./scripts/smoke.sh     # leave postgres + api running on exit
#                                       # (useful for poking with curl after)
#
# Exits non-zero on the first failing assertion. Each step prints a clear
# OK/FAIL line so CI logs are easy to grep.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PG_CONTAINER="indexer-rs-smoke-pg"
PG_PORT="${PG_PORT:-54329}"   # non-default so we don't clash with a host PG
API_PORT="${API_PORT:-58080}"
DB_URL="postgres://indexer:indexer@127.0.0.1:${PG_PORT}/indexer"
API_BASE="http://127.0.0.1:${API_PORT}"
API_PID=""

cleanup() {
    local code=$?
    if [[ -n "$API_PID" ]] && kill -0 "$API_PID" 2>/dev/null; then
        kill "$API_PID" 2>/dev/null || true
        wait "$API_PID" 2>/dev/null || true
    fi
    if [[ "${SMOKE_KEEP:-0}" != "1" ]]; then
        docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
    else
        echo "SMOKE_KEEP=1 set — leaving postgres + api running"
        echo "  postgres: $DB_URL"
        echo "  api: $API_BASE"
    fi
    if [[ $code -ne 0 ]]; then
        echo "✗ smoke FAILED (exit $code)"
    fi
    exit $code
}
trap cleanup EXIT

note() { printf "▸ %s\n" "$*"; }
ok()   { printf "  ✓ %s\n" "$*"; }
fail() { printf "  ✗ %s\n" "$*" >&2; exit 1; }

# Tools we need on the host. psql runs inside the postgres container so we
# don't need a host-side libpq install.
for cmd in docker curl jq cargo; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        fail "missing tool: $cmd"
    fi
done

# Helper: run psql inside the postgres container, piping stdin (sql file).
psql_in() {
    docker exec -i "$PG_CONTAINER" \
        psql -v ON_ERROR_STOP=1 -q -U indexer -d indexer
}

# ── 1. Start postgres ─────────────────────────────────────────────────
note "starting postgres on :$PG_PORT (container $PG_CONTAINER)"
docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$PG_CONTAINER" \
    -e POSTGRES_USER=indexer \
    -e POSTGRES_PASSWORD=indexer \
    -e POSTGRES_DB=indexer \
    -p "127.0.0.1:${PG_PORT}:5432" \
    postgres:17-alpine >/dev/null
ok "container started"

note "waiting for postgres ready"
pg_ok=0
for _ in $(seq 1 60); do
    if docker exec "$PG_CONTAINER" pg_isready -U indexer -d indexer -q 2>/dev/null; then
        pg_ok=1
        break
    fi
    sleep 1
done
[[ "$pg_ok" == "1" ]] || fail "postgres never came up in 60s"
# Tiny settle pause — pg_isready can return ok while the role/db is still
# being initialised by docker-entrypoint scripts.
sleep 1
ok "postgres ready"

# ── 2. Apply migrations ───────────────────────────────────────────────
note "applying migrations"
for f in crates/db/migrations/*.sql; do
    psql_in <"$f" >/dev/null || fail "migration $f failed"
done
ok "migrations applied"

# ── 3. Seed fixtures ──────────────────────────────────────────────────
note "seeding fixtures"
psql_in <scripts/smoke-fixtures.sql >/dev/null || fail "fixtures failed"
ok "fixtures seeded"

# ── 4. Build + start API binary ───────────────────────────────────────
note "building api binary (release)"
cargo build --release --bin api >/dev/null 2>&1 || fail "cargo build failed"
ok "build complete"

note "starting api binary on :$API_PORT"
DATABASE_URL="$DB_URL" \
INDEXER_API_BIND="127.0.0.1:${API_PORT}" \
RUST_LOG="warn" \
    ./target/release/api &
API_PID=$!
ok "api started (pid $API_PID)"

note "waiting for /health"
for _ in $(seq 1 30); do
    if curl -fsS "$API_BASE/health" >/dev/null 2>&1; then
        ok "/health responded"
        break
    fi
    sleep 0.5
done
curl -fsS "$API_BASE/health" >/dev/null || fail "api never came up"

# ── 5. Hit every endpoint + verify shape ──────────────────────────────
note "asserting response shapes"

# /health -> {"ok": true}
v=$(curl -fsS "$API_BASE/health" | jq -er '.ok')
[[ "$v" == "true" ]] || fail "/health.ok != true (got '$v')"
ok "/health"

# /blocks -> { blocks: [3 items, newest first] }
v=$(curl -fsS "$API_BASE/blocks" | jq -r '.blocks | length')
[[ "$v" == "3" ]] || fail "/blocks length != 3 (got $v)"
v=$(curl -fsS "$API_BASE/blocks" | jq -r '.blocks[0].height')
[[ "$v" == "3" ]] || fail "/blocks[0].height != '3' (got '$v')"
ok "/blocks (3 rows, height ordered desc)"

# /blocks/2 -> { block: { height: "2", transactions: [2 txs] } }
v=$(curl -fsS "$API_BASE/blocks/2" | jq -r '.block.height')
[[ "$v" == "2" ]] || fail "/blocks/2 height != '2' (got '$v')"
v=$(curl -fsS "$API_BASE/blocks/2" | jq -r '.block.transactions | length')
[[ "$v" == "2" ]] || fail "/blocks/2 tx count != 2 (got $v)"
ok "/blocks/2 (nested transactions)"

# /blocks/999 -> 404
code=$(curl -s -o /dev/null -w '%{http_code}' "$API_BASE/blocks/999")
[[ "$code" == "404" ]] || fail "/blocks/999 != 404 (got $code)"
ok "/blocks/999 returns 404"

# /tx/<hash> -> tx with from_addr renamed to 'from', logs array
v=$(curl -fsS "$API_BASE/tx/0xtxcccc00000000000000000000000000000000000000000000000000000000cc" | jq -r '.tx.from')
[[ "$v" == "0xfeedfacefeedfacefeedfacefeedfacefeedface" ]] || fail "/tx.from rename broken (got '$v')"
v=$(curl -fsS "$API_BASE/tx/0xtxcccc00000000000000000000000000000000000000000000000000000000cc" | jq -r '.logs | length')
[[ "$v" == "2" ]] || fail "/tx logs count != 2 (got $v)"
ok "/tx/:hash (from_addr->from, logs[2])"

# /tx/<unknown> -> 404
code=$(curl -s -o /dev/null -w '%{http_code}' "$API_BASE/tx/0xdeadbeef")
[[ "$code" == "404" ]] || fail "/tx/<unknown> != 404 (got $code)"
ok "/tx/<unknown> returns 404"

# /address/.../txs -> 3 txs (sender on 2, receiver on 1)
v=$(curl -fsS "$API_BASE/address/0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef/txs" | jq -r '.transactions | length')
[[ "$v" == "3" ]] || fail "/address/:addr/txs len != 3 (got $v)"
ok "/address/:addr/txs (sender + receiver union)"

# /address/.../transfers -> 1 transfer
v=$(curl -fsS "$API_BASE/address/0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef/transfers" | jq -r '.transfers | length')
[[ "$v" == "1" ]] || fail "/address/:addr/transfers len != 1 (got $v)"
ok "/address/:addr/transfers"

# /accounts/active -> 3 accounts (3 distinct senders), top sender deadbeef with tx_count=2
v=$(curl -fsS "$API_BASE/accounts/active" | jq -r '.accounts | length')
[[ "$v" == "3" ]] || fail "/accounts/active len != 3 (got $v)"
v=$(curl -fsS "$API_BASE/accounts/active" | jq -r '.accounts[0].tx_count')
[[ "$v" == "2" ]] || fail "/accounts/active top sender tx_count != 2 (got $v)"
ok "/accounts/active (top sender ranked correctly)"

# /whale/transfers -> 4 txs ordered by value DESC, top = txaaaa
v=$(curl -fsS "$API_BASE/whale/transfers" | jq -r '.transfers[0].hash')
[[ "$v" == "0xtxaaaa00000000000000000000000000000000000000000000000000000000aa" ]] || \
    fail "/whale/transfers top != txaaaa (got '$v')"
ok "/whale/transfers (sorted by value)"

# /coinblast/tokens -> 1 curve
v=$(curl -fsS "$API_BASE/coinblast/tokens" | jq -r '.tokens | length')
[[ "$v" == "1" ]] || fail "/coinblast/tokens len != 1 (got $v)"
v=$(curl -fsS "$API_BASE/coinblast/tokens" | jq -r '.tokens[0].symbol')
[[ "$v" == "SMOKE" ]] || fail "/coinblast/tokens[0].symbol != SMOKE (got '$v')"
ok "/coinblast/tokens"

# /coinblast/tokens/<curve> -> detail
v=$(curl -fsS "$API_BASE/coinblast/tokens/0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb" | jq -r '.token.symbol')
[[ "$v" == "SMOKE" ]] || fail "/coinblast/tokens/:curve symbol != SMOKE (got '$v')"
ok "/coinblast/tokens/:curve"

# /coinblast/trades -> 2 trades, newest first (block 3 sell before block 2 buy)
v=$(curl -fsS "$API_BASE/coinblast/trades" | jq -r '.trades | length')
[[ "$v" == "2" ]] || fail "/coinblast/trades len != 2 (got $v)"
v=$(curl -fsS "$API_BASE/coinblast/trades" | jq -r '.trades[0].type')
[[ "$v" == "sell" ]] || fail "/coinblast/trades[0].type != sell (got '$v')"
ok "/coinblast/trades (newest first)"

# /stats/daily -> 3 day rows (each fixture block is 86400s apart = distinct day)
v=$(curl -fsS "$API_BASE/stats/daily" | jq -r '.daily | length')
[[ "$v" == "3" ]] || fail "/stats/daily len != 3 (got $v)"
# Highest bucket should be the newest (block 3 day).
v=$(curl -fsS "$API_BASE/stats/daily" | jq -r '.daily[0].day_bucket | tonumber')
prev=$(curl -fsS "$API_BASE/stats/daily" | jq -r '.daily[1].day_bucket | tonumber')
[[ "$v" -gt "$prev" ]] || fail "/stats/daily not ordered DESC (got $v <= $prev)"
ok "/stats/daily (3 day buckets, ordered DESC)"

# /api?module=account&action=txlist (etherscan compat)
v=$(curl -fsS "$API_BASE/api?module=account&action=txlist&address=0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef" | jq -r '.status')
[[ "$v" == "1" ]] || fail "/api?module=account txlist status != 1 (got '$v')"
v=$(curl -fsS "$API_BASE/api?module=account&action=txlist&address=0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef" | jq -r '.result | length')
[[ "$v" == "3" ]] || fail "/api?module=account txlist result len != 3 (got $v)"
ok "/api?module=account&action=txlist (etherscan envelope)"

# /api?module=block&action=getblocknobytime
v=$(curl -fsS "$API_BASE/api?module=block&action=getblocknobytime&timestamp=1700086400&closest=before" | jq -r '.result')
[[ "$v" == "2" ]] || fail "/api block/getblocknobytime != 2 (got '$v')"
ok "/api?module=block&action=getblocknobytime"

# /api unknown module -> status 0
v=$(curl -fsS "$API_BASE/api?module=foo&action=bar" | jq -r '.status')
[[ "$v" == "0" ]] || fail "/api unknown != status 0 (got '$v')"
ok "/api unknown module returns status=0"

# GraphQL: introspection-like query for blocks.
v=$(curl -fsS -X POST "$API_BASE/graphql" \
        -H 'content-type: application/json' \
        -d '{"query":"{ blocks(first: 1) { height } }"}' | \
    jq -r '.data.blocks[0].height')
[[ "$v" == "3" ]] || fail "graphql blocks(first:1)[0].height != 3 (got '$v')"
ok "POST /graphql blocks(first:1)"

# GraphiQL playground page renders.
code=$(curl -s -o /dev/null -w '%{http_code}' "$API_BASE/graphql/playground")
[[ "$code" == "200" ]] || fail "/graphql/playground != 200 (got $code)"
ok "GET /graphql/playground (200)"

# ── 6. Restart with auth token, verify gating ────────────────────────
note "restarting api with INDEXER_API_BEARER_TOKEN to verify auth"
kill "$API_PID" 2>/dev/null || true
wait "$API_PID" 2>/dev/null || true

DATABASE_URL="$DB_URL" \
INDEXER_API_BIND="127.0.0.1:${API_PORT}" \
INDEXER_API_BEARER_TOKEN="smoke-secret-token-xyz" \
RUST_LOG="warn" \
    ./target/release/api &
API_PID=$!

for _ in $(seq 1 30); do
    if curl -fsS "$API_BASE/health" >/dev/null 2>&1; then break; fi
    sleep 0.5
done
curl -fsS "$API_BASE/health" >/dev/null || fail "api (auth mode) never came up"

# /health bypasses auth.
v=$(curl -fsS "$API_BASE/health" | jq -er '.ok')
[[ "$v" == "true" ]] || fail "/health failed under auth (got '$v')"
ok "/health bypasses auth"

# /blocks without token -> 401
code=$(curl -s -o /dev/null -w '%{http_code}' "$API_BASE/blocks")
[[ "$code" == "401" ]] || fail "/blocks without token != 401 (got $code)"
ok "/blocks without token -> 401"

# /blocks with wrong token -> 401
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer wrong' "$API_BASE/blocks")
[[ "$code" == "401" ]] || fail "/blocks with wrong token != 401 (got $code)"
ok "/blocks with wrong token -> 401"

# /blocks with correct token -> 200
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer smoke-secret-token-xyz' "$API_BASE/blocks")
[[ "$code" == "200" ]] || fail "/blocks with correct token != 200 (got $code)"
ok "/blocks with correct token -> 200"

echo
note "verifying observability + readyz (no auth)"

# Restart API once more without bearer token but WITH metrics + readyz active.
# Per audit 2026-05-13, /metrics now binds on a separate loopback listener
# (INDEXER_API_METRICS_BIND, default 127.0.0.1:9080) so it can never leak
# via the public router. We pick a fresh port + scrape it separately.
METRICS_PORT=$((API_PORT + 1))
METRICS_URL="http://127.0.0.1:${METRICS_PORT}"
kill "$API_PID" 2>/dev/null || true
wait "$API_PID" 2>/dev/null || true
DATABASE_URL="$DB_URL" \
INDEXER_API_BIND="127.0.0.1:${API_PORT}" \
INDEXER_API_METRICS_BIND="127.0.0.1:${METRICS_PORT}" \
RUST_LOG="warn" \
    ./target/release/api &
API_PID=$!
for _ in $(seq 1 30); do
    if curl -fsS "$API_BASE/health" >/dev/null 2>&1; then break; fi
    sleep 0.5
done
curl -fsS "$API_BASE/health" >/dev/null || fail "api never came up after 3rd boot"

# /readyz reports PG ok + cache disabled.
v=$(curl -fsS "$API_BASE/readyz" | jq -er '.ok')
[[ "$v" == "true" ]] || fail "/readyz.ok != true (got '$v')"
v=$(curl -fsS "$API_BASE/readyz" | jq -r '.checks.pg')
[[ "$v" == "ok" ]] || fail "/readyz.checks.pg != ok (got '$v')"
v=$(curl -fsS "$API_BASE/readyz" | jq -r '.checks.cache')
[[ "$v" == "disabled" ]] || fail "/readyz.checks.cache != disabled (got '$v')"
ok "/readyz (pg ok, cache disabled)"

# /metrics is on the loopback-only listener now, not on the public router.
# Drive a couple of requests through the public API first so the counters
# have something to render, then scrape from the metrics port.
curl -fsS "$API_BASE/blocks" >/dev/null
curl -fsS "$API_BASE/blocks" >/dev/null
# Public router must NOT serve /metrics — confirm that's a 404.
code=$(curl -s -o /dev/null -w '%{http_code}' "$API_BASE/metrics")
[[ "$code" == "404" ]] || fail "/metrics leaked on public router (got $code, want 404)"
metrics=$(curl -fsS "$METRICS_URL/metrics") || fail "metrics listener not reachable on $METRICS_URL"
echo "$metrics" | grep -q '^indexer_api_requests_total{' || fail "/metrics missing indexer_api_requests_total"
echo "$metrics" | grep -q '^indexer_api_request_seconds_bucket{' || fail "/metrics missing latency histogram"
ok "/metrics (loopback listener: counters + latency histogram present, public router 404s)"

# request_id header set on every response (generated server-side; clients
# pass through if they want).
id=$(curl -fsS -D - "$API_BASE/health" 2>&1 | tr -d '\r' | awk -F': ' '/^x-request-id/ {print $2}')
[[ -n "$id" ]] || fail "x-request-id header missing from /health response"
ok "x-request-id header echoed ($id)"

# request_id passthrough — client-provided header is preserved server-side.
id2=$(curl -fsS -D - -H "x-request-id: client-supplied-abc" "$API_BASE/health" 2>&1 | tr -d '\r' | awk -F': ' '/^x-request-id/ {print $2}')
[[ "$id2" == "client-supplied-abc" ]] || fail "x-request-id passthrough broken (got '$id2')"
ok "x-request-id passthrough preserves client-supplied value"

# /openapi.json valid JSON containing the expected title.
v=$(curl -fsS "$API_BASE/openapi.json" | jq -er '.info.title')
[[ "$v" == "Sentrix Chain Indexer API" ]] || fail "/openapi.json title mismatch (got '$v')"
ok "/openapi.json valid spec served"

# /docs Swagger UI renders.
code=$(curl -s -o /dev/null -w '%{http_code}' "$API_BASE/docs")
[[ "$code" == "200" ]] || fail "/docs != 200 (got $code)"
ok "/docs (Swagger UI rendered)"

# /blocks list returns next_cursor for pagination.
cur=$(curl -fsS "$API_BASE/blocks?limit=2" | jq -r '.next_cursor')
[[ "$cur" != "null" && -n "$cur" ]] || fail "/blocks?limit=2 next_cursor missing"
ok "/blocks pagination cursor returned ($cur)"

# Page 2 via cursor returns the rest.
v=$(curl -fsS "$API_BASE/blocks?limit=2&before=$cur" | jq -r '.blocks | length')
[[ "$v" -ge "1" ]] || fail "/blocks page 2 via cursor returned 0 rows"
ok "/blocks page 2 via cursor returns more rows"

echo
echo "✓ smoke PASSED — 30+ assertions: REST + GraphQL + auth + readyz + metrics + openapi + cursor + request_id all green"
