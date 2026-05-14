//! Atomic block-write transactions — single-block + bulk-COPY batch paths.
//!
//! Two write modes share one bundle type:
//!
//! - [`write_block`] — single-block path used by the tail loop. Uses per-row
//!   INSERT with `ON CONFLICT DO NOTHING` for at-least-once delivery semantics
//!   at the tip (where the same height may be re-attempted across reconnects).
//! - [`write_block_batch`] — bulk-COPY path used by the backfill loop. Streams
//!   N blocks worth of rows into one PG transaction via `COPY ... FROM STDIN`,
//!   advances the cursor to `MAX(height)` of the batch, and commits. Trades
//!   the `ON CONFLICT` idempotency-on-replay for raw write throughput; the
//!   backfill cursor invariant (monotonic, only advances after commit) means
//!   we never re-walk a height inside a single backfill run anyway.
//!
//! Both paths keep the cursor advance inside the same `sqlx::Transaction` as
//! the data writes, so a crash mid-write leaves the cursor pointing at the
//! previous height with the partial rows rolled back (spec §5 invariants 1, 2).
//!
//! After commit, optionally pushes one [`indexer_analytics::RawTxRow`] per
//! tx into the analytics buffer. The push is fire-and-forget — analytics is
//! observability, not correctness, so a closed channel logs a warning but
//! doesn't fail the write.

use crate::cursor::write_cursor;
use crate::{SyncError, SyncResult};
use indexer_analytics::{AnalyticsHandle, RawTxRow};
use indexer_db::{PgPool, blocks, logs, transactions};
use indexer_domain::{Block, BlockHeight, Log, Transaction};
use sqlx::{Postgres, Transaction as SqlxTransaction};

/// Bundle of rows to write atomically. Built by the sync loop before calling
/// [`write_block`] or buffered for [`write_block_batch`].
pub struct BlockBundle {
    /// The block header row.
    pub block: Block,
    /// All txs in the block, ordered by `tx_index`.
    pub txs: Vec<Transaction>,
    /// All logs emitted during the block's txs, ordered by `log_index`.
    pub logs: Vec<Log>,
}

/// Write a block bundle + advance the chain-wide cursor in one transaction.
/// `analytics` is optional — when wired, each tx in the bundle gets pushed
/// to the analytics buffer after the SQL commit.
///
/// Returns Ok on commit. Returns Err with the underlying sqlx/db error on
/// rollback — the cursor stays at its previous value, the writer can retry
/// the same height.
pub async fn write_block(
    pool: &PgPool,
    b: BlockBundle,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<()> {
    let mut tx = pool.begin().await.map_err(SyncError::from)?;

    // Order matters: blocks first (FK target), then transactions (FK target
    // for logs), then logs.
    blocks::insert(&mut *tx, &b.block).await?;
    for t in &b.txs {
        transactions::insert(&mut *tx, t).await?;
    }
    for l in &b.logs {
        logs::insert(&mut *tx, l).await?;
    }

    // Cursor advance shares the transaction so it lands or rolls back with
    // the data. `now_ts` = the block's chain timestamp so cursor staleness
    // is comparable to chain time.
    write_cursor(&mut *tx, b.block.height, b.block.timestamp).await?;

    tx.commit().await.map_err(SyncError::from)?;

    push_analytics_for_block(analytics, &b.block, &b.txs);

    Ok(())
}

/// Write a batch of blocks via PG `COPY FROM STDIN` — one transaction, three
/// COPY streams (blocks → transactions → logs in FK order), one cursor bump
/// to `MAX(height)`. Drains the buffer on success or returns Err and leaves
/// the buffer untouched on failure (caller decides retry).
///
/// Cursor advance lives inside the same transaction as the data, so a crash
/// mid-batch rolls everything back and the cursor stays at the previous
/// batch's MAX. Spec §5 invariant 2 (cursor never lands ahead of data) holds.
///
/// `cursor_only_heights` lets the backfill loop fold 404 / damaged-block
/// gaps into the batch without extra round-trips: heights listed here have
/// no rows of their own but contribute to the cursor max so the gap doesn't
/// stall the batch boundary.
pub async fn write_block_batch(
    pool: &PgPool,
    bundles: &mut Vec<BlockBundle>,
    cursor_only_heights: &mut Vec<BlockHeight>,
    analytics: Option<&AnalyticsHandle>,
) -> SyncResult<()> {
    if bundles.is_empty() && cursor_only_heights.is_empty() {
        return Ok(());
    }

    // Pick the max-height block whose timestamp seeds the cursor's `updated_at`
    // (best chain-time we have for this batch). If the batch is cursor-only
    // (all 404 gaps), we have no timestamp — fall back to 0 like the per-row
    // gap-skip path does.
    let (cursor_height, cursor_ts) = batch_cursor(bundles, cursor_only_heights);

    let mut tx = pool.begin().await.map_err(SyncError::from)?;

    // FK ordering: blocks first, then transactions (FK → blocks), then logs
    // (FK → transactions). Each COPY drains its slice in one round-trip.
    if !bundles.is_empty() {
        copy_blocks(&mut tx, bundles).await?;
        copy_transactions(&mut tx, bundles).await?;
        copy_logs(&mut tx, bundles).await?;
    }

    write_cursor(&mut *tx, cursor_height, cursor_ts).await?;

    tx.commit().await.map_err(SyncError::from)?;

    // Best-effort analytics push, after the SQL boundary.
    if let Some(handle) = analytics {
        for b in bundles.iter() {
            push_analytics_for_block(Some(handle), &b.block, &b.txs);
        }
    }

    bundles.clear();
    cursor_only_heights.clear();
    Ok(())
}

fn batch_cursor(
    bundles: &[BlockBundle],
    cursor_only: &[BlockHeight],
) -> (BlockHeight, i64) {
    // Track the highest block + its timestamp; cursor-only heights still
    // contribute to the max but carry no timestamp.
    let mut max_h = BlockHeight(i64::MIN);
    let mut max_ts = 0i64;
    for b in bundles {
        if b.block.height.0 > max_h.0 {
            max_h = b.block.height;
            max_ts = b.block.timestamp;
        }
    }
    for h in cursor_only {
        if h.0 > max_h.0 {
            max_h = *h;
            // No timestamp for a 404-only height; mirror the per-row skip
            // path which also writes 0 here.
            max_ts = 0;
        }
    }
    (max_h, max_ts)
}

// ─── COPY helpers ────────────────────────────────────────────────────────────
//
// All three use PG text COPY format (default) — tab-separated columns, `\N`
// for NULL, with backslash / tab / newline / CR escaping per
// <https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.2>.
// Text format handles `numeric(78,0)` (decimal string), `jsonb` (UTF-8), and
// every other column type in our schema without per-type wire encoders.
//
// We intentionally COPY directly into the target tables (not a staging temp
// table + INSERT … ON CONFLICT). The backfill cursor invariant guarantees we
// never re-walk a height inside one run, and the entire batch is one
// transaction — so duplicates inside a batch are impossible. Replay across
// runs from a manually-rewound cursor would conflict, but operators in that
// situation already truncate downstream tables. The tail loop's `write_block`
// keeps the per-row `ON CONFLICT DO NOTHING` path for at-tip re-attempts.

async fn copy_blocks(
    tx: &mut SqlxTransaction<'_, Postgres>,
    bundles: &[BlockBundle],
) -> SyncResult<()> {
    let mut buf = String::with_capacity(bundles.len() * 256);
    for b in bundles {
        let blk = &b.block;
        // jsonb: serde_json gives compact UTF-8 with no tabs/newlines inside
        // (arrays of hex strings); still pass through `escape_text` to be safe
        // if a future justifier value ever picks up control bytes.
        let signers = serde_json::Value::Array(
            blk.justification_signers
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        );
        let signers_str = signers.to_string();

        // Columns: height, hash, parent_hash, timestamp, validator, gas_used,
        // gas_limit, base_fee, tx_count, state_root, round, justification_signers.
        write_int(&mut buf, blk.height.0);
        buf.push('\t');
        write_text(&mut buf, &blk.hash);
        buf.push('\t');
        write_text(&mut buf, &blk.parent_hash);
        buf.push('\t');
        write_int(&mut buf, blk.timestamp);
        buf.push('\t');
        write_text(&mut buf, &blk.validator);
        buf.push('\t');
        write_int(&mut buf, blk.gas_used);
        buf.push('\t');
        write_int(&mut buf, blk.gas_limit);
        buf.push('\t');
        match &blk.base_fee {
            Some(w) => write_text(&mut buf, &w.to_string()),
            None => buf.push_str("\\N"),
        }
        buf.push('\t');
        write_int(&mut buf, blk.tx_count as i64);
        buf.push('\t');
        match &blk.state_root {
            Some(s) => write_text(&mut buf, s),
            None => buf.push_str("\\N"),
        }
        buf.push('\t');
        write_int(&mut buf, blk.round as i64);
        buf.push('\t');
        write_text(&mut buf, &signers_str);
        buf.push('\n');
    }

    let mut copy = tx
        .copy_in_raw(
            "COPY blocks (height, hash, parent_hash, timestamp, validator, gas_used, \
                gas_limit, base_fee, tx_count, state_root, round, justification_signers) \
             FROM STDIN",
        )
        .await
        .map_err(SyncError::from)?;
    copy.send(buf.as_bytes()).await.map_err(SyncError::from)?;
    copy.finish().await.map_err(SyncError::from)?;
    Ok(())
}

async fn copy_transactions(
    tx: &mut SqlxTransaction<'_, Postgres>,
    bundles: &[BlockBundle],
) -> SyncResult<()> {
    // Pre-size: most blocks have 1 tx (coinbase), occasional bursts higher.
    let estimate: usize = bundles.iter().map(|b| b.txs.len()).sum();
    if estimate == 0 {
        // Empty COPY is allowed but skip the round-trip.
        return Ok(());
    }
    let mut buf = String::with_capacity(estimate * 256);
    for b in bundles {
        for t in &b.txs {
            write_text(&mut buf, &t.hash);
            buf.push('\t');
            write_int(&mut buf, t.block_height.0);
            buf.push('\t');
            write_int(&mut buf, t.tx_index.0 as i64);
            buf.push('\t');
            write_text(&mut buf, &t.from_addr);
            buf.push('\t');
            match &t.to_addr {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            write_text(&mut buf, &t.value.to_string());
            buf.push('\t');
            write_int(&mut buf, t.gas_limit);
            buf.push('\t');
            match t.gas_used {
                Some(g) => write_int(&mut buf, g),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            match &t.gas_price {
                Some(w) => write_text(&mut buf, &w.to_string()),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            write_text(&mut buf, &t.fee.to_string());
            buf.push('\t');
            write_int(&mut buf, t.nonce);
            buf.push('\t');
            match &t.data {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            write_int(&mut buf, t.status as i64);
            buf.push('\t');
            match &t.contract_address {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            write_text(&mut buf, t.tx_type.as_str());
            buf.push('\n');
        }
    }
    let mut copy = tx
        .copy_in_raw(
            "COPY transactions (hash, block_height, tx_index, from_addr, to_addr, value, \
                gas_limit, gas_used, gas_price, fee, nonce, data, status, contract_address, \
                tx_type) FROM STDIN",
        )
        .await
        .map_err(SyncError::from)?;
    copy.send(buf.as_bytes()).await.map_err(SyncError::from)?;
    copy.finish().await.map_err(SyncError::from)?;
    Ok(())
}

async fn copy_logs(
    tx: &mut SqlxTransaction<'_, Postgres>,
    bundles: &[BlockBundle],
) -> SyncResult<()> {
    let estimate: usize = bundles.iter().map(|b| b.logs.len()).sum();
    if estimate == 0 {
        return Ok(());
    }
    let mut buf = String::with_capacity(estimate * 256);
    for b in bundles {
        for l in &b.logs {
            write_int(&mut buf, l.block_height.0);
            buf.push('\t');
            write_text(&mut buf, &l.tx_hash);
            buf.push('\t');
            write_int(&mut buf, l.log_index.0 as i64);
            buf.push('\t');
            write_text(&mut buf, &l.address);
            buf.push('\t');
            match &l.topic0 {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            match &l.topic1 {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            match &l.topic2 {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            match &l.topic3 {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\t');
            match &l.data {
                Some(s) => write_text(&mut buf, s),
                None => buf.push_str("\\N"),
            }
            buf.push('\n');
        }
    }
    let mut copy = tx
        .copy_in_raw(
            "COPY logs (block_height, tx_hash, log_index, address, topic0, topic1, topic2, \
                topic3, data) FROM STDIN",
        )
        .await
        .map_err(SyncError::from)?;
    copy.send(buf.as_bytes()).await.map_err(SyncError::from)?;
    copy.finish().await.map_err(SyncError::from)?;
    Ok(())
}

/// Write a numeric column without allocation.
#[inline]
fn write_int(buf: &mut String, v: i64) {
    use std::fmt::Write;
    let _ = write!(buf, "{v}");
}

/// Write a text column with PG COPY text-format escaping. Only four bytes
/// need escaping in default text format: backslash, tab, newline, CR.
/// `\b` / `\f` / `\v` are legal raw inside a field. Empty string stays
/// empty (not NULL).
fn write_text(buf: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            other => buf.push(other),
        }
    }
}

fn push_analytics_for_block(
    analytics: Option<&AnalyticsHandle>,
    block: &Block,
    txs: &[Transaction],
) {
    let Some(handle) = analytics else { return };
    // Should never hit (writer only runs on heights coming back from
    // resolved blocks, never the -1 sentinel from the cursor) but keep
    // analytics non-fatal: warn + skip the row, don't panic the loop.
    let block_height = match block.height.as_u64() {
        Some(h) => h,
        None => {
            tracing::warn!(
                height = ?block.height,
                "analytics: skipping row — block height not convertible to u64 \
                 (cursor sentinel reached writer; this should not happen)"
            );
            return;
        }
    };
    for t in txs {
        let row = RawTxRow {
            block_height,
            timestamp: block.timestamp as u64,
            tx_hash: t.hash.clone(),
            from_addr: t.from_addr.clone(),
            to_addr: t.to_addr.clone(),
            value_str: t.value.to_string(),
            fee_str: t.fee.to_string(),
            gas_used: t.gas_used.unwrap_or(0) as u64,
            status: t.status as u8,
            tx_type: t.tx_type.as_str().to_string(),
        };
        if let Err(e) = handle.push(row) {
            tracing::warn!(error = %e, "analytics push failed; flusher closed?");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_text_escapes_pg_copy_specials() {
        let mut buf = String::new();
        write_text(&mut buf, "plain");
        assert_eq!(buf, "plain");

        buf.clear();
        write_text(&mut buf, "tab\there");
        assert_eq!(buf, "tab\\there");

        buf.clear();
        write_text(&mut buf, "back\\slash");
        assert_eq!(buf, "back\\\\slash");

        buf.clear();
        write_text(&mut buf, "line\nfeed");
        assert_eq!(buf, "line\\nfeed");

        buf.clear();
        write_text(&mut buf, "carriage\rreturn");
        assert_eq!(buf, "carriage\\rreturn");

        // jsonb-shaped text passes through untouched (no specials):
        buf.clear();
        write_text(&mut buf, r#"["0xabc","0xdef"]"#);
        assert_eq!(buf, r#"["0xabc","0xdef"]"#);
    }

    #[test]
    fn batch_cursor_picks_max_height_with_timestamp() {
        // Three real blocks at heights 10/12/11 + a 404-skipped height 13.
        // Max is 13 (cursor-only) so timestamp falls back to 0.
        let mk = |h: i64, ts: i64| BlockBundle {
            block: Block {
                height: BlockHeight(h),
                hash: format!("0xh{h}"),
                parent_hash: format!("0xp{h}"),
                timestamp: ts,
                validator: "0xv".into(),
                gas_used: 0,
                gas_limit: 0,
                base_fee: None,
                tx_count: 0,
                state_root: None,
                round: 0,
                justification_signers: vec![],
            },
            txs: vec![],
            logs: vec![],
        };
        let bundles = vec![mk(10, 100), mk(12, 120), mk(11, 110)];
        let gaps = vec![BlockHeight(13)];
        let (h, ts) = batch_cursor(&bundles, &gaps);
        assert_eq!(h.0, 13);
        assert_eq!(ts, 0); // 404 height has no timestamp.

        // Without the gap, max is 12 with timestamp 120.
        let (h, ts) = batch_cursor(&bundles, &[]);
        assert_eq!(h.0, 12);
        assert_eq!(ts, 120);
    }

    #[test]
    fn batch_cursor_handles_empty_bundles_with_only_gaps() {
        let bundles: Vec<BlockBundle> = vec![];
        let gaps = vec![BlockHeight(7), BlockHeight(9), BlockHeight(8)];
        let (h, ts) = batch_cursor(&bundles, &gaps);
        assert_eq!(h.0, 9);
        assert_eq!(ts, 0);
    }
}
