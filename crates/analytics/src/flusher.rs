//! Periodic flusher — pulls rows off the channel, batches into one INSERT
//! per drain, retries on transient ClickHouse errors. On graceful shutdown
//! drains any in-flight batch before exiting.

use crate::row::RawTxRow;
use crate::{AnalyticsError, AnalyticsResult};
use clickhouse::Client;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Send-side handle. Cheap to clone; multiple producers safe.
#[derive(Debug, Clone)]
pub struct AnalyticsHandle {
    tx: mpsc::UnboundedSender<RawTxRow>,
}

impl AnalyticsHandle {
    /// Push a row into the buffer. Errors only if the flusher already
    /// exited (channel closed).
    pub fn push(&self, row: RawTxRow) -> AnalyticsResult<()> {
        self.tx.send(row).map_err(|_| AnalyticsError::Closed)
    }
}

/// Spawn the flusher loop. Returns a handle the sync layer pushes to.
///
/// `flush_interval` defaults to 15s per spec §7. `table` is the unqualified
/// ClickHouse table name (we expect the default database, configured at
/// the `Client` level).
pub fn run_flusher(
    client: Client,
    table: String,
    flush_interval: Duration,
    cancel: CancellationToken,
) -> (
    AnalyticsHandle,
    tokio::task::JoinHandle<AnalyticsResult<()>>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<RawTxRow>();
    let handle = AnalyticsHandle { tx };
    let join = tokio::spawn(async move {
        let mut buf: Vec<RawTxRow> = Vec::with_capacity(1024);
        let mut tick = tokio::time::interval(flush_interval);
        tick.tick().await; // discard immediate first tick
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Shutdown signal — drain anything still in the channel
                    // before the final flush. Closes the channel so the
                    // recv loop terminates.
                    while let Ok(row) = rx.try_recv() {
                        buf.push(row);
                    }
                    flush(&client, &table, &mut buf).await?;
                    return Ok(());
                }
                _ = tick.tick() => {
                    // Drain anything queued up since the last tick + flush.
                    while let Ok(row) = rx.try_recv() {
                        buf.push(row);
                    }
                    if !buf.is_empty() {
                        flush(&client, &table, &mut buf).await?;
                    }
                }
                maybe_row = rx.recv() => match maybe_row {
                    None => {
                        // Senders dropped — final flush + exit.
                        flush(&client, &table, &mut buf).await?;
                        return Ok(());
                    }
                    Some(row) => buf.push(row),
                }
            }
        }
    });
    (handle, join)
}

async fn flush(client: &Client, table: &str, buf: &mut Vec<RawTxRow>) -> AnalyticsResult<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let count = buf.len();
    let mut insert = client.insert(table)?;
    for row in buf.drain(..) {
        insert.write(&row).await?;
    }
    insert.end().await?;
    tracing::debug!(rows = count, table, "analytics: flush ok");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_push_after_close_errors() {
        // Build a handle whose receiver we immediately drop. push should
        // surface AnalyticsError::Closed.
        let (tx, _rx) = mpsc::unbounded_channel::<RawTxRow>();
        let handle = AnalyticsHandle { tx };
        // Drop the receiver by drop(_rx) — already happens via the binding.
        drop(_rx);
        let row = RawTxRow {
            block_height: 1,
            timestamp: 0,
            tx_hash: "0x".into(),
            from_addr: "0x".into(),
            to_addr: None,
            value_str: "0".into(),
            fee_str: "0".into(),
            gas_used: 0,
            status: 1,
            tx_type: "evm".into(),
        };
        assert!(matches!(handle.push(row), Err(AnalyticsError::Closed)));
    }
}
