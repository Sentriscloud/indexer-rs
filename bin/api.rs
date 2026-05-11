//! sentrix-indexer-rs — HTTP API server
//!
//! Phase 0 scaffold: prints version and exits. Real entry lands in Phase 5.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "api scaffold up");
    Ok(())
}
