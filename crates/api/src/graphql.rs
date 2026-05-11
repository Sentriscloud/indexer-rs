//! GraphQL schema (async-graphql). Read-only surface mirroring the REST
//! routes' logical capabilities — `block(height)`, `blocks(first, before)`,
//! `transaction(hash)`. Field shapes use the same string/snake-case
//! conventions as REST so frontends can switch transport without
//! re-shaping payloads.

use crate::SharedState;
use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use indexer_db::{blocks, logs, transactions};
use indexer_domain::{BlockHeight, Wei};

/// Indexer GraphQL schema alias.
pub type IndexerSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Build the schema. State enters via `data()`.
pub fn build_schema(state: SharedState) -> IndexerSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(state)
        .finish()
}

/// Mount `POST /graphql` handler + `GET /graphql/playground` UI.
pub fn router(schema: IndexerSchema) -> Router<SharedState> {
    Router::new()
        .route("/graphql", post(handle))
        .route("/graphql/playground", get(playground))
        .with_state(schema)
}

async fn handle(State(schema): State<IndexerSchema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn playground() -> Html<String> {
    Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}

// ── schema types ────────────────────────────────────────────────────────

/// Block — bigint heights / timestamps / gas render as strings (matches REST).
#[derive(Debug, SimpleObject)]
pub struct BlockGql {
    /// Decimal-string height.
    pub height: String,
    /// 0x-prefixed hash.
    pub hash: String,
    /// 0x-prefixed parent hash.
    pub parent_hash: String,
    /// Decimal-string Unix seconds.
    pub timestamp: String,
    /// 0x-prefixed proposer address.
    pub validator: String,
    /// Decimal-string total gas used.
    pub gas_used: String,
    /// Decimal-string block gas limit.
    pub gas_limit: String,
    /// Decimal-string EIP-1559 base fee. Null pre-fork.
    pub base_fee: Option<String>,
    /// Number of txs.
    pub tx_count: i32,
    /// 0x-prefixed state root. Null pre state-root-fork.
    pub state_root: Option<String>,
    /// BFT round.
    pub round: i32,
}

impl From<indexer_domain::Block> for BlockGql {
    fn from(b: indexer_domain::Block) -> Self {
        Self {
            height: b.height.0.to_string(),
            hash: b.hash,
            parent_hash: b.parent_hash,
            timestamp: b.timestamp.to_string(),
            validator: b.validator,
            gas_used: b.gas_used.to_string(),
            gas_limit: b.gas_limit.to_string(),
            base_fee: b.base_fee.map(|w| w.to_string()),
            tx_count: b.tx_count,
            state_root: b.state_root,
            round: b.round,
        }
    }
}

/// Transaction.
#[derive(Debug, SimpleObject)]
pub struct TransactionGql {
    /// 0x-prefixed tx hash.
    pub hash: String,
    /// Decimal-string block height.
    pub block_height: String,
    /// Position within block.
    pub tx_index: i32,
    /// 0x-prefixed sender (renamed from `from_addr`).
    pub from: String,
    /// 0x-prefixed receiver (renamed from `to_addr`).
    pub to: Option<String>,
    /// Decimal-string `numeric(78, 0)` value.
    pub value: String,
    /// Decimal-string gas limit.
    pub gas_limit: String,
    /// Decimal-string gas used. Null until receipt observed.
    pub gas_used: Option<String>,
    /// Decimal-string `numeric(78, 0)` gas price.
    pub gas_price: Option<String>,
    /// Decimal-string `numeric(78, 0)` fee.
    pub fee: String,
    /// Decimal-string nonce.
    pub nonce: String,
    /// Hex calldata.
    pub data: Option<String>,
    /// 0 = failed, 1 = success.
    pub status: i32,
    /// Address of contract created (CREATE / CREATE2).
    pub contract_address: Option<String>,
    /// native | evm | system | coinbase.
    pub tx_type: String,
}

impl From<indexer_domain::Transaction> for TransactionGql {
    fn from(t: indexer_domain::Transaction) -> Self {
        let wei_str = |w: Wei| w.to_string();
        Self {
            hash: t.hash,
            block_height: t.block_height.0.to_string(),
            tx_index: t.tx_index.0,
            from: t.from_addr,
            to: t.to_addr,
            value: wei_str(t.value),
            gas_limit: t.gas_limit.to_string(),
            gas_used: t.gas_used.map(|n| n.to_string()),
            gas_price: t.gas_price.map(wei_str),
            fee: wei_str(t.fee),
            nonce: t.nonce.to_string(),
            data: t.data,
            status: t.status as i32,
            contract_address: t.contract_address,
            tx_type: t.tx_type.as_str().to_string(),
        }
    }
}

/// Log.
#[derive(Debug, SimpleObject)]
pub struct LogGql {
    /// Decimal-string block height.
    pub block_height: String,
    /// 0x-prefixed tx hash.
    pub tx_hash: String,
    /// Block-wide log index.
    pub log_index: i32,
    /// Emitting contract.
    pub address: String,
    /// Non-null topics in topic0..topic3 order.
    pub topics: Vec<String>,
    /// Hex payload.
    pub data: Option<String>,
}

impl From<indexer_domain::Log> for LogGql {
    fn from(l: indexer_domain::Log) -> Self {
        let topics = [&l.topic0, &l.topic1, &l.topic2, &l.topic3]
            .iter()
            .filter_map(|t| t.as_ref().cloned())
            .collect();
        Self {
            block_height: l.block_height.0.to_string(),
            tx_hash: l.tx_hash,
            log_index: l.log_index.0,
            address: l.address,
            topics,
            data: l.data,
        }
    }
}

/// Tx + nested logs (returned by `transaction(hash)`).
#[derive(Debug, SimpleObject)]
pub struct TransactionWithLogs {
    /// The transaction.
    pub tx: TransactionGql,
    /// Logs emitted by this tx, ordered by `log_index`.
    pub logs: Vec<LogGql>,
}

/// Root Query.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Single block by height.
    async fn block(
        &self,
        ctx: &Context<'_>,
        height: i64,
    ) -> async_graphql::Result<Option<BlockGql>> {
        let state = ctx.data::<SharedState>()?.clone();
        let row = blocks::get_by_height(&state.pool, BlockHeight(height))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(row.map(BlockGql::from))
    }

    /// Latest blocks. `first` clamped 1..=100, default 25. `before`
    /// (inclusive upper bound on height) walks pages backwards.
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        before: Option<i64>,
    ) -> async_graphql::Result<Vec<BlockGql>> {
        let state = ctx.data::<SharedState>()?.clone();
        let limit = first.map_or(25, |n| n.clamp(1, 100)) as i64;
        let rows = blocks::list_before(&state.pool, before.map(BlockHeight), limit)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(rows.into_iter().map(BlockGql::from).collect())
    }

    /// Single transaction by hash, with its logs. Hash is lowercased.
    async fn transaction(
        &self,
        ctx: &Context<'_>,
        hash: String,
    ) -> async_graphql::Result<Option<TransactionWithLogs>> {
        let state = ctx.data::<SharedState>()?.clone();
        let hash = hash.to_lowercase();
        let tx = transactions::get_by_hash(&state.pool, &hash)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        let Some(tx) = tx else {
            return Ok(None);
        };
        let log_rows = logs::for_tx(&state.pool, &hash)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(Some(TransactionWithLogs {
            tx: tx.into(),
            logs: log_rows.into_iter().map(LogGql::from).collect(),
        }))
    }
}

/// Convenience — silence dead-code warnings on the Json import when the
/// compile re-orders use blocks during axum router merges in tests. (Real
/// Json import lives in routes/* modules.)
#[allow(dead_code)]
fn _force_json_used(_: Json<()>) {}
