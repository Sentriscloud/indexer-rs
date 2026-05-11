//! tonic gRPC client wrapper around the generated `sentrix.v1.Sentrix` stub.
//!
//! Phase 2 surface: open a connection, expose `StreamEvents` for the tail
//! loop. `GetBlock` / `GetBalance` are also generated but mostly redundant
//! with the alloy provider; we keep them available via [`Self::raw`] for
//! callers that want them without going through HTTP.

use crate::error::{ChainError, ChainResult};
use crate::pb;
use tonic::Streaming;
use tonic::transport::{Channel, Endpoint};

type SentrixGrpc = pb::sentrix_client::SentrixClient<Channel>;

/// gRPC client keyed to one Sentrix node endpoint.
#[derive(Clone)]
pub struct GrpcClient {
    inner: SentrixGrpc,
}

impl GrpcClient {
    /// Connect to a `sentrix.v1.Sentrix` server.
    ///
    /// `url` accepts the tonic schemes (`http://`, `https://`). For TLS, the
    /// caller passes an `https://` URL; the workspace's tonic feature set
    /// includes `tls-roots` so system roots are picked up automatically.
    pub async fn connect(url: impl Into<String>) -> ChainResult<Self> {
        let endpoint = Endpoint::from_shared(url.into())
            .map_err(|e| ChainError::InvalidArgument(format!("bad grpc url: {e}")))?;
        let channel = endpoint.connect().await?;
        Ok(Self {
            inner: pb::sentrix_client::SentrixClient::new(channel),
        })
    }

    /// Underlying tonic client. Useful for advanced calls (e.g. setting a
    /// per-request deadline) without re-wrapping every method.
    pub fn raw(&mut self) -> &mut SentrixGrpc {
        &mut self.inner
    }

    /// Subscribe to the chain's event stream. Yields one [`pb::ChainEvent`]
    /// per finalized block / mempool admit / round status / etc., depending
    /// on the server's `EventBus` configuration + the request's filter set.
    pub async fn stream_events(
        &mut self,
        req: pb::StreamEventsRequest,
    ) -> ChainResult<Streaming<pb::ChainEvent>> {
        let resp = self.inner.stream_events(req).await?;
        Ok(resp.into_inner())
    }
}
