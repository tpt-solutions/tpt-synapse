//! gRPC server wiring for the workflow matching service.

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::dispatch::TaskQueueManager;
use crate::service::MatchingServiceImpl;

/// Bind the matching service to `addr` (use `127.0.0.1:0` to get an ephemeral
/// port) and serve until the returned [`ServerHandle`] is dropped or aborted.
///
/// Returns the bound [`SocketAddr`] so callers can dial the assigned port.
pub async fn spawn(
    manager: std::sync::Arc<TaskQueueManager>,
    addr: SocketAddr,
) -> std::io::Result<(SocketAddr, ServerHandle)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);
    let service = MatchingServiceImpl::new(manager);
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(
                crate::proto::synapse::workflow::v1::matching_service_server::MatchingServiceServer::new(service),
            )
            .serve_with_incoming(incoming)
            .await;
    });
    Ok((local, ServerHandle { handle }))
}

/// A running matching-service server. Dropping it aborts the serve task.
pub struct ServerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// Stop the server.
    pub fn abort(&self) {
        self.handle.abort();
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
