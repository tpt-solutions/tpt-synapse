//! Minimal HTTP listener serving the Prometheus `/metrics` endpoint
//! (TODO.md Phase 1). `metrics::Metrics` already owns the registry and text
//! exposition rendering; this module is just the network front door — small
//! enough not to warrant pulling in a full HTTP framework (hyper/axum) for a
//! single GET route.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::error::{EngineError, EngineResult};
use crate::io_uring::accept_loop_tokio;
use crate::metrics::Metrics;

/// Bind a TCP listener and serve `/metrics` (any other path gets 404) until
/// the returned task is aborted. Returns the bound address (useful when
/// `addr`'s port is 0, e.g. in tests) and the server's join handle.
///
/// The accept loop runs on the shared async I/O accept-loop abstraction in
/// `io_uring.rs` (see [`crate::io_uring::current`] for backend selection:
/// `tokio` everywhere, or `io_uring` on Linux with the `io-uring` feature).
/// Adapters share this same accept-loop shape, so the data plane's network
/// backend is a single build-time decision rather than something each protocol
/// re-implements.
pub async fn spawn_metrics_server(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
) -> EngineResult<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| EngineError::internal(format!("bind metrics listener: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| EngineError::internal(format!("metrics listener local_addr: {e}")))?;

    let handle = tokio::spawn(accept_loop_tokio(listener, move |socket| {
        let metrics = metrics.clone();
        async move {
            let _ = handle_connection(socket, metrics).await;
        }
    }));

    Ok((local_addr, handle))
}

async fn handle_connection(mut socket: tokio::net::TcpStream, metrics: Arc<Metrics>) -> std::io::Result<()> {
    let mut buf = [0u8; 8192];
    let n = socket.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);
    let request_line = request.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    let response = if method == "GET" && path == "/metrics" {
        match metrics.render() {
            Ok(body) => http_response(200, "OK", "text/plain; version=0.0.4", &body),
            Err(e) => http_response(500, "Internal Server Error", "text/plain", &e.to_string()),
        }
    } else {
        http_response(404, "Not Found", "text/plain", "not found")
    };

    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await?;
    Ok(())
}

fn http_response(status: u16, reason: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        status = status,
        reason = reason,
        content_type = content_type,
        len = body.len(),
        body = body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn serves_metrics_over_http() {
        let metrics = Arc::new(Metrics::new());
        metrics.routing_op("acme", "topic");

        let (addr, handle) = spawn_metrics_server("127.0.0.1:0".parse().unwrap(), metrics)
            .await
            .unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();

        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("synapse_routing_ops_total"));
        assert!(resp.contains("acme"));

        handle.abort();
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let metrics = Arc::new(Metrics::new());
        let (addr, handle) = spawn_metrics_server("127.0.0.1:0".parse().unwrap(), metrics)
            .await
            .unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /nope HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();

        assert!(resp.starts_with("HTTP/1.1 404 Not Found"));

        handle.abort();
    }
}
