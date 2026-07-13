//! Broker admin HTTP API for Synapse Studio (TODO.md "Adoption & Tooling").
//!
//! Serves a JSON [`CoreSnapshot`] of every tenant's logs/queues/maps plus a
//! Server-Sent Events (SSE) live tail of mutations, so the web UI can browse
//! topics/queues/keys and watch messages arrive without polling. The accept
//! loop reuses the shared async I/O backend in [`crate::io_uring`]; TLS is
//! optional and shared with the protocol adapters via [`crate::tls`].

use std::net::SocketAddr;
use std::sync::Arc;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::engine::{ResourceKind, SynapseCore};
use crate::error::{EngineError, EngineResult};
use crate::metrics::Metrics;

/// A browsable snapshot of one log.
#[derive(Debug, Clone, Serialize)]
pub struct LogInfo {
    pub name: String,
    pub len: u64,
    /// Up to the last 10 record payloads, base64-previewed (capped at 64 bytes).
    pub sample: Vec<String>,
}

/// A browsable snapshot of one queue.
#[derive(Debug, Clone, Serialize)]
pub struct QueueInfo {
    pub name: String,
    pub depth: u64,
    /// Outstanding messages as `(seq, base64-preview)` pairs.
    pub sample: Vec<(u64, String)>,
}

/// A browsable snapshot of one map.
#[derive(Debug, Clone, Serialize)]
pub struct MapInfo {
    pub name: String,
    pub size: u64,
    /// Live keys as `(key, base64-preview)` pairs.
    pub keys: Vec<(String, String)>,
}

/// All primitives owned by one tenant.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshot {
    pub logs: Vec<LogInfo>,
    pub queues: Vec<QueueInfo>,
    pub maps: Vec<MapInfo>,
}

/// One tenant and its resources.
#[derive(Debug, Clone, Serialize)]
pub struct TenantSnapshot {
    pub tenant: String,
    pub resources: ResourceSnapshot,
}

/// The full broker snapshot returned by `GET /api/snapshot`.
#[derive(Debug, Clone, Serialize)]
pub struct CoreSnapshot {
    pub tenants: Vec<TenantSnapshot>,
}

fn kind_str(k: ResourceKind) -> &'static str {
    match k {
        ResourceKind::Log => "log",
        ResourceKind::Queue => "queue",
        ResourceKind::Map => "map",
    }
}

/// Bind a TCP listener and serve the admin API until the returned task is
/// aborted. Returns the bound address (useful with port 0) and the handle.
pub async fn spawn_admin_server(
    addr: SocketAddr,
    core: Arc<SynapseCore>,
    metrics: Arc<Metrics>,
) -> EngineResult<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| EngineError::internal(format!("bind admin listener: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| EngineError::internal(format!("admin listener local_addr: {e}")))?;

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut sock, _)) => {
                    let core = core.clone();
                    let metrics = metrics.clone();
                    tokio::spawn(async move {
                        let _ = handle_connection(&mut sock, &core, &metrics).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    Ok((local_addr, handle))
}

async fn handle_connection(
    sock: &mut tokio::net::TcpStream,
    core: &SynapseCore,
    metrics: &Metrics,
) -> std::io::Result<()> {
    let mut buf = [0u8; 8192];
    let n = sock.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);
    let request_line = request.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    match (method, path) {
        ("GET", "/api/snapshot") => {
            let snap = core.snapshot();
            let body = serde_json::to_string(&snap).unwrap_or_else(|_| "{}".into());
            sock.write_all(http_response(200, "OK", "application/json", &body).as_bytes())
                .await?;
        }
        ("GET", "/api/tail") => {
            // SSE: keep the connection open and stream mutations.
            sock.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
            )
            .await?;
            let mut rx = core.subscribe();
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        let payload = serde_json::json!({
                            "tenant": ev.tenant,
                            "kind": kind_str(ev.kind),
                            "name": ev.name,
                            "key": ev.key,
                            "preview": ev.preview,
                        });
                        let line = format!("data: {}\n\n", payload);
                        if sock.write_all(line.as_bytes()).await.is_err() {
                            break;
                        }
                        let _ = sock.flush().await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        }
        ("GET", "/metrics") => match metrics.render() {
            Ok(body) => {
                sock.write_all(
                    http_response(200, "OK", "text/plain; version=0.0.4", &body).as_bytes(),
                )
                .await?;
            }
            Err(e) => {
                sock.write_all(
                    http_response(500, "Internal Server Error", "text/plain", &e.to_string())
                        .as_bytes(),
                )
                .await?;
            }
        },
        _ => {
            sock.write_all(http_response(404, "Not Found", "text/plain", "not found").as_bytes())
                .await?;
        }
    }
    sock.shutdown().await
}

fn http_response(status: u16, reason: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {len}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}",
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
    use crate::engine::SynapseCore;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn serves_snapshot_over_http() {
        let core = Arc::new(SynapseCore::new());
        core.create_log("acme", "events").unwrap();
        core.log_append("acme", "events", b"hello").unwrap();
        core.create_queue("acme", "jobs").unwrap();
        core.queue_enqueue("acme", "jobs", b"task").unwrap();
        core.create_map("acme", "cache").unwrap();
        core.map_set("acme", "cache", "k", b"v", None).unwrap();

        let (addr, handle) = spawn_admin_server(
            "127.0.0.1:0".parse().unwrap(),
            core,
            Arc::new(Metrics::new()),
        )
        .await
        .unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /api/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();

        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("\"tenant\":\"acme\""));
        assert!(resp.contains("\"events\""));
        assert!(resp.contains("\"jobs\""));
        assert!(resp.contains("\"cache\""));
        assert!(resp.contains("\"k\""));

        handle.abort();
    }

    #[tokio::test]
    async fn tail_streams_mutations() {
        let core = Arc::new(SynapseCore::new());
        let (addr, handle) = spawn_admin_server(
            "127.0.0.1:0".parse().unwrap(),
            core.clone(),
            Arc::new(Metrics::new()),
        )
        .await
        .unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /api/tail HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        // Trigger a mutation after the SSE connection is open. Give the server
        // a moment to subscribe so the event is captured (broadcast only
        // delivers to subscribers present at send time).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        core.create_log("acme", "events").unwrap();
        core.log_append("acme", "events", b"live").unwrap();

        // Read until the SSE `data:` frame for the mutation arrives (the first
        // read may only contain the HTTP headers).
        let mut buf = Vec::new();
        let mut chunk = [0u8; 256];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut found = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(200), stream.read(&mut chunk)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => {
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if text.contains("data:") && text.contains("bGl2ZQ==") {
                        found = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        let text = String::from_utf8_lossy(&buf);
        assert!(found, "expected SSE data frame, got: {text}");
        // The frame carries the base64 preview of the "live" payload
        // (bGl2ZQ==), not the raw bytes.
        assert!(text.contains("bGl2ZQ=="), "frame should carry the event preview");

        handle.abort();
    }
}
