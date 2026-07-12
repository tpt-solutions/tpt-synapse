//! Kafka TCP server: binds a listener, spawns one task per connection, and
//! shuttles length-prefixed Kafka requests between the socket and the
//! in-process [`Broker`] (spec.txt §6 Phase 3). Each response is the request's
//! `correlation_id` followed by the encoded body.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::broker::Broker;
use crate::codec::decode_request;

/// Bind `addr` and serve forever.
pub async fn serve(broker: Arc<Broker>, addr: impl ToSocketAddrs) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_with_listener(broker, listener).await
}

/// Serve on an already-bound listener (used by tests to claim an ephemeral
/// port and learn the address).
pub async fn serve_with_listener(broker: Arc<Broker>, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (sock, _peer) = listener.accept().await?;
        let broker = broker.clone();
        tokio::spawn(async move {
            let _ = handle_connection(broker, sock).await;
        });
    }
}

async fn handle_connection(broker: Arc<Broker>, mut sock: TcpStream) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut tmp = [0u8; 16384];

    loop {
        match decode_request(&buf) {
            Ok(Some((frame, consumed))) => {
                buf.drain(0..consumed);
                let body = broker.handle(&frame);
                let mut resp = Vec::with_capacity(4 + body.len());
                resp.extend_from_slice(&frame.correlation_id.to_be_bytes());
                resp.extend_from_slice(&body);
                sock.write_all(&resp).await?;
                sock.flush().await?;
            }
            Ok(None) => {
                // Need more bytes.
                let n = sock.read(&mut tmp).await?;
                if n == 0 {
                    break; // peer closed
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(_) => return Ok(()), // malformed: close
        }
    }
    Ok(())
}
