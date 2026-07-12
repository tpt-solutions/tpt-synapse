//! RESP TCP server: binds a listener, spawns one task per connection, and
//! shuttles RESP values between the socket and the in-process [`RespBroker`]
//! (spec.txt §6 Phase 2). Pub/sub pushes and command responses share the same
//! per-connection `mpsc` channel the broker uses for delivery.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;

use crate::broker::RespBroker;
use crate::codec::{decode_value, encode_value, Value};

/// Bind `addr` and serve forever.
pub async fn serve(broker: Arc<RespBroker>, addr: impl ToSocketAddrs) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_with_listener(broker, listener).await
}

/// Serve on an already-bound listener (used by tests for an ephemeral port).
pub async fn serve_with_listener(broker: Arc<RespBroker>, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (sock, _peer) = listener.accept().await?;
        let broker = broker.clone();
        tokio::spawn(async move {
            let _ = handle_connection(broker, sock).await;
        });
    }
}

async fn handle_connection(broker: Arc<RespBroker>, mut sock: TcpStream) -> std::io::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    let conn_id = broker.next_conn_id();
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = [0u8; 8192];

    loop {
        tokio::select! {
            n = sock.read(&mut tmp) => {
                let n = n?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                loop {
                    match decode_value(&buf) {
                        Ok(Some((value, consumed))) => {
                            buf.drain(0..consumed);
                            if let Some(resp) = handle_value(&broker, &tx, &conn_id, &value) {
                                let mut frame = Vec::new();
                                encode_value(&resp, &mut frame);
                                sock.write_all(&frame).await?;
                                sock.flush().await?;
                                break; // command/response cycle done
                            }
                            // Returns None for pub/sub frames that are pushed
                            // asynchronously via `tx`; loop to parse more input.
                        }
                        Ok(None) => break,
                        Err(_) => return Ok(()), // malformed: close
                    }
                }
            }
            pkt = rx.recv() => {
                match pkt {
                    Some(v) => write_value(&mut sock, &v).await?,
                    None => break,
                }
            }
        }
    }

    broker.disconnect(&conn_id);
    Ok(())
}

/// Handle one decoded command. Returns `Some(response)` to write immediately
/// (request/response commands), or `None` for pub/sub commands whose frames are
/// pushed over `tx`.
fn handle_value(
    broker: &Arc<RespBroker>,
    tx: &mpsc::UnboundedSender<Value>,
    conn_id: &str,
    value: &Value,
) -> Option<Value> {
    let cmd = match value.as_array() {
        Some(c) if !c.is_empty() => c,
        _ => return Some(Value::err("ERR expected a command array")),
    };
    let name = cmd.first().and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
    match name.as_str() {
        "QUIT" => Some(Value::ok()),
        "SUBSCRIBE" => {
            for confirmation in broker.subscribe(conn_id, tx.clone(), &cmd[1..]) {
                let _ = tx.send(confirmation);
            }
            None
        }
        "UNSUBSCRIBE" => {
            for confirmation in broker.unsubscribe(conn_id, &cmd[1..]) {
                let _ = tx.send(confirmation);
            }
            None
        }
        "PUBLISH" => {
            let channel = cmd.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let payload = match cmd.get(2) {
                Some(Value::BulkString(b)) => b.clone(),
                _ => Vec::new(),
            };
            let count = broker.publish(&channel, &payload);
            Some(Value::int(count))
        }
        _ => Some(broker.exec(cmd)),
    }
}

async fn write_value(sock: &mut TcpStream, v: &Value) -> std::io::Result<()> {
    let mut frame = Vec::new();
    encode_value(v, &mut frame);
    sock.write_all(&frame).await?;
    sock.flush().await
}
