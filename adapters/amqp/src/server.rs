//! AMQP TCP server: binds a listener, performs the 0-9-1 handshake, then
//! shuttles decoded frames between the socket and the in-process [`Broker`]
//! (spec.txt §3.3, §6 Phase 3 "Lite"). Unlike the other adapters, a
//! `basic.publish` spans three frames (method + content-header + content-body),
//! so the server reads that triple inline; all other methods are dispatched
//! one frame at a time. Consumer deliveries arrive on the broker's per-connection
//! channel and are serialized into `basic.deliver` + header + body frames.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::time::sleep;

use synapse_core::SynapseCore;

use crate::broker::{Broker, ServerEvent};
use crate::codec::{
    decode_frame, encode_connection_start, parse_basic_publish, skip_properties, Frame, ProtocolError,
    PROTOCOL_HEADER, CLASS_BASIC, METHOD_BASIC_PUBLISH,
};

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
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerEvent>();

    // --- handshake: read the 8-byte protocol header --------------------
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut tmp = [0u8; 8192];
    loop {
        if buf.len() >= PROTOCOL_HEADER.len() {
            if buf[..PROTOCOL_HEADER.len()] != *PROTOCOL_HEADER {
                return Ok(()); // bad protocol header: close
            }
            break;
        }
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    buf.drain(0..PROTOCOL_HEADER.len());

    let conn_id = broker.connect(tx);
    sock.write_all(&encode_connection_start()).await?;

    // --- frame loop -----------------------------------------------------
    loop {
        tokio::select! {
            n = sock.read(&mut tmp) => {
                let n = n?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                loop {
                    match decode_frame(&buf) {
                        Ok(Some((frame, consumed))) => {
                            buf.drain(0..consumed);
                            // `basic.publish` spans method + content-header +
                            // content-body frames; handle it inline so the
                            // buffered bytes (which may include the header/body)
                            // are reused instead of re-read from the socket.
                            if let Frame::Method { channel: _, class, method, args } = &frame {
                                if (*class, *method) == (CLASS_BASIC, METHOD_BASIC_PUBLISH) {
                                    let publish = match parse_basic_publish(args) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            broker.disconnect(conn_id);
                                            return Ok(());
                                        }
                                    };
                                    let body_size = match read_header(&mut sock, &mut buf).await {
                                        Ok(s) => s,
                                        Err(_) => {
                                            broker.disconnect(conn_id);
                                            return Ok(());
                                        }
                                    };
                                    let body = match read_body(&mut buf, body_size, &mut sock).await {
                                        Ok(b) => b,
                                        Err(_) => {
                                            broker.disconnect(conn_id);
                                            return Ok(());
                                        }
                                    };
                                    if broker.publish(&publish.0, &publish.1, &body).is_err() {
                                        broker.disconnect(conn_id);
                                        return Ok(());
                                    }
                                    continue;
                                }
                            }
                            if handle_frame(&broker, conn_id, &mut sock, frame).await? {
                                broker.disconnect(conn_id);
                                return Ok(());
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            broker.disconnect(conn_id);
                            return Ok(());
                        }
                    }
                }
            }
            event = rx.recv() => {
                match event {
                    Some(ServerEvent::Bytes(b)) => {
                        sock.write_all(&b).await?;
                        sock.flush().await?;
                    }
                    Some(ServerEvent::Deliver(msg)) => {
                        let mut out = crate::codec::encode_basic_deliver(
                            msg.channel,
                            &msg.consumer_tag,
                            msg.delivery_tag,
                            false,
                            &msg.exchange,
                            &msg.routing_key,
                        );
                        out.extend_from_slice(&crate::codec::encode_header(
                            msg.channel,
                            CLASS_BASIC,
                            msg.body.len() as u64,
                        ));
                        out.extend_from_slice(&crate::codec::encode_body(msg.channel, &msg.body));
                        sock.write_all(&out).await?;
                        sock.flush().await?;
                    }
                    None => break,
                }
            }
            _ = sleep(Duration::from_secs(600)) => {
                // Idle connection watchdog; AMQP heartbeats are optional in the
                // Lite subset, so just keep the connection alive.
            }
        }
    }

    broker.disconnect(conn_id);
    Ok(())
}

/// Returns `true` if the connection should be closed.
async fn handle_frame(
    broker: &Arc<Broker>,
    conn_id: u64,
    sock: &mut TcpStream,
    frame: Frame,
) -> std::io::Result<bool> {
    match frame {
        Frame::Heartbeat => Ok(false),
        Frame::Body { .. } | Frame::Header { .. } => Ok(false), // stray; ignored
        Frame::Method { channel, class, method, args } => {
            match broker.handle_method(conn_id, channel, class, method, &args) {
                Ok(events) => {
                    for e in events {
                        match e {
                            ServerEvent::Bytes(b) => {
                                sock.write_all(&b).await?;
                                sock.flush().await?;
                            }
                            ServerEvent::Deliver(msg) => {
                                let mut out = crate::codec::encode_basic_deliver(
                                    msg.channel,
                                    &msg.consumer_tag,
                                    msg.delivery_tag,
                                    false,
                                    &msg.exchange,
                                        &msg.routing_key,
                                    );
                                    out.extend_from_slice(&crate::codec::encode_header(
                                        msg.channel,
                                        CLASS_BASIC,
                                        msg.body.len() as u64,
                                    ));
                                    out.extend_from_slice(&crate::codec::encode_body(
                                        msg.channel,
                                        &msg.body,
                                    ));
                                    sock.write_all(&out).await?;
                                    sock.flush().await?;
                                }
                            }
                        }
                        Ok(false)
                    }
                    Err(_) => Ok(true),
                }
            }
        }
    }

/// Read the next content-header frame off the socket, skipping its property
/// section and returning the declared body size.
async fn read_header(sock: &mut TcpStream, buf: &mut Vec<u8>) -> Result<u64, ProtocolError> {
    let mut tmp = [0u8; 8192];
    loop {
        if let Some((frame, consumed)) = decode_frame(buf)? {
            buf.drain(0..consumed);
            if let Frame::Header { body_size, properties, .. } = frame {
                let mut r = crate::codec::Reader::new(&properties);
                skip_properties(&mut r).ok();
                return Ok(body_size);
            }
            // Not a header (e.g. body arrived first): shouldn't happen, but keep
            // draining until we find the header.
        }
        let n = sock.read(&mut tmp).await.map_err(|_| ProtocolError::Incomplete)?;
        if n == 0 {
            return Err(ProtocolError::Incomplete);
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Read content-body frame(s) until `body_size` bytes are accumulated.
async fn read_body(buf: &mut Vec<u8>, body_size: u64, sock: &mut TcpStream) -> Result<Vec<u8>, ProtocolError> {
    let mut tmp = [0u8; 8192];
    let mut body = Vec::with_capacity(body_size as usize);
    loop {
        while body.len() as u64 >= body_size {
            return Ok(body);
        }
        if let Some((frame, consumed)) = decode_frame(buf)? {
            buf.drain(0..consumed);
            if let Frame::Body { data, .. } = frame {
                body.extend_from_slice(&data);
                if body.len() as u64 >= body_size {
                    return Ok(body);
                }
            }
        }
        let n = sock.read(&mut tmp).await.map_err(|_| ProtocolError::Incomplete)?;
        if n == 0 {
            return Err(ProtocolError::Incomplete);
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Build a broker over a fresh core — convenience for embedding.
pub fn broker_for(core: Arc<SynapseCore>) -> Arc<Broker> {
    Arc::new(Broker::new(core))
}
