//! MQTT TCP server: binds a listener, spawns one task per connection, and
//! shuttles decoded packets between the socket and the in-process [`Broker`]
//! (spec.txt §3.3, §6 Phase 2). Outbound packets are pumped through the same
//! `mpsc` channel the broker uses for delivery, so the I/O loop and the broker
//! never share a lock.
//!
//! The connection handler is generic over the stream type so the same code
//! serves both plain TCP and TLS-terminated (mTLS) connections — see
//! [`serve_tls`], which uses the shared [`synapse_core::tls`] acceptor.
//!
//! Protocol version (3.1.1 vs 5.0) is negotiated once, on CONNECT, and held
//! as per-connection state (`version` in [`handle_connection`]) for the rest
//! of the connection's life — every subsequent `decode_packet`/`encode_packet`
//! call on this connection uses that negotiated version. v5-only,
//! connection-local concerns that don't belong on the shared [`Broker`]
//! (topic aliases) are also tracked here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::time::sleep;

use synapse_core::tls::TlsAcceptor;

use crate::broker::Broker;
use crate::codec::{decode_packet, encode_packet, Packet, ProtocolVersion, QoS, ReasonCode};

/// Bind `addr` and serve forever (plain TCP).
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

/// Bind `addr` and serve over mutual TLS (TODO.md `tpt-identity`): every client
/// must present a certificate trusted by the acceptor's client CA.
pub async fn serve_tls(
    broker: Arc<Broker>,
    addr: impl ToSocketAddrs,
    acceptor: TlsAcceptor,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_tls_with_listener(broker, listener, acceptor).await
}

/// TLS variant of [`serve_with_listener`].
pub async fn serve_tls_with_listener(
    broker: Arc<Broker>,
    listener: TcpListener,
    acceptor: TlsAcceptor,
) -> std::io::Result<()> {
    loop {
        let (sock, _peer) = listener.accept().await?;
        let broker = broker.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            match acceptor.accept(sock).await {
                Ok(tls) => {
                    let _ = handle_connection(broker, tls).await;
                }
                Err(_) => {}
            }
        });
    }
}

/// Per-connection topic-alias tables (v5 only). Aliases are connection-local:
/// each direction (inbound from this client, outbound to this client) has its
/// own alias->topic mapping, so these live in `server.rs` rather than on the
/// shared [`Broker`].
#[derive(Default)]
struct AliasTables {
    /// Aliases this client has registered on inbound PUBLISH (alias -> topic).
    inbound: HashMap<u16, String>,
    /// Aliases we've assigned on outbound PUBLISH (topic -> alias).
    outbound: HashMap<String, u16>,
    /// Highest alias value the client will accept (from CONNECT's
    /// `topic_alias_maximum`); `0` means the client supports no aliases.
    outbound_max: u16,
    next_outbound_alias: u16,
}

async fn handle_connection<S>(broker: Arc<Broker>, mut sock: S) -> std::io::Result<()>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<Packet>();
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut read_tmp = [0u8; 8192];
    let mut client_id: Option<String> = None;
    let mut keep_alive: Option<Duration> = None;
    let mut last_activity = Instant::now();
    let mut version = ProtocolVersion::V311;
    let mut aliases = AliasTables { next_outbound_alias: 1, ..Default::default() };

    loop {
        let deadline = keep_alive.map(|k| last_activity + k.mul_f32(1.5));
        tokio::select! {
            // Inbound bytes from the socket.
            n = sock.read(&mut read_tmp) => {
                let n = n?;
                if n == 0 {
                    break; // peer closed
                }
                last_activity = Instant::now();
                buf.extend_from_slice(&read_tmp[..n]);
                // Decode every complete packet currently buffered.
                loop {
                    match decode_packet(&buf, version) {
                        Ok(Some((pkt, consumed))) => {
                            buf.drain(0..consumed);
                            let closed = handle_packet(
                                &broker, &tx, &mut client_id, &mut keep_alive, &mut version, &mut aliases, pkt,
                            );
                            if closed {
                                // Send any final responses before closing.
                                drain_outbound(&mut sock, &mut rx, version, &mut aliases).await?;
                                return Ok(());
                            }
                        }
                        Ok(None) => break, // incomplete; read more
                        Err(_) => return Ok(()), // malformed: close
                    }
                }
            }
            // Outbound packets to the socket.
            pkt = rx.recv() => {
                match pkt {
                    Some(p) => write_packet(&mut sock, &p, version, &mut aliases).await?,
                    None => break,
                }
            }
            // Keep-alive watchdog (only armed after CONNECT).
            _ = sleep(deadline.map_or(Duration::ZERO, |d| d.saturating_duration_since(Instant::now()))),
                if deadline.is_some() => {
                break;
            }
        }
    }

    if let Some(id) = &client_id {
        broker.disconnect(id, keep_alive.is_none());
    }
    Ok(())
}

/// Handle one inbound packet. Returns `true` if the connection should close
/// (DISCONNECT or fatal).
fn handle_packet(
    broker: &Arc<Broker>,
    tx: &mpsc::UnboundedSender<Packet>,
    client_id: &mut Option<String>,
    keep_alive: &mut Option<Duration>,
    version: &mut ProtocolVersion,
    aliases: &mut AliasTables,
    pkt: Packet,
) -> bool {
    match pkt {
        Packet::Connect(c) => {
            *version = c.protocol;
            let clean = c.clean_session;
            broker.connect(&c.client_id, clean, tx.clone());
            *client_id = Some(c.client_id.clone());
            if c.keep_alive > 0 {
                *keep_alive = Some(Duration::from_secs(c.keep_alive as u64));
            }
            if let Some(will) = c.will {
                broker.publish(
                    client_id.as_deref(),
                    &crate::codec::Publish {
                        dup: false,
                        qos: will.qos,
                        retain: will.retain,
                        topic: will.topic,
                        packet_id: None,
                        payload: will.payload,
                        properties: None,
                    },
                );
            }
            if *version == ProtocolVersion::V5 {
                let requested_auth = c.properties.as_ref().and_then(|p| p.authentication_method.clone());
                if requested_auth.is_some() {
                    // No enhanced-auth method is implemented yet: reject
                    // rather than silently pretending authentication
                    // happened when it didn't.
                    let _ = tx.send(Packet::ConnAck {
                        session_present: false,
                        code: ReasonCode::BadAuthenticationMethod.to_u8(),
                        properties: None,
                    });
                    return true;
                }
                aliases.outbound_max = c
                    .properties
                    .as_ref()
                    .and_then(|p| p.topic_alias_maximum)
                    .unwrap_or(0);
                let _ = tx.send(Packet::ConnAck {
                    session_present: false,
                    code: ReasonCode::Success.to_u8(),
                    properties: Some(crate::codec::Properties {
                        retain_available: Some(1),
                        wildcard_subscription_available: Some(1),
                        subscription_identifier_available: Some(1),
                        shared_subscription_available: Some(1),
                        maximum_qos: Some(crate::broker::MAX_QOS as u8),
                        ..Default::default()
                    }),
                });
            } else {
                let _ = tx.send(Packet::ConnAck { session_present: false, code: 0, properties: None });
            }
        }
        Packet::Publish(mut p) => {
            // Resolve/register a v5 topic alias before the broker ever sees
            // the packet — the broker deals only in real topic strings.
            if let Some(alias) = p.properties.as_ref().and_then(|props| props.topic_alias) {
                if p.topic.is_empty() {
                    if let Some(topic) = aliases.inbound.get(&alias) {
                        p.topic = topic.clone();
                    }
                } else {
                    aliases.inbound.insert(alias, p.topic.clone());
                }
            }
            broker.publish(client_id.as_deref(), &p);
            match p.qos {
                QoS::AtMostOnce => {}
                QoS::AtLeastOnce => {
                    if let Some(id) = p.packet_id {
                        let _ = tx.send(Packet::PubAck { packet_id: id, reason: ReasonCode::Success, properties: None });
                    }
                }
                QoS::ExactlyOnce => {
                    if let Some(id) = p.packet_id {
                        let _ = tx.send(Packet::PubRec { packet_id: id, reason: ReasonCode::Success, properties: None });
                    }
                }
            }
        }
        Packet::PubRel { packet_id, .. } => {
            let _ = tx.send(Packet::PubComp { packet_id, reason: ReasonCode::Success, properties: None });
        }
        Packet::PubRec { packet_id, .. } => {
            let _ = tx.send(Packet::PubRel { packet_id, reason: ReasonCode::Success, properties: None });
        }
        Packet::Subscribe { packet_id, topics, properties } => {
            if let Some(id) = &*client_id {
                let sub_id = properties.as_ref().and_then(|p| p.subscription_identifier);
                let codes = broker.subscribe(id, &topics, sub_id);
                if *version == ProtocolVersion::V5 {
                    let reasons = codes.iter().map(|c| match c {
                        crate::codec::SubAckCode::Qos0 => ReasonCode::Success,
                        crate::codec::SubAckCode::Qos1 => ReasonCode::GrantedQoS1,
                        crate::codec::SubAckCode::Qos2 => ReasonCode::GrantedQoS2,
                        crate::codec::SubAckCode::Failure => ReasonCode::UnspecifiedError,
                    }).collect();
                    let _ = tx.send(Packet::SubAck { packet_id, codes, v5_reasons: Some(reasons), properties: None });
                } else {
                    let _ = tx.send(Packet::SubAck { packet_id, codes, v5_reasons: None, properties: None });
                }
            }
        }
        Packet::Unsubscribe { packet_id, topics, .. } => {
            if let Some(id) = &*client_id {
                broker.unsubscribe(id, &topics);
            }
            if *version == ProtocolVersion::V5 {
                let reasons = vec![ReasonCode::Success; topics.len()];
                let _ = tx.send(Packet::UnsubAck { packet_id, v5_reasons: Some(reasons), properties: None });
            } else {
                let _ = tx.send(Packet::UnsubAck { packet_id, v5_reasons: None, properties: None });
            }
        }
        Packet::PingReq => {
            let _ = tx.send(Packet::PingResp);
        }
        Packet::Disconnect { .. } => {
            if let Some(id) = &*client_id {
                broker.disconnect(id, keep_alive.is_none());
            }
            return true;
        }
        Packet::Auth { .. } => {
            // The broker never initiates enhanced auth (no method is
            // advertised in CONNACK), so an unsolicited AUTH is a protocol
            // error, not a silently-ignored packet.
            let _ = tx.send(Packet::Disconnect { reason: ReasonCode::ProtocolError, properties: None });
            return true;
        }
        _ => {}
    }
    false
}

async fn write_packet<S>(sock: &mut S, pkt: &Packet, version: ProtocolVersion, aliases: &mut AliasTables) -> std::io::Result<()>
where
    S: AsyncWriteExt + Unpin,
{
    let mut frame = Vec::new();
    if version == ProtocolVersion::V5 {
        if let Packet::Publish(p) = pkt {
            let aliased = apply_outbound_alias(p.clone(), aliases);
            encode_packet(&Packet::Publish(aliased), version, &mut frame);
            sock.write_all(&frame).await?;
            return sock.flush().await;
        }
    }
    encode_packet(pkt, version, &mut frame);
    sock.write_all(&frame).await?;
    sock.flush().await
}

/// Rewrite an outbound PUBLISH to use (and, on first use, establish) a v5
/// topic alias for `p.topic`, when the connected client has advertised
/// `topic_alias_maximum > 0`. Once an alias is established for a topic,
/// subsequent publishes to that topic omit the topic name entirely.
fn apply_outbound_alias(mut p: crate::codec::Publish, aliases: &mut AliasTables) -> crate::codec::Publish {
    if aliases.outbound_max == 0 {
        return p;
    }
    if let Some(&alias) = aliases.outbound.get(&p.topic) {
        p.topic.clear();
        let mut props = p.properties.unwrap_or_default();
        props.topic_alias = Some(alias);
        p.properties = Some(props);
    } else if aliases.next_outbound_alias <= aliases.outbound_max {
        let alias = aliases.next_outbound_alias;
        aliases.next_outbound_alias += 1;
        aliases.outbound.insert(p.topic.clone(), alias);
        let mut props = p.properties.unwrap_or_default();
        props.topic_alias = Some(alias);
        p.properties = Some(props);
    }
    p
}

/// Flush any pending outbound packets before closing.
async fn drain_outbound<S>(
    sock: &mut S,
    rx: &mut mpsc::UnboundedReceiver<Packet>,
    version: ProtocolVersion,
    aliases: &mut AliasTables,
) -> std::io::Result<()>
where
    S: AsyncWriteExt + Unpin,
{
    while let Ok(p) = rx.try_recv() {
        write_packet(sock, &p, version, aliases).await?;
    }
    Ok(())
}
