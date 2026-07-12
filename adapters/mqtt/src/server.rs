//! MQTT TCP server: binds a listener, spawns one task per connection, and
//! shuttles decoded packets between the socket and the in-process [`Broker`]
//! (spec.txt §3.3, §6 Phase 2). Outbound packets are pumped through the same
//! `mpsc` channel the broker uses for delivery, so the I/O loop and the broker
//! never share a lock.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::broker::Broker;
use crate::codec::{decode_packet, encode_packet, Packet, QoS};

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
    let (tx, mut rx) = mpsc::unbounded_channel::<Packet>();
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut read_tmp = [0u8; 8192];
    let mut client_id: Option<String> = None;
    let mut keep_alive: Option<Duration> = None;
    let mut last_activity = Instant::now();

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
                    match decode_packet(&buf) {
                        Ok(Some((pkt, consumed))) => {
                            buf.drain(0..consumed);
                            let closed = handle_packet(
                                &broker, &tx, &mut client_id, &mut keep_alive, pkt,
                            );
                            if closed {
                                // Send any final responses before closing.
                                drain_outbound(&mut sock, &mut rx).await?;
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
                    Some(p) => write_packet(&mut sock, &p).await?,
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
    pkt: Packet,
) -> bool {
    match pkt {
        Packet::Connect(c) => {
            let clean = c.clean_session;
            broker.connect(&c.client_id, clean, tx.clone());
            *client_id = Some(c.client_id.clone());
            if c.keep_alive > 0 {
                *keep_alive = Some(Duration::from_secs(c.keep_alive as u64));
            }
            if let Some(will) = c.will {
                broker.publish(&crate::codec::Publish {
                    dup: false,
                    qos: will.qos,
                    retain: will.retain,
                    topic: will.topic,
                    packet_id: None,
                    payload: will.payload,
                });
            }
            let _ = tx.send(Packet::ConnAck { session_present: false, code: 0 });
        }
        Packet::Publish(p) => {
            broker.publish(&p);
            match p.qos {
                QoS::AtMostOnce => {}
                QoS::AtLeastOnce => {
                    if let Some(id) = p.packet_id {
                        let _ = tx.send(Packet::PubAck { packet_id: id });
                    }
                }
                QoS::ExactlyOnce => {
                    if let Some(id) = p.packet_id {
                        let _ = tx.send(Packet::PubRec { packet_id: id });
                    }
                }
            }
        }
        Packet::PubRel { packet_id } => {
            let _ = tx.send(Packet::PubComp { packet_id });
        }
        Packet::PubRec { packet_id } => {
            let _ = tx.send(Packet::PubRel { packet_id });
        }
        Packet::Subscribe { packet_id, topics } => {
            if let Some(id) = &*client_id {
                let codes = broker.subscribe(id, &topics);
                let _ = tx.send(Packet::SubAck { packet_id, codes });
            }
        }
        Packet::Unsubscribe { packet_id, topics } => {
            if let Some(id) = &*client_id {
                broker.unsubscribe(id, &topics);
            }
            let _ = tx.send(Packet::UnsubAck { packet_id });
        }
        Packet::PingReq => {
            let _ = tx.send(Packet::PingResp);
        }
        Packet::Disconnect => {
            if let Some(id) = &*client_id {
                broker.disconnect(id, keep_alive.is_none());
            }
            return true;
        }
        _ => {}
    }
    false
}

async fn write_packet(sock: &mut TcpStream, pkt: &Packet) -> std::io::Result<()> {
    let mut frame = Vec::new();
    encode_packet(pkt, &mut frame);
    sock.write_all(&frame).await?;
    sock.flush().await
}

/// Flush any pending outbound packets before closing.
async fn drain_outbound(
    sock: &mut TcpStream,
    rx: &mut mpsc::UnboundedReceiver<Packet>,
) -> std::io::Result<()> {
    while let Ok(p) = rx.try_recv() {
        write_packet(sock, &p).await?;
    }
    Ok(())
}
