//! End-to-end conformance: real publisher/subscriber connections talk to the
//! broker over TCP, exercising the AMQP 0-9-1 handshake, exchange/queue
//! declaration, `basic.publish`/`consume` push delivery, `basic.get`, and
//! `basic.ack` (spec.txt §6 Phase 3 "Lite", the in-repo stand-in for the
//! `pika` harness until that lands).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use synapse_adapter_amqp::codec::{
    decode_frame, encode_body, encode_header, encode_method, Frame, Reader, Writer, CLASS_BASIC,
    CLASS_CHANNEL, CLASS_CONNECTION, CLASS_EXCHANGE, CLASS_QUEUE, PROTOCOL_HEADER,
    METHOD_BASIC_ACK, METHOD_BASIC_CONSUME, METHOD_BASIC_DELIVER, METHOD_BASIC_GET,
    METHOD_BASIC_GET_OK, METHOD_CHANNEL_OPEN, METHOD_CHANNEL_OPEN_OK, METHOD_CONNECTION_OPEN_OK,
    METHOD_CONNECTION_START,
    METHOD_CONNECTION_START_OK, METHOD_CONNECTION_TUNE, METHOD_EXCHANGE_DECLARE,
    METHOD_QUEUE_BIND, METHOD_QUEUE_DECLARE,
};
use synapse_adapter_amqp::{serve_with_listener, Broker};
use synapse_core::SynapseCore;

async fn read_frame(sock: &mut TcpStream, buf: &mut Vec<u8>) -> Frame {
    let mut tmp = [0u8; 8192];
    loop {
        if let Some((f, n)) = decode_frame(buf).unwrap() {
            buf.drain(0..n);
            return f;
        }
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "connection closed before frame");
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn method(channel: u16, class: u16, m: u16, args: &[u8]) -> Vec<u8> {
    encode_method(channel, class, m, args)
}

fn args_exchange_declare(name: &str, kind: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0);
    w.long_str(name);
    w.long_str(kind);
    w.bit(false); // passive
    w.bit(false); // durable
    w.bit(false); // auto-delete
    w.bit(false); // internal
    w.bit(false); // nowait
    w.u32(0); // arguments
    w.into_bytes()
}

fn args_queue_declare(name: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0);
    w.long_str(name);
    w.bit(false);
    w.bit(false);
    w.bit(false);
    w.bit(false);
    w.bit(false);
    w.u32(0);
    w.into_bytes()
}

fn args_queue_bind(queue: &str, exchange: &str, key: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0);
    w.long_str(queue);
    w.long_str(exchange);
    w.long_str(key);
    w.bit(false); // nowait
    w.u32(0);
    w.into_bytes()
}

fn args_consume(queue: &str, tag: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0);
    w.long_str(queue);
    w.long_str(tag);
    w.bit(false); // no-local
    w.bit(false); // no-ack
    w.bit(false); // exclusive
    w.bit(false); // nowait
    w.u32(0);
    w.into_bytes()
}

fn args_publish(exchange: &str, key: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0);
    w.long_str(exchange);
    w.long_str(key);
    w.bit(false); // mandatory
    w.bit(false); // immediate
    w.into_bytes()
}

fn args_get(queue: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0);
    w.long_str(queue);
    w.bit(false); // no-ack
    w.into_bytes()
}

fn args_ack(tag: u64) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(tag);
    w.bit(false); // multiple
    w.into_bytes()
}

/// Complete the 0-9-1 handshake and open one channel.
async fn connect_and_handshake(sock: &mut TcpStream, buf: &mut Vec<u8>, channel: u16) {
    sock.write_all(PROTOCOL_HEADER).await.unwrap();
    // connection.start
    let f = read_frame(sock, buf).await;
    assert!(matches!(f, Frame::Method { class: CLASS_CONNECTION, method: METHOD_CONNECTION_START, .. }));
    // start-ok
    let mut w = Writer::new();
    w.u32(0); // client-properties (empty table)
    w.long_str("PLAIN");
    w.long_str("\0\0"); // response
    w.short_str("en_US");
    sock.write_all(&method(0, CLASS_CONNECTION, METHOD_CONNECTION_START_OK, &w.into_bytes()))
        .await
        .unwrap();
    // connection.tune
    let f = read_frame(sock, buf).await;
    assert!(matches!(f, Frame::Method { class: CLASS_CONNECTION, method: METHOD_CONNECTION_TUNE, .. }));
    // tune-ok
    let mut w = Writer::new();
    w.u16(0);
    w.u32(0);
    w.u16(0);
    sock.write_all(&method(0, CLASS_CONNECTION, 31, &w.into_bytes())).await.unwrap();
    // open
    let mut w = Writer::new();
    w.long_str("/");
    w.short_str("");
    sock.write_all(&method(0, CLASS_CONNECTION, 40, &w.into_bytes())).await.unwrap();
    let f = read_frame(sock, buf).await;
    assert!(matches!(f, Frame::Method { class: CLASS_CONNECTION, method: METHOD_CONNECTION_OPEN_OK, .. }));
    // channel.open
    let mut w = Writer::new();
    w.short_str("");
    sock.write_all(&method(channel, CLASS_CHANNEL, METHOD_CHANNEL_OPEN, &w.into_bytes()))
        .await
        .unwrap();
    let f = read_frame(sock, buf).await;
    assert!(matches!(f, Frame::Method { class: CLASS_CHANNEL, method: METHOD_CHANNEL_OPEN_OK, .. }));
}

#[tokio::test]
async fn publish_to_topic_is_delivered_to_consumer() {
    let broker = Arc::new(Broker::new(Arc::new(SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_with_listener(broker, listener));

    // Subscriber.
    let mut sub = TcpStream::connect(addr).await.unwrap();
    let mut sbuf: Vec<u8> = Vec::new();
    connect_and_handshake(&mut sub, &mut sbuf, 1).await;

    sub.write_all(&method(1, CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE, &args_exchange_declare("events", "topic")))
        .await
        .unwrap();
    let _ = read_frame(&mut sub, &mut sbuf).await;
    sub.write_all(&method(1, CLASS_QUEUE, METHOD_QUEUE_DECLARE, &args_queue_declare("q1")))
        .await
        .unwrap();
    let _ = read_frame(&mut sub, &mut sbuf).await;
    sub.write_all(&method(1, CLASS_QUEUE, METHOD_QUEUE_BIND, &args_queue_bind("q1", "events", "job.#")))
        .await
        .unwrap();
    let _ = read_frame(&mut sub, &mut sbuf).await;
    sub.write_all(&method(1, CLASS_BASIC, METHOD_BASIC_CONSUME, &args_consume("q1", "c1")))
        .await
        .unwrap();
    let _ = read_frame(&mut sub, &mut sbuf).await; // consume-ok

    // Publisher.
    let mut pubc = TcpStream::connect(addr).await.unwrap();
    let mut pbuf: Vec<u8> = Vec::new();
    connect_and_handshake(&mut pubc, &mut pbuf, 1).await;
    let body = b"hello amqp";
    let mut frame = method(1, CLASS_BASIC, 40, &args_publish("events", "job.x"));
    frame.extend_from_slice(&encode_header(1, CLASS_BASIC, body.len() as u64));
    frame.extend_from_slice(&encode_body(1, body));
    pubc.write_all(&frame).await.unwrap();

    // Subscriber receives basic.deliver + header + body.
    let deliver = timeout(Duration::from_secs(2), read_frame(&mut sub, &mut sbuf)).await.unwrap();
    match deliver {
        Frame::Method { class: CLASS_BASIC, method: METHOD_BASIC_DELIVER, .. } => {}
        other => panic!("expected deliver, got {other:?}"),
    }
    let header = read_frame(&mut sub, &mut sbuf).await;
    assert!(matches!(header, Frame::Header { .. }));
    let body_frame = read_frame(&mut sub, &mut sbuf).await;
    match body_frame {
        Frame::Body { data, .. } => assert_eq!(data, body),
        other => panic!("expected body, got {other:?}"),
    }

    pubc.shutdown().await.unwrap();
    sub.shutdown().await.unwrap();
}

#[tokio::test]
async fn basic_get_returns_enqueued_message_then_ack() {
    let broker = Arc::new(Broker::new(Arc::new(SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_with_listener(broker, listener));

    let mut sock = TcpStream::connect(addr).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    connect_and_handshake(&mut sock, &mut buf, 1).await;

    // Declare queue and publish via the default (nameless) direct exchange,
    // which routes by exact routing-key == queue-name.
    sock.write_all(&method(1, CLASS_QUEUE, METHOD_QUEUE_DECLARE, &args_queue_declare("jobs")))
        .await
        .unwrap();
    let _ = read_frame(&mut sock, &mut buf).await;

    let body = b"task-1";
    let mut frame = method(1, CLASS_BASIC, 40, &args_publish("", "jobs"));
    frame.extend_from_slice(&encode_header(1, CLASS_BASIC, body.len() as u64));
    frame.extend_from_slice(&encode_body(1, body));
    sock.write_all(&frame).await.unwrap();

    // basic.get -> get-ok + header + body.
    sock.write_all(&method(1, CLASS_BASIC, METHOD_BASIC_GET, &args_get("jobs")))
        .await
        .unwrap();
    let get_ok = timeout(Duration::from_secs(2), read_frame(&mut sock, &mut buf)).await.unwrap();
    let delivery_tag = match get_ok {
        Frame::Method { class: CLASS_BASIC, method: METHOD_BASIC_GET_OK, args, .. } => {
            Reader::new(&args).u64().unwrap()
        }
        other => panic!("expected get-ok, got {other:?}"),
    };
    let _header = read_frame(&mut sock, &mut buf).await;
    let body_frame = read_frame(&mut sock, &mut buf).await;
    match body_frame {
        Frame::Body { data, .. } => assert_eq!(data, body),
        other => panic!("expected body, got {other:?}"),
    }

    // basic.ack the delivery.
    sock.write_all(&method(1, CLASS_BASIC, METHOD_BASIC_ACK, &args_ack(delivery_tag)))
        .await
        .unwrap();

    sock.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_header_closes_connection() {
    let broker = Arc::new(Broker::new(Arc::new(SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_with_listener(broker, listener));

    let mut sock = TcpStream::connect(addr).await.unwrap();
    // Send garbage instead of the protocol header.
    sock.write_all(b"NOTAMQP\x00\x00\x09\x01").await.unwrap();
    let n = sock.read(&mut [0u8; 8]).await.unwrap();
    assert_eq!(n, 0, "server should close on bad protocol header");
}
