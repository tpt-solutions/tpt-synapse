//! End-to-end conformance for the RESP adapter over real TCP sockets
//! (spec.txt §6 Phase 2, the in-repo stand-in for the redis-rs harness).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use synapse_adapter_resp::codec::{decode_value, encode_value, Value};
use synapse_adapter_resp::RespBroker;
use synapse_core::SynapseCore;

async fn send_cmd(sock: &mut TcpStream, args: &[&[u8]]) {
    let mut frame = Vec::new();
    encode_value(
        &Value::Array(args.iter().map(|a| Value::bulk(a.to_vec())).collect()),
        &mut frame,
    );
    sock.write_all(&frame).await.unwrap();
}

async fn read_value(sock: &mut TcpStream, buf: &mut Vec<u8>) -> Value {
    loop {
        if let Some((v, n)) = decode_value(buf).unwrap() {
            buf.drain(0..n);
            return v;
        }
        let mut tmp = [0u8; 1024];
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "connection closed");
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[tokio::test]
async fn get_set_and_publish_roundtrip() {
    let broker = Arc::new(RespBroker::new(Arc::new(SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_resp::server::serve_with_listener(
        broker,
        listener,
    ));

    // Subscriber connection.
    let mut sub = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut sub, &[b"SUBSCRIBE", b"news"]).await;
    let mut subbuf = Vec::new();
    let conf = timeout(Duration::from_secs(2), read_value(&mut sub, &mut subbuf))
        .await
        .unwrap();
    assert_eq!(
        conf,
        Value::Array(vec![
            Value::bulk(b"subscribe".to_vec()),
            Value::bulk(b"news".to_vec()),
            Value::int(1),
        ])
    );

    // Publisher: SET then PUBLISH.
    let mut pubc = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut pubc, &[b"SET", b"hello", b"world"]).await;
    let mut pubbuf = Vec::new();
    assert_eq!(
        timeout(Duration::from_secs(2), read_value(&mut pubc, &mut pubbuf))
            .await
            .unwrap(),
        Value::ok()
    );

    send_cmd(&mut pubc, &[b"GET", b"hello"]).await;
    assert_eq!(
        timeout(Duration::from_secs(2), read_value(&mut pubc, &mut pubbuf))
            .await
            .unwrap(),
        Value::bulk(b"world".to_vec())
    );

    send_cmd(&mut pubc, &[b"PUBLISH", b"news", b"breaking"]).await;
    assert_eq!(
        timeout(Duration::from_secs(2), read_value(&mut pubc, &mut pubbuf))
            .await
            .unwrap(),
        Value::int(1)
    );

    // Subscriber receives the pushed message.
    let msg = timeout(Duration::from_secs(2), read_value(&mut sub, &mut subbuf))
        .await
        .unwrap();
    assert_eq!(
        msg,
        Value::Array(vec![
            Value::bulk(b"message".to_vec()),
            Value::bulk(b"news".to_vec()),
            Value::bulk(b"breaking".to_vec()),
        ])
    );
}

#[tokio::test]
async fn xadd_xrange_stream() {
    let broker = Arc::new(RespBroker::new(Arc::new(SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_resp::server::serve_with_listener(
        broker,
        listener,
    ));

    let mut sock = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut sock, &[b"XADD", b"mystream", b"*", b"temp", b"21"]).await;
    let mut buf = Vec::new();
    let id = timeout(Duration::from_secs(2), read_value(&mut sock, &mut buf))
        .await
        .unwrap();
    assert!(matches!(id, Value::BulkString(_)));

    send_cmd(&mut sock, &[b"XRANGE", b"mystream", b"-", b"+"]).await;
    let range = timeout(Duration::from_secs(2), read_value(&mut sock, &mut buf))
        .await
        .unwrap();
    match range {
        Value::Array(entries) => assert_eq!(entries.len(), 1),
        other => panic!("expected array, got {other:?}"),
    }
}
