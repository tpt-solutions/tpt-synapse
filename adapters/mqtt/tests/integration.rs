//! End-to-end conformance: a real subscriber and publisher talk to the broker
//! over TCP, exercising the wire codec, routing, and QoS flow (spec.txt §6
//! Phase 2, the in-repo stand-in for the paho-mqtt harness until that lands).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use synapse_adapter_mqtt::codec::{decode_packet, encode_packet, Packet, QoS};
use synapse_adapter_mqtt::{Broker, Publish};

async fn connect_client(addr: &str, client_id: &str) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let connect = Packet::Connect(synapse_adapter_mqtt::codec::Connect {
        client_id: client_id.to_string(),
        keep_alive: 60,
        clean_session: true,
        will: None,
        username: None,
        password: None,
    });
    let mut buf = Vec::new();
    encode_packet(&connect, &mut buf);
    sock.write_all(&buf).await.unwrap();

    // Expect CONNACK.
    let mut hdr = [0u8; 4];
    sock.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0] >> 4, 2); // CONNACK
    sock
}

async fn read_packet(sock: &mut TcpStream, buf: &mut Vec<u8>) -> Packet {
    loop {
        if let Some((pkt, n)) = decode_packet(buf).unwrap() {
            buf.drain(0..n);
            return pkt;
        }
        let mut tmp = [0u8; 1024];
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "connection closed before packet");
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[tokio::test]
async fn subscriber_receives_published_message() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(
        broker,
        listener,
    ));

    // Subscriber.
    let mut sub = connect_client(&addr.to_string(), "sub1").await;
    let mut sbuf = Vec::new();
    let sub_pkt = Packet::Subscribe {
        packet_id: 1,
        topics: vec![("sensors/#".to_string(), QoS::AtLeastOnce)],
    };
    let mut frame = Vec::new();
    encode_packet(&sub_pkt, &mut frame);
    sub.write_all(&frame).await.unwrap();
    let ack = read_packet(&mut sub, &mut sbuf).await;
    assert!(matches!(ack, Packet::SubAck { .. }));

    // Publisher.
    let mut pubc = connect_client(&addr.to_string(), "pub1").await;
    let publish = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "sensors/temp/kitchen".to_string(),
        packet_id: Some(7),
        payload: b"21.5".to_vec(),
    });
    let mut pbuf = Vec::new();
    encode_packet(&publish, &mut pbuf);
    pubc.write_all(&pbuf).await.unwrap();
    // Publisher expects PUBACK for its QoS1.
    let mut pubbuf = Vec::new();
    let puback = timeout(Duration::from_secs(2), read_packet(&mut pubc, &mut pubbuf))
        .await
        .unwrap();
    assert!(matches!(puback, Packet::PubAck { packet_id: 7 }));

    // Subscriber receives the delivered message.
    let delivered = timeout(Duration::from_secs(2), read_packet(&mut sub, &mut sbuf))
        .await
        .unwrap();
    match delivered {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "sensors/temp/kitchen");
            assert_eq!(p.payload, b"21.5");
        }
        other => panic!("expected Publish, got {other:?}"),
    }
}

#[tokio::test]
async fn pingreq_gets_pingresp() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(
        broker,
        listener,
    ));

    let mut sock = connect_client(&addr.to_string(), "keepalive-client").await;
    let mut buf = Vec::new();
    let mut frame = Vec::new();
    encode_packet(&Packet::PingReq, &mut frame);
    sock.write_all(&frame).await.unwrap();
    let resp = timeout(Duration::from_secs(2), read_packet(&mut sock, &mut buf))
        .await
        .unwrap();
    assert!(matches!(resp, Packet::PingResp));
}
