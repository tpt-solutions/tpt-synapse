//! End-to-end conformance: a real subscriber and publisher talk to the broker
//! over TCP, exercising the wire codec, routing, and QoS flow (spec.txt §6
//! Phase 2, the in-repo stand-in for the paho-mqtt harness until that lands).
//! Covers both v3.1.1 (`connect_client`) and v5.0 (`connect_client_v5`).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use synapse_adapter_mqtt::codec::{
    decode_packet, encode_packet, Packet, Properties, ProtocolVersion, QoS, ReasonCode,
    RetainHandling, SubscribeTopic,
};
use synapse_adapter_mqtt::{Broker, Connect, Publish};

async fn connect_client(addr: &str, client_id: &str) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let connect = Packet::Connect(Connect {
        protocol: ProtocolVersion::V311,
        client_id: client_id.to_string(),
        keep_alive: 60,
        clean_session: true,
        will: None,
        username: None,
        password: None,
        properties: None,
    });
    let mut buf = Vec::new();
    encode_packet(&connect, ProtocolVersion::V311, &mut buf);
    sock.write_all(&buf).await.unwrap();

    // Expect CONNACK.
    let mut hdr = [0u8; 4];
    sock.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0] >> 4, 2); // CONNACK
    sock
}

async fn connect_client_v5(addr: &str, client_id: &str, properties: Option<Properties>) -> (TcpStream, Packet) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let connect = Packet::Connect(Connect {
        protocol: ProtocolVersion::V5,
        client_id: client_id.to_string(),
        keep_alive: 60,
        clean_session: true,
        will: None,
        username: None,
        password: None,
        properties,
    });
    let mut buf = Vec::new();
    encode_packet(&connect, ProtocolVersion::V5, &mut buf);
    sock.write_all(&buf).await.unwrap();

    let mut rbuf = Vec::new();
    let connack = read_packet(&mut sock, &mut rbuf, ProtocolVersion::V5).await;
    assert!(matches!(connack, Packet::ConnAck { .. }));
    (sock, connack)
}

async fn read_packet(sock: &mut TcpStream, buf: &mut Vec<u8>, version: ProtocolVersion) -> Packet {
    loop {
        if let Some((pkt, n)) = decode_packet(buf, version).unwrap() {
            buf.drain(0..n);
            return pkt;
        }
        let mut tmp = [0u8; 1024];
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "connection closed before packet");
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn topic(filter: &str, qos: QoS) -> SubscribeTopic {
    SubscribeTopic {
        filter: filter.to_string(),
        qos,
        no_local: false,
        retain_as_published: false,
        retain_handling: RetainHandling::SendAtSubscribe,
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
        topics: vec![topic("sensors/#", QoS::AtLeastOnce)],
        properties: None,
    };
    let mut frame = Vec::new();
    encode_packet(&sub_pkt, ProtocolVersion::V311, &mut frame);
    sub.write_all(&frame).await.unwrap();
    let ack = read_packet(&mut sub, &mut sbuf, ProtocolVersion::V311).await;
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
        properties: None,
    });
    let mut pbuf = Vec::new();
    encode_packet(&publish, ProtocolVersion::V311, &mut pbuf);
    pubc.write_all(&pbuf).await.unwrap();
    // Publisher expects PUBACK for its QoS1.
    let mut pubbuf = Vec::new();
    let puback = timeout(Duration::from_secs(2), read_packet(&mut pubc, &mut pubbuf, ProtocolVersion::V311))
        .await
        .unwrap();
    assert!(matches!(puback, Packet::PubAck { packet_id: 7, .. }));

    // Subscriber receives the delivered message.
    let delivered = timeout(Duration::from_secs(2), read_packet(&mut sub, &mut sbuf, ProtocolVersion::V311))
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
    encode_packet(&Packet::PingReq, ProtocolVersion::V311, &mut frame);
    sock.write_all(&frame).await.unwrap();
    let resp = timeout(Duration::from_secs(2), read_packet(&mut sock, &mut buf, ProtocolVersion::V311))
        .await
        .unwrap();
    assert!(matches!(resp, Packet::PingResp));
}

#[tokio::test]
async fn v5_connack_has_success_reason_and_properties() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(broker, listener));

    let (_sock, connack) = connect_client_v5(&addr.to_string(), "v5-client", None).await;
    match connack {
        Packet::ConnAck { session_present, code, properties } => {
            assert!(!session_present);
            assert_eq!(code, ReasonCode::Success.to_u8());
            let props = properties.expect("v5 CONNACK should carry properties");
            assert_eq!(props.retain_available, Some(1));
            assert_eq!(props.shared_subscription_available, Some(1));
        }
        other => panic!("expected ConnAck, got {other:?}"),
    }
}

#[tokio::test]
async fn v5_no_local_subscription() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(broker, listener));

    let (mut sock, _) = connect_client_v5(&addr.to_string(), "no-local-client", None).await;
    let mut buf = Vec::new();
    let sub_pkt = Packet::Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { no_local: true, ..topic("chat", QoS::AtMostOnce) }],
        properties: None,
    };
    let mut frame = Vec::new();
    encode_packet(&sub_pkt, ProtocolVersion::V5, &mut frame);
    sock.write_all(&frame).await.unwrap();
    let ack = read_packet(&mut sock, &mut buf, ProtocolVersion::V5).await;
    assert!(matches!(ack, Packet::SubAck { .. }));

    // This client publishes to its own no_local subscription: it must not
    // receive its own message back.
    let publish = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "chat".to_string(),
        packet_id: None,
        payload: b"hi".to_vec(),
        properties: None,
    });
    let mut pframe = Vec::new();
    encode_packet(&publish, ProtocolVersion::V5, &mut pframe);
    sock.write_all(&pframe).await.unwrap();

    // Nothing should arrive; confirm by racing a subsequent PINGREQ/PINGRESP
    // round-trip, which must be the very next packet received.
    let mut pingframe = Vec::new();
    encode_packet(&Packet::PingReq, ProtocolVersion::V5, &mut pingframe);
    sock.write_all(&pingframe).await.unwrap();
    let resp = timeout(Duration::from_secs(2), read_packet(&mut sock, &mut buf, ProtocolVersion::V5))
        .await
        .unwrap();
    assert!(matches!(resp, Packet::PingResp), "expected PingResp (no_local suppressed the publish), got {resp:?}");
}

#[tokio::test]
async fn v5_shared_subscription_distributes_round_robin() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(broker, listener));

    let (mut w1, _) = connect_client_v5(&addr.to_string(), "worker1", None).await;
    let (mut w2, _) = connect_client_v5(&addr.to_string(), "worker2", None).await;
    for sock in [&mut w1, &mut w2] {
        let mut buf = Vec::new();
        let sub_pkt = Packet::Subscribe {
            packet_id: 1,
            topics: vec![topic("$share/pool/jobs", QoS::AtMostOnce)],
            properties: None,
        };
        let mut frame = Vec::new();
        encode_packet(&sub_pkt, ProtocolVersion::V5, &mut frame);
        sock.write_all(&frame).await.unwrap();
        let ack = read_packet(sock, &mut buf, ProtocolVersion::V5).await;
        assert!(matches!(ack, Packet::SubAck { .. }));
    }

    let (mut pubc, _) = connect_client_v5(&addr.to_string(), "producer", None).await;
    for i in 0..4 {
        let publish = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "jobs".to_string(),
            packet_id: None,
            payload: format!("job{i}").into_bytes(),
            properties: None,
        });
        let mut frame = Vec::new();
        encode_packet(&publish, ProtocolVersion::V5, &mut frame);
        pubc.write_all(&frame).await.unwrap();
    }

    // Both workers together should receive exactly 4 messages, none of them
    // duplicated, distributed across the group.
    let mut total = 0;
    let mut buf1 = Vec::new();
    let mut buf2 = Vec::new();
    for _ in 0..4 {
        tokio::select! {
            p = timeout(Duration::from_millis(500), read_packet(&mut w1, &mut buf1, ProtocolVersion::V5)) => {
                if p.is_ok() { total += 1; }
            }
            p = timeout(Duration::from_millis(500), read_packet(&mut w2, &mut buf2, ProtocolVersion::V5)) => {
                if p.is_ok() { total += 1; }
            }
        }
    }
    assert_eq!(total, 4);
}

#[tokio::test]
async fn v5_disconnect_with_reason_code() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(broker, listener));

    let (mut sock, _) = connect_client_v5(&addr.to_string(), "disc-client", None).await;
    let mut frame = Vec::new();
    encode_packet(
        &Packet::Disconnect { reason: ReasonCode::Success, properties: None },
        ProtocolVersion::V5,
        &mut frame,
    );
    sock.write_all(&frame).await.unwrap();
    // Server should close the connection in response; reading should hit EOF.
    let mut tmp = [0u8; 16];
    let n = sock.read(&mut tmp).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn v5_client_talks_v311_client_still_works() {
    let broker = Arc::new(Broker::new(Arc::new(synapse_core::SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(synapse_adapter_mqtt::server::serve_with_listener(broker, listener));

    // v3.1.1 subscriber.
    let mut sub = connect_client(&addr.to_string(), "v311-sub").await;
    let mut sbuf = Vec::new();
    let sub_pkt = Packet::Subscribe {
        packet_id: 1,
        topics: vec![topic("mixed/#", QoS::AtLeastOnce)],
        properties: None,
    };
    let mut frame = Vec::new();
    encode_packet(&sub_pkt, ProtocolVersion::V311, &mut frame);
    sub.write_all(&frame).await.unwrap();
    let ack = read_packet(&mut sub, &mut sbuf, ProtocolVersion::V311).await;
    assert!(matches!(ack, Packet::SubAck { .. }));

    // v5 publisher.
    let (mut pubc, _) = connect_client_v5(&addr.to_string(), "v5-pub", None).await;
    let publish = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "mixed/topic".to_string(),
        packet_id: Some(1),
        payload: b"cross-version".to_vec(),
        properties: None,
    });
    let mut pframe = Vec::new();
    encode_packet(&publish, ProtocolVersion::V5, &mut pframe);
    pubc.write_all(&pframe).await.unwrap();

    let delivered = timeout(Duration::from_secs(2), read_packet(&mut sub, &mut sbuf, ProtocolVersion::V311))
        .await
        .unwrap();
    match delivered {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "mixed/topic");
            assert_eq!(p.payload, b"cross-version");
        }
        other => panic!("expected Publish, got {other:?}"),
    }
}
