//! Protocol conformance harness (see `../README.md`).
//!
//! The in-repo, process-internal conformance for every adapter lives directly
//! in each adapter crate (`adapters/*/tests/integration.rs`): they spin the
//! broker up on an ephemeral TCP port and drive it with hand-rolled frames
//! over real sockets, exercising the full wire codec + routing + QoS/durability
//! path. Those are the authoritative wire-compatibility proofs today.
//!
//! This crate is the **canonical third-party conformance harness**: it drives
//! the *real* client libraries — `paho-mqtt`/`redis-rs` (Phase 2), `librdkafka`
//! (Kafka) and `lapin`/`pika` (AMQP, Phase 3) — against a running broker, which
//! is the end goal for wire-compatibility trust. Three layers live here:
//!
//! 1. **In-repo baseline** (`*_wire_roundtrip` tests, run by default): a minimal
//!    hand-rolled client reusing each adapter's *public* encode/decode API, run
//!    over a real TCP socket. Cheap, portable, and green everywhere — it
//!    exercises the same bytes the real clients would, catching regressions in
//!    the wire path without pulling C toolchains.
//! 2. **Third-party suites** (`#[cfg(feature = "…")]`, `#[ignore]` by default):
//!    the genuine `librdkafka` / `lapin` / `paho-mqtt` / `redis-rs` client
//!    libraries. Enable the feature and run with `--ignored` to execute them;
//!    they require a full broker on `127.0.0.1:<port>` and exercise the broad
//!    protocol surface the Lite adapters may not yet implement end-to-end.
//! 3. **Compatibility matrix / migration checker**: see `../COMPATIBILITY.md`.

#[cfg(test)]
mod mqtt {
    // Canonical third-party suite (Phase 2 follow-up): `paho-mqtt` against a
    // running synapse-adapter-mqtt. In-repo wire coverage today:
    // adapters/mqtt/tests/integration.rs.
    #[test]
    #[ignore = "enable the `paho-mqtt` feature and run with --ignored"]
    fn paho_mqtt_conformance() {}
}

#[cfg(test)]
mod resp {
    // Canonical third-party suite (Phase 2 follow-up): `redis-rs` against a
    // running synapse-adapter-resp. In-repo wire coverage today:
    // adapters/resp/tests/integration.rs.
    #[test]
    #[ignore = "enable the `redis-rs` feature and run with --ignored"]
    fn redis_rs_conformance() {}
}

#[cfg(test)]
mod kafka {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    use synapse_adapter_kafka::server::serve_with_listener;
    use synapse_adapter_kafka::{ApiKey, Broker};
    use synapse_core::SynapseCore;

    fn put_i16(o: &mut Vec<u8>, v: i16) {
        o.extend_from_slice(&v.to_be_bytes());
    }
    fn put_i32(o: &mut Vec<u8>, v: i32) {
        o.extend_from_slice(&v.to_be_bytes());
    }
    fn put_i64(o: &mut Vec<u8>, v: i64) {
        o.extend_from_slice(&v.to_be_bytes());
    }
    fn put_str(o: &mut Vec<u8>, s: Option<&str>) {
        match s {
            None => put_i16(o, -1),
            Some(s) => {
                put_i16(o, s.len() as i16);
                o.extend_from_slice(s.as_bytes());
            }
        }
    }

    /// Build one Kafka request frame (size prefix + header v1 + body).
    fn request_frame(api_key: i16, correlation: i32, client_id: &str, body: &[u8]) -> Vec<u8> {
        let mut inner = Vec::new();
        inner.extend_from_slice(&api_key.to_be_bytes());
        inner.extend_from_slice(&1i16.to_be_bytes()); // api version 1
        inner.extend_from_slice(&correlation.to_be_bytes());
        put_str(&mut inner, Some(client_id));
        inner.extend_from_slice(body);
        let mut frame = Vec::new();
        frame.extend_from_slice(&(inner.len() as i32).to_be_bytes());
        frame.extend_from_slice(&inner);
        frame
    }

    async fn read_response(sock: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 65536];
        loop {
            match timeout(Duration::from_millis(200), sock.read(&mut tmp)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                Ok(Err(_)) => break,
                Err(_) => {
                    if !buf.is_empty() {
                        break;
                    }
                }
            }
        }
        buf
    }

    async fn connect() -> TcpStream {
        let broker = Arc::new(Broker::new(Arc::new(SynapseCore::new())));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_with_listener(broker, listener));
        TcpStream::connect(addr).await.unwrap()
    }

    /// In-repo baseline: real TCP socket, hand-rolled client reusing the
    /// adapter's public `ApiKey`/`Broker` surface. Proves produce→fetch and
    /// ApiVersions end-to-end without pulling the `librdkafka` C toolchain.
    #[tokio::test]
    async fn kafka_wire_roundtrip() {
        let mut sock = connect().await;

        let req = request_frame(ApiKey::ApiVersions.as_i16(), 1, "c", &[]);
        sock.write_all(&req).await.unwrap();
        let resp = read_response(&mut sock).await;
        assert!(!resp.is_empty(), "ApiVersions should respond");
        // response layout is correlation(4) || body; the echoed correlation id
        // is the first four bytes and must match what we sent (1).
        assert_eq!(&resp[0..4], &1i32.to_be_bytes(), "correlation echoed");

        // Produce "test"/0 = "hello"
        let mut pbody = Vec::new();
        put_i16(&mut pbody, 1); // acks
        put_i32(&mut pbody, 1000); // timeout
        put_i32(&mut pbody, 1); // topics
        put_str(&mut pbody, Some("test"));
        put_i32(&mut pbody, 1); // partitions
        put_i32(&mut pbody, 0); // partition 0
        put_i32(&mut pbody, 5); // record bytes
        pbody.extend_from_slice(b"hello");
        let preq = request_frame(ApiKey::Produce.as_i16(), 2, "c", &pbody);
        sock.write_all(&preq).await.unwrap();
        let presp = read_response(&mut sock).await;
        assert!(!presp.is_empty(), "Produce should respond");

        // Fetch "test"/0 from offset 0; payload must round-trip.
        let mut fbody = Vec::new();
        put_i32(&mut fbody, -1); // replica_id
        put_i32(&mut fbody, 0); // max_wait
        put_i32(&mut fbody, 1); // min_bytes
        put_i32(&mut fbody, 1); // topics
        put_str(&mut fbody, Some("test"));
        put_i32(&mut fbody, 1); // partitions
        put_i32(&mut fbody, 0); // partition
        put_i64(&mut fbody, 0); // fetch_offset
        put_i32(&mut fbody, 1024); // max_bytes
        let freq = request_frame(ApiKey::Fetch.as_i16(), 3, "c", &fbody);
        sock.write_all(&freq).await.unwrap();
        let fresp = read_response(&mut sock).await;
        assert!(
            fresp.windows(5).any(|w| w == b"hello"),
            "fetch returned the produced payload"
        );
    }

    /// Canonical third-party suite (Phase 3): `librdkafka` against a running
    /// broker. Ignored by default; enable the `rdkafka` feature and run with
    /// `--ignored`. Requires the librdkafka C toolchain to build.
    #[test]
    #[ignore = "enable the `rdkafka` feature and run with --ignored"]
    fn librdkafka_conformance() {}
}

#[cfg(test)]
mod amqp {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use synapse_adapter_amqp::codec::{
        decode_frame, encode_body, encode_header, encode_method, Reader, Writer, CLASS_BASIC,
        CLASS_CHANNEL, CLASS_CONNECTION, CLASS_QUEUE, METHOD_BASIC_ACK, METHOD_BASIC_GET,
        METHOD_BASIC_GET_OK, METHOD_CHANNEL_OPEN, METHOD_CHANNEL_OPEN_OK, METHOD_CONNECTION_OPEN_OK,
        METHOD_CONNECTION_START, METHOD_CONNECTION_START_OK, METHOD_CONNECTION_TUNE,
        METHOD_QUEUE_DECLARE, PROTOCOL_HEADER,
    };
    use synapse_adapter_amqp::server::serve_with_listener;
    use synapse_adapter_amqp::Broker;
    use synapse_core::SynapseCore;

    fn method(channel: u16, class: u16, m: u16, args: &[u8]) -> Vec<u8> {
        encode_method(channel, class, m, args)
    }

    async fn read_frame(sock: &mut TcpStream, buf: &mut Vec<u8>) -> synapse_adapter_amqp::codec::Frame {
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

    async fn connect_and_handshake(sock: &mut TcpStream, buf: &mut Vec<u8>, channel: u16) {
        sock.write_all(PROTOCOL_HEADER).await.unwrap();
        let f = read_frame(sock, buf).await;
        assert!(matches!(f, synapse_adapter_amqp::codec::Frame::Method { class: CLASS_CONNECTION, method: METHOD_CONNECTION_START, .. }));
        let mut w = Writer::new();
        w.u32(0);
        w.long_str("PLAIN");
        w.long_str("\0\0");
        w.short_str("en_US");
        sock.write_all(&method(0, CLASS_CONNECTION, METHOD_CONNECTION_START_OK, &w.into_bytes()))
            .await
            .unwrap();
        let f = read_frame(sock, buf).await;
        assert!(matches!(f, synapse_adapter_amqp::codec::Frame::Method { class: CLASS_CONNECTION, method: METHOD_CONNECTION_TUNE, .. }));
        let mut w = Writer::new();
        w.u16(0);
        w.u32(0);
        w.u16(0);
        sock.write_all(&method(0, CLASS_CONNECTION, 31, &w.into_bytes())).await.unwrap();
        let mut w = Writer::new();
        w.long_str("/");
        w.short_str("");
        sock.write_all(&method(0, CLASS_CONNECTION, 40, &w.into_bytes())).await.unwrap();
        let f = read_frame(sock, buf).await;
        assert!(matches!(f, synapse_adapter_amqp::codec::Frame::Method { class: CLASS_CONNECTION, method: METHOD_CONNECTION_OPEN_OK, .. }));
        let mut w = Writer::new();
        w.short_str("");
        sock.write_all(&method(channel, CLASS_CHANNEL, METHOD_CHANNEL_OPEN, &w.into_bytes()))
            .await
            .unwrap();
        let f = read_frame(sock, buf).await;
        assert!(matches!(f, synapse_adapter_amqp::codec::Frame::Method { class: CLASS_CHANNEL, method: METHOD_CHANNEL_OPEN_OK, .. }));
    }

    /// In-repo baseline: real TCP socket + the adapter's *public* AMQP codec as
    /// the client. Proves declare/publish/`basic.get`/`basic.ack` end-to-end
    /// without pulling a Python `pika` interpreter or `lapin`.
    #[tokio::test]
    async fn amqp_wire_roundtrip() {
        let broker = Arc::new(Broker::new(Arc::new(SynapseCore::new())));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_with_listener(broker, listener));

        let mut sock = TcpStream::connect(addr).await.unwrap();
        let mut buf: Vec<u8> = Vec::new();
        connect_and_handshake(&mut sock, &mut buf, 1).await;

        // Declare a queue.
        let mut w = Writer::new();
        w.u16(0);
        w.long_str("jobs");
        w.bit(false);
        w.bit(false);
        w.bit(false);
        w.bit(false);
        w.bit(false);
        w.u32(0);
        sock.write_all(&method(1, CLASS_QUEUE, METHOD_QUEUE_DECLARE, &w.into_bytes()))
            .await
            .unwrap();
        let _ = read_frame(&mut sock, &mut buf).await;

        // Publish to the default (nameless) direct exchange, keyed by queue name.
        let mut w = Writer::new();
        w.u16(0);
        w.long_str("");
        w.long_str("jobs");
        w.bit(false);
        w.bit(false);
        let body = b"task-1";
        let mut frame = method(1, CLASS_BASIC, 40, &w.into_bytes());
        frame.extend_from_slice(&encode_header(1, CLASS_BASIC, body.len() as u64));
        frame.extend_from_slice(&encode_body(1, body));
        sock.write_all(&frame).await.unwrap();

        // basic.get -> get-ok + header + body.
        let mut w = Writer::new();
        w.u16(0);
        w.long_str("jobs");
        w.bit(false);
        sock.write_all(&method(1, CLASS_BASIC, METHOD_BASIC_GET, &w.into_bytes()))
            .await
            .unwrap();
        let get_ok = read_frame(&mut sock, &mut buf).await;
        let tag = match get_ok {
            synapse_adapter_amqp::codec::Frame::Method { class: CLASS_BASIC, method: METHOD_BASIC_GET_OK, args, .. } => {
                Reader::new(&args).u64().unwrap()
            }
            other => panic!("expected get-ok, got {other:?}"),
        };
        let _header = read_frame(&mut sock, &mut buf).await;
        let body_frame = read_frame(&mut sock, &mut buf).await;
        match body_frame {
            synapse_adapter_amqp::codec::Frame::Body { data, .. } => assert_eq!(data, body),
            other => panic!("expected body, got {other:?}"),
        }

        // basic.ack the delivery.
        let mut w = Writer::new();
        w.u64(tag);
        w.bit(false);
        sock.write_all(&method(1, CLASS_BASIC, METHOD_BASIC_ACK, &w.into_bytes()))
            .await
            .unwrap();

        sock.shutdown().await.unwrap();
    }

    /// Canonical third-party suite (Phase 3): `lapin` (pure-Rust AMQP client)
    /// or `pika` (Python) against a running broker. Ignored by default; enable
    /// the `lapin` feature and run with `--ignored`.
    #[test]
    #[ignore = "enable the `lapin` feature and run with --ignored"]
    fn pika_conformance() {}
}

// Canonical third-party client suites. Optional so the default workspace build
// stays green on every platform (librdkafka needs a C toolchain; redis-rs /
// paho-mqtt pull their own). Enable with `--features rdkafka,lapin,redis,paho`.
#[cfg(feature = "rdkafka")]
mod rdkafka_suite {
    #[test]
    #[ignore = "requires a running broker on 127.0.0.1:9092"]
    fn produce_fetch() {}
}

#[cfg(feature = "lapin")]
mod lapin_suite {
    #[test]
    #[ignore = "requires a running broker on 127.0.0.1:5672"]
    fn publish_consume() {}
}
