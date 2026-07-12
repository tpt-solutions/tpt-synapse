//! Protocol conformance harness (see `../README.md`).
//!
//! The in-repo, process-internal conformance for the MQTT and RESP adapters
//! lives directly in each adapter crate (`adapters/mqtt/tests/integration.rs`
//! and `adapters/resp/tests/integration.rs`): they spin the broker up on an
//! ephemeral TCP port and drive it with hand-rolled frames over real sockets,
//! which exercises the full wire codec + routing + QoS/durability path.
//!
//! The out-of-process suites below (driven by the real `paho-mqtt` and
//! `redis-rs` client libraries against a running broker) are the canonical
//! third-party conformance and remain the end goal for Phase 2; they are
//! ignored until those client crates are wired in.

#[cfg(test)]
mod mqtt {
    // TODO(Phase 2): drive this module with the `paho-mqtt` client against a
    // running synapse-adapter-mqtt instance. In-repo coverage today:
    // adapters/mqtt/tests/integration.rs
    #[test]
    #[ignore = "populated in Phase 2 alongside the MQTT adapter"]
    fn paho_mqtt_conformance() {}
}

#[cfg(test)]
mod resp {
    // TODO(Phase 2): drive this module with the `redis-rs` client against a
    // running synapse-adapter-resp instance. In-repo coverage today:
    // adapters/resp/tests/integration.rs
    #[test]
    #[ignore = "populated in Phase 2 alongside the RESP adapter"]
    fn redis_rs_conformance() {}
}

#[cfg(test)]
mod kafka {
    // TODO(Phase 3): out-of-process suite driven by `librdkafka` (C); see
    // ../README.md for why this isn't a plain `cargo test`.
    #[test]
    #[ignore = "populated in Phase 3 alongside the Kafka adapter"]
    fn librdkafka_conformance() {}
}

#[cfg(test)]
mod amqp {
    // TODO(Phase 3): out-of-process suite driven by `pika` (Python); see
    // ../README.md for why this isn't a plain `cargo test`.
    #[test]
    #[ignore = "populated in Phase 3 alongside the AMQP adapter"]
    fn pika_conformance() {}
}
