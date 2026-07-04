//! Protocol conformance harness (see `../README.md`). Empty until an adapter
//! and a running broker exist to test against.

#[cfg(test)]
mod mqtt {
    // TODO(Phase 2): drive this module with the `paho-mqtt` client against a
    // running synapse-adapter-mqtt instance.
    #[test]
    #[ignore = "populated in Phase 2 alongside the MQTT adapter"]
    fn paho_mqtt_conformance() {}
}

#[cfg(test)]
mod resp {
    // TODO(Phase 2): drive this module with the `redis-rs` client against a
    // running synapse-adapter-resp instance.
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
