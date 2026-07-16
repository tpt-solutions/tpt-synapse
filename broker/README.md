# synapse-broker

The **[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** unified broker binary — a
single process that natively speaks MQTT, Kafka, AMQP, and Redis RESP wire protocols over one
shared storage and routing core.

Drop-in replacement for Mosquitto, Kafka, RabbitMQ, and Redis. All four protocols share the
same Log/Queue/Map storage layer; a message published via MQTT is immediately consumable via
Kafka or AMQP without any bridge or translation overhead.

## Install

```sh
cargo install synapse-broker
synapse-broker --config synapse.toml
```

## Default ports

| Protocol | Default port |
|----------|-------------|
| MQTT | 1883 |
| Kafka | 9092 |
| AMQP | 5672 |
| Redis RESP | 6379 |
| Native | 7171 |
| Admin HTTP | 8080 |

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
