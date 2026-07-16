# syntax=docker/dockerfile:1
# Multi-protocol tpt-synapse broker (TODO.md "Adoption & Tooling" — Kubernetes
# operator / Helm chart). Builds the synapse-broker binary that hosts MQTT,
# Kafka, AMQP, RESP, and the native adapter over one shared core, plus the
# admin API (Synapse Studio's backend) and Prometheus /metrics.

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release -p synapse-broker

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/synapse-broker /usr/local/bin/synapse-broker

# MQTT, Kafka, AMQP, RESP, native, admin API, Prometheus metrics.
EXPOSE 1883 9092 5672 6379 7900 9091 9090

ENTRYPOINT ["/usr/local/bin/synapse-broker"]
