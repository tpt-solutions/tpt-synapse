# tpt-synapse

Unified Data Fabric: a single broker that natively speaks MQTT, Kafka, AMQP,
and Redis (RESP) wire protocols over one shared storage and routing core, so
it can be a drop-in replacement for Mosquitto, Kafka, RabbitMQ, and Redis.

The full design rationale and architecture live in [spec.txt](spec.txt) —
read that first. Implementation progress against its roadmap is tracked in
[TODO.md](TODO.md). [SPEC.md](SPEC.md) is a separate, unrelated document (the
`.tptmq` IoT telemetry frame format); see the Design Reference Notes at the
bottom of TODO.md for how it informs the (non-blocking) native protocol track.

## Layout

```
core/            Rust: unified storage engine (Log, Queue, Map primitives)
routing/         Rust: Unified Routing Engine (Topic/Stream/Graph routers, Rule Engine)
adapters/        Rust: one crate per wire protocol (mqtt, kafka, amqp, resp)
  <adapter>/fuzz/   cargo-fuzz target for that adapter's frame parser
conformance/     Rust + out-of-process suites verifying wire compatibility
                 against real client libraries (see conformance/README.md)
controlplane/    Go: cluster control plane (Raft-based HA, Phase 4)
cmd/synapsectl/  Go: CLI
scripts/         Build/test/CI entry points (see below)
```

## Building

Rust workspace:

```sh
cargo build --workspace
cargo test --workspace
```

Go module:

```sh
go build ./...
go test ./...
```

Or both at once via the CI script (see below).

## CI

This repo intentionally does not use GitHub Actions/workflows. `scripts/ci.sh`
(bash) / `scripts/ci.ps1` (PowerShell) build and test both toolchains and run
the TODO-drift check; wire either into whatever CI runner you use. A local
pre-push hook is provided in `.githooks/` — enable it with:

```sh
git config core.hooksPath .githooks
```
