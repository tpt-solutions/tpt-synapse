# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

tpt-synapse is a Unified Data Fabric: a single broker that natively speaks MQTT, Kafka, AMQP, and
Redis (RESP) wire protocols over one shared storage and routing core, intended as a drop-in
replacement for Mosquitto, Kafka, RabbitMQ, and Redis.

Read [spec.txt](spec.txt) first for design rationale and architecture — it's the source of truth
the rest of the repo implements against. [TODO.md](TODO.md) tracks implementation progress phase
by phase (Phase 0 scaffolding → Phase 1 core engine → Phase 2 MQTT/RESP → Phase 3 Kafka/AMQP →
Phase 4 clustering → the parallel native-protocol track); check it before assuming a feature is
unimplemented or fully done — most items have detailed notes on exactly what's wired up vs. stubbed.
[SPEC.md](SPEC.md) is unrelated (the `.tptmq` IoT telemetry frame format) but is the direct design
basis for the native protocol track (framing, AEAD, replay protection) — see the Design Reference
Notes at the bottom of TODO.md.

## Repo layout

Two toolchains in one repo:

```
core/                 Rust: unified storage engine — Log, Queue, Map primitives over one
                       shared tiered append-only physical layer (core/src/storage.rs)
routing/               Rust: Unified Routing Engine — Topic Router (MQTT), Stream Router
                       (Kafka), Graph Router (AMQP), embedded SQL-like Rule Engine, plus
                       WASM transform plugins (wasmtime) and shared backpressure signal
adapters/              Rust: one crate per wire protocol — mqtt, kafka, amqp, resp, native
  <adapter>/fuzz/       cargo-fuzz target for that adapter's frame parser (untrusted-bytes surface)
synapse-native-client/ Rust: client SDK for the from-scratch native protocol (adapters/native)
synapse-studio/        Rust: axum-based web dashboard (topic/queue/key browsing, metrics)
conformance/           Rust: out-of-process suites verifying wire compatibility against real
                       client libraries (paho-mqtt, redis-rs, librdkafka, pika) — see
                       conformance/README.md and conformance/COMPATIBILITY.md
controlplane/          Go: cluster control plane (membership, leader election, HTTP API)
cmd/synapsectl/        Go: CLI for the control plane
scripts/                Build/test/CI entry points
```

All Rust crates are members of the single root `Cargo.toml` workspace. Go code is a separate
module rooted at `go.mod` (module `tpt-synapse`).

## Building, testing, linting

Rust workspace:

```sh
cargo build --workspace
cargo test --workspace
cargo test -p synapse-routing --release sustains_one_million_ops_per_sec   # perf gate test
cargo test -p synapse-core --features consensus consensus                  # Raft consensus tests
```

Go module:

```sh
go build ./...
go vet ./...
go test ./...
```

Full CI (both toolchains + TODO-drift check) via `scripts/ci.sh` (bash) or `scripts/ci.ps1`
(PowerShell) — this repo has no GitHub Actions by design; wire these scripts into whatever CI
runner is in use. A local pre-push hook running the same checks is available via
`.githooks/pre-push`; enable with `git config core.hooksPath .githooks`.

`scripts/check_todo.sh` fails if a checked-off (`- [x]`) TODO.md item references a backtick-quoted
path that no longer exists — keeps the checklist from drifting ahead of reality. Run it after
renaming/removing files that TODO.md points to.

Fuzz targets (per-adapter, in `adapters/<name>/fuzz/`) use `cargo-fuzz`; each targets the
adapter's wire-frame parser since that's untrusted-network-input surface.

## Architecture notes

- **Everything shares one physical storage layer.** `Log`, `Queue`, and `Map` in `core/src/` are
  three different access patterns (append-only sequence, FIFO+ack, KV+TTL) over the same
  `SegmentedLog`/`TieredSegmentedLog` in `core/src/storage.rs`. Protocol adapters don't own
  storage — they translate wire semantics onto these three primitives via the routing engine.
- **Consistency/durability model** (pre-Phase-4, single node) is documented at the top of
  `core/src/lib.rs` — read it before touching the write path. Short version: every mutation is
  durably WAL-written before being acked; `committed` means "in the resident segment," not yet
  "replicated" (that's Phase 4's job once the WAL becomes the Raft log); Log is strictly ordered,
  Queue is at-least-once FIFO, Map is last-writer-wins with optional TTL; everything is namespaced
  by `tenant::TenantId` with per-tenant quotas enforced on every charge.
  Tiered storage (`TieredSegmentedLog::offload_sealed`) moves sealed segments to object storage
  and is currently best-effort (a real `S3ObjectStore` backend is still a stub).
- **Adapters are thin wire-protocol translators.** Each `adapters/<proto>/src/` crate has the
  same shape: `codec.rs` (wire framing/(de)serialization), `broker.rs` (protocol semantics against
  core primitives + routing engine), `server.rs` (TCP accept loop / connection handling). MQTT
  v3.1.1 and v5.0 share one `Packet` enum and codec rather than parallel packet sets — protocol
  version is negotiated once on CONNECT.
  Adapter TCP integration tests live in `adapters/<proto>/tests/integration.rs`; broker/codec unit
  tests live inline in the adapter crate.
- **The native protocol** (`adapters/native/`) is a from-scratch, non-blocking parallel track (not
  in the original spec.txt roadmap) — self-describing fixed-header framing, mandatory
  ChaCha20-Poly1305 AEAD on every frame (no unencrypted carve-out), CRC pre-auth filter, AAD-bound
  header fields, boot-salt+counter nonces, epoch-based key rotation, and an asymmetric (X25519)
  rekey handshake — modeled on but improving upon `.tptmq` (see SPEC.md and the Design Reference
  Notes in TODO.md). One connection carries all four primitives (pub/sub, log tailing/consumer
  groups, queue+ack, KV) via a single `Opcode` enum, no per-protocol translation layer.
- **Networking backend is a build-time choice, not per-adapter.** `core/src/io_uring.rs`'s
  `IoEngine` abstraction provides `accept_loop_tokio`/`accept_loop_uring`; everything (metrics
  server, adapters) shares the same accept-loop shape. Linux + the `io-uring` cargo feature uses
  `tokio-uring`; elsewhere (including Windows dev boxes) it's plain `tokio`.
  `consensus` and `io-uring` are both opt-in Cargo features on `core` — most local dev/testing runs
  without them.
- **Clustering is embedded, not external.** `core/src/consensus.rs` is a self-contained Raft core
  (`RequestVote`/`AppendEntries`, follower/candidate/leader state machine) behind the `consensus`
  feature, chosen deliberately over depending on ZooKeeper-style external coordination. It is
  unit-tested standalone but not yet wired to the running broker's replication path (transport +
  apply-loop are still open). The Go `controlplane/` package (membership, leader election, HTTP API
  at `GET /cluster`, `GET/POST/DELETE /nodes`) and `cmd/synapsectl` CLI are the separate,
  already-functional control-plane layer adapters and tooling talk to.
- **Routing is the shared dispatch layer** all adapters route through: `routing/src/topic.rs`
  (MQTT-style wildcard topic matching, optimized via a pre-compiled zero-heap `CompiledFilter`
  matcher — this is the perf-gated hot path, see the `sustains_one_million_ops_per_sec` test),
  `stream.rs` (Kafka-style), `graph.rs` (AMQP exchanges/bindings), `rule.rs` (SQL-like rule engine),
  `wasm_transform.rs` (sandboxed per-tenant WASM transforms via wasmtime, fuel-limited),
  `backpressure.rs` (one internal backpressure signal each adapter maps its own flow-control
  concept onto — MQTT inflight windows, Kafka fetch/produce quotas, AMQP prefetch/credit).
