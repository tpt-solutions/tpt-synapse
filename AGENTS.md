# AGENTS.md — tpt-synapse

Unified Data Fabric: one broker speaking MQTT, Kafka, AMQP, and RESP over a shared
storage/routing core (drop-in for Mosquitto/Kafka/RabbitMQ/Redis).

## Two toolchains — both matter

This is **not** a single-language repo. Read and test both:

- **Rust Cargo workspace** (data plane): `cargo build --workspace`, `cargo test --workspace`
- **Go module** (control plane + CLI): `go build ./...`, `go vet ./...`, `go test ./...`

No hosted CI. Verify with `scripts/ci.sh` (bash) or `scripts/ci.ps1` (PowerShell),
which run both toolchains plus a TODO-drift check. Do **not** add GitHub Actions;
the repo intentionally avoids them.

## What lives where

- `core/` — storage engine: Log / Queue / Map primitives (`core/src/{log,queue,map,storage}.rs`), metrics `/metrics` HTTP server (`core/src/http.rs`).
- `routing/` — Unified Routing Engine: Topic/Stream/Graph routers + Rule Engine (`routing/src/`).
- `adapters/<mqtt|kafka|amqp|resp|native>/` — one crate per wire protocol. Each has `src/{broker,codec,server}.rs`, a `tests/integration.rs` TCP suite, and a `fuzz/` crate for its frame parser (highest-value fuzz surface — untrusted bytes off the network).
- `synapse-native-client/` — Rust SDK for the native protocol track.
- `synapse-studio/` — `axum` web UI; currently surfaces only `/metrics`.
- `conformance/harness/` — wire-roundtrip tests vs real client libs (see below).
- `controlplane/` — Go cluster control plane (Raft/HA, Phase 4). `cmd/synapsectl/` — Go CLI.

## Commands that are easy to get wrong

- **Single perf gate test** (must pass in `--release`):
  `cargo test -p synapse-routing --release sustains_one_million_ops_per_sec`
- **Raft consensus tests** need the optional feature:
  `cargo test -p synapse-core --features consensus consensus`
- **Real third-party client conformance suites** are `#[ignore]`d and need features:
  `cargo test -p synapse-conformance-harness --features rdkafka -- --ignored` (needs a C toolchain)
  `cargo test -p synapse-conformance-harness --features lapin -- --ignored`
- **TODO drift check** (runs in CI): `scripts/check_todo.sh`. It fails if a checked-off
  `TODO.md` item (`- [x]`) references a path that does not exist — so when you implement
  something, check off the matching item and ensure the referenced paths exist.
- **Local pre-push "CI"**: `git config core.hooksPath .githooks` (not enabled by default);
  then every push runs `scripts/ci.sh`.

## Feature flags (in `synapse-core`)

- `consensus` — enables the embedded `openraft` Raft core. Off by default.
- `io-uring` — Linux-only `tokio-uring` network backend. **Off by default** so the crate
  builds on any OS (the dev box is Windows). Do not enable `io-uring` off Linux.

## Docs to read first

- `spec.txt` — the main design rationale / architecture. Read this before coding.
- `TODO.md` — implementation progress and the per-phase milestones; it is the
  source of truth for "what's done".
- `README.md` — layout and build summary.
- **`SPEC.md` is unrelated** — it specifies the `.tptmq` Fleet IoT telemetry frame
  format, which is **not** part of tpt-synapse's protocol surface and is out of scope.
  The Native Protocol track borrows its design ideas only; don't implement `.tptmq` here.

## Conventions

- Rust 2021, Apache-2.0, `publish = false` for all crates.
- Wire adapters must stay drop-in compatible with their incumbent client libraries.
  Track wire-compat status and migration checks in `conformance/COMPATIBILITY.md`.
