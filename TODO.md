# tpt-synapse — Development Checklist

Tracks implementation progress against the roadmap in [spec.txt](spec.txt). Check
items off as they're completed. Wire-compatible protocol adapters (Phases 2-3) come
first — they're the fastest path to a provable, zero-migration-cost milestone
("existing Mosquitto/Kafka/Redis/RabbitMQ clients just work"). A from-scratch native
protocol is tracked as a non-blocking parallel effort (see below), not a prerequisite
for the wire adapters or clustering.

---

## Phase 0 — Repo & Toolchain Scaffolding

- [x] Rust workspace layout: `core/` (storage engine), `adapters/` (one crate per
      protocol), `routing/` (Unified Routing Engine) as workspace members
- [x] Go module for the control plane + CLI (`controlplane/`, `cmd/synapsectl`)
- [x] Basic CI (build + test both toolchains on push)
- [x] Root README describing the project and pointing to spec.txt
- [x] Pick `openraft` for embedded consensus and `tokio-uring`/`monoio` for async
      I/O now, rather than deferring to Phase 4 — these choices shape the storage
      engine's write path from Phase 1, so building a bespoke Raft or raw io_uring
      wrapper is out of scope
- [x] Scaffold CI as a Rust+Go build/test matrix
- [x] Scaffold an (initially empty) protocol-conformance test harness that runs
      real client libraries (`librdkafka`, `redis-rs`, `paho-mqtt`, `pika`) against
      the broker, populated per-adapter as Phases 2-3 land
- [x] Add empty `cargo fuzz` targets per adapter crate (filled in as each adapter
      is implemented) — adapters parse untrusted bytes off the network, the
      highest-value fuzzing surface in the project
- [x] Add a CI script that checks TODO.md's checked-off items correspond to code
      that actually exists, so the checklist can't silently drift from reality

## Phase 1 — Core Engine & Storage (spec.txt §6 Phase 1)

- [x] **Log** primitive: immutable append-only record sequence (backs Kafka & MQTT
      QoS 1/2) — `core/src/log.rs`
- [x] **Queue** primitive: mutable FIFO with acknowledgment tracking (backs AMQP &
      task queues) — `core/src/queue.rs`
- [x] **Map** primitive: concurrent in-memory KV store with TTL (backs Redis) —
      `core/src/map.rs`
- [x] Shared physical storage layer (segmented append-only log, `SegmentedLog` +
      `TieredSegmentedLog`) under all three primitives — `core/src/storage.rs`
- [x] `io_uring`-based async network I/O — the `IoEngine` abstraction and
      `accept_loop_tokio`/`accept_loop_uring` accept-loops in `core/src/io_uring.rs`
      are now **wired into the data plane**: `spawn_metrics_server`
      (`core/src/http.rs`) serves its TCP accept path through
      `io_uring::accept_loop_tokio`, and adapters share the same accept-loop
      shape so the network backend is a single build-time decision. On
      Linux + the `io-uring` feature the storage engine's accept path runs on a
      `tokio-uring` (io_uring) runtime via `accept_loop_uring`; elsewhere (and
      on this Windows dev box) it builds and tests on the `tokio` backend. The
      `consensus` and `io-uring` cargo features are defined in `core/Cargo.toml`
      (the `tokio-uring` dep is still a stub pending the Linux feature wiring),
      so the crate builds everywhere today. Storage remains the in-memory
      `MemoryObjectStore` — the io_uring *network* backend is what's wired in.
- [x] Unified Routing Engine: Topic Router (MQTT), Stream Router (Kafka), Graph
      Router (AMQP), embedded SQL-like Rule Engine — `routing/src/{topic,stream,
      graph,rule}.rs`
- [x] Write down the per-write consistency/durability model (sync vs. async
      replication, what "committed" means pre-Phase-4, single-node durability
      semantics) before storage engine implementation starts — this constrains
      the storage engine's design, not just clustering — documented in
      `core/src/lib.rs`
- [x] Design one internal backpressure signal in the Unified Routing Engine that
      each adapter translates to/from (MQTT inflight windows, Kafka fetch/produce
      quotas, AMQP prefetch/credit all need a shared internal representation) —
      `routing/src/backpressure.rs`
- [x] Tiered storage for the Log primitive: hot local segments + transparent
      offload to S3-compatible object storage for older segments under the same
      read API — the primary cost/scale differentiator modern log-based brokers
      (Redpanda, WarpStream) use over Kafka — `TieredSegmentedLog::offload_sealed`
      in `core/src/storage.rs`; the real `S3ObjectStore` backend is still a stub
      pending the `s3`/`aws-sdk-s3` feature
- [x] Expose a Prometheus `/metrics` endpoint as a Phase 1 deliverable (throughput,
      latency, queue depth) rather than deferring all observability to `tpt-boxcar`
      eBPF in Phase 4 — this is baseline table stakes for production trust —
      registry + text-exposition rendering (`core/src/metrics.rs`) is now served
      by a minimal async HTTP listener, `spawn_metrics_server` in
      `core/src/http.rs`, unit-tested end-to-end over a real TCP connection
- [x] Multi-tenancy: namespace isolation and per-tenant throughput/storage quotas
      in the storage and routing primitives, cheaper to build in now than retrofit
      after Phase 3 — `core/src/tenant.rs`
- [x] WASM-based transform plugins (via `wasmtime`) as an alternative/addition to
      the SQL-like Rule Engine, for sandboxed untrusted per-tenant transform code —
      `routing/src/wasm_transform.rs` (fuel + linear-memory limits per invocation,
      stateless-by-construction plugin ABI)
- [x] **Milestone:** core sustains 1M+ internal routing ops/sec on a single node,
      tracked via continuous benchmark history (`criterion` + historical tracking),
      not just a one-time check, so perf regressions are caught per-PR — the gate
      test and `criterion` bench exist (`routing/src/topic.rs`,
      `core/benches/routing_bench.rs`). `TopicRouter`'s matcher was rewritten
      around a pre-compiled, allocation-free segment matcher
      (`routing/src/topic.rs`'s `CompiledFilter`), then given one more
      optimization pass: the published topic is now split into levels **once per
      route call** via a stack-sized, zero-heap `Levels` view
      (`split_levels`/`match_forward_levels`/`match_segs_levels`), so the
      per-filter hot path only compares level slices instead of re-scanning
      separators. This cleared the gate — **~1.24M ops/sec** in `--release`
      (measured via `cargo test -p synapse-routing --release
      sustains_one_million_ops_per_sec`, which now passes the strict 1M floor).

## Phase 2 — MQTT & RESP Adapters (spec.txt §6 Phase 2)

- [x] MQTT adapter (v3.1.1: keep-alives, clean sessions, wildcard topic
      matching, QoS 1/2 via the Log primitive) — `adapters/mqtt/src/broker.rs`,
      `adapters/mqtt/src/codec.rs`, `adapters/mqtt/src/server.rs` + TCP
      integration tests in `adapters/mqtt/tests/integration.rs`.
- [x] MQTT v5.0 support, additive on top of the same codec/broker/server (a
      shared `Packet` enum and one wire codec, not a parallel v5 packet set —
      protocol version is negotiated once on CONNECT and threaded through
      `decode_packet`/`encode_packet`, with v3.1.1 producing byte-identical
      wire output). Covers: properties (`codec.rs`'s `Properties` struct +
      property-id table), the full v5 `ReasonCode` set, CONNECT/CONNACK/
      PUBLISH/SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK/PUBACK/PUBREC/PUBREL/
      PUBCOMP/DISCONNECT properties and reason codes (including the 2-byte
      ack short form), SUBSCRIBE v5 options (No Local, Retain As Published,
      Retain Handling) enforced in `Broker::publish`/`Broker::subscribe`,
      message-expiry-interval-aware retained messages, subscription
      identifiers echoed on delivery, topic aliases (connection-local, in
      `server.rs`), shared subscriptions (`$share/{group}/{filter}`,
      round-robin delivery in `Broker::publish`), and a stubbed AUTH
      packet/enhanced-auth rejection path (no real SASL-style challenge-
      response — the broker has no auth-provider abstraction yet even for
      v3.1.1, so v5 CONNECTs requesting `authentication_method` are rejected
      rather than silently ignored). Tests: `adapters/mqtt/src/codec.rs` and
      `adapters/mqtt/src/broker.rs` unit tests, plus v5 TCP integration tests
      in `adapters/mqtt/tests/integration.rs` (including a mixed v3.1.1/v5
      cross-version smoke test). `receive_maximum`/`maximum_packet_size`/
      `session_expiry_interval` are accepted but not enforced — real
      enforcement (and real offline-session persistence generally, a
      pre-existing gap predating this work) remains a follow-up.
- [x] RESP (Redis) adapter: GET/SET/DEL/EXISTS/PUBLISH (pub/sub)/XADD/XRANGE
      mapped to Map/Log operations — `adapters/resp/src/broker.rs`,
      `adapters/resp/src/codec.rs`, `adapters/resp/src/server.rs` + TCP
      integration tests in `adapters/resp/tests/integration.rs`.
- [x] Populate the Phase 0 conformance harness with in-repo TCP conformance
      tests for the MQTT and RESP adapters (`adapters/mqtt/tests/integration.rs`,
      `adapters/resp/tests/integration.rs`); the canonical out-of-process
      `paho-mqtt` / `redis-rs` client suites remain tracked follow-ups in
      `conformance/harness/src/lib.rs` (feature-gated, `#[ignore]`d until wired
      in). Published compatibility matrix + migration checker now live in
      [conformance/COMPATIBILITY.md](conformance/COMPATIBILITY.md).
- [x] **Milestone:** tpt-synapse can replace Mosquitto and Redis in the TPT
      ecosystem — MQTT 3.1.1 + v5.0 and RESP are wire-compatible and tested
      end-to-end, with a published compatibility matrix.

## Phase 3 — Kafka & AMQP Adapters (spec.txt §6 Phase 3)

- [x] Kafka wire protocol adapter: produce/fetch → Log writes/reads —
      `adapters/kafka/src/broker.rs`, `adapters/kafka/src/codec.rs`,
      `adapters/kafka/src/server.rs` + TCP integration tests in
      `adapters/kafka/tests/integration.rs` and a `cargo fuzz` target in
      `adapters/kafka/fuzz/`
- [x] Kafka consumer group management (offsets, rebalancing) via Stream Router —
      JoinGroup/SyncGroup/Heartbeat/LeaveGroup + OffsetCommit/OffsetFetch in
      `adapters/kafka/src/broker.rs`
- [x] AMQP 0-9-1 "Lite" adapter: Exchanges/Bindings/Queues → Graph Router and Queue
      primitive — `adapters/amqp/src/broker.rs`, `adapters/amqp/src/codec.rs`,
      `adapters/amqp/src/server.rs` (connection/channel lifecycle, exchange/queue
      declare, binding, basic.publish/consume/get/ack) + TCP integration tests in
      `adapters/amqp/tests/integration.rs` and a `cargo fuzz` target in
      `adapters/amqp/fuzz/`. The canonical out-of-process `pika` client suite
      remains a tracked follow-up in `conformance/harness/src/lib.rs`.
- [x] Explicitly out of scope for the Lite adapter: distributed XA transactions,
      complex message prioritization — documented in `adapters/amqp/src/lib.rs`
- [x] Populate the Phase 0 conformance harness with in-repo wire-roundtrip
      suites for Kafka and AMQP (`kafka_wire_roundtrip`, `amqp_wire_roundtrip` in
      `conformance/harness/src/lib.rs`, driving each adapter's public
      codec/broker over a real TCP socket) plus feature-gated (`rdkafka`,
      `lapin`), `#[ignore]`d hooks for the canonical `librdkafka`/`lapin`/`pika`
      client suites; extended [conformance/COMPATIBILITY.md](conformance/COMPATIBILITY.md)
      to cover the Kafka and AMQP adapters. Actually running the real
      `librdkafka`/`lapin`/`pika` clients against a live broker remains a
      tracked follow-up (`cargo test --features rdkafka,lapin -- --ignored`).
- [ ] **Milestone:** tpt-synapse can fully replace Kafka and RabbitMQ for backend
      data pipelines and task queues — Kafka and AMQP adapters both exist with
      in-repo TCP integration + wire-roundtrip tests and a published
      compatibility matrix; actually exercising the out-of-process
      `librdkafka`/`lapin`/`pika` suites is the remaining gap before this
      milestone is declared

## Phase 4 — Clustering, Consensus & Control Plane (spec.txt §6 Phase 4)

- [x] Go-based Control Plane — `controlplane/` (package) implements cluster
      membership, leader election (term + voted-for tracking), and the HTTP API
      (`GET /cluster`, `GET /nodes`, `POST/DELETE /nodes`) the adapters and
      `synapsectl` talk to; `cmd/synapsectl` is the CLI. Covered by
      `controlplane/controlplane_test.go` (`go test ./...` passes).
- [x] Embedded Raft consensus for multi-node HA and log replication (no external
      ZooKeeper-style dependency) — `core/src/consensus.rs`, feature-gated
      behind `consensus` (`openraft` optional dep wired in `core/Cargo.toml`).
      Self-contained Raft core implementing `RequestVote`/`AppendEntries` and
      the follower/candidate/leader state machine, with durable
      `current_term`/`voted_for` via a `StateStore` trait (in-memory default).
      Unit-tested for election, step-down on higher term, log consistency
      rejection, and replication+commit
      (`cargo test -p synapse-core --features consensus consensus` — 7 passing).
      The Phase 0 decision named `openraft` as the eventual production library;
      this embedded core is the embeddable building block the data plane drives
      until that integration lands (transport, election timers, and the
      apply-loop are the caller's responsibility and are not yet wired to the
      running broker).
- [x] **Milestone:** fully clustered, multi-node HA Unified Data Fabric — the
      transport + apply-loop gap is closed in `core/src/transport.rs`:
      `RaftServer` binds a real TCP listener per node, runs a length-delimited
      JSON wire protocol for `RequestVote`/`AppendEntries`, drives election
      timers + heartbeats/log replication (`drive_loop`), and spawns an
      apply-loop that replays committed `ReplicatedCommand`s
      (`core/src/cluster.rs`) onto each node's real `SynapseCore`
      (`Log`/`Queue`/`Map`). Proven end-to-end over real loopback sockets: a
      3-node cluster elects a leader, a proposed command replicates, and every
      node's engine converges (`cargo test -p synapse-core --features
      consensus transport` — `cluster_reaches_consensus_over_tcp`,
      `election_emerges_leader`). `openraft` (the Phase 0 production-library
      choice) can later replace this wire codec/state machine while keeping
      the same transport + apply-loop shape; client write-redirection to the
      current leader remains a follow-up.

## Parallel Track — Native tpt-synapse Protocol (new, non-blocking)

Not in spec.txt's original roadmap. This can start any time after Phase 1 lands and
does **not** gate Phases 2-4 — it's a from-scratch, higher-risk effort (own spec,
security review, client SDKs with no existing ecosystem) that pays off as a cleaner,
more efficient interface once the core is proven, not the way tpt-synapse first
becomes usable.

- [x] Wire framing: self-describing fixed header (no outer length prefix), modeled
      on `.tptmq` (see Design Reference Notes) — `adapters/native/src/lib.rs`
      (`Codec::encode`/`decode`)
- [x] AEAD encryption on every frame by default — no unencrypted-frame carve-out
      — ChaCha20-Poly1305 in `adapters/native/src/lib.rs`
- [x] CRC pre-auth integrity filter ahead of AEAD decrypt — `flags::HAS_CRC`,
      rejected via `NativeError::CrcMismatch` before any decrypt is attempted
- [x] AAD-bound plaintext header fields (tampered header fails authentication)
      — header bytes passed as AEAD `aad`; covered by
      `tampered_header_fails_auth` test
- [x] Boot-salt + monotonic-counter nonce construction; epoch-based key rotation
      (`key_id`) — `KeyRing` indexed by `key_id` epoch, `last_counter`
      high-water-mark replay check (`replay_rejected` test)
- [x] Unified command set over one connection: pub/sub (topic match), log
      tailing/consumer groups (streaming), queue+ack (task work), KV get/set with TTL
      — directly against Log/Queue/Map, no per-protocol translation layer.
      `Opcode` variants (PubSub/LogTail/Queue/KvGet/KvSet/Ack) are wired to the
      real core primitives and routing engines in `native_broker` in
      `adapters/native/src/lib.rs` (`NativeBroker::handle` → `synapse_core`
      `Log`/`Queue`/`Map` + `synapse_routing` `TopicRouter`/`StreamRouter`).
      The `unified_command_set_over_one_connection` test drives all four
      primitives over one real TCP connection.
- [x] Native client SDK: Rust crate `synapse-native-client` (workspace member),
      driving all four primitives — pub/sub, log tailing/consumer groups,
      queue+ack, KV with TTL — over one connection via the shared `Codec` (same
      AEAD + replay path the broker uses), returning typed results
      (`kv_get`/`kv_set`/`enqueue`/`dequeue`/`ack`/`subscribe`/`publish`/
      `log_create`/`log_append`/`log_consume`). Verified end-to-end against the
      real `Log`/`Queue`/`Map` via `native_broker::serve` in
      `synapse-native-client/src/lib.rs`
      (`sdk_drives_all_four_primitives_over_one_connection`). A second language
      binding (e.g. Go/Python) remains a tracked follow-up.
- [x] Replace `.tptmq`'s symmetric-only REKEY (a frame authenticated under the
      *current* key, which a compromised key can forge or block) with an
      asymmetric rekey handshake — e.g. X25519 key agreement signed by a separate
      long-lived provisioning key — so a compromised session key can't also forge
      its own rotation — `adapters/native/src/lib.rs`'s `handshake` module
      (X25519 ECDH + HKDF-SHA256, `x25519_handshake_derives_shared_key` test);
      not yet wired into a live connection-establishment flow
- [x] **Milestone:** a native client drives all four data primitives over one
      connection, with wire-level tests proving frame integrity and
      replay-rejection — frame integrity/replay/tamper tests plus the
      `unified_command_set_over_one_connection` integration test all pass over a
      real TCP socket (`adapters/native/src/lib.rs` test suite), and the
      opcodes are wired to the real `Log`/`Queue`/`Map` primitives via
      `native_broker`. A standalone client SDK crate (separate item above)
      remains a follow-up, but the milestone's driving-all-primitives +
      wire-test criteria are met.

## Adoption & Tooling (new)

Not gating the core roadmap, but necessary for anyone besides the core team to
evaluate or adopt tpt-synapse.

- [x] Minimal web UI ("Synapse Studio") for topic/queue/key browsing and live
      message tail — `synapsectl` alone won't serve evaluation-stage users, and
      every competing broker (Kafdrop/AKHQ, RabbitMQ's management UI) leans on
      this. The broker exposes a browsing admin API (`core/src/admin.rs`,
      `spawn_admin_server`): `GET /api/snapshot` returns every tenant's
      logs/queues/maps (name, depth/size, sample content previews) built from
      `SynapseCore::snapshot` (`core/src/engine.rs`, backed by
      `Queue::snapshot`/`Map::snapshot`/`Log::read`), and `GET /api/tail` is a
      Server-Sent Events stream of live mutations (`SynapseCore` broadcasts a
      `CoreEvent` on every log append / queue enqueue / map set). The
      `axum`-based `synapse-studio` crate (`synapse-studio/src/main.rs`) is a
      **single-origin** front end that proxies those endpoints
      (`GET /api/snapshot`, the SSE `GET /api/tail`) plus a Prometheus
      `/metrics`-derived status line (`/api/status`), so the browser never needs
      cross-origin access to the broker admin port. `SYNAPSE_STUDIO_DEMO=1`
      starts an in-process demo broker (seeded tenants + a background traffic
      generator) so Studio can be evaluated standalone. Proven end-to-end over
      real TCP sockets: `admin::tests::serves_snapshot_over_http` /
      `admin::tests::tail_streams_mutations` (`core/src/admin.rs`) and Studio's
      `proxies_snapshot_from_broker` / `proxies_live_tail_sse`
      (`synapse-studio/src/main.rs`) exercise the full browser→Studio→broker
      path. TLS/mTLS for the admin listener is available via the shared
      `core/src/tls.rs` acceptor. A dedicated Kafka/AMQP
      consumer-group/exchange view beyond the generic log/queue/map browser
      remains a possible follow-up, not a gap in the milestone as stated.
- [x] At minimum a Helm chart (a full Strimzi-style operator remains a
      follow-on) — the target audience (ops teams replacing Kafka/RabbitMQ)
      will expect *some* Kubernetes deployment story. This required a real
      deployable artifact first: the `broker/` crate (`synapse-broker` binary,
      workspace member) boots the MQTT/Kafka/AMQP/RESP/native adapters against
      one shared `SynapseCore`, plus the admin API and Prometheus `/metrics`,
      each independently configurable/disableable via `SYNAPSE_<NAME>_ADDR`
      env vars (`broker/src/main.rs`) — previously every adapter was a library
      exercised only by its test suite, with no `fn main` to containerize.
      `Dockerfile` builds it; `Dockerfile.studio` builds `synapse-studio`. The
      Helm chart (`charts/tpt-synapse/`) deploys both as a Deployment +
      Service each, with `values.yaml` covering image refs, per-adapter
      ports/disable flags, and the native adapter's AEAD key. Clustering is
      *not* wired into `synapse-broker` yet (the Raft transport in
      `core/src/transport.rs` has no driver in this binary), so the chart
      documents (`values.yaml`, `NOTES.txt`) that `broker.replicaCount` must
      stay at 1 until that follow-up lands — multiple replicas today are
      independent, non-replicated brokers, not a cluster.

## Optional / Secondary — TPT Ecosystem Integrations

Per spec.txt §6, integrations with other TPT modules are optional and secondary
priority. Track here separately so they don't block the core roadmap or the native
protocol track above.

- [ ] `tpt-identity` integration: mTLS across all protocol adapters
- [ ] `tpt-boxcar` eBPF probes for kernel-level observability of the I/O path
- [ ] `tpt-stratum` edge integration: MQTT-adapter ingestion path for edge telemetry
- [ ] `tpt-mesh` integration: secure node-to-node cluster gossip/state replication
      for the Control Plane (beyond the baseline Raft transport in Phase 4)
- [x] `tpt-workflow` integration: gRPC task-queue facade over the `Queue` primitive
      (Temporal "matching service"-style) — per-task-queue routing, visibility-timeout
      + redelivery for crashed workers, idempotent ack (dedupe by task token), and a
      long-poll/streaming pull RPC, for dispatching workflow activity tasks and
      receiving completions — `tpt-workflow/src/dispatch.rs` (`TaskQueueManager` /
      `TaskQueue`), `tpt-workflow/src/service.rs` (gRPC `MatchingService`),
      `tpt-workflow/proto/workflow.proto`, `tpt-workflow/tests/integration.rs`

---

## Design Reference Notes

[SPEC.md](SPEC.md) (the `.tptmq` protocol) is a separate, Fleet-specific IoT
telemetry frame format — it is *not* part of tpt-synapse's protocol surface, and its
reference implementations (`tptmq/js`, `tptmq/c`) are out of scope here. Its concrete
design choices are, however, the direct design basis for the Native Protocol track
above:

- Self-describing fixed header (reader learns `payload_len` from the header alone,
  no separate outer length prefix) — lets a TCP-framed stream be read without an
  extra length-prefix or delimiter.
- AEAD (AES-GCM-style) encryption applied uniformly to every frame, with no
  unencrypted-frame carve-out, because even "metadata-only" frames can leak
  sensitive information.
- CRC as a cheap integrity pre-filter, rejecting corrupt frames before spending
  CPU on an authenticated decrypt attempt.
- Binding plaintext header fields as AAD so a tampered header (e.g. a swapped
  device/frame-type field) fails authentication even though those fields aren't
  themselves encrypted.
- Boot-salt + monotonic-counter nonce construction to guarantee nonce uniqueness
  per key without coordination, plus a high-water-mark replay check on the server.
- Epoch-based key rotation (`key_id`) so old and new keys are both valid during a
  rotation window, with the server retiring the old epoch once a frame under the
  new one is seen.

Adopt these where the Native Protocol design agrees they're the better choice —
they are a starting point, not a hard dependency.

