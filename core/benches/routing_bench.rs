//! Historical throughput tracking for the Phase 1 "1M+ routing ops/sec"
//! milestone (TODO.md). Run with `cargo bench -p synapse-core`; the numbers
//! produced here are the continuous baseline that PRs must not regress.

use criterion::{criterion_group, criterion_main, Criterion};
use synapse_core::SynapseCore;

fn bench_log_append(c: &mut Criterion) {
    let core = SynapseCore::new();
    core.create_log("acme", "events").unwrap();
    c.bench_function("core_log_append", |b| {
        b.iter(|| {
            core.log_append("acme", "events", b"payload").unwrap();
        });
    });
}

fn bench_engine_roundtrip(c: &mut Criterion) {
    let core = SynapseCore::new();
    core.create_log("acme", "events").unwrap();
    core.create_queue("acme", "jobs").unwrap();
    core.create_map("acme", "cache").unwrap();
    c.bench_function("engine_roundtrip", |b| {
        b.iter(|| {
            let off = core.log_append("acme", "events", b"x").unwrap();
            criterion::black_box(off);
            core.queue_enqueue("acme", "jobs", b"y").unwrap();
            core.map_set("acme", "cache", "k", b"v", None).unwrap();
        });
    });
}

criterion_group!(benches, bench_log_append, bench_engine_roundtrip);
criterion_main!(benches);
