//! Throughput benchmark for the Topic Router — tracks the "1M+ routing ops/sec"
//! milestone from TODO.md. Run with `cargo bench -p synapse-routing`.
//! The `sustains_one_million_ops_per_sec` unit test in topic.rs is the CI gate;
//! this bench provides the continuous baseline numbers.

use criterion::{criterion_group, criterion_main, Criterion};
use synapse_routing::topic::TopicRouter;

fn bench_topic_route(c: &mut Criterion) {
    let r = TopicRouter::new();
    for i in 0..64 {
        r.subscribe(&format!("s{i}"), "sensors/+/temp");
    }
    c.bench_function("routing_topic_route", |b| {
        b.iter(|| {
            criterion::black_box(r.route("sensors/room1/temp").len());
        });
    });
}

criterion_group!(benches, bench_topic_route);
criterion_main!(benches);
