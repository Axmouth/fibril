//! Microbench for the stream fan-out filter hot path. Filters are evaluated per
//! record per subscriber, so this characterizes the per-match cost and the
//! aggregate cost of evaluating one filter across a high-fan-out subscriber set
//! (the figure that a future "evaluate once per shared filter" dedup would save).

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use fibril_broker::stream::{StreamFilter, WildcardPattern};

fn headers() -> HashMap<String, String> {
    let mut h = HashMap::new();
    h.insert("region".to_string(), "eu-west-1".to_string());
    h.insert("tier".to_string(), "gold".to_string());
    h.insert("type".to_string(), "order.created".to_string());
    h.insert("tenant".to_string(), "acme-corp".to_string());
    h
}

fn bench_wildcard(c: &mut Criterion) {
    let exact = WildcardPattern::new("eu-west-1");
    let glob = WildcardPattern::new("eu-*");
    let multi = WildcardPattern::new("order.*.v*");

    c.bench_function("wildcard_exact_hit", |b| {
        b.iter(|| black_box(exact.matches(black_box("eu-west-1"))))
    });
    c.bench_function("wildcard_glob_hit", |b| {
        b.iter(|| black_box(glob.matches(black_box("eu-west-1"))))
    });
    c.bench_function("wildcard_glob_miss", |b| {
        b.iter(|| black_box(glob.matches(black_box("us-east-1"))))
    });
    c.bench_function("wildcard_multi_segment", |b| {
        b.iter(|| black_box(multi.matches(black_box("order.created.v2"))))
    });
}

fn bench_filter(c: &mut Criterion) {
    let headers = headers();
    let empty = StreamFilter::new();
    let single = StreamFilter::from_pairs([("region", "eu-*")]);
    let multi = StreamFilter::from_pairs([("region", "eu-*"), ("tier", "gold"), ("type", "order.*")]);
    let miss = StreamFilter::from_pairs([("region", "us-*")]);

    c.bench_function("filter_empty", |b| {
        b.iter(|| black_box(empty.matches(black_box(&headers))))
    });
    c.bench_function("filter_single_clause_hit", |b| {
        b.iter(|| black_box(single.matches(black_box(&headers))))
    });
    c.bench_function("filter_multi_clause_hit", |b| {
        b.iter(|| black_box(multi.matches(black_box(&headers))))
    });
    c.bench_function("filter_miss", |b| {
        b.iter(|| black_box(miss.matches(black_box(&headers))))
    });
}

/// One published record fanning out to N subscribers that all carry the same
/// filter: matches() runs N times today. This is the cost a per-record dedup
/// (evaluate once per distinct filter) would collapse to a single call.
fn bench_fanout_sweep(c: &mut Criterion) {
    let headers = headers();
    let filter = StreamFilter::from_pairs([("region", "eu-*"), ("tier", "gold")]);

    for n in [16_usize, 256, 4096] {
        c.bench_function(&format!("fanout_shared_filter_{n}_subscribers"), |b| {
            b.iter(|| {
                let mut delivered = 0u64;
                for _ in 0..n {
                    if filter.matches(black_box(&headers)) {
                        delivered += 1;
                    }
                }
                black_box(delivered)
            })
        });
    }
}

criterion_group!(benches, bench_wildcard, bench_filter, bench_fanout_sweep);
criterion_main!(benches);
