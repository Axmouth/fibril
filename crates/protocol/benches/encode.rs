use criterion::{Criterion, criterion_group, criterion_main};
use fibril_protocol::v1::{
    Op, Publish,
    helper::{try_decode, try_encode},
};
use std::collections::HashMap;

fn bench_encode_decode(c: &mut Criterion) {
    let payload = vec![1u8; 1024];

    let frame = try_encode(
        Op::Publish,
        1,
        &Publish {
            topic: "test".into(),
            group: None,
            partition: 0,
            payload: payload.clone(),
            published: 1234567890,
            content_type: None,
            headers: HashMap::new(),
            require_confirm: false,
        },
    )
    .unwrap();

    c.bench_function("encode_decode_publish", |b| {
        b.iter(|| {
            let encoded = frame.clone();
            let _: Publish = try_decode(&encoded).unwrap();
        })
    });
}

criterion_group!(benches, bench_encode_decode);
criterion_main!(benches);
