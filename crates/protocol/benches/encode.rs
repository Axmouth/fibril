use criterion::{Criterion, criterion_group, criterion_main};
use fibril_protocol::v1::{
    Op, Publish,
    helper::{decode, encode},
};

fn bench_encode_decode(c: &mut Criterion) {
    let payload = vec![1u8; 1024];

    let frame = encode(
        Op::Publish,
        1,
        &Publish {
            topic: "test".into(),
            group: None,
            partition: 0,
            payload: payload.clone(),
            published: 1234567890,
            require_confirm: false,
        },
    );

    c.bench_function("encode_decode_publish", |b| {
        b.iter(|| {
            let encoded = frame.clone();
            let _: Publish = decode(&encoded);
        })
    });
}

criterion_group!(benches, bench_encode_decode);
criterion_main!(benches);
