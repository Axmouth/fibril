use std::{collections::HashMap, hint::black_box};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fibril_protocol::v1::{
    ContentType, Op, Publish,
    helper::{try_decode, try_encode},
};

fn publish_frame(
    payload_size: usize,
    content_type: Option<ContentType>,
    headers: HashMap<String, String>,
) -> Publish {
    Publish {
        topic: "test".into(),
        group: None,
        partition: 0,
        payload: vec![1u8; payload_size],
        published: 1234567890,
        content_type,
        headers,
        require_confirm: false,
        partition_key: None,
    }
}

fn bench_case(c: &mut Criterion, name: &str, publish: Publish) {
    c.bench_function(&format!("encode_publish/{name}"), |b| {
        b.iter_batched(
            || publish.clone(),
            |publish| try_encode(Op::Publish, 1, black_box(&publish)).unwrap(),
            BatchSize::SmallInput,
        )
    });

    let frame = try_encode(Op::Publish, 1, &publish).unwrap();
    c.bench_function(&format!("decode_publish/{name}"), |b| {
        b.iter_batched(
            || frame.clone(),
            |frame| {
                let decoded: Publish = try_decode(black_box(&frame)).unwrap();
                black_box(decoded);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function(&format!("encode_decode_publish/{name}"), |b| {
        b.iter_batched(
            || publish.clone(),
            |publish| {
                let frame = try_encode(Op::Publish, 1, black_box(&publish)).unwrap();
                let decoded: Publish = try_decode(black_box(&frame)).unwrap();
                black_box(decoded);
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_encode_decode(c: &mut Criterion) {
    bench_case(
        c,
        "1k_no_headers",
        publish_frame(1024, None, HashMap::new()),
    );
    bench_case(
        c,
        "1k_content_type",
        publish_frame(1024, Some(ContentType::Json), HashMap::new()),
    );
    bench_case(
        c,
        "1k_user_headers",
        publish_frame(
            1024,
            None,
            HashMap::from([
                ("x-trace-id".into(), "trace-123".into()),
                ("x-tenant".into(), "demo".into()),
                ("x-route".into(), "email".into()),
            ]),
        ),
    );
    bench_case(
        c,
        "64k_no_headers",
        publish_frame(64 * 1024, None, HashMap::new()),
    );
}

criterion_group!(benches, bench_encode_decode);
criterion_main!(benches);
