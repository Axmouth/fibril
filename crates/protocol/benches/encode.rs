use std::{collections::HashMap, hint::black_box};

use bytes::{BufMut, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use fibril_protocol::v1::{
    ContentType, Op, Publish, ReplicationEventRead, ReplicationEventRecord, ReplicationMessageRead,
    ReplicationMessageRecord, ReplicationReadOk,
    helper::{try_decode, try_encode},
};
use fibril_storage::Partition;

fn publish_frame(
    payload_size: usize,
    content_type: Option<ContentType>,
    headers: HashMap<String, String>,
) -> Publish {
    Publish {
        topic: "test".into(),
        group: None,
        partition: Partition::new(0),
        payload: vec![1u8; payload_size],
        published: 1234567890,
        content_type,
        headers,
        require_confirm: false,
        partition_key: None,
        partitioning_version: 0,
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

struct RawDecodeStats {
    records: usize,
    bytes: usize,
    checksum: u64,
}

fn replication_read_ok(message_count: usize, payload_size: usize) -> ReplicationReadOk {
    let messages = (0..message_count)
        .map(|idx| ReplicationMessageRecord {
            offset: idx as u64,
            flags: (idx % 8) as u16,
            headers: vec![idx as u8; 16],
            payload: vec![(idx % 251) as u8; payload_size],
        })
        .collect();
    let events = (0..message_count)
        .map(|idx| ReplicationEventRecord {
            offset: idx as u64,
            payload: vec![(idx % 127) as u8; 48],
        })
        .collect();

    ReplicationReadOk {
        messages: ReplicationMessageRead::Batch {
            epoch: 42,
            requested_offset: 0,
            next_offset: message_count as u64,
            records: messages,
        },
        events: ReplicationEventRead::Batch {
            epoch: 42,
            requested_offset: 0,
            next_offset: message_count as u64,
            records: events,
        },
    }
}

fn raw_replication_payload(read: &ReplicationReadOk) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"FRR2");

    match &read.messages {
        ReplicationMessageRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            buf.put_u8(0);
            buf.put_u64(*epoch);
            buf.put_u64(*requested_offset);
            buf.put_u64(*next_offset);
            buf.put_u32(records.len() as u32);
            for record in records {
                buf.put_u16(record.flags);
                buf.put_u32(record.headers.len() as u32);
                buf.put_u32(record.payload.len() as u32);
                buf.extend_from_slice(&record.headers);
                buf.extend_from_slice(&record.payload);
            }
        }
        ReplicationMessageRead::CheckpointRequired(required) => {
            buf.put_u8(1);
            buf.put_u64(required.epoch);
            buf.put_u64(required.requested_offset);
            buf.put_u64(required.head_offset);
            buf.put_u64(required.next_offset);
        }
    }

    match &read.events {
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            buf.put_u8(0);
            buf.put_u64(*epoch);
            buf.put_u64(*requested_offset);
            buf.put_u64(*next_offset);
            buf.put_u32(records.len() as u32);
            for record in records {
                buf.put_u32(record.payload.len() as u32);
                buf.extend_from_slice(&record.payload);
            }
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            buf.put_u8(1);
            buf.put_u64(required.epoch);
            buf.put_u64(required.requested_offset);
            buf.put_u64(required.head_offset);
            buf.put_u64(required.next_offset);
        }
    }

    buf.freeze().to_vec()
}

fn take<'a>(payload: &'a [u8], cursor: &mut usize, len: usize) -> &'a [u8] {
    let end = *cursor + len;
    let bytes = &payload[*cursor..end];
    *cursor = end;
    bytes
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> u8 {
    let value = payload[*cursor];
    *cursor += 1;
    value
}

fn read_u16(payload: &[u8], cursor: &mut usize) -> u16 {
    let bytes = take(payload, cursor, 2);
    u16::from_be_bytes([bytes[0], bytes[1]])
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> u32 {
    let bytes = take(payload, cursor, 4);
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> u64 {
    let bytes = take(payload, cursor, 8);
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

fn decode_raw_borrowed(payload: &[u8]) -> RawDecodeStats {
    let mut cursor = 0;
    assert_eq!(take(payload, &mut cursor, 4), b"FRR2");

    let mut stats = RawDecodeStats {
        records: 0,
        bytes: 0,
        checksum: 0,
    };

    let message_kind = read_u8(payload, &mut cursor);
    if message_kind == 0 {
        let _epoch = read_u64(payload, &mut cursor);
        let requested_offset = read_u64(payload, &mut cursor);
        let next_offset = read_u64(payload, &mut cursor);
        let count = read_u32(payload, &mut cursor) as usize;
        stats.checksum ^= requested_offset ^ next_offset;
        for _ in 0..count {
            let flags = read_u16(payload, &mut cursor);
            let headers_len = read_u32(payload, &mut cursor) as usize;
            let payload_len = read_u32(payload, &mut cursor) as usize;
            let headers = take(payload, &mut cursor, headers_len);
            let body = take(payload, &mut cursor, payload_len);
            stats.records += 1;
            stats.bytes += headers.len() + body.len();
            stats.checksum ^= u64::from(flags)
                ^ headers.first().copied().unwrap_or_default() as u64
                ^ body.first().copied().unwrap_or_default() as u64;
        }
    } else {
        for _ in 0..4 {
            stats.checksum ^= read_u64(payload, &mut cursor);
        }
    }

    let event_kind = read_u8(payload, &mut cursor);
    if event_kind == 0 {
        let _epoch = read_u64(payload, &mut cursor);
        let requested_offset = read_u64(payload, &mut cursor);
        let next_offset = read_u64(payload, &mut cursor);
        let count = read_u32(payload, &mut cursor) as usize;
        stats.checksum ^= requested_offset ^ next_offset;
        for _ in 0..count {
            let payload_len = read_u32(payload, &mut cursor) as usize;
            let event = take(payload, &mut cursor, payload_len);
            stats.records += 1;
            stats.bytes += event.len();
            stats.checksum ^= event.first().copied().unwrap_or_default() as u64;
        }
    } else {
        for _ in 0..4 {
            stats.checksum ^= read_u64(payload, &mut cursor);
        }
    }

    assert_eq!(cursor, payload.len());
    stats
}

fn decode_raw_owned(payload: &[u8]) -> ReplicationReadOk {
    let mut cursor = 0;
    assert_eq!(take(payload, &mut cursor, 4), b"FRR2");

    let messages = if read_u8(payload, &mut cursor) == 0 {
        let epoch = read_u64(payload, &mut cursor);
        let requested_offset = read_u64(payload, &mut cursor);
        let next_offset = read_u64(payload, &mut cursor);
        let count = read_u32(payload, &mut cursor) as usize;
        let mut records = Vec::with_capacity(count);
        for idx in 0..count {
            let flags = read_u16(payload, &mut cursor);
            let headers_len = read_u32(payload, &mut cursor) as usize;
            let payload_len = read_u32(payload, &mut cursor) as usize;
            let headers = take(payload, &mut cursor, headers_len).to_vec();
            let payload = take(payload, &mut cursor, payload_len).to_vec();
            records.push(ReplicationMessageRecord {
                offset: requested_offset + idx as u64,
                flags,
                headers,
                payload,
            });
        }
        ReplicationMessageRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        }
    } else {
        ReplicationMessageRead::CheckpointRequired(
            fibril_protocol::v1::ReplicationCheckpointRequired {
                epoch: read_u64(payload, &mut cursor),
                requested_offset: read_u64(payload, &mut cursor),
                head_offset: read_u64(payload, &mut cursor),
                next_offset: read_u64(payload, &mut cursor),
            },
        )
    };

    let events = if read_u8(payload, &mut cursor) == 0 {
        let epoch = read_u64(payload, &mut cursor);
        let requested_offset = read_u64(payload, &mut cursor);
        let next_offset = read_u64(payload, &mut cursor);
        let count = read_u32(payload, &mut cursor) as usize;
        let mut records = Vec::with_capacity(count);
        for idx in 0..count {
            let payload_len = read_u32(payload, &mut cursor) as usize;
            let payload = take(payload, &mut cursor, payload_len).to_vec();
            records.push(ReplicationEventRecord {
                offset: requested_offset + idx as u64,
                payload,
            });
        }
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        }
    } else {
        ReplicationEventRead::CheckpointRequired(
            fibril_protocol::v1::ReplicationCheckpointRequired {
                epoch: read_u64(payload, &mut cursor),
                requested_offset: read_u64(payload, &mut cursor),
                head_offset: read_u64(payload, &mut cursor),
                next_offset: read_u64(payload, &mut cursor),
            },
        )
    };

    assert_eq!(cursor, payload.len());
    ReplicationReadOk { messages, events }
}

fn bench_replication_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("replication_read_ok_decode");
    for (message_count, payload_size) in [(4096, 1024), (128, 64 * 1024)] {
        let name = format!("{message_count}x{payload_size}");
        let read = replication_read_ok(message_count, payload_size);
        let msgpack = rmp_serde::to_vec_named(&read).unwrap();
        let raw = raw_replication_payload(&read);
        let raw_stats = decode_raw_borrowed(&raw);
        assert_eq!(raw_stats.records, message_count * 2);
        assert_eq!(raw_stats.bytes, message_count * (payload_size + 16 + 48));
        assert_eq!(decode_raw_owned(&raw), read);

        group.throughput(criterion::Throughput::Bytes(msgpack.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("msgpack_owned", &name),
            &msgpack,
            |b, payload| {
                b.iter(|| {
                    let decoded: ReplicationReadOk =
                        rmp_serde::from_slice(black_box(payload)).unwrap();
                    black_box(decoded);
                })
            },
        );

        group.throughput(criterion::Throughput::Bytes(raw.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("raw_borrowed", &name),
            &raw,
            |b, payload| b.iter(|| black_box(decode_raw_borrowed(black_box(payload)))),
        );
        group.bench_with_input(BenchmarkId::new("raw_owned", &name), &raw, |b, payload| {
            b.iter(|| black_box(decode_raw_owned(black_box(payload))))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode_decode, bench_replication_decode);
criterion_main!(benches);
