//! Pins the Rust wire encoders to the shared cross-client vectors at
//! `clients/wire_vectors.json`. The Python and TypeScript clients assert against
//! the same file, so all three implementations agree on the exact bytes for
//! every op body.
//!
//! This Rust test is the canonical generator: run
//! `WIRE_VECTORS_REGEN=1 cargo test -p fibril-protocol --test wire_vectors`
//! to rewrite the fixture from these encoders (it records every `check`ed vector
//! and writes them sorted), then the TypeScript and Python suites must still
//! match. There is no separate generator script.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

use fibril_protocol::v1::wire;
use fibril_protocol::v1::{
    Ack, AdvertisedAddress, AssignmentChanged, Auth, ContentType, DeclarePlexus, DeclarePlexusOk,
    DeclareQueue, DeclareQueueOk, Deliver, DeliveryTag, ErrorMsg, GoingAway, Hello, HelloOk, Nack,
    Partition, Publish, PublishDelayed, PublishOk, QueueDlqPolicy, QueueTopologyEntry,
    ReconcileClient, ReconcilePolicy, ReconcileSubscription, Redirect, ResumeIdentity,
    ResumeOutcome, StreamDurability, StreamRetention, StreamStart, StreamTopologyEntry, Subscribe,
    SubscribeOk, SubscribeStream, TopologyOk, TopologyRequest, TopologyUpdateAck,
};
use serde_json::Value;
use uuid::Uuid;

/// A deterministic 16-byte uuid filled with `byte` (matches the vector generator).
fn uuid_filled(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn header(key: &str, value: &str) -> HashMap<String, String> {
    let mut h = HashMap::new();
    h.insert(key.to_owned(), value.to_owned());
    h
}

fn load_vectors() -> Value {
    // CARGO_MANIFEST_DIR is crates/protocol; the shared fixture is at the repo
    // root under clients/.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../clients/wire_vectors.json");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).expect("parse wire_vectors.json")
}

// Every `(name, hex)` passed through `check`, in call order, so a regen run can
// write the full fixture from the Rust encoders.
thread_local! {
    static COLLECTED: RefCell<Vec<(String, String)>> = const { RefCell::new(Vec::new()) };
}

fn regen() -> bool {
    std::env::var_os("WIRE_VECTORS_REGEN").is_some()
}

/// Encoders return a full `Frame`; the body is the payload (magic + fields),
/// which is exactly what the other clients encode and what the fixture holds.
fn check(vectors: &Value, name: &str, payload: bytes::Bytes) {
    let actual = hex_encode(&payload);
    COLLECTED.with(|c| c.borrow_mut().push((name.to_string(), actual.clone())));
    if regen() {
        return;
    }
    let expected = vectors
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing vector {name}"));
    assert_eq!(actual, expected, "{name} bytes diverge from shared vectors");
}

/// Write the collected vectors to the shared fixture, sorted by name for a
/// deterministic file. Called at the end of the test only under `WIRE_VECTORS_REGEN`.
fn write_regenerated_vectors() {
    let mut items = COLLECTED.with(|c| c.borrow().clone());
    items.sort_by(|a, b| a.0.cmp(&b.0));
    let mut map = serde_json::Map::new();
    for (name, hex) in items {
        map.insert(name, Value::String(hex));
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../clients/wire_vectors.json");
    let json = serde_json::to_string_pretty(&Value::Object(map)).expect("serialize vectors");
    std::fs::write(&path, json + "\n").unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    eprintln!(
        "regenerated {} vectors at {}",
        path.display(),
        path.display()
    );
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[test]
fn wire_encoders_match_shared_vectors() {
    let v = load_vectors();
    let rid = 0; // request id never affects the payload body

    check(
        &v,
        "hello",
        wire::encode_hello(
            rid,
            &Hello {
                client_name: "py-client".into(),
                client_version: "0.1.0".into(),
                protocol_version: 1,
                resume: Some(ResumeIdentity {
                    owner_id: uuid_filled(1),
                    client_id: uuid_filled(2),
                    resume_token: uuid_filled(3),
                }),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "hello_no_resume",
        wire::encode_hello(
            rid,
            &Hello {
                client_name: "c".into(),
                client_version: "v".into(),
                protocol_version: 1,
                resume: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "hello_ok",
        wire::encode_hello_ok(
            rid,
            &HelloOk {
                protocol_version: 1,
                owner_id: uuid_filled(9),
                client_id: uuid_filled(8),
                resume_token: uuid_filled(7),
                resume_outcome: ResumeOutcome::Resumed,
                server_name: "srv".into(),
                compliance: "v=1;x".into(),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "auth",
        wire::encode_auth(
            rid,
            &Auth {
                username: "u".into(),
                password: "p".into(),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "error",
        wire::encode_error_message(
            fibril_protocol::v1::Op::Error,
            rid,
            &ErrorMsg {
                code: 409,
                message: "not owner".into(),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "publish",
        wire::encode_publish(
            rid,
            &Publish {
                topic: "orders".into(),
                partition: Partition::new(3),
                group: Some("g".into()),
                require_confirm: true,
                content_type: Some(ContentType::Json),
                headers: header("x-a", "1"),
                payload: vec![1, 2, 3, 4],
                published: 1234567890,
                partition_key: Some(vec![9, 9]),
                partitioning_version: 5,
                ttl_ms: Some(60000),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "publish_no_ttl",
        wire::encode_publish(
            rid,
            &Publish {
                topic: "t".into(),
                partition: Partition::new(0),
                group: None,
                require_confirm: false,
                content_type: None,
                headers: HashMap::new(),
                payload: vec![],
                published: 0,
                partition_key: None,
                partitioning_version: 0,
                ttl_ms: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "publish_custom_ct",
        wire::encode_publish(
            rid,
            &Publish {
                topic: "t".into(),
                partition: Partition::new(0),
                group: None,
                require_confirm: false,
                content_type: Some(ContentType::Custom("application/x-thing".into())),
                headers: HashMap::new(),
                payload: vec![7],
                published: 1,
                partition_key: None,
                partitioning_version: 0,
                ttl_ms: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "publish_delayed",
        wire::encode_publish_delayed(
            rid,
            &PublishDelayed {
                topic: "t".into(),
                partition: Partition::new(1),
                group: None,
                require_confirm: true,
                not_before: 999,
                content_type: Some(ContentType::Text),
                headers: header("k", "v"),
                payload: vec![5, 6],
                published: 42,
                partition_key: None,
                partitioning_version: 2,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "publish_ok",
        wire::encode_publish_ok(rid, &PublishOk { offset: 777 })
            .unwrap()
            .payload,
    );

    check(
        &v,
        "deliver",
        wire::encode_deliver(
            rid,
            &Deliver {
                sub_id: 11,
                topic: "t".into(),
                group: Some("g".into()),
                partition: Partition::new(2),
                offset: 100,
                delivery_tag: DeliveryTag { epoch: 5 },
                published: 7,
                publish_received: 8,
                content_type: Some(ContentType::MsgPack),
                headers: header("h", "1"),
                payload: vec![3, 2, 1],
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "ack",
        wire::encode_ack(
            rid,
            &Ack {
                topic: "t".into(),
                group: None,
                partition: Partition::new(0),
                tags: vec![DeliveryTag { epoch: 1 }, DeliveryTag { epoch: 2 }],
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "nack",
        wire::encode_nack(
            rid,
            &Nack {
                topic: "t".into(),
                group: Some("g".into()),
                partition: Partition::new(1),
                tags: vec![DeliveryTag { epoch: 9 }],
                requeue: true,
                not_before: Some(5000),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "nack_no_nb",
        wire::encode_nack(
            rid,
            &Nack {
                topic: "t".into(),
                group: None,
                partition: Partition::new(0),
                tags: vec![],
                requeue: false,
                not_before: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "declare",
        wire::encode_declare_queue(
            rid,
            &DeclareQueue {
                topic: "t".into(),
                group: Some("g".into()),
                dlq_policy: Some(QueueDlqPolicy::Custom {
                    topic: "dlq".into(),
                    group: None,
                }),
                dlq_max_retries: Some(3),
                partition_count: Some(4),
                default_message_ttl_ms: Some(30000),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "declare_min",
        wire::encode_declare_queue(
            rid,
            &DeclareQueue {
                topic: "t".into(),
                group: None,
                dlq_policy: None,
                dlq_max_retries: None,
                partition_count: None,
                default_message_ttl_ms: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "declare_ok",
        wire::encode_declare_queue_ok(
            rid,
            &DeclareQueueOk {
                status: "created".into(),
                partition_count: 4,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "assignment",
        wire::encode_assignment_changed(
            rid,
            &AssignmentChanged {
                topic: "t".into(),
                group: None,
                consumer_group: "cg".into(),
                generation: 6,
                assigned: vec![Partition::new(0), Partition::new(1), Partition::new(2)],
                added: vec![Partition::new(2)],
                revoked: vec![],
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "subscribe",
        wire::encode_subscribe(
            rid,
            &Subscribe {
                topic: "t".into(),
                partition: Partition::new(1),
                group: Some("g".into()),
                prefetch: 32,
                auto_ack: false,
                consumer_group: Some("cg".into()),
                consumer_target: Some(2),
                member_id: Some(uuid_filled(4)),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "subscribe_min",
        wire::encode_subscribe(
            rid,
            &Subscribe {
                topic: "t".into(),
                partition: Partition::new(0),
                group: None,
                prefetch: 0,
                auto_ack: true,
                consumer_group: None,
                consumer_target: None,
                member_id: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "subscribe_ok",
        wire::encode_subscribe_ok(
            rid,
            &SubscribeOk {
                sub_id: 5,
                topic: "t".into(),
                group: Some("g".into()),
                partition: Partition::new(1),
                prefetch: 16,
                consumer_group: Some("cg".into()),
                consumer_target: None,
                member_id: Some(uuid_filled(4)),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "topology_req",
        wire::encode_topology_request(
            rid,
            &TopologyRequest {
                topic: Some("t".into()),
                group: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "topology_ok",
        wire::encode_topology_ok(
            rid,
            &TopologyOk {
                generation: 12,
                queues: vec![
                    QueueTopologyEntry {
                        topic: "t".into(),
                        partition: Partition::new(0),
                        group: None,
                        owner_endpoints: vec![
                            AdvertisedAddress::parse("127.0.0.1:7000")
                                .expect("valid test owner endpoint"),
                        ],
                        partitioning_version: 1,
                        partition_count: 2,
                    },
                    QueueTopologyEntry {
                        topic: "t".into(),
                        partition: Partition::new(1),
                        group: None,
                        owner_endpoints: vec![],
                        partitioning_version: 1,
                        partition_count: 2,
                    },
                ],
                streams: vec![StreamTopologyEntry {
                    topic: "s".into(),
                    partition: Partition::new(2),
                    owner_endpoints: vec![
                        AdvertisedAddress::parse("10.0.0.9:7100")
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 4,
                    partition_count: 3,
                }],
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "topology_update",
        wire::encode_topology_update(
            rid,
            &TopologyOk {
                generation: 12,
                queues: vec![QueueTopologyEntry {
                    topic: "t".into(),
                    partition: Partition::new(0),
                    group: None,
                    owner_endpoints: vec![
                        AdvertisedAddress::parse("127.0.0.1:7000")
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 1,
                    partition_count: 2,
                }],
                streams: vec![StreamTopologyEntry {
                    topic: "s".into(),
                    partition: Partition::new(2),
                    owner_endpoints: vec![
                        AdvertisedAddress::parse("10.0.0.9:7100")
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 4,
                    partition_count: 3,
                }],
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "topology_update_ack",
        wire::encode_topology_update_ack(rid, &TopologyUpdateAck { generation: 12 })
            .unwrap()
            .payload,
    );

    check(
        &v,
        "going_away",
        wire::encode_going_away(
            rid,
            &GoingAway {
                grace_ms: 30_000,
                message: "broker restarting for upgrade".into(),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "redirect",
        wire::encode_redirect(
            rid,
            &Redirect {
                topic: "t".into(),
                partition: Partition::new(1),
                group: Some("g".into()),
                owner_endpoints: vec![
                    AdvertisedAddress::parse("h:1").expect("valid test owner endpoint"),
                ],
                partitioning_version: 3,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "declare_plexus",
        wire::encode_declare_plexus(
            rid,
            &DeclarePlexus {
                topic: "t".into(),
                partition_count: Some(4),
                durability: StreamDurability::Speculative,
                retention: StreamRetention {
                    max_age_ms: Some(60000),
                    max_bytes: None,
                    max_records: Some(1_000_000),
                },
                replication_factor: Some(2),
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "declare_plexus_min",
        wire::encode_declare_plexus(
            rid,
            &DeclarePlexus {
                topic: "t".into(),
                partition_count: None,
                durability: StreamDurability::Durable,
                retention: StreamRetention::default(),
                replication_factor: None,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "declare_plexus_ok",
        wire::encode_declare_plexus_ok(
            rid,
            &DeclarePlexusOk {
                status: "created".into(),
                partition_count: 4,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "subscribe_stream",
        wire::encode_subscribe_stream(
            rid,
            &SubscribeStream {
                topic: "t".into(),
                partition: Partition::new(1),
                durable_name: Some("c1".into()),
                start: StreamStart::ByTime { time_ms: 1234 },
                filter: vec![
                    ("region".into(), "eu-*".into()),
                    ("kind".into(), "order".into()),
                ],
                prefetch: 16,
                auto_ack: false,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "subscribe_stream_min",
        wire::encode_subscribe_stream(
            rid,
            &SubscribeStream {
                topic: "t".into(),
                partition: Partition::new(0),
                durable_name: None,
                start: StreamStart::Latest,
                filter: vec![],
                prefetch: 0,
                auto_ack: true,
            },
        )
        .unwrap()
        .payload,
    );

    check(
        &v,
        "reconcile_client",
        wire::encode_reconcile_client(
            rid,
            &ReconcileClient {
                policy: ReconcilePolicy::RestoreClientSubscriptions,
                subscriptions: vec![ReconcileSubscription {
                    sub_id: 1,
                    topic: "t".into(),
                    group: None,
                    partition: Partition::new(0),
                    auto_ack: false,
                    prefetch: 8,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                }],
            },
        )
        .unwrap()
        .payload,
    );

    if regen() {
        write_regenerated_vectors();
    }
}
