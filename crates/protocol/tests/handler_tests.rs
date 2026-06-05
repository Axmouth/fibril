use std::{collections::HashMap, sync::Arc, time::Duration, time::Instant};

use bytes::Bytes;
use fibril_broker::{
    broker::{Broker, BrokerConfig, QueueEvictionAttempt},
    queue_engine::{EvictOutcome, StromaEngine},
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    Deliver, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, Publish, PublishDelayed, Subscribe,
    frame::{Frame, ProtoCodec},
    handler::{ConnectionSettings, handle_connection},
    helper::{try_decode, try_encode},
};
use fibril_util::{StaticAuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use stroma_core::{KeratinConfig, SnapshotConfig, TempDir};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use uuid::Uuid;

async fn open_test_broker() -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("protocol_handler_test-{}", Uuid::now_v7())),
    };
    std::fs::create_dir_all(&dir.root).unwrap();
    let engine = StromaEngine::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new(
        engine,
        BrokerConfig {
            inflight_ttl_ms: 2_000,
            expiry_poll_min_ms: 50,
            expiry_batch_max: 100,
            delivery_poll_max_ms: 50,
            queue_idle_evict_after_ms: None,
            queue_idle_sweep_interval_ms: 60_000,
        },
        None,
    );

    (broker, dir)
}

async fn open_protocol_connection() -> (
    Framed<TcpStream, ProtoCodec>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
) {
    let (framed, server_task, dir, _broker) =
        open_protocol_connection_with_settings(ConnectionSettings::new(Some(60))).await;
    (framed, server_task, dir)
}

async fn open_protocol_connection_with_settings(
    settings: ConnectionSettings,
) -> (
    Framed<TcpStream, ProtoCodec>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    let (broker, dir) = open_test_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client = TcpStream::connect(addr).await.unwrap();
    let (server, peer) = listener.accept().await.unwrap();
    let tcp_stats = TcpStats::new(10);
    let connection_stats = ConnectionStats::new();
    let conn_id = connection_stats.add_connection(peer, Instant::now(), false);

    let server_task = tokio::spawn(handle_connection(
        server,
        broker.clone(),
        tcp_stats,
        connection_stats,
        conn_id,
        None::<StaticAuthHandler>,
        settings,
    ));

    (Framed::new(client, ProtoCodec), server_task, dir, broker)
}

async fn recv_frame(framed: &mut Framed<TcpStream, ProtoCodec>) -> Frame {
    tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn handshake(framed: &mut Framed<TcpStream, ProtoCodec>) {
    framed
        .send(
            try_encode(
                Op::Hello,
                1,
                &Hello {
                    client_name: "protocol-test".into(),
                    client_version: "0.1.0".into(),
                    protocol_version: PROTOCOL_V1,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    let hello_ok: HelloOk = try_decode(&frame).unwrap();
    assert_eq!(hello_ok.protocol_version, PROTOCOL_V1);
}

async fn assert_error_frame(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    code: u16,
) {
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::Error as u16);
    assert_eq!(frame.request_id, request_id);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, code);
    assert!(!err.message.is_empty());
}

async fn assert_connection_still_responds(framed: &mut Framed<TcpStream, ProtoCodec>) {
    framed
        .send(try_encode(Op::Ping, 99, &()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::Pong as u16);
    assert_eq!(frame.request_id, 99);
}

async fn wait_for_queue_idle(broker: &Broker<StromaEngine>, topic: &str, group: Option<&str>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker
                .queue_activity_snapshot(topic, group)
                .is_some_and(|snapshot| {
                    snapshot.active_publishers == 0
                        && snapshot.active_subscribers == 0
                        && snapshot.idle_since_ms.is_some()
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_hello_returns_error_without_panicking() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;

    framed
        .send(Frame {
            version: PROTOCOL_V1,
            opcode: Op::Hello as u16,
            flags: 0,
            request_id: 9,
            payload: Bytes::from_static(b"not msgpack"),
        })
        .await
        .unwrap();

    assert_error_frame(&mut framed, 9, 400).await;

    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_publish_returns_error_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(Frame {
            version: PROTOCOL_V1,
            opcode: Op::Publish as u16,
            flags: 0,
            request_id: 2,
            payload: Bytes::from_static(b"bad publish"),
        })
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 400).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publish_with_reserved_header_returns_error_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "reserved.headers".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    headers: HashMap::from([("fibril.retries".into(), "1".into())]),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 400).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn delayed_publish_with_reserved_header_returns_error_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::PublishDelayed,
                2,
                &PublishDelayed {
                    topic: "reserved.delayed.headers".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    not_before: unix_millis() + 150,
                    headers: HashMap::from([("fibril.retries".into(), "1".into())]),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 400).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn delayed_publish_over_tcp_waits_until_not_before() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "delayed.tcp".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: true,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);

    let not_before = unix_millis() + 150;
    framed
        .send(
            try_encode(
                Op::PublishDelayed,
                3,
                &PublishDelayed {
                    topic: "delayed.tcp".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    not_before,
                    headers: HashMap::new(),
                    payload: b"delayed".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 3);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), framed.next())
            .await
            .is_err()
    );

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Deliver as u16);
    let delivered: Deliver = try_decode(&frame).unwrap();
    assert_eq!(delivered.payload, b"delayed".to_vec());
    assert!(unix_millis() >= not_before);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publisher_cache_idle_timeout_allows_queue_eviction_while_connection_stays_open() {
    let (mut framed, server_task, _dir, broker) = open_protocol_connection_with_settings(
        ConnectionSettings::new(Some(1)).with_publisher_cache_idle_timeout_ms(Some(0)),
    )
    .await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "publisher.cache.eviction".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 2);
    assert!(broker.is_queue_materialized("publisher.cache.eviction", None));
    assert_eq!(
        broker
            .queue_activity_snapshot("publisher.cache.eviction", None)
            .unwrap()
            .active_publishers,
        1
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Ping as u16);

    wait_for_queue_idle(&broker, "publisher.cache.eviction", None).await;

    let attempt = broker
        .try_evict_inactive_queue("publisher.cache.eviction", None, 0)
        .await
        .unwrap();
    assert_eq!(
        attempt,
        QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
    );

    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}
