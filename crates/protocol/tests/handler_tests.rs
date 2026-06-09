use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
    time::Instant,
};

use bytes::Bytes;
use fibril_broker::{
    broker::{
        Broker, BrokerConfig, BrokerOwnerReplicationPeer, BrokerOwnerReplicationPeerResolver,
        FollowerReplicationWorkerConfig, FollowerReplicationWorkerLoopExit,
        FollowerReplicationWorkerStatus, QueueEvictionAttempt, StaticQueueOwnership,
    },
    coordination::{
        LocalAssignmentIntent, LocalAssignmentRole, LocalAssignmentTransition, PartitionAssignment,
        QueueIdentity,
    },
    queue_engine::{
        EvictOutcome, GlobalDLQ, OwnerReplicationRead, QueueEngine, QueuePromotionOutcome,
        StromaEngine,
    },
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    Ack, ContentType, DeclareQueue, Deliver, ErrorMsg, Hello, HelloOk, Nack, Op, PROTOCOL_V1,
    Publish, PublishDelayed, QueueDlqPolicy, ReconcileAction, ReconcileClient, ReconcilePolicy,
    ReconcileResult, ReconcileSubscription, ReplicationApply, ReplicationApplyOk,
    ReplicationCheckpointExport, ReplicationCheckpointExportOk, ReplicationCheckpointInstall,
    ReplicationCheckpointInstallOk, ReplicationCheckpointRequired, ReplicationEventApplyBatch,
    ReplicationEventRead, ReplicationEventRecord, ReplicationMessageApplyBatch,
    ReplicationMessageRead, ReplicationMessageRecord, ReplicationRead, ReplicationReadOk,
    ReplicationStateCheckpoint, ResumeIdentity, ResumeOutcome, Subscribe,
    frame::{Frame, ProtoCodec},
    handler::{ConnectionSettings, handle_connection},
    helper::{try_decode, try_encode},
    replication::{
        ProtocolOwnerPeerResolverConfig, ProtocolOwnerReplicationPeer, ProtocolReplicationCatchUp,
        ProtocolReplicationCatchUpOptions, StaticProtocolOwnerPeerResolver,
        catch_up_replication_over_protocol,
    },
};
use fibril_util::{StaticAuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use stroma_core::{KeratinConfig, SnapshotConfig, StromaEvent, StromaKeratinConfig, TempDir};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn open_test_engine() -> (StromaEngine, TempDir) {
    let dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("protocol_handler_test-{}", Uuid::now_v7())),
    };
    std::fs::create_dir_all(&dir.root).unwrap();
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    (engine, dir)
}

async fn open_test_broker() -> (Arc<Broker<StromaEngine>>, TempDir) {
    let (engine, dir) = open_test_engine().await;
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

async fn open_test_broker_with_ownership(
    ownership: Arc<StaticQueueOwnership>,
) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let (engine, dir) = open_test_engine().await;
    let broker = Broker::new_with_ownership(engine, BrokerConfig::default(), None, ownership);

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
    open_protocol_connection_for_broker(settings, broker, dir).await
}

async fn open_protocol_connection_for_broker(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
    dir: TempDir,
) -> (
    Framed<TcpStream, ProtoCodec>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
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

async fn start_protocol_listener_for_broker(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
    dir: TempDir,
    auth: Option<StaticAuthHandler>,
) -> (
    SocketAddr,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_broker = broker.clone();

    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            server_broker,
            tcp_stats,
            connection_stats,
            conn_id,
            auth,
            settings,
        )
        .await
    });

    (addr, server_task, dir, broker)
}

fn follower_assignment_transition(topic: &str, group: Option<&str>) -> LocalAssignmentTransition {
    LocalAssignmentTransition {
        queue: QueueIdentity::new(topic, 0, group),
        previous_role: None,
        next_role: Some(LocalAssignmentRole::Follower),
        previous: None,
        next: None,
        intent: LocalAssignmentIntent::BecomeFollower,
    }
}

async fn start_checkpoint_required_owner_server(
    checkpoint: ReplicationStateCheckpoint,
    message_records: Vec<ReplicationMessageRecord>,
    event_records: Vec<ReplicationEventRecord>,
) -> (SocketAddr, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut conn = Framed::new(stream, ProtoCodec);

        let frame = conn
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("client closed before hello"))??;
        assert_eq!(frame.opcode, Op::Hello as u16);
        conn.send(try_encode(
            Op::HelloOk,
            frame.request_id,
            &HelloOk {
                protocol_version: PROTOCOL_V1,
                owner_id: Uuid::now_v7(),
                client_id: Uuid::now_v7(),
                resume_token: Uuid::now_v7(),
                resume_outcome: ResumeOutcome::New,
                server_name: "fake-owner".into(),
                compliance: "test".into(),
            },
        )?)
        .await?;

        while let Some(frame) = conn.next().await {
            let frame = frame?;
            match frame.opcode {
                x if x == Op::ReplicationRead as u16 => {
                    let read: ReplicationRead = try_decode(&frame)?;
                    // This fake models an owner whose event log has been
                    // compacted but whose messages are still available from
                    // the checkpoint message offset.
                    let response = if read.event_from < checkpoint.event_next_offset {
                        ReplicationReadOk {
                            messages: read_fake_message_batch(
                                &checkpoint,
                                &message_records,
                                read.message_from,
                                read.max_messages,
                            ),
                            events: ReplicationEventRead::CheckpointRequired(
                                ReplicationCheckpointRequired {
                                    epoch: checkpoint.event_epoch,
                                    requested_offset: read.event_from,
                                    head_offset: checkpoint.event_next_offset,
                                    next_offset: checkpoint.event_next_offset,
                                },
                            ),
                        }
                    } else {
                        ReplicationReadOk {
                            messages: read_fake_message_batch(
                                &checkpoint,
                                &message_records,
                                read.message_from,
                                read.max_messages,
                            ),
                            events: read_fake_event_batch(
                                &checkpoint,
                                &event_records,
                                read.event_from,
                                read.max_events,
                            ),
                        }
                    };
                    conn.send(try_encode(
                        Op::ReplicationReadOk,
                        frame.request_id,
                        &response,
                    )?)
                    .await?;
                }
                x if x == Op::ReplicationCheckpointExport as u16 => {
                    let _: ReplicationCheckpointExport = try_decode(&frame)?;
                    conn.send(try_encode(
                        Op::ReplicationCheckpointExportOk,
                        frame.request_id,
                        &ReplicationCheckpointExportOk {
                            checkpoint: checkpoint.clone(),
                        },
                    )?)
                    .await?;
                }
                other => anyhow::bail!("unexpected fake owner opcode {other}"),
            }
        }
        Ok(())
    });
    (addr, server_task)
}

fn read_fake_message_batch(
    checkpoint: &ReplicationStateCheckpoint,
    message_records: &[ReplicationMessageRecord],
    from: u64,
    max: u32,
) -> ReplicationMessageRead {
    let records = message_records
        .iter()
        .filter(|record| record.offset >= from)
        .take(max as usize)
        .cloned()
        .collect::<Vec<_>>();
    let next_offset = records.last().map_or(from, |record| record.offset + 1);
    ReplicationMessageRead::Batch {
        epoch: checkpoint.message_epoch,
        requested_offset: from,
        next_offset,
        records,
    }
}

fn read_fake_event_batch(
    checkpoint: &ReplicationStateCheckpoint,
    event_records: &[ReplicationEventRecord],
    from: u64,
    max: u32,
) -> ReplicationEventRead {
    let records = event_records
        .iter()
        .filter(|record| record.offset >= from)
        .take(max as usize)
        .cloned()
        .collect::<Vec<_>>();
    let next_offset = records.last().map_or(from, |record| record.offset + 1);
    ReplicationEventRead::Batch {
        epoch: checkpoint.event_epoch,
        requested_offset: from,
        next_offset,
        records,
    }
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
                    resume: None,
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
    assert_eq!(hello_ok.resume_outcome, ResumeOutcome::New);
}

async fn handshake_with_resume(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    resume: Option<ResumeIdentity>,
) -> HelloOk {
    framed
        .send(
            try_encode(
                Op::Hello,
                1,
                &Hello {
                    client_name: "protocol-test".into(),
                    client_version: "0.1.0".into(),
                    protocol_version: PROTOCOL_V1,
                    resume,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    try_decode(&frame).unwrap()
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

async fn assert_subscribe_error_frame(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    code: u16,
) {
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::SubscribeErr as u16);
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

async fn framed_subscribe(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    topic: &str,
    group: Option<&str>,
    auto_ack: bool,
) -> fibril_protocol::v1::SubscribeOk {
    framed
        .send(
            try_encode(
                Op::Subscribe,
                request_id,
                &Subscribe {
                    topic: topic.into(),
                    group: group.map(str::to_string),
                    prefetch: 1,
                    auto_ack,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    try_decode(&frame).unwrap()
}

async fn framed_publish(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    topic: &str,
    group: Option<&str>,
    payload: &[u8],
) {
    framed
        .send(
            try_encode(
                Op::Publish,
                request_id,
                &Publish {
                    topic: topic.into(),
                    partition: 0,
                    group: group.map(str::to_string),
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: payload.to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, request_id);
}

#[tokio::test]
async fn hello_can_resume_with_owner_scoped_identity() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    assert_eq!(first_ok.resume_outcome, ResumeOutcome::New);
    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;

    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);
    assert_eq!(second_ok.client_id, first_ok.client_id);
    assert_eq!(second_ok.owner_id, first_ok.owner_id);
    assert_eq!(second_ok.resume_token, first_ok.resume_token);

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconcile_after_resume_keeps_matching_subscription() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    first
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "reconcile.keep".into(),
                    group: Some("workers".into()),
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut first).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    let sub_ok: fibril_protocol::v1::SubscribeOk = try_decode(&frame).unwrap();

    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);

    second
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: sub_ok.sub_id,
                        topic: sub_ok.topic,
                        group: sub_ok.group,
                        partition: sub_ok.partition,
                        auto_ack: false,
                        prefetch: sub_ok.prefetch,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut second).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    assert_eq!(frame.request_id, 3);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(result.subscriptions[0].action, ReconcileAction::Keep);
    assert_eq!(result.subscriptions[0].reason, "matched");

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconcile_after_resume_closes_mismatched_subscription() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    first
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "reconcile.recreate".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut first).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    let sub_ok: fibril_protocol::v1::SubscribeOk = try_decode(&frame).unwrap();

    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);

    second
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: sub_ok.sub_id,
                        topic: sub_ok.topic,
                        group: sub_ok.group,
                        partition: sub_ok.partition,
                        auto_ack: false,
                        prefetch: sub_ok.prefetch + 1,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut second).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::CloseClientSide
    );
    assert_eq!(result.subscriptions[0].reason, "server_mismatch");

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn conservative_reconcile_drops_server_only_subscription() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut framed, task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let _hello = handshake_with_resume(&mut framed, None).await;
    let sub_ok = framed_subscribe(&mut framed, 2, "reconcile.server.only", None, false).await;
    assert!(
        broker
            .queue_activity_snapshot("reconcile.server.only", None)
            .is_some_and(|snapshot| snapshot.active_subscribers == 1)
    );

    framed
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(result.subscriptions[0].client, None);
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::CloseServerSide
    );
    assert_eq!(result.subscriptions[0].reason, "client_missing");
    assert_eq!(
        result.subscriptions[0]
            .server
            .as_ref()
            .map(|sub| sub.sub_id),
        Some(sub_ok.sub_id)
    );
    wait_for_queue_idle(&broker, "reconcile.server.only", None).await;

    drop(framed);
    task.await.unwrap().unwrap();
    drop(dir);
}

#[tokio::test]
async fn restore_policy_recreates_client_only_subscription() {
    let (mut framed, task, dir, _broker) =
        open_protocol_connection_with_settings(ConnectionSettings::new(Some(60))).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReconcileClient,
                2,
                &ReconcileClient {
                    policy: ReconcilePolicy::RestoreClientSubscriptions,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: 99,
                        topic: "reconcile.restore".into(),
                        group: Some("workers".into()),
                        partition: 0,
                        auto_ack: false,
                        prefetch: 2,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    let item = &result.subscriptions[0];
    assert_eq!(item.action, ReconcileAction::Keep);
    assert_eq!(item.reason, "server_restored");
    assert_eq!(item.client.as_ref().map(|sub| sub.sub_id), Some(99));
    let restored = item.server.as_ref().unwrap();
    assert_ne!(restored.sub_id, 99);
    assert_eq!(restored.topic, "reconcile.restore");
    assert_eq!(restored.group.as_deref(), Some("workers"));
    assert_eq!(restored.prefetch, 2);

    framed_publish(
        &mut framed,
        3,
        "reconcile.restore",
        Some("workers"),
        b"restored",
    )
    .await;
    let delivered = recv_delivery_for_topic(&mut framed, "reconcile.restore").await;
    assert_eq!(delivered.sub_id, restored.sub_id);
    assert_eq!(delivered.payload, Bytes::from_static(b"restored"));

    drop(framed);
    task.await.unwrap().unwrap();
    drop(dir);
}

#[tokio::test]
async fn reconnect_grace_accepts_late_ack_after_resume() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(100));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    framed_subscribe(&mut first, 2, "grace.ack", None, false).await;
    framed_publish(&mut first, 3, "grace.ack", None, b"ack-after-resume").await;
    let delivered = recv_delivery_for_topic(&mut first, "grace.ack").await;

    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, dir, broker) =
        open_protocol_connection_for_broker(settings.clone(), broker, dir).await;
    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);

    second
        .send(
            try_encode(
                Op::Ack,
                2,
                &Ack {
                    topic: "grace.ack".into(),
                    group: None,
                    partition: 0,
                    tags: vec![delivered.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    broker.wait_for_pending_settles().await;

    drop(second);
    second_task.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (mut third, third_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    handshake(&mut third).await;
    framed_subscribe(&mut third, 2, "grace.ack", None, false).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), third.next())
            .await
            .is_err()
    );

    drop(third);
    third_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_grace_runtime_update_affects_future_disconnects() {
    let settings = ConnectionSettings::new(Some(60));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    framed_subscribe(&mut first, 2, "grace.live", None, false).await;
    framed_publish(&mut first, 3, "grace.live", None, b"ack-after-live-update").await;
    let delivered = recv_delivery_for_topic(&mut first, "grace.live").await;

    settings.update_runtime(fibril_protocol::v1::handler::ConnectionRuntimeSettings {
        reconnect_grace_ms: Some(100),
        ..Default::default()
    });

    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, dir, broker) =
        open_protocol_connection_for_broker(settings.clone(), broker, dir).await;
    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);

    second
        .send(
            try_encode(
                Op::Ack,
                2,
                &Ack {
                    topic: "grace.live".into(),
                    group: None,
                    partition: 0,
                    tags: vec![delivered.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    broker.wait_for_pending_settles().await;

    drop(second);
    second_task.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (mut third, third_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    handshake(&mut third).await;
    framed_subscribe(&mut third, 2, "grace.live", None, false).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), third.next())
            .await
            .is_err()
    );

    drop(third);
    third_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_grace_expiry_requeues_unsettled_inflight() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(100));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    handshake(&mut first).await;
    framed_subscribe(&mut first, 2, "grace.requeue", None, false).await;
    framed_publish(
        &mut first,
        3,
        "grace.requeue",
        None,
        b"requeued-after-grace",
    )
    .await;
    let delivered = recv_delivery_for_topic(&mut first, "grace.requeue").await;
    assert_eq!(delivered.payload, b"requeued-after-grace".to_vec());

    drop(first);
    first_task.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    handshake(&mut second).await;
    framed_subscribe(&mut second, 2, "grace.requeue", None, false).await;
    let redelivered = recv_delivery_for_topic(&mut second, "grace.requeue").await;
    assert_eq!(redelivered.payload, b"requeued-after-grace".to_vec());

    drop(second);
    second_task.await.unwrap().unwrap();
}

async fn recv_delivery_for_topic(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    topic: &str,
) -> Deliver {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = recv_frame(framed).await;
            if frame.opcode == Op::Deliver as u16 {
                let delivered: Deliver = try_decode(&frame).unwrap();
                if delivered.topic == topic {
                    break delivered;
                }
            }
        }
    })
    .await
    .unwrap()
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
async fn replication_read_returns_owner_log_records() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;
    framed_publish(
        &mut framed,
        2,
        "replication.read.tcp",
        Some("workers"),
        b"replicated-payload",
    )
    .await;

    framed
        .send(
            try_encode(
                Op::ReplicationRead,
                3,
                &ReplicationRead {
                    topic: "replication.read.tcp".into(),
                    group: Some("workers".into()),
                    partition: 0,
                    message_from: 0,
                    event_from: 0,
                    max_messages: 10,
                    max_events: 10,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReplicationReadOk as u16);
    assert_eq!(frame.request_id, 3);
    let response: ReplicationReadOk = try_decode(&frame).unwrap();

    match response.messages {
        ReplicationMessageRead::Batch { records, .. } => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].offset, 0);
            assert_eq!(records[0].flags, 0);
            assert_eq!(records[0].payload, b"replicated-payload".to_vec());
        }
        ReplicationMessageRead::CheckpointRequired(required) => {
            panic!("unexpected message checkpoint requirement: {required:?}");
        }
    }

    match response.events {
        ReplicationEventRead::Batch { records, .. } => {
            assert!(!records.is_empty());
            assert_eq!(records[0].offset, 0);
            assert!(!records[0].payload.is_empty());
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            panic!("unexpected event checkpoint requirement: {required:?}");
        }
    }

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unowned_replication_read_returns_not_owner_error_and_keeps_connection_open() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReplicationRead,
                2,
                &ReplicationRead {
                    topic: "unowned".into(),
                    group: None,
                    partition: 0,
                    message_from: 0,
                    event_from: 0,
                    max_messages: 10,
                    max_events: 10,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replication_apply_writes_follower_log_records() {
    let (broker, dir) = open_test_broker().await;
    broker
        .become_replication_follower("replication.apply.tcp", 0, None)
        .await
        .unwrap();
    let (mut framed, server_task, _dir, broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    let event_payload = StromaEvent::Enqueue { off: 0, retries: 0 }
        .encode()
        .unwrap();
    framed
        .send(
            try_encode(
                Op::ReplicationApply,
                2,
                &ReplicationApply {
                    topic: "replication.apply.tcp".into(),
                    group: None,
                    partition: 0,
                    messages: Some(ReplicationMessageApplyBatch {
                        epoch: 0,
                        records: vec![ReplicationMessageRecord {
                            offset: 0,
                            flags: 0,
                            headers: Vec::new(),
                            payload: b"replicated-follower-payload".to_vec(),
                        }],
                    }),
                    events: Some(ReplicationEventApplyBatch {
                        epoch: 0,
                        records: vec![ReplicationEventRecord {
                            offset: 0,
                            payload: event_payload,
                        }],
                    }),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReplicationApplyOk as u16);
    assert_eq!(frame.request_id, 2);
    let response: ReplicationApplyOk = try_decode(&frame).unwrap();
    assert!(response.messages_applied);
    assert!(response.events_applied);

    let promoted = broker
        .promote_replication_follower_if_caught_up("replication.apply.tcp", 0, None, 1, 1)
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 1,
            event_next_offset: 1,
            applied_event_offset: Some(0),
        }
    );

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replication_apply_rejects_non_contiguous_records_and_keeps_connection_open() {
    let (broker, dir) = open_test_broker().await;
    broker
        .become_replication_follower("replication.apply.bad", 0, None)
        .await
        .unwrap();
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReplicationApply,
                2,
                &ReplicationApply {
                    topic: "replication.apply.bad".into(),
                    group: None,
                    partition: 0,
                    messages: Some(ReplicationMessageApplyBatch {
                        epoch: 0,
                        records: vec![
                            ReplicationMessageRecord {
                                offset: 0,
                                flags: 0,
                                headers: Vec::new(),
                                payload: b"first".to_vec(),
                            },
                            ReplicationMessageRecord {
                                offset: 2,
                                flags: 0,
                                headers: Vec::new(),
                                payload: b"gap".to_vec(),
                            },
                        ],
                    }),
                    events: None,
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
async fn replication_read_and_apply_compose_for_manual_catch_up() {
    let topic = "replication.catchup.tcp";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_protocol_connection().await;
    handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"first-replicated-payload",
    )
    .await;
    framed_publish(
        &mut owner_framed,
        3,
        topic,
        group.as_deref(),
        b"second-replicated-payload",
    )
    .await;

    let (follower_broker, follower_dir) = open_test_broker().await;
    follower_broker
        .become_replication_follower(topic, 0, group.as_deref())
        .await
        .unwrap();
    let (mut follower_framed, follower_task, _follower_dir, follower_broker) =
        open_protocol_connection_for_broker(
            ConnectionSettings::new(Some(60)),
            follower_broker,
            follower_dir,
        )
        .await;
    handshake(&mut follower_framed).await;

    let outcome = catch_up_replication_over_protocol(
        &mut owner_framed,
        &mut follower_framed,
        topic,
        0,
        group.as_deref(),
        ProtocolReplicationCatchUpOptions {
            max_messages_per_read: 1,
            max_events_per_read: 1,
            max_iterations: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        ProtocolReplicationCatchUp::CaughtUp(
            fibril_protocol::v1::replication::ProtocolReplicationCatchUpProgress {
                iterations: 2,
                applied_message_records: 2,
                applied_event_records: 2,
                message_next_offset: 2,
                event_next_offset: 2,
            }
        )
    );

    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(topic, 0, group.as_deref(), 2, 2)
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 2,
            event_next_offset: 2,
            applied_event_offset: Some(1),
        }
    );

    drop(owner_framed);
    drop(follower_framed);
    owner_task.await.unwrap().unwrap();
    follower_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_owner_replication_peer_reads_owner_records() {
    let topic = "replication.peer.read";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_protocol_connection().await;
    handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"peer-replicated-payload",
    )
    .await;

    let peer = ProtocolOwnerReplicationPeer::new(owner_framed);
    let records = peer
        .read_owner_replication_records(topic, 0, group.as_deref(), 0, 0, 8, 8)
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.requested_offset, 0);
    assert_eq!(messages.next_offset, 1);
    assert_eq!(messages.records.len(), 1);
    assert_eq!(messages.records[0].0, 0);
    assert_eq!(messages.records[0].1.payload, b"peer-replicated-payload");

    let OwnerReplicationRead::Batch(events) = records.events else {
        panic!("expected event batch");
    };
    assert_eq!(events.requested_offset, 0);
    assert_eq!(events.next_offset, 1);
    assert_eq!(events.records.len(), 1);

    drop(peer);
    owner_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_owner_replication_peer_exports_checkpoint() {
    let topic = "replication.peer.checkpoint";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_protocol_connection().await;
    handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"checkpointed-payload",
    )
    .await;

    let peer = ProtocolOwnerReplicationPeer::new(owner_framed);
    let checkpoint = peer
        .export_owner_state_checkpoint(topic, 0, group.as_deref())
        .await
        .unwrap();

    assert_eq!(checkpoint.message_checkpoint_offset, 0);
    assert_eq!(checkpoint.message_next_offset, 1);
    assert_eq!(checkpoint.event_next_offset, 1);
    assert_eq!(checkpoint.applied_event_offset, 0);
    assert!(!checkpoint.state_snapshot.is_empty());

    drop(peer);
    owner_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_owner_replication_peer_maps_not_owner_error() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    let peer = ProtocolOwnerReplicationPeer::new(framed);
    let err = peer
        .read_owner_replication_records("unowned", 0, None, 0, 0, 8, 8)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        fibril_broker::broker::BrokerError::NotOwner { .. }
    ));

    drop(peer);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_reads_from_owner_node() {
    let topic = "replication.resolver.read";
    let group = Some("workers".to_string());
    let (owner_broker, owner_dir) = open_test_broker().await;
    let (publisher, _confirms) = owner_broker.get_publisher(topic, &group).await.unwrap();
    let reply = publisher
        .publish(
            b"resolver-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let (addr, server_task, _dir, _broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        None,
    )
    .await;
    let resolver =
        StaticProtocolOwnerPeerResolver::new(HashMap::from([("owner-a".to_string(), addr)]));
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, 0, group.as_deref()),
        "owner-a",
        vec![],
        1,
    );

    let peer = resolver
        .resolve_owner_peer(&assignment)
        .await
        .unwrap()
        .expect("owner peer");
    let records = peer
        .read_owner_replication_records(topic, 0, group.as_deref(), 0, 0, 8, 8)
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.records.len(), 1);
    assert_eq!(messages.records[0].1.payload, b"resolver-payload");

    drop(peer);
    drop(resolver);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_returns_none_for_unknown_owner() {
    let resolver = StaticProtocolOwnerPeerResolver::new(HashMap::new());
    let assignment = PartitionAssignment::new(
        QueueIdentity::new("replication.resolver.missing", 0, None),
        "missing-owner",
        vec![],
        1,
    );

    assert!(
        resolver
            .resolve_owner_peer(&assignment)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_reuses_peer_for_owner() {
    let addr = "127.0.0.1:9".parse().unwrap();
    let resolver =
        StaticProtocolOwnerPeerResolver::new(HashMap::from([("owner-a".to_string(), addr)]));
    let assignment = PartitionAssignment::new(
        QueueIdentity::new("replication.resolver.cached", 0, None),
        "owner-a",
        vec![],
        1,
    );

    let first = resolver
        .resolve_owner_peer(&assignment)
        .await
        .unwrap()
        .expect("first owner peer");
    let second = resolver
        .resolve_owner_peer(&assignment)
        .await
        .unwrap()
        .expect("second owner peer");

    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_can_authenticate() {
    let topic = "replication.resolver.auth";
    let (owner_broker, owner_dir) = open_test_broker().await;
    let (publisher, _confirms) = owner_broker.get_publisher(topic, &None).await.unwrap();
    let reply = publisher
        .publish(
            b"auth-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let (addr, server_task, _dir, _broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(StaticAuthHandler::new("fibril".into(), "secret".into())),
    )
    .await;
    let resolver = StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([("owner-a".to_string(), addr)]))
            .with_auth("fibril", "secret"),
    );
    let assignment =
        PartitionAssignment::new(QueueIdentity::new(topic, 0, None), "owner-a", vec![], 1);

    let peer = resolver
        .resolve_owner_peer(&assignment)
        .await
        .unwrap()
        .expect("owner peer");
    let records = peer
        .read_owner_replication_records(topic, 0, None, 0, 0, 8, 8)
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.records[0].1.payload, b"auth-payload");

    drop(peer);
    drop(resolver);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn follower_worker_loop_catches_up_over_static_protocol_resolver() {
    let topic = "replication.resolver.loop";
    let group = Some("workers".to_string());
    let (owner_broker, owner_dir) = open_test_broker().await;
    let (publisher, _confirms) = owner_broker.get_publisher(topic, &group).await.unwrap();
    for payload in [b"loop-first".as_slice(), b"loop-second".as_slice()] {
        let reply = publisher
            .publish(
                payload.to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }
    let owner_checkpoint = owner_broker
        .export_owner_state_checkpoint(topic, 0, group.as_deref())
        .await
        .unwrap();

    let (addr, server_task, _owner_dir, owner_broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        None,
    )
    .await;
    let resolver =
        StaticProtocolOwnerPeerResolver::new(HashMap::from([("owner-a".to_string(), addr)]));
    let resolver = Arc::new(resolver);

    let (follower_broker, _follower_dir) = open_test_broker().await;
    follower_broker
        .apply_assignment_transition(&follower_assignment_transition(topic, group.as_deref()))
        .await
        .unwrap();

    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, 0, group.as_deref()),
        "owner-a",
        vec!["follower-a".to_string()],
        1,
    );
    let shutdown = CancellationToken::new();
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    let observer = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = follower_broker
                    .follower_replication_worker_snapshot(topic, 0, group.as_deref())
                    .await;
                if state
                    .as_ref()
                    .is_some_and(|state| state.status == FollowerReplicationWorkerStatus::CaughtUp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower worker should catch up over protocol");
        shutdown.cancel();
    };
    let loop_task = follower_broker.run_follower_replication_worker_loop(
        assignment,
        resolver,
        cfg,
        shutdown.clone(),
    );
    let (_, loop_outcome) = tokio::join!(observer, loop_task);

    assert_eq!(
        loop_outcome.unwrap(),
        FollowerReplicationWorkerLoopExit::Cancelled { ticks: 1 }
    );
    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            0,
            group.as_deref(),
            owner_checkpoint.message_next_offset,
            owner_checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: owner_checkpoint.message_next_offset,
            event_next_offset: owner_checkpoint.event_next_offset,
            applied_event_offset: Some(owner_checkpoint.applied_event_offset),
        }
    );

    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn follower_worker_loop_installs_checkpoint_over_static_protocol_resolver() {
    let topic = "replication.resolver.checkpoint";
    let group = Some("workers".to_string());
    let (owner_broker, _owner_dir) = open_test_broker().await;
    let (publisher, _confirms) = owner_broker.get_publisher(topic, &group).await.unwrap();
    for payload in [
        b"checkpoint-loop-first".as_slice(),
        b"checkpoint-loop-second".as_slice(),
    ] {
        let reply = publisher
            .publish(
                payload.to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }
    let owner_checkpoint = owner_broker
        .export_owner_state_checkpoint(topic, 0, group.as_deref())
        .await
        .unwrap();
    let owner_records = owner_broker
        .read_owner_replication_records(topic, 0, group.as_deref(), 0, 0, 8, 8)
        .await
        .unwrap();
    let OwnerReplicationRead::Batch(messages) = owner_records.messages else {
        panic!("expected owner message batch");
    };
    let OwnerReplicationRead::Batch(events) = owner_records.events else {
        panic!("expected owner event batch");
    };
    let message_records = messages
        .records
        .into_iter()
        .map(|(offset, message)| ReplicationMessageRecord {
            offset,
            flags: message.flags,
            headers: message.headers,
            payload: message.payload,
        })
        .collect::<Vec<_>>();
    let event_records = events
        .records
        .into_iter()
        .map(|(offset, event)| ReplicationEventRecord {
            offset,
            payload: event.encode().expect("owner event should encode"),
        })
        .collect::<Vec<_>>();

    let checkpoint = ReplicationStateCheckpoint {
        message_epoch: owner_checkpoint.message_epoch,
        event_epoch: owner_checkpoint.event_epoch,
        message_checkpoint_offset: owner_checkpoint.message_checkpoint_offset,
        message_next_offset: owner_checkpoint.message_next_offset,
        event_next_offset: owner_checkpoint.event_next_offset,
        applied_event_offset: owner_checkpoint.applied_event_offset,
        state_snapshot: owner_checkpoint.state_snapshot.clone(),
    };
    let (addr, server_task) =
        start_checkpoint_required_owner_server(checkpoint, message_records, event_records).await;
    let resolver =
        StaticProtocolOwnerPeerResolver::new(HashMap::from([("owner-a".to_string(), addr)]));
    let resolver = Arc::new(resolver);

    let (follower_broker, _follower_dir) = open_test_broker().await;
    follower_broker
        .apply_assignment_transition(&follower_assignment_transition(topic, group.as_deref()))
        .await
        .unwrap();

    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, 0, group.as_deref()),
        "owner-a",
        vec!["follower-a".to_string()],
        1,
    );
    let shutdown = CancellationToken::new();
    let cfg = FollowerReplicationWorkerConfig {
        allow_checkpoint_install: true,
        checkpoint_retry_poll_ms: 1,
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    let observer = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = follower_broker
                    .follower_replication_worker_snapshot(topic, 0, group.as_deref())
                    .await;
                if state
                    .as_ref()
                    .is_some_and(|state| state.status == FollowerReplicationWorkerStatus::CaughtUp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower worker should install checkpoint and catch up");
        shutdown.cancel();
    };
    let loop_task = follower_broker.run_follower_replication_worker_loop(
        assignment,
        resolver,
        cfg,
        shutdown.clone(),
    );
    let (_, loop_outcome) = tokio::join!(observer, loop_task);

    let worker_state = follower_broker
        .follower_replication_worker_snapshot(topic, 0, group.as_deref())
        .await
        .expect("follower worker state should exist after checkpoint loop");
    assert_eq!(
        worker_state.message_next_offset,
        owner_checkpoint.message_next_offset
    );
    assert_eq!(
        worker_state.event_next_offset,
        owner_checkpoint.event_next_offset
    );

    assert_eq!(
        loop_outcome.unwrap(),
        FollowerReplicationWorkerLoopExit::Cancelled { ticks: 2 }
    );
    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            0,
            group.as_deref(),
            owner_checkpoint.message_next_offset,
            owner_checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: owner_checkpoint.message_next_offset,
            event_next_offset: owner_checkpoint.event_next_offset,
            applied_event_offset: Some(owner_checkpoint.applied_event_offset),
        }
    );

    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replication_checkpoint_export_install_composes_with_catch_up() {
    let topic = "replication.checkpoint.tcp";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_protocol_connection().await;
    handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"checkpoint-first",
    )
    .await;
    framed_publish(
        &mut owner_framed,
        3,
        topic,
        group.as_deref(),
        b"checkpoint-second",
    )
    .await;

    owner_framed
        .send(
            try_encode(
                Op::ReplicationCheckpointExport,
                4,
                &ReplicationCheckpointExport {
                    topic: topic.into(),
                    group: group.clone(),
                    partition: 0,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut owner_framed).await;
    assert_eq!(frame.opcode, Op::ReplicationCheckpointExportOk as u16);
    let export: ReplicationCheckpointExportOk = try_decode(&frame).unwrap();

    let (follower_broker, follower_dir) = open_test_broker().await;
    follower_broker
        .become_replication_follower(topic, 0, group.as_deref())
        .await
        .unwrap();
    let (mut follower_framed, follower_task, _follower_dir, follower_broker) =
        open_protocol_connection_for_broker(
            ConnectionSettings::new(Some(60)),
            follower_broker,
            follower_dir,
        )
        .await;
    handshake(&mut follower_framed).await;

    follower_framed
        .send(
            try_encode(
                Op::ReplicationCheckpointInstall,
                5,
                &ReplicationCheckpointInstall {
                    topic: topic.into(),
                    group: group.clone(),
                    partition: 0,
                    checkpoint: export.checkpoint.clone(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut follower_framed).await;
    assert_eq!(frame.opcode, Op::ReplicationCheckpointInstallOk as u16);
    let install: ReplicationCheckpointInstallOk = try_decode(&frame).unwrap();
    assert_eq!(
        install.event_next_offset,
        export.checkpoint.event_next_offset
    );
    assert_eq!(
        install.applied_event_offset,
        export.checkpoint.applied_event_offset
    );

    let outcome = catch_up_replication_over_protocol(
        &mut owner_framed,
        &mut follower_framed,
        topic,
        0,
        group.as_deref(),
        ProtocolReplicationCatchUpOptions {
            message_from: export.checkpoint.message_checkpoint_offset,
            event_from: export.checkpoint.event_next_offset,
            max_messages_per_read: 10,
            max_events_per_read: 10,
            max_iterations: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(matches!(outcome, ProtocolReplicationCatchUp::CaughtUp(_)));

    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            0,
            group.as_deref(),
            export.checkpoint.message_next_offset,
            export.checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: export.checkpoint.message_next_offset,
            event_next_offset: export.checkpoint.event_next_offset,
            applied_event_offset: Some(export.checkpoint.applied_event_offset),
        }
    );

    drop(owner_framed);
    drop(follower_framed);
    owner_task.await.unwrap().unwrap();
    follower_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unowned_publish_returns_not_owner_error_and_keeps_connection_open() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "unowned".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unowned_subscribe_returns_not_owner_error_and_keeps_connection_open() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "unowned".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_subscribe_error_frame(&mut framed, 2, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn duplicate_subscribe_returns_conflict_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;
    framed_subscribe(&mut framed, 2, "duplicate.subscribe", None, false).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                3,
                &Subscribe {
                    topic: "duplicate.subscribe".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_subscribe_error_frame(&mut framed, 3, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publish_content_type_header_is_delivered_as_metadata() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "content.type".into(),
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

    framed
        .send(
            try_encode(
                Op::Publish,
                3,
                &Publish {
                    topic: "content.type".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
                    payload: br#"{"ok":true}"#.to_vec(),
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

    let delivered = recv_delivery_for_topic(&mut framed, "content.type").await;
    assert!(matches!(delivered.content_type, Some(ContentType::Json)));
    assert!(!delivered.headers.contains_key("content-type"));

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
                    content_type: None,
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
                    content_type: None,
                    not_before: unix_millis() + 150,
                    headers: HashMap::from([("stroma.source_offset".into(), "1".into())]),
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
                    content_type: None,
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
async fn delayed_retry_over_tcp_waits_until_not_before() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "delayed.retry.tcp".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Publish,
                3,
                &Publish {
                    topic: "delayed.retry.tcp".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"retry-later".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::PublishOk as u16);

    let delivered = recv_delivery_for_topic(&mut framed, "delayed.retry.tcp").await;
    let not_before = unix_millis() + 150;
    framed
        .send(
            try_encode(
                Op::Nack,
                4,
                &Nack {
                    topic: "delayed.retry.tcp".into(),
                    group: None,
                    partition: 0,
                    tags: vec![delivered.delivery_tag],
                    requeue: true,
                    not_before: Some(not_before),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), framed.next())
            .await
            .is_err()
    );

    let redelivered = recv_delivery_for_topic(&mut framed, "delayed.retry.tcp").await;
    assert_eq!(redelivered.payload, b"retry-later".to_vec());
    assert!(unix_millis() >= not_before);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn exhausted_message_routes_to_global_dlq_over_tcp() {
    let (engine, dir) = open_test_engine().await;
    engine
        .set_global_dlq(
            Some(GlobalDLQ::new("_dlq.source", 0, None).await.unwrap()),
            0,
        )
        .await
        .unwrap();
    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                2,
                &DeclareQueue {
                    topic: "source".into(),
                    group: None,
                    dlq_policy: Some(QueueDlqPolicy::Global),
                    dlq_max_retries: Some(0),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        recv_frame(&mut framed).await.opcode,
        Op::DeclareQueueOk as u16
    );

    framed
        .send(
            try_encode(
                Op::Subscribe,
                3,
                &Subscribe {
                    topic: "source".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Subscribe,
                4,
                &Subscribe {
                    topic: "_dlq.source".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Publish,
                5,
                &Publish {
                    topic: "source".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("x-trace-id".into(), "dlq-flow".into())]),
                    payload: b"poison".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let source = recv_delivery_for_topic(&mut framed, "source").await;
    assert_eq!(source.payload, b"poison".to_vec());

    framed
        .send(
            try_encode(
                Op::Nack,
                6,
                &Nack {
                    topic: "source".into(),
                    group: None,
                    partition: 0,
                    tags: vec![source.delivery_tag],
                    requeue: true,
                    not_before: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let dlq = recv_delivery_for_topic(&mut framed, "_dlq.source").await;
    assert_eq!(dlq.payload, b"poison".to_vec());
    assert_eq!(
        dlq.headers.get("x-trace-id").map(String::as_str),
        Some("dlq-flow")
    );
    assert_eq!(
        dlq.headers
            .get("stroma.dlq.source_topic")
            .map(String::as_str),
        Some("source")
    );
    assert_eq!(
        dlq.headers
            .get("stroma.dlq.source_offset")
            .map(String::as_str),
        Some("0")
    );
    assert_eq!(
        dlq.headers
            .get("stroma.dlq.retry_count")
            .map(String::as_str),
        Some("0")
    );
    assert_eq!(
        dlq.headers.get("stroma.dlq.reason").map(String::as_str),
        Some("retries_exhausted")
    );
    assert!(dlq.headers.contains_key("stroma.dlq.dead_lettered_at_ms"));

    assert_connection_still_responds(&mut framed).await;

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
                    content_type: None,
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

#[tokio::test]
async fn publisher_cache_idle_timeout_expires_on_next_frame_without_waiting_for_heartbeat() {
    let (mut framed, server_task, _dir, broker) = open_protocol_connection_with_settings(
        ConnectionSettings::new(Some(60)).with_publisher_cache_idle_timeout_ms(Some(0)),
    )
    .await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "publisher.cache.frame.expiry".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
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
    assert_eq!(
        broker
            .queue_activity_snapshot("publisher.cache.frame.expiry", None)
            .unwrap()
            .active_publishers,
        1
    );

    framed
        .send(try_encode(Op::Ping, 3, &()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Pong as u16);
    assert_eq!(frame.request_id, 3);

    wait_for_queue_idle(&broker, "publisher.cache.frame.expiry", None).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn demo_like_grouped_auto_ack_publish_survives_idle_cleanup() {
    let (engine, dir) = open_test_engine().await;
    let broker = Broker::new(
        engine,
        BrokerConfig {
            inflight_ttl_ms: 2_000,
            expiry_poll_min_ms: 10,
            expiry_batch_max: 100,
            delivery_poll_max_ms: 10,
            queue_idle_evict_after_ms: Some(5),
            queue_idle_sweep_interval_ms: 5,
        },
        None,
    );
    let (mut framed, server_task, _dir, broker) = open_protocol_connection_for_broker(
        ConnectionSettings::new(Some(60)).with_publisher_cache_idle_timeout_ms(Some(1)),
        broker,
        dir,
    )
    .await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "notices".into(),
                    group: Some("workers".into()),
                    prefetch: 20,
                    auto_ack: true,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    for i in 0..40_u64 {
        let request_id = 10 + i;
        let payload = format!("notice-{i}").into_bytes();
        framed
            .send(
                try_encode(
                    Op::Publish,
                    request_id,
                    &Publish {
                        topic: "notices".into(),
                        partition: 0,
                        group: Some("workers".into()),
                        require_confirm: true,
                        content_type: None,
                        headers: HashMap::new(),
                        payload: payload.clone(),
                        published: unix_millis(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut saw_publish_ok = false;
        let mut saw_delivery = false;
        while !saw_publish_ok || !saw_delivery {
            let frame = recv_frame(&mut framed).await;
            match frame.opcode {
                x if x == Op::PublishOk as u16 && frame.request_id == request_id => {
                    saw_publish_ok = true;
                }
                x if x == Op::Deliver as u16 => {
                    let delivered: Deliver = try_decode(&frame).unwrap();
                    if delivered.topic == "notices"
                        && delivered.group.as_deref() == Some("workers")
                        && delivered.payload == payload
                    {
                        saw_delivery = true;
                    }
                }
                x if x == Op::Error as u16 => {
                    let err: ErrorMsg = try_decode(&frame).unwrap();
                    panic!(
                        "unexpected publish flow error for request {}: {} {}",
                        frame.request_id, err.code, err.message
                    );
                }
                _ => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    framed
        .send(try_encode(Op::Ping, 1000, &()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Pong as u16);
    assert_eq!(frame.request_id, 1000);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker
                .queue_activity_snapshot("notices", Some("workers"))
                .is_some_and(|snapshot| {
                    snapshot.active_publishers == 0 && snapshot.active_subscribers == 1
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(broker.is_queue_materialized("notices", Some("workers")));

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publisher_cache_idle_timeout_updates_existing_connection() {
    let settings = ConnectionSettings::new(Some(1));
    let (mut framed, server_task, _dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "publisher.cache.live".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
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
    assert_eq!(
        broker
            .queue_activity_snapshot("publisher.cache.live", None)
            .unwrap()
            .active_publishers,
        1
    );

    settings.update_runtime(fibril_protocol::v1::handler::ConnectionRuntimeSettings {
        publisher_cache_idle_timeout_ms: Some(0),
        ..Default::default()
    });

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Ping as u16);

    wait_for_queue_idle(&broker, "publisher.cache.live", None).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}
