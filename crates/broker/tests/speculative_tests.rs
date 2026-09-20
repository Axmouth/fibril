use fibril_broker::{
    broker::{Broker, BrokerConfig, ConsumerConfig, SettleRequest, SettleType},
    coordination::{PartitionAssignment, QueueIdentity, ReplicationDurabilityPolicy},
    queue_engine::StromaEngine,
    storage::Partition,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use stroma_core::{KeratinConfig, SnapshotConfig, StromaKeratinConfig, TempDir};
use tokio::time::timeout;
use uuid::Uuid;
const TOPIC: &str = "spec.test";
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
async fn open(mode: u8) -> (Arc<Broker<StromaEngine>>, TempDir) {
    open_with_gate(mode, None).await
}
async fn open_with_gate(
    mode: u8,
    gate: Option<Arc<tokio::sync::Semaphore>>,
) -> (Arc<Broker<StromaEngine>>, TempDir) {
    open_options(mode, gate, false).await
}
async fn open_options(
    mode: u8,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    failure: bool,
) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = TempDir {
        root: std::env::temp_dir().join(format!("spec-broker-{}", Uuid::now_v7())),
    };
    std::fs::create_dir(&dir.root).unwrap();
    let mut storage = KeratinConfig::test_default();
    storage.batch_linger_ms = 150;
    storage.fsync_interval_ms = 150;
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(storage),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new(
        engine,
        BrokerConfig {
            experimental_speculation: mode,
            experimental_commit_gate: gate,
            experimental_message_failure: failure,
            delivery_poll_max_ms: 100_000,
            inflight_ttl_ms: 30_000,
            queue_idle_evict_after_ms: None,
            ..Default::default()
        },
        None,
    );
    (broker, dir)
}
async fn subscribe(
    b: &Arc<Broker<StromaEngine>>,
    prefetch: usize,
) -> fibril_broker::broker::ConsumerHandle {
    b.subscribe(
        TOPIC,
        Partition::new(0),
        None,
        Uuid::now_v7(),
        ConsumerConfig { prefetch },
    )
    .await
    .unwrap()
}
async fn publish(
    p: &fibril_broker::broker::PublisherHandle,
    n: u8,
) -> tokio::sync::oneshot::Receiver<Result<u64, fibril_broker::broker::BrokerError>> {
    p.publish(vec![n], now(), now(), None, Default::default(), None)
        .await
        .unwrap()
}
async fn recv(c: &mut fibril_broker::broker::ConsumerHandle) -> fibril_broker::DeliverableMessage {
    timeout(Duration::from_secs(3), c.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn settle(
    c: &fibril_broker::broker::ConsumerHandle,
    m: &fibril_broker::DeliverableMessage,
    kind: SettleType,
) {
    c.settle(SettleRequest {
        delivery_tag: m.delivery_tag,
        settle_type: kind,
    })
    .await
    .unwrap();
}
async fn drained(b: &Arc<Broker<StromaEngine>>) {
    timeout(Duration::from_secs(5), b.wait_for_pending_settles())
        .await
        .unwrap();
}

#[tokio::test]
async fn speculative_delivery_with_durable_confirm() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (b, _dir) = open_with_gate(1, Some(gate.clone())).await;
    let mut c = subscribe(&b, 2).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let mut confirm = publish(&p, 1).await;
    let m = recv(&mut c).await;
    assert_eq!(
        m.message
            .headers
            .get("fibril.speculative")
            .map(String::as_str),
        Some("1")
    );
    assert!(
        confirm.try_recv().is_err(),
        "delivery must precede completion in this delayed-storage fixture"
    );
    settle(&c, &m, SettleType::Ack).await;
    assert!(
        timeout(Duration::from_millis(20), &mut confirm)
            .await
            .is_err(),
        "ACK must not bypass mode 1 durability"
    );
    gate.add_permits(1);
    assert_eq!(
        timeout(Duration::from_secs(3), confirm)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    drained(&b).await;
    assert!(timeout(Duration::from_millis(50), c.recv()).await.is_err());
    assert_eq!(
        b.partition_lowest_unsettled_offset(TOPIC, Partition::new(0), None)
            .await
            .unwrap(),
        1
    );
    b.shutdown().await;
}

#[tokio::test]
async fn processed_confirm_precedes_local_apply_and_does_not_resurrect() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (b, _dir) = open_with_gate(2, Some(gate.clone())).await;
    let mut c = subscribe(&b, 2).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let mut confirm = publish(&p, 1).await;
    let m = recv(&mut c).await;
    assert!(confirm.try_recv().is_err());
    settle(&c, &m, SettleType::Ack).await;
    assert_eq!(
        timeout(Duration::from_millis(75), confirm)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    let snap = b.debug_snapshot().await.unwrap();
    let q = snap.queues.iter().find(|q| q.topic == TOPIC).unwrap();
    assert_eq!(
        q.state.settled_until, 0,
        "volatile ACK must not advance durable state"
    );
    gate.add_permits(1);
    drained(&b).await;
    assert!(timeout(Duration::from_millis(50), c.recv()).await.is_err());
    b.shutdown().await;
}

#[tokio::test]
async fn inflight_is_allowed_but_prefetch_is_enforced() {
    let (b, _dir) = open(2).await;
    let mut c = subscribe(&b, 2).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let r0 = publish(&p, 0).await;
    let m0 = recv(&mut c).await;
    let r1 = publish(&p, 1).await;
    let m1 = recv(&mut c).await;
    assert_eq!((m0.message.offset, m1.message.offset), (0, 1));
    assert!(m1.message.headers.contains_key("fibril.speculative"));
    let r2 = publish(&p, 2).await;
    assert!(timeout(Duration::from_millis(30), c.recv()).await.is_err());
    settle(&c, &m0, SettleType::Ack).await;
    let m2 = recv(&mut c).await;
    assert_eq!(m2.message.offset, 2);
    settle(&c, &m1, SettleType::Ack).await;
    settle(&c, &m2, SettleType::Ack).await;
    for r in [r0, r1, r2] {
        timeout(Duration::from_secs(3), r)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn existing_ready_backlog_keeps_dispatch_order() {
    let (b, _dir) = open(2).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let r0 = publish(&p, 0).await;
    r0.await.unwrap().unwrap();
    let mut c = subscribe(&b, 2).await;
    let r1 = publish(&p, 1).await;
    let m0 = recv(&mut c).await;
    let m1 = recv(&mut c).await;
    assert_eq!((m0.message.offset, m1.message.offset), (0, 1));
    for m in [&m0, &m1] {
        settle(&c, m, SettleType::Ack).await;
    }
    r1.await.unwrap().unwrap();
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn early_nack_requeues_after_handoff_with_same_identity_and_new_tag() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (b, _dir) = open_with_gate(2, Some(gate.clone())).await;
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let mut confirm = publish(&p, 1).await;
    let first = timeout(Duration::from_secs(3), c.recv()).await.expect("initial early delivery did not arrive").unwrap();
    settle(
        &c,
        &first,
        SettleType::Nack {
            requeue: Some(true),
            not_before: None,
        },
    )
    .await;
    assert!(
        timeout(Duration::from_millis(20), &mut confirm)
            .await
            .is_err()
    );
    gate.add_permits(1);
    let second = timeout(Duration::from_secs(3), c.recv()).await.expect("NACK redelivery did not arrive").unwrap();
    assert_eq!(second.message.offset, first.message.offset);
    assert_eq!(
        second.message.headers["fibril.message_id"],
        first.message.headers["fibril.message_id"]
    );
    assert_ne!(second.delivery_tag, first.delivery_tag);
    assert!(!second.message.headers.contains_key("fibril.speculative"));
    settle(&c, &second, SettleType::Ack).await;
    confirm.await.unwrap().unwrap();
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn unsubscribe_before_commit_releases_speculative_lease() {
    let (b, _dir) = open(2).await;
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let confirm = publish(&p, 1).await;
    let first = recv(&mut c).await;
    timeout(
        Duration::from_secs(3),
        b.unsubscribe(TOPIC, None, Partition::new(0), c.sub_id),
    )
    .await
    .unwrap()
    .unwrap();
    let mut replacement = subscribe(&b, 1).await;
    let second = recv(&mut replacement).await;
    assert_eq!(
        second.message.headers["fibril.message_id"],
        first.message.headers["fibril.message_id"]
    );
    settle(&replacement, &second, SettleType::Ack).await;
    confirm.await.unwrap().unwrap();
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn assigned_queue_keeps_replica_delivery_and_confirmation_gates() {
    let (b, _dir) = open(2).await;
    b.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(TOPIC, Partition::new(0), None),
            "owner",
            vec!["follower".into()],
            1,
        )
        .with_durability(ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 }),
    );
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let mut confirm = publish(&p, 1).await;
    assert!(timeout(Duration::from_millis(400), c.recv()).await.is_err());
    assert!(confirm.try_recv().is_err());
    b.record_follower_replication_progress(TOPIC, Partition::new(0), None, "follower", 1, 1);
    let msg = recv(&mut c).await;
    assert!(!msg.message.headers.contains_key("fibril.speculative"));
    settle(&c, &msg, SettleType::Ack).await;
    confirm.await.unwrap().unwrap();
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn wrong_consumer_and_duplicate_ack_do_not_steal_tag_or_leak_settle_drain() {
    let (b, _dir) = open(2).await;
    let mut first = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let confirm = publish(&p, 1).await;
    let msg = recv(&mut first).await;
    let other = subscribe(&b, 1).await;
    settle(&other, &msg, SettleType::Ack).await;
    drained(&b).await;
    assert_eq!(
        b.partition_lowest_unsettled_offset(TOPIC, Partition::new(0), None)
            .await
            .unwrap(),
        0
    );
    settle(&first, &msg, SettleType::Ack).await;
    confirm.await.unwrap().unwrap();
    drained(&b).await;
    settle(&first, &msg, SettleType::Ack).await;
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn injected_storage_failure_after_processed_confirm_drains_without_redelivery() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (b, _dir) = open_options(2, Some(gate.clone()), true).await;
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let confirm = publish(&p, 1).await;
    let msg = recv(&mut c).await;
    settle(&c, &msg, SettleType::Ack).await;
    assert_eq!(
        timeout(Duration::from_millis(100), confirm)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    gate.add_permits(1);
    drained(&b).await;
    assert!(timeout(Duration::from_millis(100), c.recv()).await.is_err());
    assert_eq!(
        b.debug_snapshot()
            .await
            .unwrap()
            .queues
            .iter()
            .find(|q| q.topic == TOPIC)
            .unwrap()
            .state
            .inflight_count,
        0
    );
    b.shutdown().await;
}

#[tokio::test]
async fn injected_storage_failure_without_ack_fails_publish_and_returns_credit() {
    let (b, _dir) = open_options(1, None, true).await;
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    for n in 0..2 {
        let confirm = publish(&p, n).await;
        let msg = recv(&mut c).await;
        assert_eq!(msg.message.offset, n as u64);
        assert!(
            timeout(Duration::from_secs(3), confirm)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
    b.shutdown().await;
}

#[test]
fn volatile_reservations_preserve_pending_prefix_and_atomic_handoff() {
    use stroma_core::QueueInternalState;
    use tokio_util::sync::CancellationToken;
    let mut q = QueueInternalState::new(TOPIC.into(), 0);
    let cancel = CancellationToken::new();
    assert_eq!(q.experimental_stage(0, 1, 1, 10_000, cancel.clone()), 1);
    assert_eq!(q.experimental_stage(1, 1, 1, 10_000, cancel.clone()), 1);
    q.enqueue(0, 0, None);
    assert!(q.is_inflight(0) && !q.is_ready(0));
    assert_eq!(q.experimental_stage(2, 1, 0, 10_000, cancel.clone()), 0);
    assert_eq!(q.experimental_stage(3, 1, 1, 10_000, cancel.clone()), 0);
    q.enqueue(3, 0, None);
    assert!(q.poll_ready_and_mark(5, 10_000, u64::MAX).is_empty());
    q.enqueue(2, 0, None);
    assert_eq!(q.poll_ready_and_mark(5, 10_000, u64::MAX), [(2, 0), (3, 0)]);
    cancel.cancel();
    q.enqueue(1, 0, None);
    assert!(q.is_ready(1) && !q.is_inflight(1));
}

#[tokio::test]
async fn snapshots_refuse_unresolved_staged_publications() {
    use stroma_core::Stroma;
    use tokio_util::sync::CancellationToken;
    let dir = TempDir {
        root: std::env::temp_dir().join(format!("spec-snapshot-{}", Uuid::now_v7())),
    };
    std::fs::create_dir(&dir.root).unwrap();
    let s = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let handle = s.queue_handle(TOPIC, 0, None).await.unwrap();
    {
        let h = handle.resolve().unwrap();
        let w = h.work_queue().unwrap();
        w.experimental_stage(0, 1, 1, now() + 1000, CancellationToken::new())
            .await
            .unwrap();
        assert!(h.force_encode_snapshot(0).await.is_err());
        assert!(w.export_state_checkpoint_snapshot(0).await.is_err());
        w.experimental_abandon(0, 1, true).await.unwrap();
        assert!(h.force_encode_snapshot(0).await.is_ok());
    }
    s.shutdown().await.unwrap();
}

#[test]
fn delayed_apply_releases_the_staged_ordering_barrier() {
    use stroma_core::QueueInternalState;
    use tokio_util::sync::CancellationToken;
    let mut q = QueueInternalState::new(TOPIC.into(), 0);
    assert_eq!(
        q.experimental_stage(0, 1, 0, 100, CancellationToken::new()),
        0
    );
    q.enqueue_delayed(0, 1_000);
    q.enqueue(1, 0, None);
    assert_eq!(q.poll_ready_and_mark(2, 100, u64::MAX), [(1, 0)]);
}

#[tokio::test]
async fn ttl_publication_uses_durable_fallback() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (b, _dir) = open_with_gate(2, Some(gate.clone())).await;
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let confirm = p
        .publish(
            vec![1],
            now(),
            now(),
            None,
            Default::default(),
            Some(now() + 60_000),
        )
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(30), c.recv()).await.is_err());
    gate.add_permits(1);
    let msg = recv(&mut c).await;
    assert!(!msg.message.headers.contains_key("fibril.speculative"));
    settle(&c, &msg, SettleType::Ack).await;
    confirm.await.unwrap().unwrap();
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn owner_freeze_during_staged_delivery_fences_the_old_tag() {
    use fibril_broker::coordination::{
        LocalAssignmentIntent, LocalAssignmentRole, LocalAssignmentTransition,
    };
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (b, _dir) = open_with_gate(2, Some(gate.clone())).await;
    let mut c = subscribe(&b, 1).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let confirm = publish(&p, 1).await;
    let first = recv(&mut c).await;
    let transition = LocalAssignmentTransition {
        queue: QueueIdentity::new(TOPIC, Partition::new(0), None),
        previous_role: Some(LocalAssignmentRole::Owner),
        next_role: None,
        previous: None,
        next: None,
        intent: LocalAssignmentIntent::FreezeOwner,
    };
    let broker = b.clone();
    let freezing =
        tokio::spawn(async move { broker.apply_assignment_transition(&transition).await });
    timeout(Duration::from_secs(2), async {
        while b.queue_activity_snapshot(TOPIC, None).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.add_permits(1);
    timeout(Duration::from_secs(3), freezing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _ = timeout(Duration::from_secs(3), confirm).await.unwrap();
    b.apply_assignment_transition(&LocalAssignmentTransition {
        queue: QueueIdentity::new(TOPIC, Partition::new(0), None),
        previous_role: None,
        next_role: Some(LocalAssignmentRole::Owner),
        previous: None,
        next: None,
        intent: LocalAssignmentIntent::BecomeOwner,
    })
    .await
    .unwrap();
    let mut replacement = subscribe(&b, 1).await;
    let second = recv(&mut replacement).await;
    assert_eq!(second.message.offset, first.message.offset);
    assert_ne!(second.delivery_tag, first.delivery_tag);
    // An ACK carrying the old attempt cannot settle the new owner's delivery.
    settle(&replacement, &first, SettleType::Ack).await;
    drained(&b).await;
    assert_eq!(
        b.partition_lowest_unsettled_offset(TOPIC, Partition::new(0), None)
            .await
            .unwrap(),
        0
    );
    settle(&replacement, &second, SettleType::Ack).await;
    drained(&b).await;
    b.shutdown().await;
}

#[tokio::test]
async fn exhausted_credit_preserves_order_across_a_durable_fallback_burst() {
    let (b, _dir) = open(2).await;
    let mut c = subscribe(&b, 2).await;
    let p = b
        .get_publisher(TOPIC, Partition::new(0), &None)
        .await
        .unwrap();
    let r0 = publish(&p, 0).await;
    let m0 = recv(&mut c).await;
    let r1 = publish(&p, 1).await;
    let m1 = recv(&mut c).await;
    assert!(m0.message.headers.contains_key("fibril.speculative"));
    assert!(m1.message.headers.contains_key("fibril.speculative"));

    // Keep both delivery slots occupied until the whole later burst is durable.
    // Every later offset must use ordinary delivery without passing the prefix.
    let mut confirmations = Vec::new();
    for n in 2..130u8 {
        confirmations.push((n, publish(&p, n).await));
    }
    for (n, confirm) in confirmations {
        assert_eq!(
            timeout(Duration::from_secs(3), confirm)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            n as u64
        );
    }
    assert!(timeout(Duration::from_millis(20), c.recv()).await.is_err());
    settle(&c, &m0, SettleType::Ack).await;
    settle(&c, &m1, SettleType::Ack).await;
    for n in 2..130u8 {
        let m = recv(&mut c).await;
        assert_eq!(m.message.offset, n as u64);
        assert_eq!(m.message.payload, vec![n]);
        assert!(!m.message.headers.contains_key("fibril.speculative"));
        settle(&c, &m, SettleType::Ack).await;
    }
    assert_eq!(r0.await.unwrap().unwrap(), 0);
    assert_eq!(r1.await.unwrap().unwrap(), 1);
    drained(&b).await;
    // Normal ACK completion means durable plus queued actor application.
    // The express frontier query can overtake that queued ACK; wait for apply.
    timeout(Duration::from_secs(3), async {
        loop {
            let frontier = b
                .partition_lowest_unsettled_offset(TOPIC, Partition::new(0), None)
                .await
                .unwrap();
            assert!(frontier <= 130);
            if frontier == 130 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(timeout(Duration::from_millis(30), c.recv()).await.is_err());
    b.shutdown().await;
}
