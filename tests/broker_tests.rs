use std::sync::Arc;

use thetube::broker::coordination::*;
use thetube::broker::*;
use thetube::storage::*;

// TODO: make shared
fn make_test_store() -> impl Storage {
    // make testdata dir
    std::fs::create_dir_all("test_data").unwrap();
    // make random temp filename to avoid conflicts
    let filename = format!("test_data/{}", fastrand::u64(..));
    make_rocksdb_store(&filename).unwrap()
}

async fn make_test_broker() -> Broker<NoopCoordination> {
    let store = make_test_store();
    Broker::try_new(
        store,
        NoopCoordination,
        BrokerConfig {
            inflight_ttl_secs: 3,
            publish_batch_size: 10,
            publish_batch_timeout_ms: 100,
            ack_batch_size: 10,
            ack_batch_timeout_ms: 100,
            reset_inflight: false,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn broker_basic_delivery() {
    let broker = make_test_broker().await;

    // Publish 3 messages
    broker.publish("t1", b"hello").await.unwrap();
    broker.publish("t1", b"world").await.unwrap();
    broker.publish("t1", b"!").await.unwrap();

    // Subscribe
    let mut consumer = broker.subscribe("t1", "g1").await.unwrap();

    let mut received = vec![];
    for _ in 0..3 {
        let msg = consumer.messages.recv().await.unwrap();
        received.push(String::from_utf8(msg.message.payload.clone()).unwrap());
        consumer.acker.send(msg.delivery_tag).await.unwrap();
    }

    assert_eq!(received, ["hello", "world", "!"]);
}

#[tokio::test]
async fn broker_ack_behavior() {
    let broker = make_test_broker().await;

    broker.publish("t", b"a").await.unwrap();
    broker.publish("t", b"b").await.unwrap();

    let mut consumer = broker.subscribe("t", "g").await.unwrap();

    // Receive & ack first message
    let m1 = consumer.messages.recv().await.unwrap();
    assert_eq!(m1.message.payload, b"a");
    consumer.acker.send(m1.delivery_tag).await.unwrap();

    // Receive & ack second
    let m2 = consumer.messages.recv().await.unwrap();
    assert_eq!(m2.message.payload, b"b");
    consumer.acker.send(m2.delivery_tag).await.unwrap();

    // Should not receive anything else now
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    assert!(consumer.messages.try_recv().is_err());
}

#[tokio::test]
async fn broker_work_queue_distribution() {
    let broker = make_test_broker().await;

    // Publish some work
    for i in 0..6 {
        broker
            .publish("jobs", format!("job-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Two consumers, same group (competing consumers)
    let mut c1 = broker.subscribe("jobs", "workers").await.unwrap();
    let mut c2 = broker.subscribe("jobs", "workers").await.unwrap();

    let mut got1 = 0;
    let mut got2 = 0;

    // Collect 6 messages total (in any order)
    for _ in 0..6 {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                got1 += 1;
                c1.acker.send(msg.delivery_tag).await.unwrap();
            }
            Some(msg) = c2.messages.recv() => {
                got2 += 1;
                c2.acker.send(msg.delivery_tag).await.unwrap();
            }
        }
    }

    // Work should be distributed, not replicated
    assert_eq!(got1 + got2, 6);
    assert!(got1 > 0);
    assert!(got2 > 0);
}

#[tokio::test]
async fn broker_pubsub_multiple_groups() {
    let broker = make_test_broker().await;

    broker.publish("events", b"alpha").await.unwrap();
    broker.publish("events", b"beta").await.unwrap();

    let mut g1 = broker.subscribe("events", "g1").await.unwrap();
    let mut g2 = broker.subscribe("events", "g2").await.unwrap();

    let mut recv_g1 = vec![];
    let mut recv_g2 = vec![];

    for _ in 0..2 {
        let m1 = g1.messages.recv().await.unwrap();
        recv_g1.push(String::from_utf8(m1.message.payload.clone()).unwrap());
        g1.acker.send(m1.delivery_tag).await.unwrap();

        let m2 = g2.messages.recv().await.unwrap();
        recv_g2.push(String::from_utf8(m2.message.payload.clone()).unwrap());
        g2.acker.send(m2.delivery_tag).await.unwrap();
    }

    assert_eq!(recv_g1, ["alpha", "beta"]);
    assert_eq!(recv_g2, ["alpha", "beta"]);
}

#[tokio::test]
async fn broker_delivery_in_order() {
    let broker = make_test_broker().await;

    for i in 0..10 {
        broker
            .publish("numbers", format!("{}", i).as_bytes())
            .await
            .unwrap();
    }

    let mut consumer = broker.subscribe("numbers", "g").await.unwrap();

    let mut collected = vec![];

    for _ in 0..10 {
        let msg = consumer.messages.recv().await.unwrap();
        collected.push(String::from_utf8(msg.message.payload.clone()).unwrap());
        consumer.acker.send(msg.delivery_tag).await.unwrap();
    }

    assert_eq!(
        collected,
        (0..10).map(|i| i.to_string()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn broker_consumer_drop_stops_delivery() {
    let broker = make_test_broker().await;

    broker.publish("t", b"x").await.unwrap();

    let consumer = broker.subscribe("t", "g").await.unwrap();

    // Drop receiver
    drop(consumer.messages);

    // Delivery loop should stop without panicking
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;

    // Publishing more messages should not cause issues
    assert!(broker.publish("t", b"y").await.is_ok());
}

#[tokio::test]
async fn redelivery_occurs_after_expiration() {
    let broker = make_test_broker().await;
    broker.start_redelivery_worker();

    // Publish 1 message
    broker.publish("topic", b"hello").await.unwrap();

    // Subscribe
    let mut cons = broker.subscribe("topic", "g").await.unwrap();

    // First delivery
    let m1 = cons.messages.recv().await.unwrap();
    assert_eq!(m1.message.payload, b"hello");

    // Do NOT ack → let it expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await; // >3s TTL

    // Message should be redelivered
    let m2 = cons.messages.recv().await.unwrap();
    assert_eq!(m2.message.offset, m1.message.offset);
    assert_eq!(m2.message.payload, b"hello");

    // Now ACK to finish
    cons.acker.send(m2.delivery_tag).await.unwrap();
}

#[tokio::test]
async fn ack_prevents_redelivery() {
    let broker = make_test_broker().await;
    broker.start_redelivery_worker();

    broker.publish("topic", b"hi").await.unwrap();

    let mut cons = broker.subscribe("topic", "g").await.unwrap();

    let msg = cons.messages.recv().await.unwrap();
    cons.acker.send(msg.delivery_tag).await.unwrap();

    // Wait past expiration window
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should NOT redeliver
    assert!(cons.messages.try_recv().is_err());

    drop(cons);
    drop(broker);
}

#[tokio::test]
async fn redelivery_load_balanced_to_consumers() {
    let broker = make_test_broker().await;
    broker.start_redelivery_worker();

    broker.publish("t", b"x").await.unwrap();

    let mut c1 = broker.subscribe("t", "g").await.unwrap();
    let mut c2 = broker.subscribe("t", "g").await.unwrap();

    let m = c1.messages.recv().await.unwrap();
    assert_eq!(m.message.payload, b"x");

    // Let it expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should go to any consumer — we just verify it redelivers
    let redelivered = tokio::select! {
        Some(m2) = c1.messages.recv() => m2,
        Some(m2) = c2.messages.recv() => m2,
    };

    assert_eq!(redelivered.message.payload, b"x");
}

#[tokio::test]
async fn selective_ack_out_of_order() {
    let broker = make_test_broker().await;
    broker.start_redelivery_worker();

    // Publish messages 0..4
    for i in 0..5 {
        broker
            .publish("t", format!("m{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut cons = broker.subscribe("t", "g").await.unwrap();

    // Receive messages
    let mut messages = vec![];
    for _ in 0..5 {
        let msg = cons.messages.recv().await.unwrap();
        messages.push(msg);
    }

    // ACK out-of-order: ACK offset 3 and 4 first
    cons.acker.send(messages[3].delivery_tag).await.unwrap();
    cons.acker.send(messages[4].delivery_tag).await.unwrap();

    // ACK 0 next
    cons.acker.send(messages[0].delivery_tag).await.unwrap();

    // ACK 2 before 1
    cons.acker.send(messages[2].delivery_tag).await.unwrap();
    cons.acker.send(messages[1].delivery_tag).await.unwrap();

    // No redelivery should occur
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());
}

#[tokio::test]
async fn selective_ack_expiry_redelivery() {
    let broker = make_test_broker().await;
    broker.start_redelivery_worker();

    for i in 0..5 {
        broker
            .publish("t", format!("v{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut cons = broker.subscribe("t", "g").await.unwrap();

    // Receive all messages 0..4
    let mut msgs = vec![];
    for _ in 0..5 {
        msgs.push(cons.messages.recv().await.unwrap());
    }

    // ACK everything except offset 2
    for i in [0, 1, 3, 4] {
        cons.acker.send(msgs[i].delivery_tag).await.unwrap();
    }

    // Let inflight(2) expire
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Should re-deliver only offset 2
    let redelivered = cons.messages.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, 2);

    // ACK it finally
    cons.acker.send(redelivered.delivery_tag).await.unwrap();

    // Should not redeliver again
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let next = cons.messages.try_recv();
    assert!(next.is_err());
}

#[tokio::test]
async fn selective_ack_no_wrong_rewind() {
    let broker = make_test_broker().await;
    broker.start_redelivery_worker();

    for i in 0..3 {
        broker
            .publish("t", format!("x{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut cons = broker.subscribe("t", "g").await.unwrap();

    let m0 = cons.messages.recv().await.unwrap();
    let _m1 = cons.messages.recv().await.unwrap();
    let m2 = cons.messages.recv().await.unwrap();

    // ACK 2 then 0 (skipping 1)
    cons.acker.send(m2.delivery_tag).await.unwrap();
    cons.acker.send(m0.delivery_tag).await.unwrap();

    // No redelivery yet
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());

    // Let 1 expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should redeliver only offset=1
    let redelivered = cons.messages.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, 1);
}

#[tokio::test]
async fn batch_basic() {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 5,
        publish_batch_timeout_ms: 10,
        ..Default::default()
    };
    let broker = Arc::new(Broker::try_new(store, coord, cfg).await.unwrap());

    // Publish 10 messages → expect two batches
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn({
            let b = broker.clone();
            async move { b.publish("topic", b"x").await.unwrap() }
        }));
    }

    let mut offsets = Vec::new();
    for h in handles {
        offsets.push(h.await.unwrap());
    }

    offsets.sort();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // Verify fetch delivers all
    let mut c = broker.subscribe("topic", "g").await.unwrap();
    let mut recv = Vec::new();
    for _ in 0..10 {
        let msg = c.messages.recv().await.unwrap();
        recv.push(msg.message.offset);
    }
    recv.sort();
    assert_eq!(recv, offsets);
}

#[tokio::test]
async fn batch_timeout_flushes() {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 100,
        publish_batch_timeout_ms: 20,
        ..Default::default()
    }; // Large batch size, short timeout
    let broker = Broker::try_new(store, coord, cfg).await.unwrap();

    // Publish 3 messages, waiting briefly so timeout triggers
    let off1 = broker.publish("topic", b"a").await.unwrap();
    let off2 = broker.publish("topic", b"b").await.unwrap();
    let off3 = broker.publish("topic", b"c").await.unwrap();

    assert_eq!(vec![off1, off2, off3], vec![0, 1, 2]);
}

#[tokio::test]
async fn batch_concurrent_ordering() {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 10,
        publish_batch_timeout_ms: 50,
        ..Default::default()
    };
    let broker = Arc::new(Broker::try_new(store, coord, cfg).await.unwrap());

    let publish_count = 200;
    let mut tasks = Vec::new();

    for _ in 0..publish_count {
        let b = broker.clone();
        tasks.push(tokio::spawn(
            async move { b.publish("t", b"m").await.unwrap() },
        ));
    }

    let mut offsets = Vec::new();
    for t in tasks {
        offsets.push(t.await.unwrap());
    }

    offsets.sort();
    let expected: Vec<u64> = (0..publish_count).collect();

    assert_eq!(offsets, expected);
}

#[tokio::test]
async fn batch_publish_and_consume() {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 5,
        publish_batch_timeout_ms: 50,
        ..Default::default()
    };
    let broker = Broker::try_new(store, coord, cfg).await.unwrap();

    broker.start_redelivery_worker();

    // Publish 12 messages
    for i in 0..12 {
        broker
            .publish("topic", format!("x{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut c = broker.subscribe("topic", "g").await.unwrap();

    let mut seen = Vec::new();
    for _ in 0..12 {
        let msg = c.messages.recv().await.unwrap();
        seen.push((msg.message.offset, msg.message.payload.clone()));
        c.acker.send(msg.delivery_tag).await.unwrap();
    }

    seen.sort_by_key(|x| x.0);

    for (i, s) in seen.iter().enumerate().take(12) {
        assert_eq!(s.0, i as u64);
        assert_eq!(s.1, format!("x{i}").as_bytes());
    }
}

#[tokio::test]
async fn batch_multiple_topics() {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 4,
        publish_batch_timeout_ms: 20,
        ..Default::default()
    };
    let broker = Broker::try_new(store, coord, cfg).await.unwrap();

    // Publish into two topics interleaved
    let off_a1 = broker.publish("A", b"a1").await.unwrap();
    let off_b1 = broker.publish("B", b"b1").await.unwrap();
    let off_a2 = broker.publish("A", b"a2").await.unwrap();
    let off_b2 = broker.publish("B", b"b2").await.unwrap();

    assert_eq!(off_a1, 0);
    assert_eq!(off_a2, 1);
    assert_eq!(off_b1, 0);
    assert_eq!(off_b2, 1);
}

#[tokio::test]
async fn publish_burst_then_consume_everything() {
    let total = 50_000;
    let max_payload = 512;

    let store = make_test_store();
    let coord = NoopCoordination {};
    let mut cfg = BrokerConfig::default();
    cfg.publish_batch_size = 64;
    cfg.publish_batch_timeout_ms = 1;

    let broker = Broker::try_new(store, coord, cfg).await.unwrap();

    // Insert messages with known patterns
    let mut payloads = Vec::new();
    for i in 0..total {
        let size = fastrand::usize(32..max_payload);
        let mut buf = vec![0u8; size];
        fastrand::fill(&mut buf);
        payloads.push((i as u64, buf.clone()));
        broker.publish("topic", &buf).await.unwrap();
    }

    // Now consume them
    let mut c = broker.subscribe("topic", "group").await.unwrap();

    let mut seen = Vec::new();
    for _ in 0..total {
        let msg = c.messages.recv().await.unwrap();
        seen.push((msg.message.offset, msg.message.payload.clone()));
        c.acker.send(msg.message.offset).await.unwrap();
    }

    // Sort by offset
    seen.sort_by_key(|x| x.0);

    // Validate offsets
    for i in 0..total {
        assert_eq!(seen[i].0, i as u64, "Offset mismatch");
        assert_eq!(seen[i].1, payloads[i].1, "Payload mismatch at offset {}", i,);
    }
}

#[tokio::test]
async fn concurrent_publish_and_consume() {
    let total = 100_000;

    let store = make_test_store();
    let coord = NoopCoordination {};
    let mut cfg = BrokerConfig::default();
    cfg.publish_batch_size = 64;
    cfg.publish_batch_timeout_ms = 1;

    let broker = Arc::new(Broker::try_new(store, coord, cfg).await.unwrap());

    // Start consumer
    let mut consumer = broker.subscribe("topic", "g").await.unwrap();

    // Start publishers
    let mut pub_tasks = Vec::new();
    for _ in 0..4 {
        let b = broker.clone();
        pub_tasks.push(tokio::spawn(async move {
            for i in 0..(total / 4) {
                let mut buf = vec![0u8; 128];
                buf[0..8].copy_from_slice(&(i as u64).to_be_bytes());
                b.publish("topic", &buf).await.unwrap();
            }
        }));
    }

    // Collect consumed
    let mut seen = Vec::with_capacity(total);
    for _ in 0..total {
        let msg = consumer.messages.recv().await.unwrap();
        consumer.acker.send(msg.delivery_tag).await.unwrap();
        seen.push(msg);
    }

    // Check for duplicates
    let mut offsets: Vec<u64> = seen.iter().map(|m| m.message.offset).collect();
    offsets.sort();
    offsets.dedup();
    assert_eq!(offsets.len(), total, "Duplicates detected");

    // Check consecutive offsets
    for i in 0..total {
        assert_eq!(offsets[i], i as u64, "Missing or reordered messages");
    }
}

#[tokio::test]
async fn redelivery_under_load_intense() {
    let total = 20_000;

    redelivery_under_load(total).await;
}

#[tokio::test]
async fn redelivery_under_load_lax() {
    let total = 2_000;

    redelivery_under_load(total).await;
}

async fn redelivery_under_load(total: usize) {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 32,
        publish_batch_timeout_ms: 1,
        inflight_ttl_secs: 5,
        ..Default::default()
    }; // <-- generous TTL
    let broker = Broker::try_new(store, coord, cfg).await.unwrap();
    broker.start_redelivery_worker();

    // Publish burst
    for _ in 0..total {
        broker.publish("t", b"x").await.unwrap();
    }

    let mut c = broker.subscribe("t", "g").await.unwrap();

    // FIRST PHASE: receive *all* unique offsets once
    use std::collections::HashSet;
    let mut seen_once = HashSet::new();
    while seen_once.len() < total {
        let msg = c.messages.recv().await.unwrap();
        seen_once.insert(msg.message.offset);
        // DON'T ACK
    }

    assert_eq!(seen_once.len(), total);

    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // SECOND PHASE: receive at least one more of each
    let mut counts = vec![0usize; total];

    // allow some slack, since we may see dupes
    let max_deliveries = total * 3;
    let mut received = 0;

    while counts.contains(&0) && received < max_deliveries {
        let msg = c.messages.recv().await.unwrap();
        let idx = msg.message.offset as usize;
        if idx < total {
            counts[idx] += 1;
        }
        // ACK now
        c.acker.send(msg.message.offset).await.unwrap();
        received += 1;
    }

    assert!(
        counts.iter().all(|&c| c >= 1),
        "Some offsets never redelivered at least once"
    );
}

fn dupes_with_counts<T>(v: &[T]) -> std::collections::BTreeMap<&T, usize>
where
    T: std::hash::Hash + Eq + Ord,
{
    let mut counts = std::collections::BTreeMap::new();
    for x in v {
        *counts.entry(x).or_insert(0) += 1;
    }
    counts.into_iter().filter(|(_, c)| *c > 1).collect()
}

#[tokio::test]
async fn restart_persists_messages() {
    use thetube::storage::make_rocksdb_store;

    let coord = NoopCoordination {};
    let mut cfg = BrokerConfig::default();
    cfg.publish_batch_size = 10;
    cfg.publish_batch_timeout_ms = 10;

    // Dedicated DB path for this test
    std::fs::create_dir_all("test_data").unwrap();
    let db_path = format!("test_data/restart_persist_{}", fastrand::u64(..));

    // 1) First broker instance: publish messages
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        for i in 0..20 {
            broker
                .publish("restart_topic", format!("m{i}").as_bytes())
                .await
                .unwrap();
        }

        // Drop broker (and storage), simulating process exit
        broker.shutdown().await;
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // 2) New broker instance on the same path
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        let mut cons = broker.subscribe("restart_topic", "g").await.unwrap();

        let mut msgs = Vec::new();
        for _ in 0..20 {
            let m = cons.messages.recv().await.unwrap();
            msgs.push(String::from_utf8(m.message.payload.clone()).unwrap());
            cons.acker.send(m.delivery_tag).await.unwrap();
        }

        assert_eq!(msgs, (0..20).map(|i| format!("m{i}")).collect::<Vec<_>>());

        // No more messages
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(cons.messages.try_recv().is_err());
    }
}

#[tokio::test]
async fn restart_persists_ack_state() {
    use thetube::storage::make_rocksdb_store;

    let coord = NoopCoordination {};
    let mut cfg = BrokerConfig {
        inflight_ttl_secs: 3,
        ..BrokerConfig::default()
    };
    cfg.publish_batch_size = 10;
    cfg.publish_batch_timeout_ms = 10;

    std::fs::create_dir_all("test_data").unwrap();
    let db_path = format!("test_data/restart_acks_{}", fastrand::u64(..));

    // 1) First broker: publish and ACK some messages
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        for i in 0..10 {
            broker
                .publish("restart_ack_topic", format!("m{i}").as_bytes())
                .await
                .unwrap();
        }

        let mut cons = broker.subscribe("restart_ack_topic", "g").await.unwrap();

        // ACK first 7 messages
        for _ in 0..7 {
            let m = cons.messages.recv().await.unwrap();
            cons.acker.send(m.delivery_tag).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        // Drop broker (simulating crash/restart)
        drop(cons);
        broker.shutdown().await;
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // 2) Second broker: should only see remaining messages
    {
        // Wait past inflight TTL to ensure no inflight entries remain
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        let mut cons = broker.subscribe("restart_ack_topic", "g").await.unwrap();

        let mut seen = Vec::new();
        // Expect only 3 messages left
        for _ in 0..3 {
            let m = cons.messages.recv().await.unwrap();
            seen.push(String::from_utf8(m.message.payload.clone()).unwrap());
            cons.acker.send(m.delivery_tag).await.unwrap();
        }

        // No more messages should arrive
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(cons.messages.try_recv().is_err());

        assert_eq!(
            seen,
            vec!["m7".to_string(), "m8".to_string(), "m9".to_string()]
        );
    }
}

#[tokio::test]
async fn restart_redelivery_across_restart() {
    use thetube::storage::make_rocksdb_store;

    let coord = NoopCoordination {};
    let mut cfg = BrokerConfig::default();
    cfg.publish_batch_size = 10;
    cfg.publish_batch_timeout_ms = 10;
    cfg.inflight_ttl_secs = 1;

    std::fs::create_dir_all("test_data").unwrap();
    let db_path = format!("test_data/restart_redel_{}", fastrand::u64(..));

    let offset: Offset;

    // 1) First broker: publish + first delivery (no ACK)
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();
        broker.start_redelivery_worker();

        broker.publish("rr_topic", b"hello").await.unwrap();

        let mut cons = broker.subscribe("rr_topic", "g").await.unwrap();
        let m1 = cons.messages.recv().await.unwrap();
        assert_eq!(m1.message.payload, b"hello");
        offset = m1.message.offset;

        // Do not ACK
        drop(cons);
        broker.shutdown().await;
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Wait past TTL so the inflight entry is expired in storage
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 2) Second broker: same DB, new worker, must redeliver
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Arc::new(
            Broker::try_new(store, coord.clone(), cfg.clone())
                .await
                .unwrap(),
        );
        broker.start_redelivery_worker();

        let mut cons = broker.subscribe("rr_topic", "g").await.unwrap();

        let redelivered = cons.messages.recv().await.unwrap();
        assert_eq!(redelivered.message.offset, offset);
        assert_eq!(redelivered.message.payload, b"hello");

        cons.acker.send(redelivered.delivery_tag).await.unwrap();
    }
}

#[tokio::test]
async fn work_queue_fair_distribution() {
    let broker = make_test_broker().await;

    let total = 6000;

    // Publish work
    for i in 0..total {
        broker
            .publish("jobs_fair", format!("job-{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut c1 = broker.subscribe("jobs_fair", "workers").await.unwrap();
    let mut c2 = broker.subscribe("jobs_fair", "workers").await.unwrap();
    let mut c3 = broker.subscribe("jobs_fair", "workers").await.unwrap();

    let mut counts = [0usize; 3];

    for _ in 0..total {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                counts[0] += 1;
                c1.acker.send(msg.delivery_tag).await.unwrap();
            }
            Some(msg) = c2.messages.recv() => {
                counts[1] += 1;
                c2.acker.send(msg.delivery_tag).await.unwrap();
            }
            Some(msg) = c3.messages.recv() => {
                counts[2] += 1;
                c3.acker.send(msg.delivery_tag).await.unwrap();
            }
        }
    }

    let sum: usize = counts.iter().sum();
    assert_eq!(sum, total);

    // Very loose fairness bounds: each should get at least 10% of the work
    for (i, c) in counts.iter().enumerate() {
        assert!(
            *c >= total / 10,
            "consumer {} got too little work: {} of {}",
            i,
            c,
            total
        );
    }
}

#[tokio::test]
async fn multi_topic_multi_group_isolation() {
    let broker = make_test_broker().await;

    // Topic A: 100 messages, group GA
    for i in 0..100 {
        broker
            .publish("A", format!("a-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Topic B: 60 messages, groups GB1 and GB2
    for i in 0..60 {
        broker
            .publish("B", format!("b-{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut a_ga = broker.subscribe("A", "GA").await.unwrap();

    let mut b_gb1 = broker.subscribe("B", "GB1").await.unwrap();
    let mut b_gb2 = broker.subscribe("B", "GB2").await.unwrap();

    // Collect A/GA
    let mut a_seen = Vec::new();
    for _ in 0..100 {
        let m = a_ga.messages.recv().await.unwrap();
        a_seen.push(String::from_utf8(m.message.payload.clone()).unwrap());
        a_ga.acker.send(m.delivery_tag).await.unwrap();
    }

    // Collect B/GB1 and B/GB2 (fanout: both see all messages)
    let mut b1_seen = Vec::new();
    let mut b2_seen = Vec::new();

    for _ in 0..60 {
        let m1 = b_gb1.messages.recv().await.unwrap();
        let m2 = b_gb2.messages.recv().await.unwrap();

        b1_seen.push(String::from_utf8(m1.message.payload.clone()).unwrap());
        b2_seen.push(String::from_utf8(m2.message.payload.clone()).unwrap());

        b_gb1.acker.send(m1.delivery_tag).await.unwrap();
        b_gb2.acker.send(m2.delivery_tag).await.unwrap();
    }

    // Assertions
    assert_eq!(
        a_seen,
        (0..100).map(|i| format!("a-{i}")).collect::<Vec<_>>()
    );
    assert_eq!(
        b1_seen,
        (0..60).map(|i| format!("b-{i}")).collect::<Vec<_>>()
    );
    assert_eq!(b1_seen, b2_seen); // both groups see same B stream

    // No topic bleed
    assert!(a_seen.iter().all(|s| s.starts_with("a-")));
    assert!(b1_seen.iter().all(|s| s.starts_with("b-")));
}

#[tokio::test]
async fn randomized_publish_consume_fuzz() {
    let store = make_test_store();
    let coord = NoopCoordination {};
    let mut cfg = BrokerConfig::default();
    cfg.publish_batch_size = 32;
    cfg.publish_batch_timeout_ms = 5;
    cfg.inflight_ttl_secs = 2;

    let broker = Arc::new(Broker::try_new(store, coord, cfg).await.unwrap());
    broker.start_redelivery_worker();

    let mut cons = broker.subscribe("fuzz_topic", "g").await.unwrap();

    let total = 10_000;

    // Publisher task
    let b_pub = broker.clone();
    let pub_task = tokio::spawn(async move {
        for i in 0..total {
            let action = fastrand::u8(..100);

            if action < 80 {
                // publish
                let payload = format!("m-{i}").into_bytes();
                b_pub.publish("fuzz_topic", &payload).await.unwrap();
            } else {
                // short pause to mix timing
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            }
        }
    });

    // Consumer/ACK behavior with some random drops
    let mut received = Vec::new();
    let mut acked = Vec::new();

    while received.len() < total {
        if let Some(msg) = cons.messages.recv().await {
            let offset = msg.message.offset;
            let payload = msg.message.payload.clone();
            received.push((offset, payload.clone()));

            let r = fastrand::u8(..100);
            if r < 70 {
                // ACK most of the time
                cons.acker.send(msg.delivery_tag).await.unwrap();
                acked.push(offset);
            } else {
                // Let some expire for redelivery
            }
        }
    }

    pub_task.await.unwrap();

    // Wait for redeliveries to settle
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Drain any last messages
    while let Ok(msg) = cons.messages.try_recv() {
        received.push((msg.message.offset, msg.message.payload.clone()));
        cons.acker.send(msg.delivery_tag).await.unwrap();
    }

    // Invariants:
    // - No gaps in offsets from 0..max_offset
    // - Every offset that ever existed appears at least once in `received`
    let mut offsets: Vec<u64> = received.iter().map(|(o, _)| *o).collect();
    offsets.sort();
    offsets.dedup();

    let max_off = *offsets.last().unwrap();
    assert_eq!(offsets, (0..=max_off).collect::<Vec<_>>());
}
