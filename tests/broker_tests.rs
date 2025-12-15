use std::sync::Arc;

use thetube::broker::coordination::*;
use thetube::broker::*;
use thetube::storage::*;

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

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
            cleanup_interval_secs: 5,
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

async fn make_test_broker_with_cfg(config: BrokerConfig) -> Broker<NoopCoordination> {
    let store = make_test_store();
    Broker::try_new(store, NoopCoordination, config)
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
    let mut consumer = broker.subscribe("t1", "g1", 1).await.unwrap();

    let mut received = vec![];
    for _ in 0..3 {
        let msg = consumer.messages.recv().await.unwrap();
        received.push(String::from_utf8(msg.message.payload.clone()).unwrap());
        consumer
            .acker
            .send(AckRequest {
                offset: msg.delivery_tag,
            })
            .await
            .unwrap();
    }

    assert_eq!(received, ["hello", "world", "!"]);
}

#[tokio::test]
async fn broker_ack_behavior() {
    let broker = make_test_broker().await;

    broker.publish("t", b"a").await.unwrap();
    broker.publish("t", b"b").await.unwrap();

    let mut consumer = broker.subscribe("t", "g", 1).await.unwrap();

    // Receive & ack first message
    let m1 = consumer.messages.recv().await.unwrap();
    assert_eq!(m1.message.payload, b"a");
    consumer
        .acker
        .send(AckRequest {
            offset: m1.delivery_tag,
        })
        .await
        .unwrap();

    // Receive & ack second
    let m2 = consumer.messages.recv().await.unwrap();
    assert_eq!(m2.message.payload, b"b");
    consumer
        .acker
        .send(AckRequest {
            offset: m2.delivery_tag,
        })
        .await
        .unwrap();

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
    let mut c1 = broker.subscribe("jobs", "workers", 2).await.unwrap();
    let mut c2 = broker.subscribe("jobs", "workers", 2).await.unwrap();

    let mut got1 = 0;
    let mut got2 = 0;

    // Collect 6 messages total (in any order)
    for _ in 0..6 {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                got1 += 1;
                c1.acker.send(AckRequest { offset: msg.delivery_tag }).await.unwrap();
            }
            Some(msg) = c2.messages.recv() => {
                got2 += 1;
                c2.acker.send(AckRequest { offset: msg.delivery_tag }).await.unwrap();
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

    let mut g1 = broker.subscribe("events", "g1", 1).await.unwrap();
    let mut g2 = broker.subscribe("events", "g2", 1).await.unwrap();

    let mut recv_g1 = vec![];
    let mut recv_g2 = vec![];

    for _ in 0..2 {
        let m1 = g1.messages.recv().await.unwrap();
        recv_g1.push(String::from_utf8(m1.message.payload.clone()).unwrap());
        g1.acker
            .send(AckRequest {
                offset: m1.delivery_tag,
            })
            .await
            .unwrap();

        let m2 = g2.messages.recv().await.unwrap();
        recv_g2.push(String::from_utf8(m2.message.payload.clone()).unwrap());
        g2.acker
            .send(AckRequest {
                offset: m2.delivery_tag,
            })
            .await
            .unwrap();
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

    let mut consumer = broker.subscribe("numbers", "g", 1).await.unwrap();

    let mut collected = vec![];

    for _ in 0..10 {
        let msg = consumer.messages.recv().await.unwrap();
        collected.push(String::from_utf8(msg.message.payload.clone()).unwrap());
        consumer
            .acker
            .send(AckRequest {
                offset: msg.delivery_tag,
            })
            .await
            .unwrap();
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

    let consumer = broker.subscribe("t", "g", 1).await.unwrap();

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

    // Publish 1 message
    broker.publish("topic", b"hello").await.unwrap();

    // Subscribe
    let mut cons = broker.subscribe("topic", "g", 1).await.unwrap();

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
    cons.acker
        .send(AckRequest {
            offset: m2.delivery_tag,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn ack_prevents_redelivery() {
    let broker = make_test_broker().await;

    broker.publish("topic", b"hi").await.unwrap();

    let mut cons = broker.subscribe("topic", "g", 1).await.unwrap();

    let msg = cons.messages.recv().await.unwrap();
    cons.acker
        .send(AckRequest {
            offset: msg.delivery_tag,
        })
        .await
        .unwrap();

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

    broker.publish("t", b"x").await.unwrap();

    let mut c1 = broker.subscribe("t", "g", 1).await.unwrap();
    let mut c2 = broker.subscribe("t", "g", 1).await.unwrap();

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

    // Publish messages 0..4
    for i in 0..5 {
        broker
            .publish("t", format!("m{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut cons = broker.subscribe("t", "g", 1).await.unwrap();

    // Receive messages
    let mut messages = vec![];
    for _ in 0..5 {
        let msg = cons.messages.recv().await.unwrap();
        messages.push(msg);
    }

    // ACK out-of-order: ACK offset 3 and 4 first
    cons.acker
        .send(AckRequest {
            offset: messages[3].delivery_tag,
        })
        .await
        .unwrap();
    cons.acker
        .send(AckRequest {
            offset: messages[4].delivery_tag,
        })
        .await
        .unwrap();

    // ACK 0 next
    cons.acker
        .send(AckRequest {
            offset: messages[0].delivery_tag,
        })
        .await
        .unwrap();

    // ACK 2 before 1
    cons.acker
        .send(AckRequest {
            offset: messages[2].delivery_tag,
        })
        .await
        .unwrap();
    cons.acker
        .send(AckRequest {
            offset: messages[1].delivery_tag,
        })
        .await
        .unwrap();

    // No redelivery should occur
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());
}

#[tokio::test]
async fn selective_ack_expiry_redelivery() {
    let broker = make_test_broker().await;

    for i in 0..5 {
        broker
            .publish("t", format!("v{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut cons = broker.subscribe("t", "g", 1).await.unwrap();

    // Receive all messages 0..4
    let mut msgs = vec![];
    for _ in 0..5 {
        msgs.push(cons.messages.recv().await.unwrap());
    }

    // ACK everything except offset 2
    for i in [0, 1, 3, 4] {
        cons.acker
            .send(AckRequest {
                offset: msgs[i].delivery_tag,
            })
            .await
            .unwrap();
    }

    // Let inflight(2) expire
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Should re-deliver only offset 2
    let redelivered = cons.messages.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, 2);

    // ACK it finally
    cons.acker
        .send(AckRequest {
            offset: redelivered.delivery_tag,
        })
        .await
        .unwrap();

    // Should not redeliver again
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let next = cons.messages.try_recv();
    assert!(next.is_err());
}

#[tokio::test]
async fn selective_ack_no_wrong_rewind() {
    let broker = make_test_broker().await;

    for i in 0..3 {
        broker
            .publish("t", format!("x{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut cons = broker.subscribe("t", "g", 1).await.unwrap();

    let m0 = cons.messages.recv().await.unwrap();
    let _m1 = cons.messages.recv().await.unwrap();
    let m2 = cons.messages.recv().await.unwrap();

    // ACK 2 then 0 (skipping 1)
    cons.acker
        .send(AckRequest {
            offset: m2.delivery_tag,
        })
        .await
        .unwrap();
    cons.acker
        .send(AckRequest {
            offset: m0.delivery_tag,
        })
        .await
        .unwrap();

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
    let mut c = broker.subscribe("topic", "g", 1).await.unwrap();
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
    assert_eq!(
        offsets,
        (0..offsets.len() as u64).collect::<Vec<_>>(),
        "phantom or missing offsets detected"
    );
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

    // Publish 12 messages
    for i in 0..12 {
        broker
            .publish("topic", format!("x{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut c = broker.subscribe("topic", "g", 1).await.unwrap();

    let mut seen = Vec::new();
    for _ in 0..12 {
        let msg = c.messages.recv().await.unwrap();
        seen.push((msg.message.offset, msg.message.payload.clone()));
        c.acker
            .send(AckRequest {
                offset: msg.delivery_tag,
            })
            .await
            .unwrap();
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
    let cfg = BrokerConfig {
        publish_batch_size: 64,
        publish_batch_timeout_ms: 1,
        ..Default::default()
    };

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
    let mut c = broker.subscribe("topic", "group", 1).await.unwrap();

    let mut seen = Vec::new();
    for _ in 0..total {
        let msg = c.messages.recv().await.unwrap();
        seen.push((msg.message.offset, msg.message.payload.clone()));
        c.acker
            .send(AckRequest {
                offset: msg.message.offset,
            })
            .await
            .unwrap();
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
    let cfg = BrokerConfig {
        publish_batch_size: 64,
        publish_batch_timeout_ms: 1,
        ..Default::default()
    };

    let broker = Arc::new(Broker::try_new(store, coord, cfg).await.unwrap());

    // Start consumer
    let mut consumer = broker.subscribe("topic", "g", 1).await.unwrap();

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
        consumer
            .acker
            .send(AckRequest {
                offset: msg.delivery_tag,
            })
            .await
            .unwrap();
        seen.push(msg);
    }

    // Check for duplicates
    let mut offsets: Vec<u64> = seen.iter().map(|m| m.message.offset).collect();
    offsets.sort();
    offsets.dedup();
    assert_eq!(offsets.len(), total, "Duplicates detected");
    assert_eq!(
        offsets,
        (0..offsets.len() as u64).collect::<Vec<_>>(),
        "phantom or missing offsets detected"
    );

    // Check consecutive offsets
    for (i, offset) in offsets.into_iter().enumerate().take(total) {
        assert_eq!(offset, i as u64, "Missing or reordered messages");
    }
}

#[tokio::test]
async fn redelivery_under_load_8k() {
    let total = 8_000;

    redelivery_under_load(total).await;
}

#[tokio::test]
async fn redelivery_under_load_2k() {
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

    // Publish burst
    for _ in 0..total {
        broker.publish("t", b"x").await.unwrap();
    }

    let mut c = broker.subscribe("t", "g", 1).await.unwrap();

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
        c.acker
            .send(AckRequest {
                offset: msg.message.offset,
            })
            .await
            .unwrap();
        received += 1;
    }

    assert!(
        counts.iter().all(|&c| c >= 1),
        "Some offsets never redelivered at least once"
    );
}

fn _dupes_with_counts<T>(v: &[T]) -> std::collections::BTreeMap<&T, usize>
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
    let cfg = BrokerConfig {
        publish_batch_size: 10,
        publish_batch_timeout_ms: 10,
        cleanup_interval_secs: 1,
        ..Default::default()
    };

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
        drop(broker);
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }

    // 2) New broker instance on the same path
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        let mut cons = broker.subscribe("restart_topic", "g", 1).await.unwrap();

        let mut msgs = Vec::new();
        for _ in 0..20 {
            let m = cons.messages.recv().await.unwrap();
            msgs.push(String::from_utf8(m.message.payload.clone()).unwrap());
            cons.acker
                .send(AckRequest {
                    offset: m.delivery_tag,
                })
                .await
                .unwrap();
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
    let cfg = BrokerConfig {
        inflight_ttl_secs: 3,
        cleanup_interval_secs: 1,
        publish_batch_size: 10,
        publish_batch_timeout_ms: 10,
        ..BrokerConfig::default()
    };

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

        let mut cons = broker.subscribe("restart_ack_topic", "g", 1).await.unwrap();

        // ACK first 7 messages
        for _ in 0..7 {
            let m = cons.messages.recv().await.unwrap();
            cons.acker
                .send(AckRequest {
                    offset: m.delivery_tag,
                })
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        // Drop broker (simulating crash/restart)
        drop(cons);
        broker.shutdown().await;
        drop(broker);
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

        let mut cons = broker.subscribe("restart_ack_topic", "g", 1).await.unwrap();

        let mut seen = Vec::new();
        // Expect only 3 messages left
        for _ in 0..3 {
            let m = cons.messages.recv().await.unwrap();
            seen.push(String::from_utf8(m.message.payload.clone()).unwrap());
            cons.acker
                .send(AckRequest {
                    offset: m.delivery_tag,
                })
                .await
                .unwrap();
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
    let cfg = BrokerConfig {
        publish_batch_size: 10,
        publish_batch_timeout_ms: 10,
        inflight_ttl_secs: 1,
        cleanup_interval_secs: 7,
        ..Default::default()
    };

    std::fs::create_dir_all("test_data").unwrap();
    let db_path = format!("test_data/restart_redel_{}", fastrand::u64(..));

    let offset: Offset;

    // 1) First broker: publish + first delivery (no ACK)
    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        broker.publish("rr_topic", b"hello").await.unwrap();

        let mut cons = broker.subscribe("rr_topic", "g", 1).await.unwrap();
        let m1 = cons.messages.recv().await.unwrap();
        assert_eq!(m1.message.payload, b"hello");
        offset = m1.message.offset;

        // Do not ACK
        drop(cons);
        broker.shutdown().await;
        drop(broker);
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

        let mut cons = broker.subscribe("rr_topic", "g", 1).await.unwrap();

        let redelivered = cons.messages.recv().await.unwrap();
        assert_eq!(redelivered.message.offset, offset);
        assert_eq!(redelivered.message.payload, b"hello");

        cons.acker
            .send(AckRequest {
                offset: redelivered.delivery_tag,
            })
            .await
            .unwrap();
        drop(cons);
        broker.shutdown().await;
        drop(broker);
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

    let mut c1 = broker.subscribe("jobs_fair", "workers", 1).await.unwrap();
    let mut c2 = broker.subscribe("jobs_fair", "workers", 1).await.unwrap();
    let mut c3 = broker.subscribe("jobs_fair", "workers", 1).await.unwrap();

    let mut counts = [0usize; 3];

    for _ in 0..total {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                counts[0] += 1;
                c1.acker.send(AckRequest { offset: msg.delivery_tag }).await.unwrap();
            }
            Some(msg) = c2.messages.recv() => {
                counts[1] += 1;
                c2.acker.send(AckRequest { offset: msg.delivery_tag }).await.unwrap();
            }
            Some(msg) = c3.messages.recv() => {
                counts[2] += 1;
                c3.acker.send(AckRequest { offset: msg.delivery_tag }).await.unwrap();
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

    let mut a_ga = broker.subscribe("A", "GA", 1).await.unwrap();

    let mut b_gb1 = broker.subscribe("B", "GB1", 1).await.unwrap();
    let mut b_gb2 = broker.subscribe("B", "GB2", 1).await.unwrap();

    // Collect A/GA
    let mut a_seen = Vec::new();
    for _ in 0..100 {
        let m = a_ga.messages.recv().await.unwrap();
        a_seen.push(String::from_utf8(m.message.payload.clone()).unwrap());
        a_ga.acker
            .send(AckRequest {
                offset: m.delivery_tag,
            })
            .await
            .unwrap();
    }

    // Collect B/GB1 and B/GB2 (fanout: both see all messages)
    let mut b1_seen = Vec::new();
    let mut b2_seen = Vec::new();

    for _ in 0..60 {
        let m1 = b_gb1.messages.recv().await.unwrap();
        let m2 = b_gb2.messages.recv().await.unwrap();

        b1_seen.push(String::from_utf8(m1.message.payload.clone()).unwrap());
        b2_seen.push(String::from_utf8(m2.message.payload.clone()).unwrap());

        b_gb1
            .acker
            .send(AckRequest {
                offset: m1.delivery_tag,
            })
            .await
            .unwrap();
        b_gb2
            .acker
            .send(AckRequest {
                offset: m2.delivery_tag,
            })
            .await
            .unwrap();
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
    let cfg = BrokerConfig {
        publish_batch_size: 32,
        publish_batch_timeout_ms: 5,
        inflight_ttl_secs: 2,
        ..Default::default()
    };

    let broker = Arc::new(Broker::try_new(store, coord, cfg).await.unwrap());

    let mut cons = broker.subscribe("fuzz_topic", "g", 1).await.unwrap();

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
                cons.acker
                    .send(AckRequest {
                        offset: msg.delivery_tag,
                    })
                    .await
                    .unwrap();
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
        cons.acker
            .send(AckRequest {
                offset: msg.delivery_tag,
            })
            .await
            .unwrap();
    }

    // Invariants:
    // - No gaps in offsets from 0..max_offset
    // - Every offset that ever existed appears at least once in `received`
    let mut offsets: Vec<u64> = received.iter().map(|(o, _)| *o).collect();
    offsets.sort();
    offsets.dedup();

    let max_off = *offsets.last().unwrap();
    assert_eq!(offsets, (0..=max_off).collect::<Vec<_>>());
    assert_eq!(
        offsets,
        (0..offsets.len() as u64).collect::<Vec<_>>(),
        "phantom or missing offsets detected"
    );
}

#[tokio::test]
async fn storage_inflight_implies_message_exists() {
    let store = make_test_store();

    let topic = "t".to_string();
    let group = "g".to_string();
    let partition = 0;

    let off = store.append(&topic, partition, b"x").await.unwrap();
    store
        .mark_inflight(&topic, partition, &group, off, unix_ts() + 100)
        .await
        .unwrap();

    // Run cleanup aggressively
    store.cleanup_topic(&topic, partition).await.unwrap();

    // Invariant: if inflight exists, message must exist OR inflight must be gone
    let inflight = store
        .is_inflight_or_acked(&topic, partition, &group, off)
        .await
        .unwrap();

    if inflight {
        let res = store.fetch_by_offset(&topic, partition, off).await;
        assert!(
            res.is_ok(),
            "inflight exists but message missing: {:?}",
            res
        );
    }
}

#[tokio::test]
async fn cursor_never_moves_backwards() {
    let broker = make_test_broker().await;

    for i in 0..10 {
        broker
            .publish("t", format!("m{i}").as_bytes())
            .await
            .unwrap();
    }

    let mut c = broker.subscribe("t", "g", 1).await.unwrap();

    let mut last = None;

    for _ in 0..10 {
        let m = c.messages.recv().await.unwrap();
        if let Some(prev) = last {
            assert!(m.message.offset > prev, "cursor went backwards");
        }
        last = Some(m.message.offset);
        c.acker
            .send(AckRequest {
                offset: m.delivery_tag,
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn crash_after_send_before_inflight_causes_redelivery() {
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        inflight_ttl_secs: 1,
        cleanup_interval_secs: 5,
        ..Default::default()
    };

    let db_path = format!("test_data/crash_inflight_{}", fastrand::u64(..));
    let offset;

    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord.clone(), cfg.clone())
            .await
            .unwrap();

        broker.publish("t", b"x").await.unwrap();
        let mut c = broker.subscribe("t", "g", 1).await.unwrap();

        let m = c.messages.recv().await.unwrap();
        offset = m.message.offset;

        // NO ACK, NO TIME for inflight batch flush
        drop(c);
        broker.shutdown().await;
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    {
        let store = make_rocksdb_store(&db_path).unwrap();
        let broker = Broker::try_new(store, coord, cfg).await.unwrap();
        let mut c = broker.subscribe("t", "g", 1).await.unwrap();

        let redelivered = c.messages.recv().await.unwrap();
        assert_eq!(redelivered.message.offset, offset);
    }
}

#[tokio::test]
async fn ack_before_delivery_is_ignored() {
    let broker = make_test_broker().await;

    broker.publish("t", b"x").await.unwrap();

    let mut c = broker.subscribe("t", "g", 1).await.unwrap();

    // ACK offset 0 before receiving it
    c.acker.send(AckRequest { offset: 0 }).await.unwrap();

    // Must still be delivered
    let m = c.messages.recv().await.unwrap();
    assert_eq!(m.message.offset, 0);

    c.acker
        .send(AckRequest {
            offset: m.delivery_tag,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn redelivery_does_not_advance_cursor() {
    let broker = make_test_broker().await;

    broker.publish("t", b"a").await.unwrap();
    broker.publish("t", b"b").await.unwrap();
    broker.publish("t", b"c").await.unwrap();
    broker.publish("t", b"d").await.unwrap();

    let mut c = broker.subscribe("t", "g", 1).await.unwrap();

    let m0 = c.messages.recv().await.unwrap(); // don't ack

    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    let redelivered = c.messages.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, m0.message.offset);

    // ACK now
    c.acker
        .send(AckRequest {
            offset: redelivered.delivery_tag,
        })
        .await
        .unwrap();

    // Next message must be offset 1
    let m1 = c.messages.recv().await.unwrap();
    assert_eq!(m1.message.offset, 1);
}

#[tokio::test]
async fn prefetch_limits_inflight() {
    let broker = make_test_broker().await;

    // publish 3 messages
    for b in [b"a", b"b", b"c", b"d", b"e", b"f", b"g"] {
        broker.publish("t", b).await.unwrap();
    }

    let mut c = broker.subscribe("t", "g", 1).await.unwrap(); // prefetch = 1

    // receive first
    let m0 = c.recv().await.unwrap();
    assert_eq!(m0.message.offset, 0);

    // should NOT receive second until ack or expiry
    let mut extra = 0;
    let start = tokio::time::Instant::now();

    loop {
        let res = tokio::time::timeout(std::time::Duration::from_millis(50), c.recv()).await;

        match res {
            Ok(Some(m)) => {
                extra += 1;
                println!("extra recv offset={}", m.message.offset);
            }
            _ => break,
        }

        if start.elapsed().as_millis() > 300 {
            break;
        }
    }

    assert_eq!(extra, 0, "received {extra} extra messages beyond prefetch");
}

#[tokio::test]
async fn prefetch_releases_on_ack() {
    let broker = make_test_broker().await;

    broker.publish("t", b"a").await.unwrap();
    broker.publish("t", b"b").await.unwrap();
    broker.publish("t", b"c").await.unwrap();
    broker.publish("t", b"d").await.unwrap();

    let mut c = broker.subscribe("t", "g", 1).await.unwrap();

    let m0 = c.recv().await.unwrap();
    assert_eq!(m0.message.offset, 0);

    c.ack(AckRequest {
        offset: m0.delivery_tag,
    })
    .await
    .unwrap();

    let m1 = c.recv().await.unwrap();
    assert_eq!(m1.message.offset, 1);
}

#[tokio::test]
async fn prefetch_releases_on_expiry() {
    let broker = make_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_secs: 1,
        ..Default::default()
    })
    .await;

    broker.publish("t", b"a").await.unwrap();
    broker.publish("t", b"b").await.unwrap();
    broker.publish("t", b"c").await.unwrap();
    broker.publish("t", b"d").await.unwrap();

    let mut c = broker.subscribe("t", "g", 1).await.unwrap();

    let m0 = c.recv().await.unwrap();
    assert_eq!(m0.message.offset, 0);

    // don't ack, wait for expiry
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // redelivery should come, not offset 1
    let redelivered = c.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, 0);

    // ack redelivery
    c.ack(AckRequest {
        offset: redelivered.delivery_tag,
    })
    .await
    .unwrap();

    // now offset 1
    let m1 = c.recv().await.unwrap();
    assert_eq!(m1.message.offset, 1);
}

#[tokio::test]
async fn offsets_are_never_reused_after_cleanup() {
    let broker = make_test_broker().await;

    let _o0 = broker.publish("t", b"a").await.unwrap();
    let o1 = broker.publish("t", b"b").await.unwrap();

    let mut c = broker.subscribe("t", "g", 10).await.unwrap();

    let m0 = c.recv().await.unwrap();
    c.ack(AckRequest {
        offset: m0.delivery_tag,
    })
    .await
    .unwrap();

    let m1 = c.recv().await.unwrap();
    c.ack(AckRequest {
        offset: m1.delivery_tag,
    })
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // force cleanup
    broker.forced_cleanup(&"t".into(), 0).await.unwrap();

    let o2 = broker.publish("t", b"c").await.unwrap();

    assert!(o2 > o1, "offset reused after cleanup");
}
