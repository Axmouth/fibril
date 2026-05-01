use std::{sync::Arc, time::Duration};

use fibril_broker::{
    broker::{Broker, BrokerConfig, ConsumerConfig, SettleRequest, SettleType},
    queue_engine::StromaEngine,
    test_util::TestState,
};
use hashbrown::HashSet;
use stroma_core::{KeratinConfig, SnapshotConfig, TempDir, test_dir};

async fn open_test_broker() -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = test_dir!("broker_test");

    let engine = StromaEngine::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker_cfg = BrokerConfig {
        inflight_ttl_ms: 2000,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000, // Make tests timeout if they rely on polling to pass, to indicate the issue
    };
    let broker = Broker::new(engine, broker_cfg, None);

    (broker, dir)
}

#[tokio::test]
async fn broker_delivers_messages_in_order() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();

    for _ in 0..5 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 10 })
        .await
        .unwrap();

    let mut offs = Vec::new();
    for _ in 0..5 {
        let msg = sub.recv().await.unwrap();
        offs.push(msg.message.offset);
        sub.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: msg.delivery_tag,
        })
        .await
        .unwrap();
    }

    assert_eq!(offs, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn broker_respects_prefetch() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..10 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        println!("Published message {}", i);
    }

    let mut sub = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 3 })
        .await
        .unwrap();

    let mut msgs = Vec::new();
    for _ in 0..3 {
        msgs.push(sub.recv().await.unwrap());
        println!(
            "Received message with offset {}",
            msgs.last().unwrap().message.offset
        );
    }

    // Should block / timeout / return None depending on API
    assert!(
        tokio::time::timeout(Duration::from_millis(50), sub.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ack_releases_prefetch_slot() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..5 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 2 })
        .await
        .unwrap();

    let m1 = sub.recv().await.unwrap();
    let m2 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m1.delivery_tag,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await; // allow for async processing

    assert!(!sub.is_empty());

    let m3 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m2.delivery_tag,
    })
    .await
    .unwrap();

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m3.delivery_tag,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await; // allow for async processing

    assert!(!sub.is_empty());

    let m4 = sub.recv().await.unwrap();
    let m5 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    assert_eq!(m1.message.offset, 0);
    assert_eq!(m1.message.payload, b"x0");
    assert_eq!(m2.message.offset, 1);
    assert_eq!(m2.message.payload, b"x1");
    assert_eq!(m3.message.offset, 2);
    assert_eq!(m3.message.payload, b"x2");
    assert_eq!(m4.message.offset, 3);
    assert_eq!(m4.message.payload, b"x3");
    assert_eq!(m5.message.offset, 4);
    assert_eq!(m5.message.payload, b"x4");
}

#[tokio::test]
async fn ack_releases_prefetch_slot2() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..5 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 2 })
        .await
        .unwrap();

    let m1 = sub.recv().await.unwrap();
    let m2 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m1.delivery_tag,
    })
    .await
    .unwrap();

    let m3 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m2.delivery_tag,
    })
    .await
    .unwrap();

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m3.delivery_tag,
    })
    .await
    .unwrap();

    let m4 = sub.recv().await.unwrap();
    let m5 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    dbg!(&m1, &m2, &m3, &m4, &m5);

    assert_eq!(m1.message.offset, 0);
    assert_eq!(m1.message.payload, b"x0");
    assert_eq!(m2.message.offset, 1);
    assert_eq!(m2.message.payload, b"x1");
    assert_eq!(m3.message.offset, 2);
    assert_eq!(m3.message.payload, b"x2");
    assert_eq!(m4.message.offset, 3);
    assert_eq!(m4.message.payload, b"x3");
    assert_eq!(m5.message.offset, 4);
    assert_eq!(m5.message.payload, b"x4");
}

#[tokio::test]
async fn broker_redelivers_after_expiry() {
    // fibril_util::init_tracing_dbg();
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    println!("Published message with offset 0");

    let mut sub = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 1 })
        .await
        .unwrap();
    println!("Receiving first message");

    let msg1 = sub.recv().await.unwrap();
    assert_eq!(msg1.message.offset, 0);

    // do NOT ack - let it expire
    tokio::time::sleep(Duration::from_millis(2200)).await;

    println!("Attempting to receive after expiry");

    let msg2 = sub.recv().await.unwrap();
    println!(
        "Received message with offset {} after expiry",
        msg2.message.offset
    );
    assert_eq!(msg2.message.offset, 0);
}

#[tokio::test]
async fn broker_distributes_across_consumers() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for _ in 0..10 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut c1 = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 1 })
        .await
        .unwrap();
    let mut c2 = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 1 })
        .await
        .unwrap();

    let m1 = c1.recv().await.unwrap();
    let m2 = c2.recv().await.unwrap();

    assert_ne!(m1.message.offset, m2.message.offset);
}

#[tokio::test]
async fn slow_consumer_does_not_starve_fast_one() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for _ in 0..5 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut slow = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 1 })
        .await
        .unwrap();
    let mut fast = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 5 })
        .await
        .unwrap();

    let _ = slow.recv().await.unwrap(); // never ack

    let mut got = Vec::new();
    for _ in 0..4 {
        let m = fast.recv().await.unwrap();
        got.push(m.message.offset);
        fast.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: m.delivery_tag,
        })
        .await
        .unwrap();
    }

    assert_eq!(got.len(), 4);
}

#[tokio::test]
async fn nack_requeue_redelivers() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m1 = t.recv(&c).await?;
    assert_eq!(m1.offset, 0);

    t.nack(&c, m1, true).await?;

    let m2 = t.recv(&c).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn nack_without_requeue_drops_message() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m = t.recv(&c).await?;
    t.nack(&c, m, false).await?;

    t.expect_no_message(&c, 50).await;

    Ok(())
}

#[tokio::test]
async fn restart_redelivers_unacked() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m1 = t.recv(&c).await?;
    assert_eq!(m1.offset, 0);

    // no ack
    t.restart_broker("b1").await?;

    // must resubscribe
    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m2 = t.recv(&c2).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn restart_does_not_redeliver_acked() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m = t.recv(&c).await?;
    t.ack(&c, m).await?;

    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    t.expect_no_message(&c2, 50).await;

    Ok(())
}

#[tokio::test]
async fn restart_preserves_prefetch_semantics() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish_many("b1", "t", None, b"x", 3).await?;

    let c = t.sub("b1", "t", None).prefetch(2).create().await?;

    let _m1 = t.recv(&c).await?;
    let _m2 = t.recv(&c).await?;

    // No ack → prefetch exhausted
    t.expect_no_message(&c, 50).await;

    Ok(())
}

#[tokio::test]
async fn double_ack_is_ignored() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m = t.recv(&c).await?;

    t.ack(&c, m.clone()).await?;
    // second ack — should not panic
    let _ = t.ack(&c, m).await;

    Ok(())
}

#[tokio::test]
async fn expired_after_consumer_drop() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 1,
        expiry_poll_min_ms: 10,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
    });

    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c1 = t.sub("b1", "t", None).prefetch(1).create().await?;

    let _m = t.recv(&c1).await?;

    // drop consumer
    t.remove_consumer(&c1.id);

    t.sleep_ms(1200).await;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m2 = t.recv(&c2).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn restart_during_ack_completion() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    // Fire ack but do not await anything else
    let _ = t.ack(&c, m).await;

    // Immediately restart
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    // Should NOT redeliver
    t.expect_no_message(&c2, 50).await;

    Ok(())
}

#[tokio::test]
async fn nack_requeue_then_restart() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    t.nack(&c, m, true).await?;
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m2 = t.recv(&c2).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn expiry_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 1000,
        expiry_poll_min_ms: 10,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
    });

    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let _ = t.recv(&c).await?;

    t.sleep_ms(1200).await;
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c2).await?;
    assert_eq!(m.offset, 0);

    Ok(())
}

#[tokio::test]
async fn uneven_prefetch_fairness() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish_many("b1", "t", None, b"x", 20).await?;

    let slow = t.sub("b1", "t", None).prefetch(1).create().await?;
    let fast = t.sub("b1", "t", None).prefetch(10).create().await?;

    let _ = t.recv(&slow).await?; // never ack

    let msgs = t.recv_n(&fast, 10).await?;
    t.ack_all(&fast, msgs).await?;

    // Fast should continue progressing
    let more = t.recv_n(&fast, 5).await?;
    assert!(!more.is_empty());

    Ok(())
}

#[tokio::test]
async fn chaos_small_run() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish_many("b1", "t", None, b"x", 50).await?;

    let mut c = t.sub("b1", "t", None).prefetch(5).create().await?;

    for i in 0..100 {
        dbg!("Iteration ", i);
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(1000), t.recv(&c)).await {
            if i % 3 == 0 {
                t.nack(&c, m, true).await?;
            } else {
                t.ack(&c, m).await?;
            }
        }

        if i % 10 == 0 {
            dbg!("Restarting broker");
            t.restart_broker("b1").await?;
            dbg!("Broker restarted");
            c = t.sub("b1", "t", None).prefetch(5).create().await?;
            dbg!("Resubscribed");
        }
    }

    Ok(())
}

#[tokio::test]
async fn chaos_deterministic_restart_ack_nack() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 4_000,
        expiry_poll_min_ms: 50,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
    });
    t.start_broker("b1").await?;

    let total = 50;
    t.publish_many("b1", "t", None, b"x", total).await?;

    let mut c = t.sub("b1", "t", None).prefetch(5).create().await?;

    let mut seen = std::collections::HashSet::new();
    let mut acked = std::collections::HashSet::new();

    // Phase 1: mixed processing + periodic restart
    for i in 0..250 {
        // bounded wait so we never hang
        if let Ok(Ok(m)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), t.recv(&c)).await
        {
            // detect duplicate delivery within this test run
            assert!(
                !acked.contains(&m.offset),
                "Offset {} redelivered after ack",
                m.offset
            );
            seen.insert(m.offset);

            // deterministic pattern
            let offset = m.offset;
            if i % 3 == 0 {
                t.nack(&c, m, true).await?;
                println!("Nacked offset {}", offset);
            } else {
                t.ack(&c, m).await?;
                acked.insert(offset);
                println!("Acked offset {}", offset);
            }
        }

        // restart every 10 iterations (deterministic)
        if i > 0 && i % 10 == 0 {
            println!("Restarting broker at iteration {}", i);
            t.restart_broker("b1").await?;
            c = t.sub("b1", "t", None).prefetch(5).create().await?;
        }
    }

    // Phase 2: drain remaining messages
    let mut drained = 0;
    while let Ok(Ok(m)) =
        tokio::time::timeout(std::time::Duration::from_millis(100), t.recv(&c)).await
    {
        if !seen.insert(m.offset) {
            panic!(
                "Duplicate delivery detected in drain for offset {}",
                m.offset
            );
        }
        acked.insert(m.offset);
        t.ack(&c, m).await?;
        drained += 1;
    }

    println!("Drained {} messages after chaos phase", drained);

    // Final verification

    // All offsets must have been eventually acked
    assert_eq!(acked.len(), total);

    // No phantom offsets
    for off in acked {
        assert!(off < total as u64, "Unexpected offset {}", off);
    }

    Ok(())
}

#[tokio::test]
async fn restart_race_with_ack() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 50,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
    });
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    t.ack(&c, m).await?;

    // Immediately restart
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    // What do we expect here?
    match tokio::time::timeout(Duration::from_millis(100), t.recv(&c2)).await {
        Ok(Ok(m2)) => {
            println!("Redelivered offset {}", m2.offset);
        }
        _ => {
            println!("Not redelivered");
        }
    }

    Ok(())
}

#[tokio::test]
async fn restart_race_with_ack2() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 500,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
    });
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    t.ack(&c, m).await?;

    // Immediately restart
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    // What do we expect here?
    match tokio::time::timeout(Duration::from_millis(100), t.recv(&c2)).await {
        Ok(Ok(m2)) => {
            panic!("Redelivered offset {}", m2.offset);
        }
        _ => {
            println!("Not redelivered");
        }
    }

    Ok(())
}

#[tokio::test]
async fn stress_single_consumer_100k() {
    stress_single_consumer(100_000).await;
}

// Only on `--release` builds
#[cfg(not(debug_assertions))]
#[tokio::test]
async fn stress_single_consumer_500k() {
    stress_single_consumer(500_000).await;
}

#[cfg(not(debug_assertions))]
#[tokio::test]
async fn stress_single_consumer_1m() {
    stress_single_consumer(1_000_000).await;
}

async fn stress_single_consumer(total: usize) {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, mut confirmer) = broker.get_publisher("t", &None).await.unwrap();

    let mut to_publish_list = (0..total)
        .map(|i| format!("b{i}").as_bytes().to_vec())
        .collect::<Vec<_>>();
    to_publish_list.sort_unstable();

    let confirme_task = tokio::spawn(async move {
        let mut i = 0;
        while i < total {
            confirmer.recv().await.unwrap();
            i += 1;
        }
    });

    for (i, to_publish) in to_publish_list.iter().enumerate() {
        pubh.publish(
            to_publish.to_vec(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        if i % 5_000 == 0 {
            println!("Published {}", i);
        }
    }

    println!("Published {} messages", total);

    confirme_task.await.unwrap();

    println!("Confirmed {} messages", total);

    let mut sub = broker
        .subscribe("t", None, ConsumerConfig { prefetch: 1000 })
        .await
        .unwrap();

    let mut seen = 0;

    let mut received = Vec::new();
    while seen < total {
        let m = sub.recv().await.unwrap();
        received.push(m.message.payload);

        sub.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: m.delivery_tag,
        })
        .await
        .unwrap();

        seen += 1;

        if seen % 10_000 == 0 {
            println!("Processed {}", seen);
        }
    }

    // TODO: verify no duplicates or missing messages via offsets as well?
    received.sort_unstable();

    // Same payloads sent and received
    assert_eq!(to_publish_list, received);

    let received_len = received.len();
    let received_set: HashSet<Vec<u8>> = HashSet::from_iter(received.into_iter());

    // No duplicates
    assert_eq!(received_len, received_set.len());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
    // No overdelivery
    assert!(sub.is_empty());

    assert_eq!(seen, total);
}
