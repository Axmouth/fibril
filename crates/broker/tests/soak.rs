//! Chaos and soak suite (task #115).
//!
//! The complement to the deterministic simulation tests: these run a real broker
//! over real wall-clock time with real fsync, exercising the things a simulator
//! abstracts away - crash recovery from disk, sustained concurrent throughput,
//! and resource behavior over a long run. Deterministic simulation finds rare
//! interleavings and reproduces them exactly; this suite proves the real engine
//! holds up under load and restarts.
//!
//! Defaults are small so the suite runs in CI. Scale it into a real soak with
//! environment variables (read once at start):
//!
//!   FIBRIL_SOAK_CYCLES   crash/recovery cycles (default 4)
//!   FIBRIL_SOAK_BATCH    messages published per cycle (default 64)
//!   FIBRIL_SOAK_SECS     concurrent-load duration in seconds (default 2)
//!   FIBRIL_SOAK_PRODUCERS concurrent producers (default 4)

use std::sync::Arc;
use std::time::{Duration, Instant};

use fibril_broker::broker::{Broker, BrokerConfig, ConsumerConfig, SettleRequest, SettleType};
use fibril_broker::queue_engine::StromaEngine;
use fibril_storage::Partition;
use fibril_util::unix_millis;
use stroma_core::{KeratinConfig, SnapshotConfig, StromaKeratinConfig, TempDir, test_dir};
use uuid::Uuid;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn soak_broker_config() -> BrokerConfig {
    BrokerConfig {
        inflight_ttl_ms: 5_000,
        expiry_poll_min_ms: 50,
        expiry_batch_max: 256,
        delivery_poll_max_ms: 50,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    }
}

/// Open a broker over the message log rooted at `dir`. Reopening the same `dir`
/// recovers committed state from the WAL and snapshot.
async fn open_broker_at(dir: &std::path::Path) -> Arc<Broker<StromaEngine>> {
    let engine = StromaEngine::open(
        dir,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .expect("engine opens/recovers");
    Broker::new(engine, soak_broker_config(), None)
}

async fn publish_durable(broker: &Arc<Broker<StromaEngine>>, topic: &str, payload: Vec<u8>) -> u64 {
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .expect("publisher");
    publisher
        .publish(payload, unix_millis(), unix_millis(), None, Default::default(), None)
        .await
        .expect("publish accepted")
        .await
        .expect("confirm channel")
        .expect("durable confirm")
}

/// Crash-recovery soak: across many restart cycles, every durably confirmed
/// message survives the restart and is delivered exactly once, with strictly
/// increasing offsets and no loss or duplication. A restart between cycles
/// reopens the engine from disk - the real fsync/recovery path a simulator with
/// in-memory time does not exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_crash_recovery_soak() {
    let cycles = env_usize("FIBRIL_SOAK_CYCLES", 4);
    let batch = env_usize("FIBRIL_SOAK_BATCH", 64);
    let topic = "soak.recovery";
    let dir: TempDir = test_dir!("soak-recovery");

    let mut produced: u64 = 0;
    let mut consumed: u64 = 0;
    let mut last_offset: Option<u64> = None;

    for cycle in 0..cycles {
        let broker = open_broker_at(&dir.root).await;

        // Publish a fresh durable batch, tagging each payload with its global
        // sequence so delivery can be checked against what was produced.
        for _ in 0..batch {
            let payload = produced.to_le_bytes().to_vec();
            let offset = publish_durable(&broker, topic, payload).await;
            if let Some(prev) = last_offset {
                assert!(
                    offset > prev,
                    "offsets must strictly increase across the soak: {offset} after {prev}"
                );
            }
            last_offset = Some(offset);
            produced += 1;
        }

        // Restart mid-stream on every other cycle without consuming, proving the
        // unconsumed durable backlog survives a restart before it is drained.
        let broker = if cycle % 2 == 0 {
            broker.shutdown().await;
            open_broker_at(&dir.root).await
        } else {
            broker
        };

        // Drain everything published so far that has not yet been consumed, ack
        // each, and verify the payload sequence numbers are exactly the expected
        // contiguous range (no loss, no duplication).
        let mut sub = broker
            .subscribe(topic, Partition::new(0), None, Uuid::now_v7(), ConsumerConfig { prefetch: 64 })
            .await
            .expect("subscribe");
        let expect_this_cycle = produced - consumed;
        for _ in 0..expect_this_cycle {
            let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
                .await
                .expect("delivery within deadline")
                .expect("queue still open");
            let seq = u64::from_le_bytes(msg.message.payload[..8].try_into().unwrap());
            assert_eq!(seq, consumed, "messages delivered in produced order with no gaps");
            sub.settle(SettleRequest {
                settle_type: SettleType::Ack,
                delivery_tag: msg.delivery_tag,
            })
            .await
            .expect("ack");
            consumed += 1;
        }
        drop(sub);

        // The acks are durable: restart and confirm none of the consumed
        // messages are redelivered (settled state survived).
        broker.shutdown().await;
        let broker = open_broker_at(&dir.root).await;
        let mut sub = broker
            .subscribe(topic, Partition::new(0), None, Uuid::now_v7(), ConsumerConfig { prefetch: 8 })
            .await
            .expect("subscribe after restart");
        let redelivered = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(
            redelivered.is_err(),
            "settled messages must not be redelivered after a restart"
        );
        drop(sub);
        broker.shutdown().await;
    }

    assert_eq!(produced, consumed, "every produced message was consumed once");
    assert_eq!(produced as usize, cycles * batch, "soak produced the expected total");
    drop(dir);
}

/// Sustained concurrent-load soak: several producers and one consumer hammer a
/// single durable queue for a wall-clock duration. Every durably confirmed
/// message is delivered exactly once - no loss, no duplication - and the broker
/// stays healthy throughout. This exercises real throughput, contention, and
/// resource behavior the simulator does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_load_no_loss_soak() {
    let secs = env_usize("FIBRIL_SOAK_SECS", 2) as u64;
    let producers = env_usize("FIBRIL_SOAK_PRODUCERS", 4);
    let topic = "soak.load";
    let dir: TempDir = test_dir!("soak-load");
    let broker = open_broker_at(&dir.root).await;

    let deadline = Instant::now() + Duration::from_secs(secs);

    // Producers publish durably until the deadline, counting confirmed publishes.
    let mut tasks = Vec::new();
    for _ in 0..producers {
        let broker = broker.clone();
        tasks.push(tokio::spawn(async move {
            let (publisher, _confirms) = broker
                .get_publisher(topic, Partition::new(0), &None)
                .await
                .expect("publisher");
            let mut count: u64 = 0;
            while Instant::now() < deadline {
                let confirmed = publisher
                    .publish(
                        b"x".to_vec(),
                        unix_millis(),
                        unix_millis(),
                        None,
                        Default::default(),
                        None,
                    )
                    .await
                    .expect("publish accepted")
                    .await
                    .expect("confirm channel")
                    .is_ok();
                if confirmed {
                    count += 1;
                }
            }
            count
        }));
    }

    let mut produced: u64 = 0;
    for task in tasks {
        produced += task.await.expect("producer task");
    }
    assert!(produced > 0, "the soak produced at least some messages");

    // Drain and ack everything; each confirmed publish is delivered exactly once.
    let mut sub = broker
        .subscribe(topic, Partition::new(0), None, Uuid::now_v7(), ConsumerConfig { prefetch: 128 })
        .await
        .expect("subscribe");
    let mut consumed: u64 = 0;
    let mut seen_offsets = std::collections::HashSet::new();
    while consumed < produced {
        let msg = tokio::time::timeout(Duration::from_secs(20), sub.recv())
            .await
            .expect("drain within deadline")
            .expect("queue still open");
        assert!(
            seen_offsets.insert(msg.message.offset),
            "offset {} delivered more than once",
            msg.message.offset
        );
        sub.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: msg.delivery_tag,
        })
        .await
        .expect("ack");
        consumed += 1;
    }
    assert_eq!(produced, consumed, "every confirmed publish was consumed exactly once");

    drop(sub);
    broker.shutdown().await;
    drop(dir);
}
