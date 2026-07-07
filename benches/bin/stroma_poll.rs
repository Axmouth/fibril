//! Stroma-only delivery-fetch micro-bench: no broker, no network, no client.
//!
//! Opens a StromaEngine directly, preloads one topic-partition, then drains it
//! through `poll_ready` (the same fetch+lease call the broker's delivery loop
//! makes, which runs through stroma's spawn_blocking read path). This isolates
//! the stroma delivery ceiling from broker fan-out and the network, filling the
//! rung between raw keratin scan and the networked broker delivery number.
//!
//! Env knobs: MESSAGES (default 2_000_000), PAYLOAD bytes (1024),
//! POLL_MAX per poll_ready call (8192), ROOT (defaults under TMPDIR).

use std::collections::HashMap;
use std::time::Instant;

use fibril_broker::queue_engine::{KeratinConfig, QueueEngine, StromaEngine, StromaKeratinConfig};
use fibril_broker::{BrokerCompletionPair, CompletionPair};
use fibril_util::unix_millis;
use stroma_core::{DeclareMeta, MessageHeaders, PublishItem};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let messages = env_usize("MESSAGES", 2_000_000);
    let payload_size = env_usize("PAYLOAD", 1024);
    let poll_max = env_usize("POLL_MAX", 8192);
    let root = std::env::var("ROOT").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join(format!("stroma-poll-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    });
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let engine = StromaEngine::open(
        &root,
        StromaKeratinConfig::from_message_log(KeratinConfig {
            batch_linger_ms: 5,
            ..Default::default()
        }),
        Default::default(),
    )
    .await
    .unwrap();

    engine
        .declare_queue("topic1", 0, None, DeclareMeta::default())
        .await
        .unwrap();
    engine.materialize("topic1", 0, None).await.unwrap();

    // Preload the partition, awaiting durability so the drain measures reads only.
    let now = unix_millis();
    let load_start = Instant::now();
    let chunk = 4096usize;
    let mut loaded = 0usize;
    while loaded < messages {
        let n = chunk.min(messages - loaded);
        let mut items = Vec::with_capacity(n);
        let mut rxs = Vec::with_capacity(n);
        for _ in 0..n {
            let (completion, rx) = BrokerCompletionPair::pair();
            items.push(PublishItem {
                headers: MessageHeaders {
                    published: now,
                    publish_received: now,
                    content_type: None,
                    extra: HashMap::new(),
                },
                payload: vec![0u8; payload_size],
                not_before: None,
                expire_at: None,
                completion,
            });
            rxs.push(rx);
        }
        engine
            .publish_batch("topic1", 0, None, items)
            .await
            .unwrap();
        for rx in rxs {
            let _ = rx.await;
        }
        loaded += n;
    }
    let load_s = load_start.elapsed().as_secs_f64();
    println!(
        "loaded {messages} in {load_s:.2}s = {:.0} msgs/s",
        messages as f64 / load_s
    );

    // Drain via poll_ready (fetch + lease) - the broker's delivery-fetch call,
    // through stroma's spawn_blocking read. u64::MAX upper disables the ceiling
    // (local-durable), a far-future lease keeps leased messages from re-readying.
    let lease_deadline = unix_millis() + 3_600_000;
    let drain_start = Instant::now();
    let mut fetched = 0usize;
    loop {
        let batch = engine
            .poll_ready("topic1", 0, None, poll_max, lease_deadline, u64::MAX)
            .await
            .unwrap();
        if batch.is_empty() {
            break;
        }
        fetched += batch.len();
    }
    let drain_s = drain_start.elapsed().as_secs_f64();
    println!(
        "poll_ready fetched {fetched} in {drain_s:.3}s = {:.0} msgs/s",
        fetched as f64 / drain_s
    );

    let _ = std::fs::remove_dir_all(&root);
}
