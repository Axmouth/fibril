//! Identity-based failover correctness check (Jepsen-lite).
//!
//! Counts conflate completeness with duplication, so this verifies by message
//! IDENTITY instead. A single producer publishes `count` messages, each carrying
//! a marker + monotonic sequence id, and records the set it CONFIRMED (durable).
//! A concurrent consumer records the received multiset. An external orchestrator
//! kills the owner mid-run; the client retry (producer) and subscription
//! supervisor (consumer) must ride through. At the end it decomposes:
//!
//!   loss      = confirmed ids absent from received   -> MUST be empty
//!   phantom   = received ids never attempted         -> MUST be empty
//!   duplicates= received ids seen more than once      -> at-least-once cost
//!   unconfirmed_delivered = received not in confirmed -> saved-but-unacked (fine)
//!
//! The guarantee is: every CONFIRMED id appears in received at least once, and
//! no phantom ids appear. Duplicates are expected (at-least-once) and quantified.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use clap::Parser;
use fibril_client::{ClientOptions, NewMessage};
use fibril_util::unix_millis;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::{Instant, MissedTickBehavior};

/// Identifies this run's messages in a topic that may also hold others.
const MARKER: u32 = 0xF1B2_0001;
const HEADER_LEN: usize = 12; // marker(4) + seq(8)

#[derive(Parser)]
struct Args {
    #[arg(long)]
    broker_addr: String,
    #[arg(long, default_value = "orders")]
    topic: String,
    #[arg(long, default_value_t = 200_000)]
    count: u64,
    #[arg(long, default_value_t = 10_000)]
    rate_per_sec: u64,
    #[arg(long, default_value_t = 2048)]
    confirm_window: usize,
    #[arg(long, default_value_t = 16384)]
    prefetch: u32,
    /// Stop draining once this many seconds pass with no new message after the
    /// producer has finished (and coverage not yet reached).
    #[arg(long, default_value_t = 20)]
    drain_idle_secs: u64,
}

fn encode(seq: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size.max(HEADER_LEN)];
    payload[0..4].copy_from_slice(&MARKER.to_le_bytes());
    payload[4..12].copy_from_slice(&seq.to_le_bytes());
    payload
}

fn decode(payload: &[u8]) -> Option<u64> {
    if payload.len() < HEADER_LEN {
        return None;
    }
    let marker = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    if marker != MARKER {
        return None; // not ours (e.g. a warm-up message) - ignore
    }
    Some(u64::from_le_bytes(payload[4..12].try_into().ok()?))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = args.broker_addr.clone();

    // Shared received multiset + last-activity clock, filled by the consumer.
    let received: Arc<Mutex<HashMap<u64, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let last_rx_ms = Arc::new(AtomicU64::new(unix_millis()));
    let stop = Arc::new(AtomicBool::new(false));

    // ---- consumer ----
    let consumer = {
        let topic = args.topic.clone();
        let addr = addr.clone();
        let received = received.clone();
        let last_rx_ms = last_rx_ms.clone();
        let stop = stop.clone();
        let prefetch = args.prefetch;
        tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(addr)
                .await
                .expect("consumer connect");
            let mut sub = client
                .subscribe(&topic)
                .expect("subscribe")
                .prefetch(prefetch)
                .sub_manual_ack()
                .await
                .expect("sub_manual_ack");
            while !stop.load(Ordering::Acquire) {
                match tokio::time::timeout(Duration::from_millis(500), sub.recv()).await {
                    Ok(Some(msg)) => {
                        if let Some(seq) = decode(&msg.payload) {
                            *received.lock().unwrap().entry(seq).or_insert(0) += 1;
                            last_rx_ms.store(unix_millis(), Ordering::Release);
                        }
                        let _ = msg.complete().await;
                    }
                    Ok(None) => break, // stream fully closed
                    Err(_) => {}       // idle tick; loop and re-check stop
                }
            }
        })
    };

    // ---- producer ----
    let attempted: HashSet<u64> = (0..args.count).collect();
    let confirmed: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let send_failures = Arc::new(AtomicU64::new(0));
    {
        let confirmed = confirmed.clone();
        let send_failures = send_failures.clone();
        let topic = args.topic.clone();
        let addr = addr.clone();
        let count = args.count;
        let rate = args.rate_per_sec.max(1);
        let window = args.confirm_window.max(1);
        let producer = tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(addr)
                .await
                .expect("producer connect");
            let publisher = client.publisher(&topic).expect("publisher");
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
            tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
            let mut inflight = FuturesUnordered::new();
            for seq in 0..count {
                while inflight.len() >= window {
                    if let Some((s, ok)) = inflight.next().await {
                        if ok {
                            confirmed.lock().unwrap().insert(s);
                        }
                    }
                }
                tick.tick().await;
                match publisher
                    .publish_with_confirmation(NewMessage::raw(encode(seq, 256)))
                    .await
                {
                    Ok(conf) => inflight.push(async move {
                        let ok = conf.confirmed().await.is_ok();
                        (seq, ok)
                    }),
                    Err(_) => {
                        send_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            while let Some((s, ok)) = inflight.next().await {
                if ok {
                    confirmed.lock().unwrap().insert(s);
                }
            }
        });
        producer.await?;
    }

    // ---- drain: wait until every confirmed id is received, or the stream goes
    // idle past the budget (so we do not hang on a genuinely lost id) ----
    let drain_idle = Duration::from_secs(args.drain_idle_secs);
    let confirmed_snapshot: HashSet<u64> = confirmed.lock().unwrap().clone();
    loop {
        let covered = {
            let recv = received.lock().unwrap();
            confirmed_snapshot.iter().all(|id| recv.contains_key(id))
        };
        if covered {
            break;
        }
        let idle = Duration::from_millis(
            unix_millis().saturating_sub(last_rx_ms.load(Ordering::Acquire)),
        );
        if idle >= drain_idle {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::Release);
    let _ = tokio::time::timeout(Duration::from_secs(2), consumer).await;

    // ---- decompose ----
    let received = received.lock().unwrap();
    let received_keys: HashSet<u64> = received.keys().copied().collect();

    let loss: Vec<u64> = confirmed_snapshot
        .difference(&received_keys)
        .copied()
        .collect();
    let phantom: Vec<u64> = received_keys.difference(&attempted).copied().collect();
    let duplicates: u64 = received.values().filter(|&&c| c > 1).map(|&c| (c - 1) as u64).sum();
    let unconfirmed_delivered = received_keys.difference(&confirmed_snapshot).count();

    println!("=== identity failover verification ===");
    println!("attempted:            {}", attempted.len());
    println!("send_failures:        {}", send_failures.load(Ordering::Relaxed));
    println!("confirmed (durable):  {}", confirmed_snapshot.len());
    println!("received unique ids:  {}", received_keys.len());
    println!("duplicate deliveries: {duplicates}");
    println!("unconfirmed_delivered:{unconfirmed_delivered}  (saved-but-unacked, expected ok)");
    println!("LOSS (confirmed not received): {}", loss.len());
    println!("PHANTOM (received never sent): {}", phantom.len());
    if !loss.is_empty() {
        let sample: Vec<u64> = loss.iter().take(10).copied().collect();
        println!("  loss sample: {sample:?}");
    }
    if !phantom.is_empty() {
        let sample: Vec<u64> = phantom.iter().take(10).copied().collect();
        println!("  phantom sample: {sample:?}");
    }
    let verdict = loss.is_empty() && phantom.is_empty();
    println!(
        "VERDICT: {}",
        if verdict {
            "PASS - every confirmed id delivered, no phantoms"
        } else {
            "FAIL - durability/correctness violated"
        }
    );
    if verdict { Ok(()) } else { std::process::exit(1) }
}
