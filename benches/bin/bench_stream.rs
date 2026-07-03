//! Plexus stream steady-state benchmark over full TCP.
//!
//! The stream counterpart of `steady_c`: it connects real clients to a running
//! broker (`--broker-addr`) and runs rate-limited publishers plus fan-out
//! subscribers, reporting publish/deliver throughput and the publish->deliver
//! latency distribution. Run it per durability tier (`--durability`) to compare
//! the express lane (speculative, ephemeral) against the durable default.
//!
//! Streams fan out: every reader sees every record, so the delivered total is
//! `readers * published`. Start a broker first (this does not spawn one).

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use clap::{Parser, ValueEnum};
use fibril_client::{ClientOptions, NewMessage, StreamConfig};
use fibril_util::unix_millis;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::{Instant, MissedTickBehavior, timeout};

#[derive(Default)]
struct ReaderStats {
    received_total: u64,
    measured_received: u64,
    publish_to_server_receive_ms: Vec<u64>,
    publish_to_delivery_ms: Vec<u64>,
    server_receive_to_delivery_ms: Vec<u64>,
}

#[derive(Default)]
struct WriterStats {
    sent_total: u64,
    measured_sent: u64,
    confirmed_total: u64,
    publish_errors: u64,
    confirm_errors: u64,
    confirm_latency_ms: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum BenchMode {
    /// Publish and fan-out consume at the same time.
    Mixed,
    /// Publish only (isolate publish/confirm throughput).
    PublishOnly,
    /// Consume only, expecting a concurrent publish-only process on the same
    /// topic. Lets readers run in separate OS processes so a fan-out measurement
    /// is not limited by one client process. Readers run for the warmup plus
    /// duration window (plus drain) and count the measured records the publisher
    /// marks, so rates and latencies line up with the publisher's window.
    SubscribeOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Durability {
    Durable,
    Speculative,
    Ephemeral,
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[idx]
}

fn print_latency(label: &str, values: &mut [u64]) {
    if values.is_empty() {
        println!("{label}: no samples");
        return;
    }
    values.sort_unstable();
    println!(
        "{label}: p50={}, p95={}, p99={}, max={}",
        percentile(values, 0.50),
        percentile(values, 0.95),
        percentile(values, 0.99),
        values.last().copied().unwrap_or_default(),
    );
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long, value_enum, default_value_t = BenchMode::Mixed)]
    mode: BenchMode,

    /// Durability tier the stream is declared with.
    #[arg(long, value_enum, default_value_t = Durability::Durable)]
    durability: Durability,

    #[arg(long, default_value_t = 4)]
    writers: usize,

    /// Independent fan-out readers (each sees every record).
    #[arg(long, default_value_t = 4)]
    readers: usize,

    /// Give each reader a unique durable cursor name so auto-ack commits its
    /// cursor per record. This exercises the cursor-commit path (and its
    /// microbatcher) - the durable-subscriber bottleneck, distinct from the
    /// cursorless ephemeral fan-out the default measures.
    #[arg(long, default_value_t = false)]
    durable_readers: bool,

    #[arg(long, default_value_t = 100_000)]
    rate_per_sec: u64,

    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    #[arg(long, default_value_t = 10)]
    drain_timeout_secs: u64,

    #[arg(short, long, default_value_t = 1024)]
    size: usize,

    #[arg(long, default_value_t = 1024 * 8 * 2)]
    prefetch: u32,

    /// Wait for server publish confirmations (measures confirm latency per tier).
    #[arg(long, default_value_t = false)]
    confirmed: bool,

    /// Maximum in-flight publish confirmations per writer when --confirmed is set.
    #[arg(long, default_value_t = 1024)]
    confirm_window: usize,

    #[arg(long, default_value = "127.0.0.1:9876")]
    broker_addr: SocketAddr,

    #[arg(long, default_value = "stream1")]
    topic: String,

    /// Partition count for the stream.
    #[arg(long, default_value_t = 1)]
    partitions: u32,
}

fn rate_for_writer(rate_per_sec: u64, writers: usize, writer_id: usize) -> u64 {
    let base = rate_per_sec / writers as u64;
    let remainder = rate_per_sec % writers as u64;
    base + u64::from((writer_id as u64) < remainder)
}

fn is_measured_payload(payload: &[u8]) -> bool {
    payload.first().copied() == Some(1)
}

#[tokio::main]
async fn main() {
    fibril_util::init_tracing();
    let args = Args::parse();
    if args.mode != BenchMode::SubscribeOnly {
        assert!(args.writers > 0, "writers must be > 0");
    }
    if args.mode != BenchMode::PublishOnly {
        assert!(args.readers > 0, "readers must be > 0");
    }

    let address = args.broker_addr;
    let measure_start = Instant::now() + Duration::from_secs(args.warmup_secs);
    let measure_end = measure_start + Duration::from_secs(args.duration_secs);
    let writers_done = Arc::new(AtomicBool::new(false));
    let measured_sent_total = Arc::new(AtomicU64::new(0));

    println!(
        "Stream steady benchmark: mode={:?}, durability={:?}, writers={}, readers={}, rate_per_sec={}, warmup_secs={}, duration_secs={}, size={}, prefetch={}, confirmed={}, partitions={}, topic={}, broker_addr={}",
        args.mode,
        args.durability,
        args.writers,
        args.readers,
        args.rate_per_sec,
        args.warmup_secs,
        args.duration_secs,
        args.size,
        args.prefetch,
        args.confirmed,
        args.partitions,
        args.topic,
        args.broker_addr,
    );

    // Declare the stream with the requested durability tier.
    {
        let admin = ClientOptions::new()
            .auth("fibril", "fibril")
            .connect(address)
            .await
            .unwrap();
        let mut config = StreamConfig::new(&args.topic)
            .unwrap()
            .partitions(args.partitions);
        config = match args.durability {
            Durability::Durable => config.durable(),
            Durability::Speculative => config.speculative(),
            Durability::Ephemeral => config.ephemeral(),
        };
        admin.declare_plexus(config).await.unwrap();
        admin.shutdown().await;
    }

    let reader_handles = if args.mode == BenchMode::PublishOnly {
        Vec::new()
    } else {
        start_readers(
            address,
            args.topic.clone(),
            args.partitions,
            args.readers,
            args.prefetch,
            args.durable_readers,
            writers_done.clone(),
            measured_sent_total.clone(),
            args.drain_timeout_secs,
        )
    };

    // Give subscriptions a moment to attach before publishing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let writer_stats = if args.mode == BenchMode::SubscribeOnly {
        // No local writers: hold the readers open across the concurrent
        // publisher's window (they never see a local "all sent" count), then
        // let the drain timeout end them.
        measured_sent_total.store(u64::MAX, Ordering::Release);
        let hold = Duration::from_secs(args.warmup_secs + args.duration_secs + 2);
        tokio::time::sleep(hold).await;
        WriterStats::default()
    } else {
        run_rate_limited_writers(
            address,
            args.topic.clone(),
            args.writers,
            args.rate_per_sec,
            args.size.max(1),
            args.confirmed,
            args.confirm_window,
            measure_start,
            measure_end,
            measured_sent_total.clone(),
        )
        .await
    };
    writers_done.store(true, Ordering::Release);

    let mut reader_stats = ReaderStats::default();
    for handle in reader_handles {
        let stats = handle.await.unwrap();
        reader_stats.received_total += stats.received_total;
        reader_stats.measured_received += stats.measured_received;
        reader_stats
            .publish_to_server_receive_ms
            .extend(stats.publish_to_server_receive_ms);
        reader_stats
            .publish_to_delivery_ms
            .extend(stats.publish_to_delivery_ms);
        reader_stats
            .server_receive_to_delivery_ms
            .extend(stats.server_receive_to_delivery_ms);
    }

    let secs = args.duration_secs.max(1) as f64;
    let publish_rate = writer_stats.measured_sent as f64 / secs;
    let deliver_rate = reader_stats.measured_received as f64 / secs;
    let per_reader_rate = if args.readers > 0 {
        deliver_rate / args.readers as f64
    } else {
        0.0
    };

    println!("--- results ---");
    println!("Sent total: {}", writer_stats.sent_total);
    if args.confirmed {
        println!("Confirmed total: {}", writer_stats.confirmed_total);
        println!("Confirm errors: {}", writer_stats.confirm_errors);
    }
    println!("Publish errors: {}", writer_stats.publish_errors);
    println!("Measured sent: {}", writer_stats.measured_sent);
    println!(
        "Measured delivered (all readers): {}",
        reader_stats.measured_received
    );
    println!("Measured publish rate: {publish_rate:.0} msg/s");
    println!("Measured deliver rate (all readers): {deliver_rate:.0} msg/s");
    println!("Measured deliver rate (per reader): {per_reader_rate:.0} msg/s");
    print_latency(
        "Latency publish->server-receive ms",
        &mut reader_stats.publish_to_server_receive_ms,
    );
    print_latency(
        "Latency publish->deliver ms",
        &mut reader_stats.publish_to_delivery_ms,
    );
    print_latency(
        "Latency server-receive->deliver ms",
        &mut reader_stats.server_receive_to_delivery_ms,
    );
    if args.confirmed {
        print_latency(
            "Latency publish confirmation ms",
            &mut writer_stats.confirm_latency_ms.clone(),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn start_readers(
    address: SocketAddr,
    topic: String,
    partitions: u32,
    readers: usize,
    prefetch: u32,
    durable_readers: bool,
    writers_done: Arc<AtomicBool>,
    measured_sent_total: Arc<AtomicU64>,
    drain_timeout_secs: u64,
) -> Vec<tokio::task::JoinHandle<ReaderStats>> {
    let mut handles = Vec::new();
    for reader_index in 0..readers {
        let writers_done = writers_done.clone();
        let measured_sent_total = measured_sent_total.clone();
        let topic = topic.clone();
        handles.push(tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();
            // Fan-out reader: auto-ack, all partitions. With --durable-readers each
            // reader carries a unique durable name so auto-ack commits its cursor
            // per record (exercising the cursor-commit microbatcher); otherwise it
            // reads cursorless from the live tail (pure fan-out).
            let builder = client
                .stream(&topic)
                .unwrap()
                .partitions(partitions)
                .prefetch(prefetch);
            let builder = if durable_readers {
                builder.durable(format!("bench-reader-{reader_index}"))
            } else {
                builder.from_latest()
            };
            let mut sub = builder.sub_auto_ack().await.unwrap();

            let mut stats = ReaderStats::default();
            let mut drain_deadline: Option<Instant> = None;

            loop {
                if writers_done.load(Ordering::Acquire) {
                    // Each reader expects every measured record (fan-out).
                    if stats.measured_received >= measured_sent_total.load(Ordering::Acquire) {
                        break;
                    }
                    drain_deadline.get_or_insert_with(|| {
                        Instant::now() + Duration::from_secs(drain_timeout_secs)
                    });
                }
                if drain_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    break;
                }

                let received = match timeout(Duration::from_millis(100), sub.recv()).await {
                    Ok(msg) => msg,
                    Err(_) => continue,
                };
                let Some(msg) = received else { break };

                stats.received_total += 1;
                if is_measured_payload(&msg.payload) {
                    stats.measured_received += 1;
                    let now = unix_millis();
                    let published = u64::from(msg.published);
                    let publish_received = u64::from(msg.publish_received);
                    stats
                        .publish_to_server_receive_ms
                        .push(publish_received.saturating_sub(published));
                    stats
                        .publish_to_delivery_ms
                        .push(now.saturating_sub(published));
                    stats
                        .server_receive_to_delivery_ms
                        .push(now.saturating_sub(publish_received));
                }
            }
            stats
        }));
    }
    handles
}

#[allow(clippy::too_many_arguments)]
async fn run_rate_limited_writers(
    address: SocketAddr,
    topic: String,
    writers: usize,
    rate_per_sec: u64,
    payload_size: usize,
    confirmed: bool,
    confirm_window: usize,
    measure_start: Instant,
    measure_end: Instant,
    measured_sent_total: Arc<AtomicU64>,
) -> WriterStats {
    let mut handles = Vec::new();
    for writer_id in 0..writers {
        let measured_sent_total = measured_sent_total.clone();
        let writer_rate = rate_for_writer(rate_per_sec, writers, writer_id);
        let topic = topic.clone();
        if writer_rate == 0 {
            continue;
        }
        handles.push(tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();
            // Warm the topology cache so the publisher learns the stream's partition
            // count and spreads across partitions on its own (no explicit override).
            let _ = client.fetch_topology().await;
            let publisher = client.publisher(&topic).unwrap();
            let mut stats = WriterStats::default();

            let period = Duration::from_secs_f64(1.0 / writer_rate as f64);
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut inflight = FuturesUnordered::new();

            let record_confirm =
                |stats: &mut WriterStats, measured: bool, sent_at: Instant, result: Result<(), ()>| {
                    match result {
                        Ok(()) => {
                            stats.confirmed_total += 1;
                            if measured {
                                stats.confirm_latency_ms.push(sent_at.elapsed().as_millis() as u64);
                            }
                        }
                        Err(()) => stats.confirm_errors += 1,
                    }
                };

            loop {
                tokio::select! {
                    biased;
                    Some((measured, sent_at, result)) = inflight.next(), if !inflight.is_empty() => {
                        record_confirm(&mut stats, measured, sent_at, result);
                    }
                    _ = tick.tick(), if Instant::now() < measure_end
                        && (!confirmed || inflight.len() < confirm_window) => {
                        let now = Instant::now();
                        if now >= measure_end {
                            break;
                        }
                        let measured = now >= measure_start;
                        let mut payload = vec![8u8; payload_size];
                        if measured {
                            payload[0] = 1;
                        }
                        if confirmed {
                            let sent_at = Instant::now();
                            match publisher
                                .publish_with_confirmation(NewMessage::raw(payload))
                                .await
                            {
                                Ok(confirmation) => {
                                    stats.sent_total += 1;
                                    if measured {
                                        stats.measured_sent += 1;
                                        measured_sent_total.fetch_add(1, Ordering::AcqRel);
                                    }
                                    inflight.push(async move {
                                        let result =
                                            confirmation.confirmed().await.map(|_| ()).map_err(|_| ());
                                        (measured, sent_at, result)
                                    });
                                }
                                Err(_) => stats.publish_errors += 1,
                            }
                        } else {
                            match publisher.publish(NewMessage::raw(payload)).await {
                                Ok(_) => {
                                    stats.sent_total += 1;
                                    if measured {
                                        stats.measured_sent += 1;
                                        measured_sent_total.fetch_add(1, Ordering::AcqRel);
                                    }
                                }
                                Err(_) => stats.publish_errors += 1,
                            }
                        }
                    }
                    else => break,
                }
            }

            while let Some((measured, sent_at, result)) = inflight.next().await {
                record_confirm(&mut stats, measured, sent_at, result);
            }
            stats
        }));
    }

    let mut total = WriterStats::default();
    for handle in handles {
        let stats = handle.await.unwrap();
        total.sent_total += stats.sent_total;
        total.measured_sent += stats.measured_sent;
        total.confirmed_total += stats.confirmed_total;
        total.publish_errors += stats.publish_errors;
        total.confirm_errors += stats.confirm_errors;
        total.confirm_latency_ms.extend(stats.confirm_latency_ms);
    }
    total
}
