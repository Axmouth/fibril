use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use clap::{Parser, ValueEnum};
use fibril_client::{ClientOptions, InflightMessage, NewMessage, PublishConfirmation};
use fibril_util::unix_millis;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior, timeout},
};

#[derive(Default)]
struct ReaderStats {
    received_total: u64,
    measured_received: u64,
    retries_seen: u64,
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

struct PendingConfirm {
    sent_at: Instant,
    measured: bool,
    confirmation: PublishConfirmation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum BenchMode {
    /// Publish and consume at the same time.
    Mixed,
    /// Publish only. Useful for isolating publish/confirm throughput.
    PublishOnly,
    /// Preload a queue, then measure how quickly consumers drain it.
    ConsumeDrain,
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
    /// Benchmark shape to run.
    #[arg(long, value_enum, default_value_t = BenchMode::Mixed)]
    mode: BenchMode,

    /// Number of writer clients
    #[arg(long, default_value_t = 10)]
    writers: usize,

    /// Number of reader clients
    #[arg(long, default_value_t = 10)]
    readers: usize,

    /// Target aggregate publish rate
    #[arg(long, default_value_t = 100_000)]
    rate_per_sec: u64,

    /// Warmup duration excluded from latency/results
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    /// Measurement duration
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// Reader drain timeout after writers finish
    #[arg(long, default_value_t = 10)]
    drain_timeout_secs: u64,

    /// Payload size in bytes
    #[arg(short, long, default_value_t = 1024)]
    size: usize,

    /// Subscription prefetch for reader clients
    #[arg(long, default_value_t = 1024 * 8 * 2)]
    prefetch: u32,

    /// Wait for server publish confirmations
    #[arg(long, default_value_t = false)]
    confirmed: bool,

    /// Maximum in-flight publish confirmations per writer when --confirmed is set
    #[arg(long, default_value_t = 1024)]
    confirm_window: usize,

    /// Broker TCP address to target.
    #[arg(long, default_value = "127.0.0.1:9876")]
    broker_addr: SocketAddr,

    /// Human-readable durability contract for the run.
    #[arg(long, default_value = "local")]
    durability_label: String,

    /// Queue topic used by writers and readers.
    #[arg(long, default_value = "topic1")]
    topic: String,

    /// Messages to preload before consume-drain mode starts measuring.
    #[arg(long, default_value_t = 100_000)]
    preload_messages: u64,

    /// Do not wait for confirmations while preloading consume-drain messages.
    #[arg(long, default_value_t = false)]
    preload_unconfirmed: bool,
}

#[tokio::main]
async fn main() {
    fibril_util::init_tracing();

    let args = Args::parse();
    let preload_confirmed = !args.preload_unconfirmed;
    match args.mode {
        BenchMode::Mixed => {
            assert!(args.writers > 0, "writers must be > 0");
            assert!(args.readers > 0, "readers must be > 0");
            assert!(args.rate_per_sec > 0, "rate_per_sec must be > 0");
        }
        BenchMode::PublishOnly => {
            assert!(args.writers > 0, "writers must be > 0");
            assert!(args.rate_per_sec > 0, "rate_per_sec must be > 0");
        }
        BenchMode::ConsumeDrain => {
            assert!(args.readers > 0, "readers must be > 0");
            assert!(args.preload_messages > 0, "preload_messages must be > 0");
        }
    }
    assert!(
        !args.confirmed || args.confirm_window > 0,
        "confirm_window must be > 0 when confirmed"
    );
    assert!(
        !preload_confirmed || args.confirm_window > 0,
        "confirm_window must be > 0 when preload_confirmed"
    );

    let address = args.broker_addr;
    let run_started = Instant::now();
    let measure_start = run_started + Duration::from_secs(args.warmup_secs);
    let measure_end = measure_start + Duration::from_secs(args.duration_secs);
    let writers_done = Arc::new(AtomicBool::new(false));
    let measured_sent_total = Arc::new(AtomicU64::new(0));
    let measured_received_total = Arc::new(AtomicU64::new(0));
    let drain_timeout_secs = args.drain_timeout_secs;

    println!(
        "Steady benchmark: mode={:?}, writers={}, readers={}, rate_per_sec={}, warmup_secs={}, duration_secs={}, size={}, prefetch={}, confirmed={}, confirm_window={}, broker_addr={}, durability={}, topic={}, preload_messages={}, preload_confirmed={}",
        args.mode,
        args.writers,
        args.readers,
        args.rate_per_sec,
        args.warmup_secs,
        args.duration_secs,
        args.size,
        args.prefetch,
        args.confirmed,
        args.confirm_window,
        args.broker_addr,
        args.durability_label,
        args.topic,
        args.preload_messages,
        preload_confirmed,
    );

    let mut writer_stats = WriterStats::default();
    let mut preload_elapsed = None;
    let mut drain_elapsed = None;

    if args.mode == BenchMode::ConsumeDrain {
        let preload_started = Instant::now();
        writer_stats = preload_messages(
            address,
            args.topic.clone(),
            args.writers.max(1),
            args.preload_messages,
            args.size.max(1),
            preload_confirmed,
            args.confirm_window,
            measured_sent_total.clone(),
        )
        .await;
        preload_elapsed = Some(preload_started.elapsed());
        writers_done.store(true, Ordering::Release);
    }

    let reader_handles = if args.mode == BenchMode::PublishOnly {
        Vec::new()
    } else {
        start_readers(
            address,
            args.topic.clone(),
            args.readers,
            args.prefetch,
            writers_done.clone(),
            measured_sent_total.clone(),
            measured_received_total.clone(),
            drain_timeout_secs,
        )
    };

    if args.mode == BenchMode::Mixed {
        // Give subscriptions a short moment to reach the server before publishing starts.
        tokio::time::sleep(Duration::from_millis(500)).await;
        writer_stats = run_rate_limited_writers(
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
        .await;
        writers_done.store(true, Ordering::Release);
    } else if args.mode == BenchMode::PublishOnly {
        writer_stats = run_rate_limited_writers(
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
        .await;
        writers_done.store(true, Ordering::Release);
    }

    let drain_started = (args.mode == BenchMode::ConsumeDrain).then(Instant::now);

    let mut reader_stats = ReaderStats::default();
    for handle in reader_handles {
        let mut stats = handle.await.unwrap();
        reader_stats.received_total += stats.received_total;
        reader_stats.measured_received += stats.measured_received;
        reader_stats.retries_seen += stats.retries_seen;
        reader_stats
            .publish_to_server_receive_ms
            .append(&mut stats.publish_to_server_receive_ms);
        reader_stats
            .publish_to_delivery_ms
            .append(&mut stats.publish_to_delivery_ms);
        reader_stats
            .server_receive_to_delivery_ms
            .append(&mut stats.server_receive_to_delivery_ms);
    }
    if let Some(started) = drain_started {
        drain_elapsed = Some(started.elapsed());
    }

    let actual_rate = if let Some(elapsed) = preload_elapsed {
        writer_stats.measured_sent as f64 / elapsed.as_secs_f64()
    } else {
        writer_stats.measured_sent as f64 / args.duration_secs as f64
    };
    let receive_rate = if let Some(elapsed) = drain_elapsed {
        reader_stats.measured_received as f64 / elapsed.as_secs_f64()
    } else {
        reader_stats.measured_received as f64 / args.duration_secs as f64
    };
    println!("Sent total: {}", writer_stats.sent_total);
    let preload_confirmed_used = args.mode == BenchMode::ConsumeDrain && preload_confirmed;
    if args.confirmed || preload_confirmed_used {
        println!("Confirmed total: {}", writer_stats.confirmed_total);
    }
    println!("Publish errors: {}", writer_stats.publish_errors);
    if args.confirmed || preload_confirmed_used {
        println!("Confirm errors: {}", writer_stats.confirm_errors);
    }
    println!("Received total: {}", reader_stats.received_total);
    println!("Measured sent: {}", writer_stats.measured_sent);
    println!("Measured received: {}", reader_stats.measured_received);
    let measured_missing = if args.mode == BenchMode::PublishOnly {
        0
    } else {
        writer_stats
            .measured_sent
            .saturating_sub(reader_stats.measured_received)
    };
    println!("Measured missing: {measured_missing}");
    if args.mode == BenchMode::PublishOnly {
        println!("Measured unconsumed: {}", writer_stats.measured_sent);
    }
    println!("Actual measured publish rate: {actual_rate}");
    println!("Measured receive rate: {receive_rate}");
    if let Some(elapsed) = preload_elapsed {
        println!("Preload seconds: {:.3}", elapsed.as_secs_f64());
    }
    if let Some(elapsed) = drain_elapsed {
        println!("Consume drain seconds: {:.3}", elapsed.as_secs_f64());
    }
    println!("Retries seen: {}", reader_stats.retries_seen);
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
    print_latency(
        "Latency publish confirmation ms",
        &mut writer_stats.confirm_latency_ms,
    );
}

fn start_readers(
    address: SocketAddr,
    topic: String,
    readers: usize,
    prefetch: u32,
    writers_done: Arc<AtomicBool>,
    measured_sent_total: Arc<AtomicU64>,
    measured_received_total: Arc<AtomicU64>,
    drain_timeout_secs: u64,
) -> Vec<tokio::task::JoinHandle<ReaderStats>> {
    let mut reader_handles = Vec::new();
    for reader_id in 0..readers {
        let writers_done = writers_done.clone();
        let measured_sent_total = measured_sent_total.clone();
        let measured_received_total = measured_received_total.clone();
        let topic = topic.clone();
        reader_handles.push(tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();
            let mut sub = client
                .subscribe(&topic)
                .unwrap()
                .prefetch(prefetch)
                .sub()
                .await
                .unwrap();
            let (tx_acker, mut rx_acker) = mpsc::unbounded_channel::<InflightMessage>();
            let acker = tokio::spawn(async move {
                while let Some(msg) = rx_acker.recv().await {
                    msg.complete().await.unwrap();
                }
            });

            let mut stats = ReaderStats::default();
            let mut drain_deadline: Option<Instant> = None;

            loop {
                if writers_done.load(Ordering::Acquire) {
                    let measured_sent = measured_sent_total.load(Ordering::Acquire);
                    let measured_received = measured_received_total.load(Ordering::Acquire);
                    if measured_received >= measured_sent {
                        break;
                    }
                    drain_deadline.get_or_insert_with(|| {
                        Instant::now() + Duration::from_secs(drain_timeout_secs)
                    });
                }

                if drain_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    println!(
                        "Reader {reader_id}: drain timeout, measured_received={}, measured_sent={}",
                        measured_received_total.load(Ordering::Acquire),
                        measured_sent_total.load(Ordering::Acquire)
                    );
                    break;
                }

                let received = match timeout(Duration::from_millis(100), sub.recv()).await {
                    Ok(msg) => msg,
                    Err(_) => continue,
                };

                let Some(msg) = received else {
                    break;
                };

                stats.received_total += 1;
                let now = unix_millis();
                if is_measured_payload(&msg.payload) {
                    stats.measured_received += 1;
                    measured_received_total.fetch_add(1, Ordering::AcqRel);
                    stats
                        .publish_to_server_receive_ms
                        .push(msg.publish_received.saturating_sub(msg.published));
                    stats
                        .publish_to_delivery_ms
                        .push(now.saturating_sub(msg.published));
                    stats
                        .server_receive_to_delivery_ms
                        .push(now.saturating_sub(msg.publish_received));
                    if let Some(retries) = msg.headers.get("fibril.retries") {
                        stats.retries_seen += retries.parse::<u64>().unwrap_or(1);
                    }
                }
                tx_acker.send(msg).unwrap();
            }

            drop(tx_acker);
            acker.await.unwrap();
            stats
        }));
    }
    reader_handles
}

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
    let mut writer_handles = Vec::new();
    for writer_id in 0..writers {
        let measured_sent_total = measured_sent_total.clone();
        let writer_rate = rate_for_writer(rate_per_sec, writers, writer_id);
        let topic = topic.clone();
        if writer_rate == 0 {
            continue;
        }
        writer_handles.push(tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();
            let publisher = client.publisher(&topic).unwrap();
            let mut stats = WriterStats::default();

            let period = Duration::from_secs_f64(1.0 / writer_rate as f64);
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

            // Confirms are recorded WHEN THEY RESOLVE (event-driven). The window
            // gates the INPUT (publish only while in-flight < window) rather than
            // withholding confirm reads, so confirm latency reflects the real
            // round trip, not how long an already-arrived ack sat in a FIFO drain.
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
                    // Reap each confirm the instant it resolves.
                    Some((measured, sent_at, result)) = inflight.next(), if !inflight.is_empty() => {
                        record_confirm(&mut stats, measured, sent_at, result);
                    }
                    // Publish, paced by the tick, gated by the in-flight window.
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

            // Drain remaining in-flight confirms (still recorded at resolution).
            while let Some((measured, sent_at, result)) = inflight.next().await {
                record_confirm(&mut stats, measured, sent_at, result);
            }
            stats
        }));
    }
    collect_writer_stats(writer_handles).await
}

async fn preload_messages(
    address: SocketAddr,
    topic: String,
    writers: usize,
    total_messages: u64,
    payload_size: usize,
    confirmed: bool,
    confirm_window: usize,
    measured_sent_total: Arc<AtomicU64>,
) -> WriterStats {
    let mut writer_handles = Vec::new();
    for writer_id in 0..writers {
        let measured_sent_total = measured_sent_total.clone();
        let topic = topic.clone();
        let count = count_for_writer(total_messages, writers, writer_id);
        if count == 0 {
            continue;
        }
        writer_handles.push(tokio::spawn(async move {
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();
            let publisher = client.publisher(&topic).unwrap();
            let mut stats = WriterStats::default();
            let mut pending_confirms = VecDeque::<PendingConfirm>::new();

            for _ in 0..count {
                let mut payload = vec![8u8; payload_size];
                payload[0] = 1;
                publish_one(
                    writer_id,
                    &publisher,
                    payload,
                    true,
                    confirmed,
                    confirm_window,
                    &mut pending_confirms,
                    &mut stats,
                )
                .await;
                stats.measured_sent += 1;
                measured_sent_total.fetch_add(1, Ordering::AcqRel);
            }

            drain_confirms(writer_id, &mut pending_confirms, &mut stats).await;
            stats
        }));
    }
    collect_writer_stats(writer_handles).await
}

async fn publish_one(
    writer_id: usize,
    publisher: &fibril_client::Publisher,
    payload: Vec<u8>,
    measured: bool,
    confirmed: bool,
    confirm_window: usize,
    pending_confirms: &mut VecDeque<PendingConfirm>,
    stats: &mut WriterStats,
) {
    if confirmed {
        while pending_confirms.len() >= confirm_window {
            let pending = pending_confirms.pop_front().unwrap();
            match pending.confirmation.confirmed().await {
                Ok(_) => {
                    stats.confirmed_total += 1;
                    if pending.measured {
                        stats
                            .confirm_latency_ms
                            .push(pending.sent_at.elapsed().as_millis() as u64);
                    }
                }
                Err(err) => {
                    eprintln!("Writer {writer_id}: publish confirm failed: {err}");
                    stats.confirm_errors += 1;
                }
            }
        }
        let sent_at = Instant::now();
        let Ok(confirmation) = publisher
            .publish_with_confirmation(NewMessage::raw(payload))
            .await
        else {
            eprintln!("Writer {writer_id}: publish failed");
            stats.publish_errors += 1;
            return;
        };
        pending_confirms.push_back(PendingConfirm {
            sent_at,
            measured,
            confirmation,
        });
    } else if let Err(err) = publisher.publish(NewMessage::raw(payload)).await {
        eprintln!("Writer {writer_id}: publish failed: {err}");
        stats.publish_errors += 1;
        return;
    }

    stats.sent_total += 1;
}

async fn drain_confirms(
    writer_id: usize,
    pending_confirms: &mut VecDeque<PendingConfirm>,
    stats: &mut WriterStats,
) {
    while let Some(pending) = pending_confirms.pop_front() {
        match pending.confirmation.confirmed().await {
            Ok(_) => {
                stats.confirmed_total += 1;
                if pending.measured {
                    stats
                        .confirm_latency_ms
                        .push(pending.sent_at.elapsed().as_millis() as u64);
                }
            }
            Err(err) => {
                eprintln!("Writer {writer_id}: publish confirm failed: {err}");
                stats.confirm_errors += 1;
            }
        }
    }
}

async fn collect_writer_stats(
    writer_handles: Vec<tokio::task::JoinHandle<WriterStats>>,
) -> WriterStats {
    let mut writer_stats = WriterStats::default();
    for handle in writer_handles {
        let mut stats = handle.await.unwrap();
        writer_stats.sent_total += stats.sent_total;
        writer_stats.measured_sent += stats.measured_sent;
        writer_stats.confirmed_total += stats.confirmed_total;
        writer_stats.publish_errors += stats.publish_errors;
        writer_stats.confirm_errors += stats.confirm_errors;
        writer_stats
            .confirm_latency_ms
            .append(&mut stats.confirm_latency_ms);
    }
    writer_stats
}

fn rate_for_writer(rate_per_sec: u64, writers: usize, writer_id: usize) -> u64 {
    let base = rate_per_sec / writers as u64;
    let remainder = rate_per_sec % writers as u64;
    base + u64::from((writer_id as u64) < remainder)
}

fn count_for_writer(total: u64, writers: usize, writer_id: usize) -> u64 {
    let base = total / writers as u64;
    let remainder = total % writers as u64;
    base + u64::from((writer_id as u64) < remainder)
}

fn is_measured_payload(payload: &[u8]) -> bool {
    payload.first().copied() == Some(1)
}
