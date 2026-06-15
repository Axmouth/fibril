use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use clap::Parser;
use fibril_client::{ClientOptions, InflightMessage, NewMessage, PublishConfirmation};
use fibril_util::unix_millis;
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior, timeout},
};

#[derive(Default)]
struct ReaderStats {
    received_total: u64,
    measured_received: u64,
    retries_seen: u64,
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
}

#[tokio::main]
async fn main() {
    fibril_util::init_tracing();

    let args = Args::parse();
    assert!(args.writers > 0, "writers must be > 0");
    assert!(args.readers > 0, "readers must be > 0");
    assert!(args.rate_per_sec > 0, "rate_per_sec must be > 0");
    assert!(
        !args.confirmed || args.confirm_window > 0,
        "confirm_window must be > 0 when confirmed"
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
        "Steady benchmark: writers={}, readers={}, rate_per_sec={}, warmup_secs={}, duration_secs={}, size={}, prefetch={}, confirmed={}, confirm_window={}, broker_addr={}, durability={}, topic={}",
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
    );

    let mut reader_handles = Vec::new();
    for reader_id in 0..args.readers {
        let writers_done = writers_done.clone();
        let measured_sent_total = measured_sent_total.clone();
        let measured_received_total = measured_received_total.clone();
        let prefetch = args.prefetch;
        let topic = args.topic.clone();
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
                .sub_manual_ack()
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

    // Give subscriptions a short moment to reach the server before publishing starts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut writer_handles = Vec::new();
    for writer_id in 0..args.writers {
        let measured_sent_total = measured_sent_total.clone();
        let payload_size = args.size.max(1);
        let writer_rate = rate_for_writer(args.rate_per_sec, args.writers, writer_id);
        let confirmed = args.confirmed;
        let confirm_window = args.confirm_window;
        let topic = args.topic.clone();
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
            let mut pending_confirms = VecDeque::<PublishConfirmation>::new();

            let period = Duration::from_secs_f64(1.0 / writer_rate as f64);
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tick.tick().await;
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
                    while pending_confirms.len() >= confirm_window {
                        match pending_confirms.pop_front().unwrap().confirmed().await {
                            Ok(_) => stats.confirmed_total += 1,
                            Err(err) => {
                                eprintln!("Writer {writer_id}: publish confirm failed: {err}");
                                stats.confirm_errors += 1;
                            }
                        }
                    }
                    let Ok(confirmation) = publisher
                        .publish_with_confirmation(NewMessage::raw(payload.clone()))
                        .await
                    else {
                        eprintln!("Writer {writer_id}: publish failed");
                        stats.publish_errors += 1;
                        break;
                    };
                    pending_confirms.push_back(confirmation);
                } else {
                    if let Err(err) = publisher.publish(NewMessage::raw(payload.clone())).await {
                        eprintln!("Writer {writer_id}: publish failed: {err}");
                        stats.publish_errors += 1;
                        break;
                    }
                }

                stats.sent_total += 1;
                if measured {
                    stats.measured_sent += 1;
                    measured_sent_total.fetch_add(1, Ordering::AcqRel);
                }
            }

            while let Some(confirmation) = pending_confirms.pop_front() {
                match confirmation.confirmed().await {
                    Ok(_) => stats.confirmed_total += 1,
                    Err(err) => {
                        eprintln!("Writer {writer_id}: publish confirm failed: {err}");
                        stats.confirm_errors += 1;
                    }
                }
            }

            stats
        }));
    }

    let mut writer_stats = WriterStats::default();
    for handle in writer_handles {
        let stats = handle.await.unwrap();
        writer_stats.sent_total += stats.sent_total;
        writer_stats.measured_sent += stats.measured_sent;
        writer_stats.confirmed_total += stats.confirmed_total;
        writer_stats.publish_errors += stats.publish_errors;
        writer_stats.confirm_errors += stats.confirm_errors;
    }
    writers_done.store(true, Ordering::Release);

    let mut reader_stats = ReaderStats::default();
    for handle in reader_handles {
        let mut stats = handle.await.unwrap();
        reader_stats.received_total += stats.received_total;
        reader_stats.measured_received += stats.measured_received;
        reader_stats.retries_seen += stats.retries_seen;
        reader_stats
            .publish_to_delivery_ms
            .append(&mut stats.publish_to_delivery_ms);
        reader_stats
            .server_receive_to_delivery_ms
            .append(&mut stats.server_receive_to_delivery_ms);
    }

    let actual_rate = writer_stats.measured_sent as f64 / args.duration_secs as f64;
    let receive_rate = reader_stats.measured_received as f64 / args.duration_secs as f64;
    println!("Sent total: {}", writer_stats.sent_total);
    if args.confirmed {
        println!("Confirmed total: {}", writer_stats.confirmed_total);
    }
    println!("Publish errors: {}", writer_stats.publish_errors);
    if args.confirmed {
        println!("Confirm errors: {}", writer_stats.confirm_errors);
    }
    println!("Received total: {}", reader_stats.received_total);
    println!("Measured sent: {}", writer_stats.measured_sent);
    println!("Measured received: {}", reader_stats.measured_received);
    println!(
        "Measured missing: {}",
        writer_stats
            .measured_sent
            .saturating_sub(reader_stats.measured_received)
    );
    println!("Actual measured publish rate: {actual_rate}");
    println!("Measured receive rate: {receive_rate}");
    println!("Retries seen: {}", reader_stats.retries_seen);
    print_latency(
        "Latency publish->deliver ms",
        &mut reader_stats.publish_to_delivery_ms,
    );
    print_latency(
        "Latency server-receive->deliver ms",
        &mut reader_stats.server_receive_to_delivery_ms,
    );
}

fn rate_for_writer(rate_per_sec: u64, writers: usize, writer_id: usize) -> u64 {
    let base = rate_per_sec / writers as u64;
    let remainder = rate_per_sec % writers as u64;
    base + u64::from((writer_id as u64) < remainder)
}

fn is_measured_payload(payload: &[u8]) -> bool {
    payload.first().copied() == Some(1)
}
