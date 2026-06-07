use std::{
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use clap::Parser;
use fibril_client::{ClientOptions, InflightMessage, NewMessage};
use fibril_util::unix_millis;
use tokio::{
    sync::oneshot,
    time::{Instant, timeout},
};

#[derive(Default)]
struct ClientStats {
    sent: usize,
    received: usize,
    retries_seen: u64,
    publish_to_delivery_ms: Vec<u64>,
    server_receive_to_delivery_ms: Vec<u64>,
    first_receive_ms: Option<u128>,
    last_receive_ms: Option<u128>,
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[idx]
}

async fn run_load_test(
    num_clients: usize,
    msgs_per_client: usize,
    start_reader: bool,
    start_writer: bool,
    txb: oneshot::Sender<()>,
    payload_size: usize,
    prefetch: u32,
    ready_dir: Option<PathBuf>,
    idle_timeout_ms: u64,
    confirmed: bool,
) {
    let start = Instant::now();
    let expected_total = num_clients * msgs_per_client;
    let shared_received = Arc::new(AtomicUsize::new(0));
    let readers_done = Arc::new(AtomicBool::new(false));
    let readers_done_notify = Arc::new(tokio::sync::Notify::new());

    let mut handles: Vec<tokio::task::JoinHandle<ClientStats>> = vec![];

    for j in 0..num_clients {
        let ready_dir = ready_dir.clone();
        let shared_received = shared_received.clone();
        let readers_done = readers_done.clone();
        let readers_done_notify = readers_done_notify.clone();
        handles.push(tokio::spawn(async move {
            let payload = vec![8u8; payload_size];
            let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([127, 0, 0, 1]), 9876));
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();

            let (tx_acker, mut rx_acker) =
                tokio::sync::mpsc::unbounded_channel::<InflightMessage>();
            let client_reader = client.clone();
            let reader = tokio::spawn(async move {
                let mut stats = ClientStats::default();
                if !start_reader {
                    return stats;
                }
                let mut sub = client_reader
                    .subscribe("topic1")
                    .unwrap()
                    .prefetch(prefetch)
                    .sub_manual_ack()
                    .await
                    .unwrap();
                if let Some(ready_dir) = ready_dir {
                    fs::write(ready_dir.join(format!("{j}.ready")), b"ready").unwrap();
                }
                let start_inner = Instant::now();
                let mut i: usize = 0;

                loop {
                    if readers_done.load(Ordering::Acquire) {
                        break;
                    }

                    let received = tokio::select! {
                        _ = readers_done_notify.notified() => {
                            if readers_done.load(Ordering::Acquire) {
                                break;
                            }
                            continue;
                        }
                        received = timeout(Duration::from_millis(idle_timeout_ms), sub.recv()) => {
                            match received {
                                Ok(msg) => msg,
                                Err(_) => {
                                    println!(
                                        "Client {j}: idle timeout after {idle_timeout_ms} ms, received {i}"
                                    );
                                    break;
                                }
                            }
                        }
                    };
                    let Some(msg) = received else {
                        break;
                    };
                    // just drain
                    i += 1;
                    let receive_elapsed = start_inner.elapsed().as_millis();
                    stats.first_receive_ms.get_or_insert(receive_elapsed);
                    stats.last_receive_ms = Some(receive_elapsed);
                    let now = unix_millis();
                    stats
                        .publish_to_delivery_ms
                        .push(now.saturating_sub(msg.published));
                    stats
                        .server_receive_to_delivery_ms
                        .push(now.saturating_sub(msg.publish_received));
                    if let Some(retries) = msg.headers.get("fibril.retries") {
                        stats.retries_seen += retries.parse::<u64>().unwrap_or(1);
                    }
                    if i.is_multiple_of(5000) {
                        let elapsed = start_inner.elapsed();
                        println!(
                            "Client {j}: received {i}, after {:.5} secs",
                            elapsed.as_secs_f64()
                        );
                    }
                    tx_acker.send(msg).unwrap();
                    let total_received = shared_received.fetch_add(1, Ordering::AcqRel) + 1;
                    if total_received >= expected_total {
                        readers_done.store(true, Ordering::Release);
                        readers_done_notify.notify_waiters();
                        break;
                    }
                }
                stats.received = i;
                stats
            });

            let client_pub = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();

            let pubber = tokio::spawn(async move {
                if !start_writer {
                    return;
                }
                let start_inner = Instant::now();
                let publisher = client_pub.publisher("topic1").unwrap();

                for i in 1..=msgs_per_client {
                    if confirmed {
                        publisher
                            .publish(NewMessage::raw(payload.clone()))
                            .await
                            .unwrap();
                    } else {
                        publisher
                            .publish_unconfirmed(NewMessage::raw(payload.clone()))
                            .await
                            .unwrap();
                    }

                    if i.is_multiple_of(5000) {
                        let elapsed = start_inner.elapsed();
                        println!(
                            "Client {j}: sent {i}, after {:.5} secs",
                            elapsed.as_secs_f64()
                        );
                    }
                }
            });

            let acker = tokio::spawn(async move {
                while let Some(msg) = rx_acker.recv().await {
                    msg.complete().await.unwrap();
                }
            });

            let (reader, pubber, acker) = tokio::join!(reader, pubber, acker);
            pubber.unwrap();
            acker.unwrap();

            let mut stats = reader.unwrap();
            if start_writer {
                stats.sent = msgs_per_client;
            }
            stats
        }));
    }

    let mut total_sent = 0usize;
    let mut total_received = 0usize;
    let mut total_retries_seen = 0u64;
    let mut publish_to_delivery_ms = Vec::new();
    let mut server_receive_to_delivery_ms = Vec::new();
    let mut first_receive_ms: Option<u128> = None;
    let mut last_receive_ms: Option<u128> = None;

    for h in handles {
        let mut stats = h.await.unwrap();
        total_sent += stats.sent;
        total_received += stats.received;
        total_retries_seen += stats.retries_seen;
        publish_to_delivery_ms.append(&mut stats.publish_to_delivery_ms);
        server_receive_to_delivery_ms.append(&mut stats.server_receive_to_delivery_ms);
        if let Some(first) = stats.first_receive_ms {
            first_receive_ms = Some(first_receive_ms.map_or(first, |current| current.min(first)));
        }
        if let Some(last) = stats.last_receive_ms {
            last_receive_ms = Some(last_receive_ms.map_or(last, |current| current.max(last)));
        }
    }

    let elapsed = start.elapsed();

    let mode = match (start_reader, start_writer) {
        (true, true) => "reader+writer",
        (true, false) => "reader",
        (false, true) => "writer",
        (false, false) => "idle",
    };

    let measured = if total_received > 0 {
        total_received
    } else {
        total_sent
    };
    println!("Run mode: {mode}, clients: {num_clients}");
    println!(
        "Wall throughput: {} msgs/sec",
        measured as f64 / elapsed.as_secs_f64()
    );
    println!("Sent: {total_sent}, received: {total_received}, retries seen: {total_retries_seen}");

    if start_reader {
        let expected = num_clients * msgs_per_client;
        let missing = expected.saturating_sub(total_received);
        println!("Expected receive count: {expected}, missing: {missing}");
    }

    if let (Some(first), Some(last)) = (first_receive_ms, last_receive_ms) {
        let receive_span_secs = (last.saturating_sub(first).max(1)) as f64 / 1000.0;
        println!(
            "Active receive span: {:.3}s, active receive throughput: {} msgs/sec",
            receive_span_secs,
            total_received as f64 / receive_span_secs,
        );
        println!(
            "Receive first/last after reader start: {:.3}s / {:.3}s",
            first as f64 / 1000.0,
            last as f64 / 1000.0,
        );
    }

    if !publish_to_delivery_ms.is_empty() {
        publish_to_delivery_ms.sort_unstable();
        println!(
            "Latency publish->deliver ms: p50={}, p95={}, p99={}, max={}",
            percentile(&publish_to_delivery_ms, 0.50),
            percentile(&publish_to_delivery_ms, 0.95),
            percentile(&publish_to_delivery_ms, 0.99),
            publish_to_delivery_ms.last().copied().unwrap_or_default(),
        );
    }

    if !server_receive_to_delivery_ms.is_empty() {
        server_receive_to_delivery_ms.sort_unstable();
        println!(
            "Latency server-receive->deliver ms: p50={}, p95={}, p99={}, max={}",
            percentile(&server_receive_to_delivery_ms, 0.50),
            percentile(&server_receive_to_delivery_ms, 0.95),
            percentile(&server_receive_to_delivery_ms, 0.99),
            server_receive_to_delivery_ms
                .last()
                .copied()
                .unwrap_or_default(),
        );
    }

    txb.send(()).unwrap();
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Number of concurrent clients
    #[arg(short, long, default_value_t = 10)]
    clients: usize,

    /// Messages per client
    #[arg(short, long, default_value_t = 500_000)]
    messages: usize,

    /// Enable reader
    #[arg(long, default_value_t = false)]
    reader: bool,

    /// Enable writer
    #[arg(long, default_value_t = false)]
    writer: bool,

    /// Size of each payload
    #[arg(short, long, default_value_t = 1024)]
    size: usize,

    /// Subscription prefetch for reader clients
    #[arg(long, default_value_t = 1024 * 8 * 2)]
    prefetch: u32,

    /// Directory where reader clients write readiness files after subscribing
    #[arg(long)]
    ready_dir: Option<PathBuf>,

    /// Reader idle timeout before ending with partial receive counts
    #[arg(long, default_value_t = 10_000)]
    idle_timeout_ms: u64,

    /// Wait for server publish confirmations before counting writes complete
    #[arg(long, default_value_t = false)]
    confirmed: bool,
}

#[tokio::main]
async fn main() {
    fibril_util::init_tracing();

    let args = Args::parse();

    let (txb, rxb) = oneshot::channel::<()>();

    let writer_only = args.writer && !args.reader;

    tokio::spawn(async move {
        run_load_test(
            args.clients,
            args.messages,
            args.reader,
            args.writer,
            txb,
            args.size,
            args.prefetch,
            args.ready_dir,
            args.idle_timeout_ms,
            args.confirmed,
        )
        .await;
    });

    rxb.await.unwrap();
    if writer_only {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
