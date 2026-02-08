use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use fibril_broker::{Broker, BrokerConfig, ConsumerHandle};
use fibril_broker::{ConsumerConfig, SettleRequest};
use fibril_broker::{SettleType, coordination::NoopCoordination};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_storage::{
    DeliveryTag, Offset, make_rocksdb_store, make_stroma_store,
    observable_storage::ObservableStorage,
};

use clap::{Parser, ValueEnum};
use fibril_util::init_tracing;

#[derive(Parser, Debug)]
#[command(name = "fibril")]
pub enum Command {
    Bench(BenchCmd),
}

#[derive(Parser, Debug)]
pub struct BenchCmd {
    #[command(subcommand)]
    pub mode: BenchMode,
}

#[derive(Parser, Debug)]
pub enum BenchMode {
    E2E(E2EBench),
}

#[derive(Parser, Debug, Clone)]
pub struct E2EBench {
    #[arg(long, default_value = "1000000")]
    pub messages: u64,

    #[arg(long, default_value = "4")]
    pub producers: usize,

    #[arg(long, default_value = "1")]
    pub consumers: usize,

    #[arg(long, default_value = "32")]
    pub payload_min: usize,

    #[arg(long, default_value = "256")]
    pub payload_max: usize,

    #[arg(long, default_value = "64")]
    pub batch_size: usize,

    #[arg(long, default_value = "25")]
    pub batch_timeout_ms: u64,

    /// Sync Writes for RocksDB backend
    #[arg(long, default_value = "false")]
    pub sync_write: bool,

    #[arg(long, default_value = "16384")]
    pub producer_inflight: usize,

    #[arg(long, default_value = "1")]
    pub report_interval_secs: u64,

    #[arg(long, value_enum, default_value = "async")]
    pub ack_mode: AckMode,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum AckMode {
    Sync,
    Async,
    None,
}

#[repr(C)]
struct BenchPayload {
    msg_id: u64,
    producer_id: u32,
    payload: Vec<u8>,
}

fn make_payload(msg_id: u64, producer_id: u32, size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + size);
    buf.extend_from_slice(&msg_id.to_be_bytes());
    buf.extend_from_slice(&producer_id.to_be_bytes());

    let mut body = vec![0u8; size];
    fastrand::fill(&mut body);
    buf.extend_from_slice(&body);

    buf
}

fn decode_payload(buf: &[u8]) -> (u64, u32) {
    let msg_id = u64::from_be_bytes(buf[0..8].try_into().unwrap());
    let producer = u32::from_be_bytes(buf[8..12].try_into().unwrap());
    (msg_id, producer)
}

struct BenchMetrics {
    published: AtomicU64,
    confirmed: AtomicU64,
    consumed: AtomicU64,
    acked: AtomicU64,
    inflight: AtomicUsize,

    start: Instant,

    publish_done_at: AtomicU64, // nanos since start
    consume_done_at: AtomicU64,
    ack_done_at: AtomicU64,
}

impl BenchMetrics {
    fn new() -> Self {
        Self {
            published: AtomicU64::new(0),
            confirmed: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),

            start: Instant::now(),
            publish_done_at: AtomicU64::new(0),
            consume_done_at: AtomicU64::new(0),
            ack_done_at: AtomicU64::new(0),
        }
    }
}

fn mark_done_once(slot: &AtomicU64, start: Instant) {
    let elapsed = start.elapsed().as_nanos() as u64;
    let _ = slot.compare_exchange(0, elapsed, Ordering::SeqCst, Ordering::SeqCst);
}

#[allow(clippy::too_many_arguments)]
async fn producer_task(
    broker: Arc<Broker<NoopCoordination>>,
    topic: String,
    producer_id: u32,
    produce_counter: Arc<AtomicU64>,
    total: u64,
    inflight_limit: usize,
    prod_id_tx: tokio::sync::mpsc::UnboundedSender<(u64, u32)>,
    metrics: Arc<BenchMetrics>,
) {
    // tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    let (publisher, mut confirm_stream) = broker.get_publisher(&topic, &None).await.unwrap();

    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        while let Some(res) = confirm_stream.recv_confirm().await {
            if res.is_ok() {
                metrics_clone.confirmed.fetch_add(1, Ordering::SeqCst);
                metrics_clone.inflight.fetch_sub(1, Ordering::SeqCst);
            } else {
                tracing::error!("Error receiving confirm: {res:?}");
            }
        }
    });

    let mut msg_id;
    loop {
        while metrics.inflight.load(Ordering::SeqCst) >= inflight_limit {
            tokio::task::yield_now().await;
        }

        msg_id = produce_counter.fetch_add(1, Ordering::SeqCst);
        if msg_id >= total {
            break;
        }
        let size = 1024;
        let payload = make_payload(msg_id, producer_id, size);

        publisher.publish(payload).await.unwrap();
        prod_id_tx.send((msg_id, producer_id)).unwrap();

        metrics.inflight.fetch_add(1, Ordering::SeqCst);
        metrics.published.fetch_add(1, Ordering::SeqCst);
    }

    if msg_id == total {
        mark_done_once(&metrics.publish_done_at, metrics.start);
    }
}

#[must_use]
async fn consumer_task(
    mut consumer: ConsumerHandle,
    ack_mode: AckMode,
    metrics: Arc<BenchMetrics>,
    total: u64,
    res_tx: tokio::sync::mpsc::UnboundedSender<(Offset, DeliveryTag, Vec<u8>)>,
) {
    // tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let (ackq_tx, mut ackq_rx) = tokio::sync::mpsc::unbounded_channel::<DeliveryTag>();

    if let AckMode::Async = ack_mode {
        // One task that actually sends acks
        let acker = consumer.settler.clone();
        let metrics2 = metrics.clone();
        tokio::spawn(async move {
            while let Some(tag) = ackq_rx.recv().await {
                let req = SettleRequest {
                    delivery_tag: tag,
                    settle_type: SettleType::Ack,
                };
                let res = acker.send(req).await;
                if res.is_ok() {
                    metrics2.acked.fetch_add(1, Ordering::SeqCst);
                } else {
                    tracing::error!("Error sending ack/settle: {res:?}");
                }
            }
        });
    }

    while let Some(msg) = consumer.messages.recv().await {
        res_tx
            .send((msg.message.offset, msg.delivery_tag, msg.message.payload))
            .unwrap();
        let c = metrics.consumed.fetch_add(1, Ordering::SeqCst) + 1;
        if c == total {
            mark_done_once(&metrics.consume_done_at, metrics.start);
        }

        match ack_mode {
            AckMode::Sync => {
                let req = SettleRequest {
                    delivery_tag: msg.delivery_tag,
                    settle_type: SettleType::Ack,
                };
                let res = consumer.settler.send(req).await;
                if let Err(err) = res {
                    tracing::error!("Error sending ack/settle: {err:?}");
                }
                let a = metrics.acked.fetch_add(1, Ordering::SeqCst);
                if a == total {
                    mark_done_once(&metrics.ack_done_at, metrics.start);
                }
            }
            AckMode::Async => {
                // queue ack, backpressure here is fine
                ackq_tx.send(msg.delivery_tag).unwrap();
            }
            AckMode::None => {}
        }
    }
}

async fn reporter(
    metrics: Arc<BenchMetrics>,
    broker: Arc<Broker<NoopCoordination>>,
    topic: String,
    interval: u64,
    total: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
    let upper = broker.debug_upper(&topic, 0, &None).await;

    loop {
        ticker.tick().await;
        let acked = metrics.acked.load(Ordering::SeqCst);
        tracing::info!(
            "pub={} conf={} inflight={} cons={} ack={} upper={}",
            metrics.published.load(Ordering::SeqCst),
            metrics.confirmed.load(Ordering::SeqCst),
            metrics.inflight.load(Ordering::SeqCst),
            metrics.consumed.load(Ordering::SeqCst),
            acked,
            upper
        );

        if acked >= total {
            break;
        }
    }
}

async fn make_broker_with_cfg(cmd: E2EBench) -> Broker<NoopCoordination> {
    let cfg = BrokerConfig {
        publish_batch_size: cmd.batch_size,
        publish_batch_timeout_ms: cmd.batch_timeout_ms,
        ack_batch_size: 8192,
        ack_batch_timeout_ms: 15,
        inflight_batch_size: 8192,
        inflight_batch_timeout_ms: 15,
        // IMPORTANT: inflight TTL should exceed bench duration
        inflight_ttl_secs: 15,
        cleanup_interval_secs: 50,
        ..Default::default()
    };

    let storage_path = format!("bench_data/{}", fastrand::u64(..)); // stable path for E2E
    let _ = std::fs::remove_dir_all(&storage_path);
    std::fs::create_dir_all(&storage_path).unwrap();

    // let _store = make_rocksdb_store(&db_path, cmd.sync_write).unwrap();
    let store = make_stroma_store(&storage_path, cmd.sync_write).await.unwrap();
    let metrics = Metrics::new(60 * 60);
    let store = Arc::new(ObservableStorage::new(store, metrics.storage()));
    let coord = NoopCoordination {};

    let broker = Broker::try_new(store, coord, metrics.broker(), cfg)
        .await
        .unwrap();

    metrics.start(MetricsConfig {
        log_broker: true,
        log_storage: true,
        log_tcp: false,
    }, std::time::Duration::from_secs(1));

    broker
}

async fn run_e2e_bench(cmd: E2EBench) {
    let broker = make_broker_with_cfg(cmd.clone()).await;
    let broker = Arc::new(broker);
    let broker_clone = broker.clone();

    let metrics = Arc::new(BenchMetrics::new());

    let topic = format!("bench_topic_{}", fastrand::u64(..));

    // Consumers
    let mut c_handles = Vec::new();
    let (res_tx, mut res_rx) =
        tokio::sync::mpsc::unbounded_channel::<(Offset, DeliveryTag, Vec<u8>)>();
    for _ in 0..cmd.consumers {
        // let topic = format!("{}_{}", topic, i);
        let res_tx = res_tx.clone();
        let consumer = broker
            .subscribe(
                &topic,
                None,
                ConsumerConfig::default().with_prefetch_count(8192 * 8),
            )
            .await
            .unwrap();
        let c_handle = tokio::spawn(consumer_task(
            consumer,
            cmd.ack_mode.clone(),
            metrics.clone(),
            cmd.messages,
            res_tx,
        ));
        c_handles.push(c_handle);
    }

    // Producers
    let produce_counter = Arc::new(AtomicU64::new(0));

    let (prod_id_tx, mut prod_id_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u32)>();
    for p in 0..cmd.producers {
        let prod_id_tx = prod_id_tx.clone();
        let produce_counter = produce_counter.clone();
        // let topic = format!("{}_{}", topic, p);
        tokio::spawn(producer_task(
            broker.clone(),
            topic.clone(),
            p as u32,
            produce_counter,
            cmd.messages,
            cmd.producer_inflight,
            prod_id_tx,
            metrics.clone(),
        ));
    }

    drop(prod_id_tx);

    // Reporter
    let handle = tokio::spawn(reporter(
        metrics.clone(),
        broker.clone(),
        topic.clone(),
        cmd.report_interval_secs,
        cmd.messages,
    ));

    // Wait until done
    while metrics.acked.load(Ordering::SeqCst) < cmd.messages {
        tokio::time::sleep(std::time::Duration::from_millis(1001)).await;
    }

    handle.await.unwrap();

    let total = cmd.messages as f64;

    let pub_ns = metrics.publish_done_at.load(Ordering::SeqCst);
    let con_ns = metrics.consume_done_at.load(Ordering::SeqCst);
    let ack_ns = metrics.ack_done_at.load(Ordering::SeqCst);

    tracing::info!("\n");
    tracing::info!("=== FINAL THROUGHPUT ===");

    if pub_ns > 0 {
        let secs = pub_ns as f64 / 1e9;
        tracing::info!("Published: {:.0} msg/s, at {secs} secs", total / secs);
    }

    if con_ns > 0 {
        let secs = con_ns as f64 / 1e9;
        tracing::info!("Consumed:  {:.0} msg/s, at {secs} secs", total / secs);
    }

    if ack_ns > 0 {
        let secs = ack_ns as f64 / 1e9;
        tracing::info!("Acked:     {:.0} msg/s, at {secs} secs", total / secs);
    }

    broker.flush_storage().await.unwrap();
    let partition = 0;
    broker
        .forced_cleanup(&topic, partition, &None)
        .await
        .unwrap();

    broker_clone.dump_meta_keys().await;
    broker.shutdown().await;

    drop(res_tx);
    // let mut received = vec![];
    for c_handle in c_handles {
        c_handle.await.unwrap();
    }

    let mut received: Vec<(u64, DeliveryTag, Vec<u8>)> = vec![];
    let mut got = res_rx.recv_many(&mut received, 10_000_000).await;
    while got > 0 {
        tracing::info!("Got msgs: {}", got);
        got = res_rx.recv_many(&mut received, 10_000_000).await;
    }

    let mut seen = HashSet::with_capacity(received.len());
    let mut dup_offsets = 0usize;

    for (off, _, _) in &received {
        if !seen.insert(*off) {
            dup_offsets += 1;
        }
    }
    drop(seen);

    tracing::info!("duplicate offsets: {}", dup_offsets);

    let mut seen = HashSet::with_capacity(received.len());
    let mut dup = 0usize;

    for (off, tag, _) in &received {
        if !seen.insert((*off, *tag)) {
            dup += 1;
        }
    }

    tracing::info!("duplicate delivery tags: {}", dup);
    drop(seen);

    let mut seen = HashMap::with_capacity(received.len());
    let mut seen_offset_counts: HashMap<u64, i32> = HashMap::with_capacity(received.len());
    let mut seen_fp_counts: HashMap<u64, usize> = HashMap::with_capacity(received.len());
    let mut payload_dupes = 0usize;

    let mut received_prod_ids = vec![];
    for (off, _, payload) in &received {
        let mut h = std::hash::DefaultHasher::new();
        payload.hash(&mut h);
        let fp = h.finish();

        received_prod_ids.push(decode_payload(payload));

        if let Some(_prev_off) = seen.insert(fp, *off) {
            payload_dupes += 1;
            // tracing::info!("payload dup: prev_off={} now_off={}", prev_off, off);
        }

        let entry = seen_offset_counts.entry(*off).or_insert(0);
        *entry += 1;

        let entry = seen_fp_counts.entry(fp).or_insert(0);
        *entry += 1;

    }
    drop(seen);
    drop(received);

    let max_fp_dup = seen_fp_counts.values().max().copied().unwrap_or_default();
    let max_off_dup = seen_offset_counts.values().max().copied().unwrap_or_default();

    if max_fp_dup > 1 {
        tracing::info!("Max duplicity per payload {max_fp_dup}");
        let total_fp_dupes = seen_fp_counts.values().filter(|&&v| v > 1).count();
        tracing::info!("{total_fp_dupes} payload dupes")
    }

    if max_off_dup > 1 {
        tracing::info!("Max duplicity per offset {max_off_dup}");
        let total_off_dupes = seen_offset_counts.values().filter(|&&v| v > 1).count();
        tracing::info!("{total_off_dupes} offset dupes");
        let min_dupe_offset = seen_offset_counts.iter().filter(|(_k, v)| **v > 1).map(|(k, _)| k).min();
        let max_dupe_offset = seen_offset_counts.iter().filter(|(_k, v)| **v > 1).map(|(k, _)| k).max();
        if let Some(min_dupe_offset) = min_dupe_offset {
            tracing::info!("{min_dupe_offset} min dupe offset");
        }
        if let Some(max_dupe_offset) = max_dupe_offset {
            tracing::info!("{max_dupe_offset} max dupe offset");
        }
    }
    drop(seen_offset_counts);
    drop(seen_fp_counts);

    tracing::info!("payload-level duplicates: {}", payload_dupes);

    let mut prod_ids: Vec<(u64, u32)> = vec![];
    let mut got = prod_id_rx.recv_many(&mut prod_ids, 10_000_000).await;
    while got > 0 {
        tracing::info!("Got prod ids: {}", got);
        got = prod_id_rx.recv_many(&mut prod_ids, 10_000_000).await;
    }

    received_prod_ids.sort();
    prod_ids.sort();

    if received_prod_ids.len() != prod_ids.len() {
        tracing::info!(
            "received_prod_ids.len() {} != prod_ids.len() {}",
            received_prod_ids.len(),
            prod_ids.len()
        );
    }

    if received_prod_ids != prod_ids {
        tracing::info!("received_prod_ids != prod_ids");
    }

    received_prod_ids.sort_unstable();
    received_prod_ids.dedup();
    prod_ids.sort_unstable();
    prod_ids.dedup();
    if received_prod_ids != prod_ids {
        tracing::info!("deduped received_prod_ids != prod_ids");
    }
    drop(prod_ids);
    drop(received_prod_ids);

    tracing::info!("Bench complete.");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    init_tracing();

    let cmd = Command::parse();

    match cmd {
        Command::Bench(b) => match b.mode {
            BenchMode::E2E(e2e) => {
                tracing::info!("Starting E2E bench: {:#?}", e2e);
                run_e2e_bench(e2e).await;
            }
        },
    }
}
