use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::task::JoinSet;

use thetube::storage::make_rocksdb_store;
use thetube::{broker::coordination::NoopCoordination, storage::Storage};
use thetube::{
    broker::{Broker, BrokerConfig, BrokerError, ConsumerHandle},
    storage::Offset,
};

use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "thetube")]
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

#[derive(Parser, Debug)]
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

    #[arg(long, default_value = "1")]
    pub batch_timeout_ms: u64,

    #[arg(long, default_value = "10000")]
    pub producer_inflight: usize,

    #[arg(long, default_value = "1")]
    pub report_interval_secs: u64,

    #[arg(long, value_enum, default_value = "async")]
    pub ack_mode: AckMode,
}

#[derive(ValueEnum, Clone, Debug)]
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

struct Metrics {
    published: AtomicU64,
    confirmed: AtomicU64,
    consumed: AtomicU64,
    acked: AtomicU64,
    inflight: AtomicUsize,
    duplicates: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            published: AtomicU64::new(0),
            confirmed: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            duplicates: AtomicU64::new(0),
        }
    }
}

async fn producer_task(
    broker: Arc<Broker<NoopCoordination>>,
    topic: String,
    producer_id: u32,
    start_id: u64,
    count: u64,
    inflight_limit: usize,
    metrics: Arc<Metrics>,
) {
    let (publisher, mut confirm_stream) = broker.get_publisher(&topic).await.unwrap();

    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        while let Some(res) = confirm_stream.recv_confirm().await {
            if res.is_ok() {
                metrics_clone.confirmed.fetch_add(1, Ordering::Relaxed);
                metrics_clone.inflight.fetch_sub(1, Ordering::Relaxed);
            }
        }
    });

    for i in 0..count {
        while metrics.inflight.load(Ordering::Relaxed) >= inflight_limit {
            tokio::task::yield_now().await;
        }

        let msg_id = start_id + i;
        let size = fastrand::usize(32..256);
        let payload = make_payload(msg_id, producer_id, size);

        metrics.inflight.fetch_add(1, Ordering::Relaxed);
        metrics.published.fetch_add(1, Ordering::Relaxed);

        publisher.publish(payload).await.unwrap();
    }
}

async fn consumer_task(mut consumer: ConsumerHandle, ack_mode: AckMode, metrics: Arc<Metrics>) {
    let (ackq_tx, mut ackq_rx) = tokio::sync::mpsc::channel::<u64>(10_000);

    // One task that actually sends acks
    let acker = consumer.acker.clone();
    let metrics2 = metrics.clone();
    tokio::spawn(async move {
        while let Some(off) = ackq_rx.recv().await {
            if acker.send(off).await.is_ok() {
                metrics2.acked.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    while let Some(msg) = consumer.messages.recv().await {
        metrics.consumed.fetch_add(1, Ordering::Relaxed);

        match ack_mode {
            AckMode::Sync => {
                let _ = consumer.acker.try_send(msg.delivery_tag);
                metrics.acked.fetch_add(1, Ordering::Relaxed);
            }
            AckMode::Async => {
                // queue ack; backpressure here is fine
                ackq_tx.send(msg.delivery_tag).await.unwrap();
            }
            AckMode::None => {}
        }
    }
}

async fn reporter(
    metrics: Arc<Metrics>,
    broker: Arc<Broker<NoopCoordination>>,
    topic: String,
    interval: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
    let upper = broker.debug_upper(&topic, 0).await;

    loop {
        ticker.tick().await;
        println!(
            "pub={} conf={} inflight={} cons={} ack={} dup={} upper={}",
            metrics.published.load(Ordering::Relaxed),
            metrics.confirmed.load(Ordering::Relaxed),
            metrics.inflight.load(Ordering::Relaxed),
            metrics.consumed.load(Ordering::Relaxed),
            metrics.acked.load(Ordering::Relaxed),
            metrics.duplicates.load(Ordering::Relaxed),
            upper
        );
    }
}

async fn make_broker_with_cfg(cmd: &E2EBench) -> Broker<NoopCoordination> {
    let mut cfg = BrokerConfig::default();
    cfg.publish_batch_size = cmd.batch_size;
    cfg.publish_batch_timeout_ms = cmd.batch_timeout_ms;
    cfg.ack_batch_size = 512;
    cfg.ack_batch_timeout_ms = 1;

    // IMPORTANT: inflight TTL must exceed bench duration
    cfg.inflight_ttl_secs = 60;

    let db_path = format!("bench_data/{}", fastrand::u64(..)); // stable path for E2E
    let _ = std::fs::remove_dir_all(&db_path);
    std::fs::create_dir_all(&db_path).unwrap();

    let store = make_rocksdb_store(&db_path).unwrap();
    let coord = NoopCoordination {};

    let broker = Broker::try_new(store, coord, cfg).await.unwrap();

    // Redelivery worker is REQUIRED for correctness
    broker.start_redelivery_worker();

    broker
}

async fn run_e2e_bench(cmd: E2EBench) {
    let broker = make_broker_with_cfg(&cmd).await;
    let broker = Arc::new(broker);
    let broker_clone = broker.clone();

    let metrics = Arc::new(Metrics::new());

    let topic = format!("bench_topic_{}", fastrand::u64(..));

    // Consumers
    for _ in 0..cmd.consumers {
        let consumer = broker.subscribe(&topic, "bench_group").await.unwrap();
        tokio::spawn(consumer_task(
            consumer,
            cmd.ack_mode.clone(),
            metrics.clone(),
        ));
    }

    // Producers
    let per_producer = cmd.messages / cmd.producers as u64;

    for p in 0..cmd.producers {
        tokio::spawn(producer_task(
            broker.clone(),
            topic.clone(),
            p as u32,
            p as u64 * per_producer,
            per_producer,
            cmd.producer_inflight,
            metrics.clone(),
        ));
    }

    // Reporter
    tokio::spawn(reporter(
        metrics.clone(),
        broker,
        topic.clone(),
        cmd.report_interval_secs,
    ));

    // Wait until done
    while metrics.acked.load(Ordering::Relaxed) < cmd.messages {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    broker_clone.dump_meta_keys().await;

    println!("Bench complete.");
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let cmd = Command::parse();

    match cmd {
        Command::Bench(b) => match b.mode {
            BenchMode::E2E(e2e) => {
                println!("Starting E2E bench: {:#?}", e2e);
                run_e2e_bench(e2e).await;
            }
        },
    }
}
