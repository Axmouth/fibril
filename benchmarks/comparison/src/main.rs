mod adapter;
mod fault;
mod metrics;
mod rpc;

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use futures::{StreamExt, stream::FuturesUnordered};
use metrics::{Ids, Latency, Stamp, scheduled_ns};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    time::{Instant, sleep_until},
};

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum Broker {
    Fibril,
    Nats,
    Rabbitmq,
}

#[derive(Clone, Debug, Parser, Serialize)]
#[command(
    about = "Shared queue workload for Fibril, JetStream and RabbitMQ; see README for guarantees"
)]
struct Args {
    #[arg(long, value_enum)]
    broker: Broker,
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "bench")]
    queue: String,
    /// Stored copies requested by the runner; cluster configuration is verified separately.
    #[arg(long, default_value_t = 1)]
    copies: usize,
    #[arg(long)]
    setup_only: bool,
    /// Independent connection pairs per queue; total credits remain shared.
    #[arg(long, default_value_t = 1)]
    connections: usize,
    #[arg(long, hide = true)]
    allow_existing: bool,
    #[arg(long, hide = true)]
    fault_role: Option<String>,
    #[arg(long, hide = true)]
    fault_setup: bool,
    #[arg(long, default_value_t = 1, hide = true)]
    fault_count: u64,
    #[arg(long, default_value_t = 0, hide = true)]
    fault_start: u64,
    #[arg(long, hide = true)]
    phase_file: Option<PathBuf>,
    #[arg(long)]
    rpc: bool,
    /// Release service workers after sending a reply; await reply confirms and request ACKs separately.
    #[arg(long)]
    pipeline_replies: bool,
    #[arg(long, default_value_t = 32)]
    request_window: usize,
    #[arg(long, default_value_t = 4)]
    service_workers: usize,
    #[arg(long, default_value_t = 0)]
    processing_us: u64,
    #[arg(long, default_value_t = 1024)]
    reply_bytes: usize,
    /// Fixed offered messages/s. Omit for saturation.
    #[arg(long, conflicts_with = "saturation")]
    rate: Option<u64>,
    #[arg(long, required_unless_present = "rate")]
    saturation: bool,
    #[arg(long, default_value_t = 1024)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,
    #[arg(long, default_value_t = 120)]
    drain_secs: u64,
    #[arg(long, default_value_t = 4096)]
    confirm_window: usize,
    #[arg(long, default_value_t = 1024)]
    prefetch: u32,
    /// JetStream transport pull batch, bounded by prefetch; not application ACK batching.
    #[arg(long, default_value_t = 1024)]
    pull_batch: usize,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    /// Abort rather than silently truncate a workload that reaches this safety limit.
    #[arg(long, default_value_t = 50_000_000)]
    max_messages: u64,
    /// Declaration recorded in output. The runner verifies the actual server config.
    #[arg(long, default_value="always", value_parser=["always","2m"])]
    nats_sync: String,
    #[arg(long)]
    output: PathBuf,
}
impl Args {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=16).contains(&self.connections) && self.prefetch as usize % self.connections == 0,
            "connections must be 1..16 and divide total prefetch"
        );
        ensure!(
            matches!(self.copies, 1 | 3 | 5 | 7),
            "copies must be 1, 3, 5 or 7"
        );
        ensure!(
            matches!(self.broker, Broker::Fibril) || matches!(self.copies, 1 | 3),
            "larger replica groups currently require the Fibril cluster runner"
        );
        ensure!(
            self.request_window > 0
                && self.request_window <= 1_000_000
                && self.service_workers > 0
                && self.service_workers <= 2000,
            "invalid RPC concurrency"
        );
        ensure!(
            (metrics::HEADER..=1024 * 1024).contains(&self.reply_bytes),
            "invalid reply size"
        );
        ensure!(
            self.payload_bytes >= metrics::HEADER && self.payload_bytes <= 1024 * 1024,
            "payload must be 32 bytes..1 MiB, including the 32-byte measurement header"
        );
        ensure!(
            self.duration_secs > 0
                && self.duration_secs <= 1800
                && self.warmup_secs <= 600
                && self.drain_secs > 0
                && self.drain_secs <= 600,
            "invalid run durations"
        );
        ensure!(
            self.confirm_window > 0 && self.confirm_window <= 1_000_000,
            "invalid confirmation window"
        );
        ensure!(
            self.prefetch > 0 && self.prefetch <= 2000,
            "common profile prefetch must be 1..2000 (Rabbit quorum limit)"
        );
        ensure!(
            self.pull_batch > 0 && self.pull_batch <= self.prefetch as usize,
            "pull batch must be 1..prefetch"
        );
        ensure!(
            self.workers > 0 && self.max_messages > 0 && self.max_messages <= 1_000_000_000,
            "invalid limits"
        );
        ensure!(
            !self.queue.is_empty()
                && self
                    .queue
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "queue name must be alphanumeric/underscore"
        );
        if let Some(rate) = self.rate {
            ensure!(
                rate > 0,
                "rate must be positive; use --saturation for an unpaced run"
            );
            let count = rate
                .checked_mul(self.warmup_secs + self.duration_secs)
                .context("offered count overflow")?;
            ensure!(
                count <= self.max_messages,
                "planned offered count exceeds --max-messages"
            );
        }
        Ok(())
    }
    fn measured(&self, intended: u64) -> bool {
        intended >= self.warmup_secs * 1_000_000_000
            && intended < (self.warmup_secs + self.duration_secs) * 1_000_000_000
    }
}

#[derive(Default)]
struct Progress {
    issued: AtomicU64,
    confirmed: AtomicU64,
    delivered: AtomicU64,
    ack_sent: AtomicU64,
}
impl Progress {
    fn snapshot(&self) -> metrics::Counts {
        metrics::Counts {
            issued: self.issued.load(Ordering::Relaxed),
            confirmed: self.confirmed.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            ack_sent: self.ack_sent.load(Ordering::Relaxed),
        }
    }
}
fn ns(start: Instant) -> u64 {
    Instant::now().saturating_duration_since(start).as_nanos() as u64
}
struct AbortTasks(Vec<tokio::task::AbortHandle>);
impl Drop for AbortTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

struct Pending {
    stamp: Stamp,
    confirm: adapter::Confirmation,
    permit: tokio::sync::OwnedSemaphorePermit,
}
#[derive(Default)]
struct Stats {
    early_total: u64,
    early_measured: u64,
    total: u64,
    measured: u64,
    last_ns: u64,
    completed_in_window: u64,
    admission: Latency,
    latency: Latency,
    scheduled: Latency,
}
impl Stats {
    fn json(&self) -> Value {
        json!({"total":self.total,"measured":self.measured,"last_ns":self.last_ns,"completed_in_window":self.completed_in_window,
        "admission":self.admission.summary(),"from_admission":self.latency.summary(),"from_schedule":self.scheduled.summary()})
    }
}

async fn issue(
    a: Arc<Args>,
    p: Arc<Progress>,
    publisher: Arc<adapter::Publisher>,
    start: Instant,
    finish: Instant,
    tx: mpsc::Sender<Pending>,
    done_tx: watch::Sender<Option<u64>>,
    credits: Arc<Semaphore>,
    request_credits: Option<Arc<Semaphore>>,
) -> Result<Stats> {
    sleep_until(start).await;
    let count = a.rate.map(|r| r * (a.warmup_secs + a.duration_secs));
    let mut stats = Stats::default();
    loop {
        let id = stats.total;
        if count.is_some_and(|count| id == count) || (count.is_none() && Instant::now() >= finish) {
            break;
        }
        ensure!(
            id < a.max_messages,
            "message safety limit reached during saturation; increase --max-messages"
        );
        let intended = a
            .rate
            .map(|r| scheduled_ns(id, r))
            .unwrap_or_else(|| ns(start));
        let deadline = start + Duration::from_nanos(intended);
        if deadline > Instant::now() {
            sleep_until(deadline).await;
        }
        if let Some(request_credits) = &request_credits {
            request_credits.clone().acquire_owned().await?.forget();
        }
        let permit = credits.clone().acquire_owned().await?;
        if count.is_none() && Instant::now() >= finish {
            break;
        }
        let stamp = Stamp {
            id,
            intended,
            admitted: ns(start),
        };
        // Allocation/encoding and client admission are included in latency.
        let payload = stamp.encode(a.payload_bytes);
        let confirm = publisher.send(payload, id).await?;
        tx.send(Pending {
            stamp,
            confirm,
            permit,
        })
        .await
        .map_err(|_| anyhow::anyhow!("confirmation collector closed"))?;
        stats.total += 1;
        stats.last_ns = ns(start);
        if a.measured(stats.last_ns) {
            stats.completed_in_window += 1;
        }
        if a.measured(intended) {
            stats.measured += 1;
            stats.admission.record(stamp.admitted - intended)?;
        }
        p.issued.store(stats.total, Ordering::Relaxed);
    }
    // Complete fixed-rate nominal duration even when final issue was early.
    if a.rate.is_some() {
        sleep_until(finish).await;
    }
    done_tx.send(Some(stats.total))?;
    Ok::<_, anyhow::Error>(stats)
}

async fn collect_confirmations(
    a: Arc<Args>,
    p: Arc<Progress>,
    start: Instant,
    mut rx: mpsc::Receiver<Pending>,
) -> Result<Stats> {
    let mut pending = FuturesUnordered::new();
    let mut open = true;
    let mut stats = Stats::default();
    loop {
        tokio::select! {
            item = rx.recv(), if open => match item {
                Some(Pending { stamp, confirm, permit }) => pending.push(async move {
                    confirm.await?;
                    let observed = ns(start);
                    drop(permit);
                    Ok::<_,anyhow::Error>((stamp,observed))
                }),
                None => open = false,
            },
            Some(result) = pending.next(), if !pending.is_empty() => {
                let (stamp, observed) = result?;
                stats.total += 1; stats.last_ns = stats.last_ns.max(observed);
                if a.measured(observed) { stats.completed_in_window += 1; }
                if a.measured(stamp.intended) {
                    stats.measured += 1; stats.latency.record(observed-stamp.admitted)?;
                    stats.scheduled.record(observed-stamp.intended)?;
                }
                p.confirmed.store(stats.total, Ordering::Relaxed);
            },
            else => break,
        }
    }
    Ok::<_, anyhow::Error>(stats)
}

async fn workload(args: Arc<Args>, progress: Arc<Progress>, origin: Instant) -> Result<Value> {
    if args.rpc {
        return rpc::run(args, progress, origin).await;
    }
    let prepared = tokio::time::timeout(Duration::from_secs(60), adapter::prepare(&args)).await??;
    if args.setup_only {
        let mut prepared = prepared;
        let settled = prepared.connections.flush().await?;
        return Ok(json!({"status":"setup_validated", "config":*args,
            "adapter_settings":prepared.settings,"settlement":settled}));
    }
    let adapter::Prepared {
        publisher,
        mut deliveries,
        mut connections,
        settings,
    } = prepared;
    // SDK publisher handles may close their channel on Drop. Keep ownership
    // through confirmation and delivery drain, not just through issuance.
    let publisher_owner = Arc::new(publisher);
    let publisher = publisher_owner.clone();
    let start = Instant::now() + Duration::from_millis(100);
    let finish = start + Duration::from_secs(args.warmup_secs + args.duration_secs);
    let (tx, rx) = mpsc::channel::<Pending>(args.confirm_window);
    let (done_tx, mut done_rx) = watch::channel(None::<u64>);
    let credits = Arc::new(Semaphore::new(args.confirm_window));

    let a = args.clone();
    let p = progress.clone();
    let producer = tokio::spawn(issue(
        a, p, publisher, start, finish, tx, done_tx, credits, None,
    ));

    let a = args.clone();
    let p = progress.clone();
    let confirmer = tokio::spawn(collect_confirmations(a, p, start, rx));

    let a = args.clone();
    let p = progress.clone();
    let consumer = tokio::spawn(async move {
        let mut ids = Ids::default();
        let mut stats = Stats::default();
        let mut expected = None;
        loop {
            if expected == Some(stats.total) {
                break;
            }
            tokio::select! {
                item = deliveries.next() => {
                    let message = item.context("consumer ended before drain completed")??;
                    let received = ns(start);
                    let stamp = Stamp::decode(message.payload(), a.payload_bytes)?;
                    ensure!(received >= stamp.admitted, "delivery precedes admission");
                    ids.insert(stamp.id, a.max_messages)?; message.check_offset(if a.connections == 1 { Some(stamp.id) } else { None })?;
                    if a.connections == 1 && matches!(a.broker, Broker::Fibril) {
                        ensure!(stamp.id == stats.total, "Fibril delivery reordered: expected {}, got {}", stats.total, stamp.id);
                    }
                    if message.speculative() {
                        stats.early_total += 1;
                        if a.measured(stamp.intended) { stats.early_measured += 1; }
                    }
                    stats.total += 1; stats.last_ns = received;
                    if a.measured(received) { stats.completed_in_window += 1; }
                    if a.measured(stamp.intended) {
                        stats.measured += 1; stats.latency.record(received-stamp.admitted)?;
                        stats.scheduled.record(received-stamp.intended)?;
                    }
                    p.delivered.store(stats.total, Ordering::Relaxed);
                    message.ack().await?;
                    p.ack_sent.store(stats.total, Ordering::Relaxed);
                }
                changed = done_rx.changed(), if expected.is_none() => {
                    changed.context("publisher exited without final count")?;
                    expected = *done_rx.borrow_and_update();
                }
            }
        }
        ids.finish(expected.context("missing published count")?)?;
        Ok::<_, anyhow::Error>(stats)
    });

    let _abort_on_failure = AbortTasks(vec![
        producer.abort_handle(),
        confirmer.abort_handle(),
        consumer.abort_handle(),
    ]);
    let (producer, confirmer, consumer) =
        tokio::time::timeout_at(finish + Duration::from_secs(args.drain_secs), async {
            tokio::try_join!(
                async { producer.await? },
                async { confirmer.await? },
                async { consumer.await? }
            )
        })
        .await
        .context("workload exceeded duration + drain timeout")??;
    ensure!(
        producer.total == confirmer.total && producer.total == consumer.total,
        "total count mismatch"
    );
    ensure!(
        producer.measured > 0
            && producer.measured == confirmer.measured
            && producer.measured == consumer.measured,
        "measurement cohort mismatch or empty measurement"
    );
    let workload_done_ns = ns(start);
    let settlement = tokio::time::timeout(Duration::from_secs(30), connections.flush()).await??;
    drop(publisher_owner);
    let elapsed = ((confirmer.last_ns.max(consumer.last_ns) as f64 / 1e9)
        .max((args.warmup_secs + args.duration_secs) as f64)
        - args.warmup_secs as f64)
        .max(1e-9);
    Ok(
        json!({"schema_version":1,"status":"client_validated", "config":*args, "adapter_settings":settings,
        "publish":producer.json(),"confirm":confirmer.json(),"delivery":consumer.json(),
        "delivery_path":{"early_total":consumer.early_total,"early_measured":consumer.early_measured,
            "ordinary_measured":consumer.measured-consumer.early_measured,
            "strict_order_checked":matches!(args.broker,Broker::Fibril)},
        "workload_start_since_setup_secs":start.duration_since(origin).as_secs_f64(),
        "observed_delivery_per_sec":consumer.completed_in_window as f64 / args.duration_secs as f64,
        "observed_confirm_per_sec":confirmer.completed_in_window as f64 / args.duration_secs as f64,
        "counts":progress.snapshot(),"cohort_completed_per_sec":producer.measured as f64/elapsed,
        "cohort_elapsed_including_drain_secs":elapsed,"workload_done_ns":workload_done_ns,
        "settlement":settlement, "measurement_clock":"one process, monotonic Instant",
        "note":"Runner must verify server settings and final settlement before accepting this result."}),
    )
}

fn client_sample() -> Value {
    // Linux RSS includes the SDKs, histograms and validation bitmap; not page cache.
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
        });
    let stat = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let fields: Vec<_> = s.rsplit_once(')')?.1.split_whitespace().collect();
            Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
        });
    json!({"rss_kib":rss,"cpu_ticks":stat})
}
fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    ensure!(!args.output.exists(), "output already exists");
    if let Some(parent) = args.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.workers)
        .enable_all()
        .build()?;
    let a = Arc::new(args.clone());
    let p = Arc::new(Progress::default());
    let (result, samples) = runtime.block_on(async {
        let origin = Instant::now();
        let (stop, mut stopped) = watch::channel(false);
        let progress = p.clone();
        let sampler = tokio::spawn(async move {
            let begin = origin; let mut rows = Vec::new();
            loop {
                rows.push(json!({"since_setup_secs":begin.elapsed().as_secs_f64(),
                    "counts":progress.snapshot(),"client":client_sample()}));
                tokio::select! { _ = stopped.changed() => break, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
            }
            rows.push(json!({"since_setup_secs":begin.elapsed().as_secs_f64(),"counts":progress.snapshot(),"client":client_sample()}));
            rows
        });
        let result = if a.fault_role.is_some() { fault::run(&a).await } else { workload(a, p.clone(), origin).await };
        let _ = stop.send(true);
        (result, sampler.await.unwrap())
    });
    let (mut output, error) = match result {
        Ok(v) => (v, None),
        Err(e) => (
            json!({"schema_version":1,"status":"failed","config":args,"counts":p.snapshot(),"error":format!("{e:#}")}),
            Some(e),
        ),
    };
    output["timeline"] = json!(samples);
    std::fs::write(&args.output, serde_json::to_vec_pretty(&output)?)?;
    if let Some(e) = error {
        return Err(e);
    }
    println!(
        "{}",
        json!({"result":args.output,"status":output["status"],"completed_per_sec":output["cohort_completed_per_sec"]})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args() -> Args {
        Args::try_parse_from([
            "bench",
            "--broker",
            "fibril",
            "--endpoint",
            "unused",
            "--rate",
            "1000",
            "--output",
            "unused.json",
        ])
        .unwrap()
    }
    #[test]
    fn larger_replica_profiles_are_explicitly_fibril_only() {
        let mut a = args();
        for copies in [1, 3, 5, 7] {
            a.copies = copies;
            a.validate().unwrap();
        }
        for copies in [0, 2, 4, 6, 8] {
            a.copies = copies;
            assert!(a.validate().is_err());
        }
        for broker in [Broker::Nats, Broker::Rabbitmq] {
            a.broker = broker;
            a.copies = 3;
            a.validate().unwrap();
            a.copies = 5;
            assert!(a.validate().is_err());
        }
    }
    #[test]
    fn warmup_cohort_has_exact_boundaries() {
        let a = args();
        assert!(!a.measured(4_999_999_999));
        assert!(a.measured(5_000_000_000));
        assert!(a.measured(34_999_999_999));
        assert!(!a.measured(35_000_000_000));
    }
    #[test]
    fn reject_incomparable_credit_and_truncated_schedule() {
        let mut a = args();
        a.validate().unwrap();
        a.prefetch = 16384;
        assert!(a.validate().is_err());
        a.prefetch = 1024;
        a.max_messages = 1000;
        assert!(a.validate().is_err());
        a.max_messages = 50000;
        a.rate = Some(0);
        assert!(a.validate().is_err());
    }
}
