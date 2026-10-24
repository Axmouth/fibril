//! Optional replication pipeline counters for benchmarks. Disabled unless
//! `FIBRIL_PIPELINE_COUNTERS_PATH` names a file. When enabled, a thread
//! rewrites that file as JSON every 250 ms, and `start_runtime_probe` measures
//! how promptly the Tokio runtime polls a task. Every record call is a single
//! check when disabled.
//!
//! Durations go into power of two histograms: bucket i counts values from 2^i
//! up to 2^(i+1) microseconds, bucket 0 also counts zero, and the last bucket
//! counts everything longer.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BUCKETS: usize = 20;

#[derive(Default)]
struct Histogram {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Histogram {
    fn record(&self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
        let bucket = (u64::BITS - us.leading_zeros()).saturating_sub(1) as usize;
        self.buckets[bucket.min(BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
    }

    fn json(&self) -> String {
        let buckets: Vec<String> = self.buckets.iter().map(|b| b.load(Ordering::Relaxed).to_string()).collect();
        format!(
            "{{\"count\":{},\"total_us\":{},\"max_us\":{},\"buckets\":[{}]}}",
            self.count.load(Ordering::Relaxed),
            self.total_us.load(Ordering::Relaxed),
            self.max_us.load(Ordering::Relaxed),
            buckets.join(",")
        )
    }
}

#[derive(Default)]
struct Counters {
    // Owner push loop.
    owner_reads: AtomicU64,
    owner_credit_capped_reads: AtomicU64,
    owner_frames: AtomicU64,
    owner_frame_records: AtomicU64,
    owner_frame_bytes: AtomicU64,
    owner_read: Histogram,
    owner_send: Histogram,
    owner_credit_park: Histogram,
    // Follower applier loop.
    follower_applies: AtomicU64,
    follower_frames: AtomicU64,
    follower_records: AtomicU64,
    follower_bytes: AtomicU64,
    follower_backlogged_applies: AtomicU64,
    follower_idle: Histogram,
    follower_collect: Histogram,
    follower_apply: Histogram,
    follower_progress_send: Histogram,
    // Owner publish sink and confirm loop.
    sink_batches: AtomicU64,
    sink_messages: AtomicU64,
    sink_wait: Histogram,
    sink_gather: Histogram,
    sink_append: Histogram,
    sink_confirm_send: Histogram,
    confirm_local: Histogram,
    confirm_replicated: Histogram,
    // Runtime probe.
    probe_sleep_late: Histogram,
    probe_yield: Histogram,
}

static COUNTERS: OnceLock<Option<&'static Counters>> = OnceLock::new();

fn counters() -> Option<&'static Counters> {
    *COUNTERS.get_or_init(|| {
        let path = std::env::var("FIBRIL_PIPELINE_COUNTERS_PATH").ok()?;
        let c: &'static Counters = Box::leak(Box::default());
        let started = std::thread::Builder::new().name("pipeline-counters".into()).spawn(move || {
            loop {
                let temp = format!("{path}.pending");
                if let Err(error) =
                    std::fs::write(&temp, c.json()).and_then(|_| std::fs::rename(&temp, &path))
                {
                    tracing::error!(%error, "writing pipeline counters");
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        });
        if let Err(error) = started {
            tracing::error!(%error, "starting pipeline counters");
            return None;
        }
        Some(c)
    })
}

impl Counters {
    fn json(&self) -> String {
        let n = |v: &AtomicU64| v.load(Ordering::Relaxed);
        let counts = [
            ("owner_reads", n(&self.owner_reads)),
            ("owner_credit_capped_reads", n(&self.owner_credit_capped_reads)),
            ("owner_frames", n(&self.owner_frames)),
            ("owner_frame_records", n(&self.owner_frame_records)),
            ("owner_frame_bytes", n(&self.owner_frame_bytes)),
            ("follower_applies", n(&self.follower_applies)),
            ("follower_frames", n(&self.follower_frames)),
            ("follower_records", n(&self.follower_records)),
            ("follower_bytes", n(&self.follower_bytes)),
            ("follower_backlogged_applies", n(&self.follower_backlogged_applies)),
            ("sink_batches", n(&self.sink_batches)),
            ("sink_messages", n(&self.sink_messages)),
        ];
        let histograms = [
            ("owner_read", &self.owner_read),
            ("owner_send", &self.owner_send),
            ("owner_credit_park", &self.owner_credit_park),
            ("follower_idle", &self.follower_idle),
            ("follower_collect", &self.follower_collect),
            ("follower_apply", &self.follower_apply),
            ("follower_progress_send", &self.follower_progress_send),
            ("sink_wait", &self.sink_wait),
            ("sink_gather", &self.sink_gather),
            ("sink_append", &self.sink_append),
            ("sink_confirm_send", &self.sink_confirm_send),
            ("confirm_local", &self.confirm_local),
            ("confirm_replicated", &self.confirm_replicated),
            ("probe_sleep_late", &self.probe_sleep_late),
            ("probe_yield", &self.probe_yield),
        ];
        let fields: Vec<String> = counts
            .iter()
            .map(|(name, v)| format!("\"{name}\":{v}"))
            .chain(histograms.iter().map(|(name, h)| format!("\"{name}\":{}", h.json())))
            .collect();
        format!("{{{}}}\n", fields.join(","))
    }
}

/// True when counters are on, so callers can skip taking timestamps.
pub fn enabled() -> bool {
    counters().is_some()
}

/// One owner read: its duration and whether credit, not the read byte limit,
/// capped its size.
pub fn owner_read(elapsed: Duration, credit_capped: bool) {
    if let Some(c) = counters() {
        c.owner_reads.fetch_add(1, Ordering::Relaxed);
        if credit_capped {
            c.owner_credit_capped_reads.fetch_add(1, Ordering::Relaxed);
        }
        c.owner_read.record(elapsed);
    }
}

/// One frame handed to the transport, with how long the send waited.
pub fn owner_frame(records: u64, bytes: u64, send: Duration) {
    if let Some(c) = counters() {
        c.owner_frames.fetch_add(1, Ordering::Relaxed);
        c.owner_frame_records.fetch_add(records, Ordering::Relaxed);
        c.owner_frame_bytes.fetch_add(bytes, Ordering::Relaxed);
        c.owner_send.record(send);
    }
}

/// Time the owner sat with no credit until the follower refilled it.
pub fn owner_credit_park(elapsed: Duration) {
    if let Some(c) = counters() {
        c.owner_credit_park.record(elapsed);
    }
}

/// Timings of one follower apply cycle.
pub struct FollowerApply {
    pub frames: u64,
    pub records: u64,
    pub bytes: u64,
    /// Wait for this apply's first frame after the previous progress was
    /// sent. None when the frame was carried over.
    pub idle: Option<Duration>,
    /// From the first frame to the start of the apply.
    pub collect: Duration,
    pub apply: Duration,
    pub progress_send: Duration,
    /// Another frame was already queued when the apply finished.
    pub backlogged: bool,
}

pub fn follower_apply(a: FollowerApply) {
    if let Some(c) = counters() {
        c.follower_applies.fetch_add(1, Ordering::Relaxed);
        c.follower_frames.fetch_add(a.frames, Ordering::Relaxed);
        c.follower_records.fetch_add(a.records, Ordering::Relaxed);
        c.follower_bytes.fetch_add(a.bytes, Ordering::Relaxed);
        if a.backlogged {
            c.follower_backlogged_applies.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(idle) = a.idle {
            c.follower_idle.record(idle);
        }
        c.follower_collect.record(a.collect);
        c.follower_apply.record(a.apply);
        c.follower_progress_send.record(a.progress_send);
    }
}

/// One owner publish sink batch.
pub struct SinkBatch {
    pub messages: u64,
    /// Idle wait for the batch's first request.
    pub wait: Duration,
    /// From the first request to the append call, coalescing included.
    pub gather: Duration,
    /// The append call, which waits when the storage writer is full.
    pub append: Duration,
    /// Handing the batch's confirmations to the confirm loop.
    pub confirm_send: Duration,
}

pub fn sink_batch(b: SinkBatch) {
    if let Some(c) = counters() {
        c.sink_batches.fetch_add(1, Ordering::Relaxed);
        c.sink_messages.fetch_add(b.messages, Ordering::Relaxed);
        c.sink_wait.record(b.wait);
        c.sink_gather.record(b.gather);
        c.sink_append.record(b.append);
        c.sink_confirm_send.record(b.confirm_send);
    }
}

/// Per message, from its batch's append to the owner's own sync.
pub fn confirm_local(elapsed: Duration) {
    if let Some(c) = counters() {
        c.confirm_local.record(elapsed);
    }
}

/// Per message, from its batch's append to the replicated confirm.
pub fn confirm_replicated(elapsed: Duration) {
    if let Some(c) = counters() {
        c.confirm_replicated.record(elapsed);
    }
}

/// Spawns a task that sleeps 1 ms, then yields, and records how late the
/// sleep woke and how long the yield took to be polled again. Tokio rounds
/// the sleep to its 1 ms tick, so lateness up to about 1 ms is normal and
/// longer means the runtime did not poll the task. Does nothing when
/// counters are off.
pub fn start_runtime_probe() {
    let Some(c) = counters() else { return };
    tokio::spawn(async move {
        loop {
            let target = Instant::now() + Duration::from_millis(1);
            tokio::time::sleep_until(target.into()).await;
            c.probe_sleep_late.record(Instant::now().saturating_duration_since(target));
            let yielded = Instant::now();
            tokio::task::yield_now().await;
            c.probe_yield.record(yielded.elapsed());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_by_power_of_two() {
        let h = Histogram::default();
        for us in [0, 1, 2, 3, 4, 1000, 10_000_000] {
            h.record(Duration::from_micros(us));
        }
        let b: Vec<u64> = h.buckets.iter().map(|b| b.load(Ordering::Relaxed)).collect();
        assert_eq!(&b[..3], &[2, 2, 1]);
        assert_eq!(b[9], 1);
        assert_eq!(b[BUCKETS - 1], 1);
        assert_eq!(h.max_us.load(Ordering::Relaxed), 10_000_000);
    }
}
