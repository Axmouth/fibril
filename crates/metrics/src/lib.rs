use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::json;
use serde_with::{DisplayFromStr, serde_as};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

// TODO: Save ip/host per connection (to display connections list)
// TODO: Save above connection info per sub too(topic, group too), plus ways to calculate uptime for each
// TODO: also similar data per publisher
// TODO: Messages per unit of time for each subs and pubs too
// TODO: Optionally persist stats too, clean up what is old, user can have a minimal idea what led uo to crashes without complex external systems

#[derive(Debug)]
pub struct RollingCounter {
    buckets: Vec<AtomicU64>,
    last_tick: AtomicU64,
    resolution_secs: u64,
}

impl RollingCounter {
    pub fn new(resolution_secs: u64, bucket_count: usize) -> Self {
        Self {
            buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            last_tick: AtomicU64::new(0),
            resolution_secs,
        }
    }

    #[inline]
    pub fn incr(&self) {
        let now = current_epoch_secs() / self.resolution_secs;
        let idx = (now as usize) % self.buckets.len();

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        let now = current_epoch_secs() / self.resolution_secs;
        let idx = (now as usize) % self.buckets.len();

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(many, Ordering::Relaxed);
    }

    pub fn sum_last(&self, seconds: usize) -> u64 {
        let now = current_epoch_secs() / self.resolution_secs;
        let mut sum = 0;

        for i in 0..seconds.min(self.buckets.len()) {
            let idx =
                ((now as isize - i as isize).rem_euclid(self.buckets.len() as isize)) as usize;
            sum += self.buckets[idx].load(Ordering::Relaxed);
        }

        sum
    }

    pub fn rate_per_sec(&self, window_secs: usize) -> f64 {
        self.sum_last(window_secs) as f64 / window_secs.min(self.buckets.len().max(1)) as f64
    }
}

#[inline]
fn current_epoch_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

#[derive(Debug)]
pub struct LatencyStats {
    pub count: AtomicU64,
    pub total_micros: AtomicU64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
        }
    }
}

impl LatencyStats {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn observe(&self, d: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_micros
            .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn avg_micros(&self) -> Option<f64> {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            None
        } else {
            Some(self.total_micros.load(Ordering::Relaxed) as f64 / count as f64)
        }
    }
}

pub struct Timer<'a> {
    start: Instant,
    stats: &'a LatencyStats,
}

impl<'a> Timer<'a> {
    #[inline]
    pub fn new(stats: &'a LatencyStats) -> Self {
        Self {
            start: Instant::now(),
            stats,
        }
    }
}

impl Drop for Timer<'_> {
    #[inline]
    fn drop(&mut self) {
        self.stats.observe(self.start.elapsed());
    }
}

#[derive(Debug)]
pub struct OpStats {
    pub ops: RollingCounter,
    pub latency: LatencyStats,
    pub total: AtomicU64,
    pub errors: AtomicU64,
}

impl OpStats {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            ops: RollingCounter::new(1, bucket_count),
            latency: LatencyStats::new(),
            total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn incr(&self) {
        self.ops.incr();
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        self.ops.incr_many(many);
        self.total.fetch_add(many, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_result<T, E>(&self, res: &Result<T, E>) {
        self.incr();
        if res.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
pub struct BatchStats {
    pub batches: OpStats,
    pub items_total: AtomicU64,
    pub bytes_total: AtomicU64,
}

impl BatchStats {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            batches: OpStats::new(bucket_count),
            items_total: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn observe(&self, items: usize, bytes: usize) {
        self.batches.incr();
        self.items_total.fetch_add(items as u64, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct StorageStats {
    pub writes: OpStats,
    pub reads: OpStats,
    pub acks: OpStats,
    pub inflight: OpStats,
    pub maintenance: OpStats,
    pub control_plane: OpStats,
    pub redelivery: OpStats,

    pub append_batches: BatchStats,
    pub ack_batches: BatchStats,
}

impl StorageStats {
    pub fn new(buckets: usize) -> Arc<Self> {
        Arc::new(Self {
            writes: OpStats::new(buckets),
            reads: OpStats::new(buckets),
            acks: OpStats::new(buckets),
            inflight: OpStats::new(buckets),
            maintenance: OpStats::new(buckets),
            control_plane: OpStats::new(buckets),
            redelivery: OpStats::new(buckets),

            append_batches: BatchStats::new(buckets),
            ack_batches: BatchStats::new(buckets),
        })
    }

    #[inline]
    pub fn record_ack(&self) {
        self.acks.incr();
    }

    #[inline]
    pub fn record_acks(&self, acks: u64) {
        self.acks.incr_many(acks);
    }

    #[inline]
    pub fn record_inflight(&self) {
        self.inflight.incr();
    }

    #[inline]
    pub fn record_inflights(&self, inflight: u64) {
        self.inflight.incr_many(inflight);
    }

    #[inline]
    pub fn record_write(&self) {
        self.writes.incr();
    }

    #[inline]
    pub fn record_writes(&self, writes: u64) {
        self.writes.incr_many(writes);
    }

    #[inline]
    pub fn record_read(&self) {
        self.reads.incr();
    }

    #[inline]
    pub fn record_reads(&self, reads: u64) {
        self.reads.incr_many(reads);
    }

    #[inline]
    pub fn record_append_batch(&self, items: usize, bytes: usize) {
        self.append_batches.observe(items, bytes);
    }

    pub fn snapshot(&self) -> StorageStatsSnapshot {
        let reads_1m = self.reads.ops.rate_per_sec(60);
        let writes_1m = self.writes.ops.rate_per_sec(60);

        StorageStatsSnapshot {
            reads_per_sec_1m: reads_1m,
            writes_per_sec_1m: writes_1m,
            total_reads: self.reads.total.load(Ordering::Relaxed),
            total_writes: self.writes.total.load(Ordering::Relaxed),

            avg_read_latency_ms: self.reads.latency.avg_micros().map(|v| v / 1000.0),

            avg_write_latency_ms: self.writes.latency.avg_micros().map(|v| v / 1000.0),

            avg_ack_latency_ms: self.acks.latency.avg_micros().map(|v| v / 1000.0),

            avg_append_batch_latency_ms: self
                .append_batches
                .batches
                .latency
                .avg_micros()
                .map(|v| v / 1000.0),

            avg_ack_batch_latency_ms: self
                .ack_batches
                .batches
                .latency
                .avg_micros()
                .map(|v| v / 1000.0),

            avg_flush_latency_ms: self.maintenance.latency.avg_micros().map(|v| v / 1000.0),
        }
    }
}

#[derive(Serialize)]
pub struct StorageStatsSnapshot {
    pub reads_per_sec_1m: f64,
    pub writes_per_sec_1m: f64,
    pub total_reads: u64,
    pub total_writes: u64,

    pub avg_read_latency_ms: Option<f64>,
    pub avg_write_latency_ms: Option<f64>,
    pub avg_ack_latency_ms: Option<f64>,
    pub avg_append_batch_latency_ms: Option<f64>,

    pub avg_ack_batch_latency_ms: Option<f64>,
    pub avg_flush_latency_ms: Option<f64>,
}

/// Per-queue stats, or the error encountered reading them. A queue reports one or
/// the other (never bogus zero counts alongside an error), so one unreachable
/// queue surfaces as an error entry rather than blanking the whole list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum QueueStateSnapshot {
    Ok {
        ready_count: usize,
        inflight_count: usize,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct QueueKey {
    pub topic: String,
    pub group: Option<String>,
}

impl fmt::Display for QueueKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.group {
            Some(g) => write!(f, "{}:{}", self.topic, g),
            None => write!(f, "{}", self.topic),
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueuesStateSnapshot {
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub queues: HashMap<QueueKey, QueueStateSnapshot>,
}

impl QueuesStateSnapshot {
    pub fn json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (key, stat) in &self.queues {
            let key_str = if let Some(group) = &key.group {
                format!("{}:{}", key.topic, group)
            } else {
                key.topic.clone()
            };
            map.insert(
                key_str,
                serde_json::to_value(stat).unwrap_or(serde_json::Value::Null),
            );
        }
        serde_json::Value::Object(map)
    }
}

type QueueStateCallback =
    dyn Fn() -> Pin<Box<dyn Future<Output = QueuesStateSnapshot> + Send>> + Send + Sync + 'static;

pub struct BrokerStats {
    pub delivered: OpStats,
    pub published: OpStats,
    pub acked: OpStats,
    pub redelivered: OpStats,
    pub expired: OpStats,

    pub publish_batches: BatchStats,
    pub ack_batches: BatchStats,

    pub queue_cleanup_attempts: AtomicU64,
    pub queue_cleanup_storage_evicted: AtomicU64,
    pub queue_cleanup_storage_not_materialized: AtomicU64,
    pub queue_cleanup_storage_not_present: AtomicU64,
    pub queue_cleanup_storage_has_inflight: AtomicU64,
    pub queue_cleanup_storage_race_lost: AtomicU64,
    pub queue_cleanup_skipped_not_tracked: AtomicU64,
    pub queue_cleanup_skipped_active: AtomicU64,
    pub queue_cleanup_skipped_not_idle_enough: AtomicU64,
    pub queue_cleanup_skipped_pending_settles: AtomicU64,
    pub queue_cleanup_skipped_has_broker_deliveries: AtomicU64,
    pub queue_cleanup_skipped_has_inflight: AtomicU64,

    pub queue_state_callback: ArcSwapOption<Arc<QueueStateCallback>>,
}

impl BrokerStats {
    pub fn new(buckets: usize) -> Arc<Self> {
        Arc::new(Self {
            delivered: OpStats::new(buckets),
            published: OpStats::new(buckets),
            acked: OpStats::new(buckets),
            redelivered: OpStats::new(buckets),
            expired: OpStats::new(buckets),
            publish_batches: BatchStats::new(buckets),
            ack_batches: BatchStats::new(buckets),
            queue_cleanup_attempts: AtomicU64::new(0),
            queue_cleanup_storage_evicted: AtomicU64::new(0),
            queue_cleanup_storage_not_materialized: AtomicU64::new(0),
            queue_cleanup_storage_not_present: AtomicU64::new(0),
            queue_cleanup_storage_has_inflight: AtomicU64::new(0),
            queue_cleanup_storage_race_lost: AtomicU64::new(0),
            queue_cleanup_skipped_not_tracked: AtomicU64::new(0),
            queue_cleanup_skipped_active: AtomicU64::new(0),
            queue_cleanup_skipped_not_idle_enough: AtomicU64::new(0),
            queue_cleanup_skipped_pending_settles: AtomicU64::new(0),
            queue_cleanup_skipped_has_broker_deliveries: AtomicU64::new(0),
            queue_cleanup_skipped_has_inflight: AtomicU64::new(0),
            queue_state_callback: ArcSwapOption::from(None),
        })
    }

    pub fn register_queue_state_callback(&self, cb: Option<Arc<QueueStateCallback>>) {
        self.queue_state_callback.store(cb.map(Arc::new));
    }

    pub async fn call_queue_state_callback(&self) -> Option<QueuesStateSnapshot> {
        let cb = self.queue_state_callback.load();
        {
            let this = cb.as_ref();
            match this {
                Some(x) => Some((|f: &Arc<QueueStateCallback>| f())(x).await),
                None => None,
            }
        }
    }

    #[inline]
    pub fn delivered(&self) {
        self.delivered.incr();
    }

    #[inline]
    pub fn delivered_many(&self, delivered: u64) {
        self.delivered.incr_many(delivered);
    }

    #[inline]
    pub fn acked(&self) {
        self.acked.incr();
    }

    #[inline]
    pub fn acked_many(&self, acked: u64) {
        self.acked.incr_many(acked);
    }

    #[inline]
    pub fn published(&self) {
        self.published.incr();
    }

    #[inline]
    pub fn published_many(&self, published: u64) {
        self.published.incr_many(published);
    }

    #[inline]
    pub fn redelivered(&self) {
        self.redelivered.incr();
    }

    #[inline]
    pub fn redelivered_many(&self, redelivered: u64) {
        self.redelivered.incr_many(redelivered);
    }

    #[inline]
    pub fn expired(&self) {
        self.expired.incr();
    }

    #[inline]
    pub fn expired_many(&self, expired: u64) {
        self.expired.incr_many(expired);
    }

    #[inline]
    pub fn ack_batch(&self, batch_size: usize, bytes: usize) {
        self.ack_batches.observe(batch_size, bytes);
    }

    #[inline]
    pub fn publish_batch(&self, batch_size: usize, bytes: usize) {
        self.publish_batches.observe(batch_size, bytes);
    }

    #[inline]
    pub fn queue_cleanup_skipped(&self, reason: &str) {
        self.queue_cleanup_attempts.fetch_add(1, Ordering::Relaxed);
        match reason {
            "not_tracked" => {
                self.queue_cleanup_skipped_not_tracked
                    .fetch_add(1, Ordering::Relaxed);
            }
            "active" => {
                self.queue_cleanup_skipped_active
                    .fetch_add(1, Ordering::Relaxed);
            }
            "not_idle_enough" => {
                self.queue_cleanup_skipped_not_idle_enough
                    .fetch_add(1, Ordering::Relaxed);
            }
            "pending_settles" => {
                self.queue_cleanup_skipped_pending_settles
                    .fetch_add(1, Ordering::Relaxed);
            }
            "has_broker_deliveries" => {
                self.queue_cleanup_skipped_has_broker_deliveries
                    .fetch_add(1, Ordering::Relaxed);
            }
            "has_inflight" => {
                self.queue_cleanup_skipped_has_inflight
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    #[inline]
    pub fn queue_cleanup_storage_outcome(&self, outcome: &str) {
        self.queue_cleanup_attempts.fetch_add(1, Ordering::Relaxed);
        match outcome {
            "evicted" => {
                self.queue_cleanup_storage_evicted
                    .fetch_add(1, Ordering::Relaxed);
            }
            "not_present" => {
                self.queue_cleanup_storage_not_present
                    .fetch_add(1, Ordering::Relaxed);
            }
            "not_materialized" => {
                self.queue_cleanup_storage_not_materialized
                    .fetch_add(1, Ordering::Relaxed);
            }
            "has_inflight" => {
                self.queue_cleanup_storage_has_inflight
                    .fetch_add(1, Ordering::Relaxed);
            }
            "race_lost" => {
                self.queue_cleanup_storage_race_lost
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn snapshot(&self) -> BrokerStatsSnapshot {
        let window = 60;

        let delivered = self.delivered.ops.sum_last(window);
        let published = self.published.ops.sum_last(window);
        let acked = self.acked.ops.sum_last(window);
        let redelivered = self.redelivered.ops.sum_last(window);
        let expired = self.expired.ops.sum_last(window);

        BrokerStatsSnapshot {
            delivered_per_sec_1m: delivered as f64 / window as f64,
            published_per_sec_1m: published as f64 / window as f64,
            acked_per_sec_1m: acked as f64 / window as f64,
            redelivered_per_sec_1m: redelivered as f64 / window as f64,
            expired_per_sec_1m: expired as f64 / window as f64,

            avg_publish_batch_size: {
                let total = self.publish_batches.items_total.load(Ordering::Relaxed);
                let batches = self.publish_batches.batches.total.load(Ordering::Relaxed);
                if batches == 0 {
                    None
                } else {
                    Some(total as f64 / batches as f64)
                }
            },

            avg_ack_batch_size: {
                let total = self.ack_batches.items_total.load(Ordering::Relaxed);
                let batches = self.ack_batches.batches.total.load(Ordering::Relaxed);
                if batches == 0 {
                    None
                } else {
                    Some(total as f64 / batches as f64)
                }
            },

            total_published: self.published.total.load(Ordering::Relaxed),
            total_delivered: self.delivered.total.load(Ordering::Relaxed),
            total_acked: self.acked.total.load(Ordering::Relaxed),
            total_redelivered: self.redelivered.total.load(Ordering::Relaxed),
            total_expired: self.expired.total.load(Ordering::Relaxed),
            queue_cleanup: QueueCleanupStatsSnapshot {
                attempts_total: self.queue_cleanup_attempts.load(Ordering::Relaxed),
                storage_evicted_total: self.queue_cleanup_storage_evicted.load(Ordering::Relaxed),
                storage_not_materialized_total: self
                    .queue_cleanup_storage_not_materialized
                    .load(Ordering::Relaxed),
                storage_not_present_total: self
                    .queue_cleanup_storage_not_present
                    .load(Ordering::Relaxed),
                storage_has_inflight_total: self
                    .queue_cleanup_storage_has_inflight
                    .load(Ordering::Relaxed),
                storage_race_lost_total: self
                    .queue_cleanup_storage_race_lost
                    .load(Ordering::Relaxed),
                skipped_not_tracked_total: self
                    .queue_cleanup_skipped_not_tracked
                    .load(Ordering::Relaxed),
                skipped_active_total: self.queue_cleanup_skipped_active.load(Ordering::Relaxed),
                skipped_not_idle_enough_total: self
                    .queue_cleanup_skipped_not_idle_enough
                    .load(Ordering::Relaxed),
                skipped_pending_settles_total: self
                    .queue_cleanup_skipped_pending_settles
                    .load(Ordering::Relaxed),
                skipped_has_broker_deliveries_total: self
                    .queue_cleanup_skipped_has_broker_deliveries
                    .load(Ordering::Relaxed),
                skipped_has_inflight_total: self
                    .queue_cleanup_skipped_has_inflight
                    .load(Ordering::Relaxed),
            },
        }
    }
}

#[derive(Serialize)]
pub struct QueueCleanupStatsSnapshot {
    pub attempts_total: u64,
    pub storage_evicted_total: u64,
    pub storage_not_materialized_total: u64,
    pub storage_not_present_total: u64,
    pub storage_has_inflight_total: u64,
    pub storage_race_lost_total: u64,
    pub skipped_not_tracked_total: u64,
    pub skipped_active_total: u64,
    pub skipped_not_idle_enough_total: u64,
    pub skipped_pending_settles_total: u64,
    pub skipped_has_broker_deliveries_total: u64,
    pub skipped_has_inflight_total: u64,
}

#[derive(Serialize)]
pub struct BrokerStatsSnapshot {
    pub delivered_per_sec_1m: f64,
    pub published_per_sec_1m: f64,
    pub acked_per_sec_1m: f64,
    pub redelivered_per_sec_1m: f64,
    pub expired_per_sec_1m: f64,

    pub avg_publish_batch_size: Option<f64>,
    pub avg_ack_batch_size: Option<f64>,

    pub total_published: u64,
    pub total_delivered: u64,
    pub total_acked: u64,
    pub total_redelivered: u64,
    pub total_expired: u64,
    pub queue_cleanup: QueueCleanupStatsSnapshot,
}

pub struct TcpStats {
    pub connections: OpStats,
    pub disconnections: OpStats,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub errors: AtomicU64,
    pub resume_new: AtomicU64,
    pub resume_accepted: AtomicU64,
    pub resume_rejected: AtomicU64,
    pub reconnect_grace_entered: AtomicU64,
    pub reconnect_grace_expired: AtomicU64,
    pub reconcile_requests: AtomicU64,
    pub reconcile_kept: AtomicU64,
    pub reconcile_restored: AtomicU64,
    pub reconcile_client_closed: AtomicU64,
    pub reconcile_server_dropped: AtomicU64,
    pub reconcile_mismatched: AtomicU64,
    pub reconcile_restore_failed: AtomicU64,
}

impl TcpStats {
    pub fn new(buckets: usize) -> Arc<Self> {
        Arc::new(Self {
            connections: OpStats::new(buckets),
            disconnections: OpStats::new(buckets),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            resume_new: AtomicU64::new(0),
            resume_accepted: AtomicU64::new(0),
            resume_rejected: AtomicU64::new(0),
            reconnect_grace_entered: AtomicU64::new(0),
            reconnect_grace_expired: AtomicU64::new(0),
            reconcile_requests: AtomicU64::new(0),
            reconcile_kept: AtomicU64::new(0),
            reconcile_restored: AtomicU64::new(0),
            reconcile_client_closed: AtomicU64::new(0),
            reconcile_server_dropped: AtomicU64::new(0),
            reconcile_mismatched: AtomicU64::new(0),
            reconcile_restore_failed: AtomicU64::new(0),
        })
    }

    #[inline]
    pub fn connection_opened(&self) {
        self.connections.incr();
    }

    #[inline]
    pub fn connection_closed(&self) {
        self.disconnections.incr();
    }

    #[inline]
    pub fn bytes_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn bytes_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn resume_new(&self) {
        self.resume_new.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn resume_accepted(&self) {
        self.resume_accepted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn resume_rejected(&self) {
        self.resume_rejected.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconnect_grace_entered(&self) {
        self.reconnect_grace_entered.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconnect_grace_expired(&self) {
        self.reconnect_grace_expired.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_request(&self) {
        self.reconcile_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_kept(&self) {
        self.reconcile_kept.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_restored(&self) {
        self.reconcile_restored.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_client_closed(&self) {
        self.reconcile_client_closed.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_server_dropped(&self) {
        self.reconcile_server_dropped
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_mismatched(&self) {
        self.reconcile_mismatched.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn reconcile_restore_failed(&self) {
        self.reconcile_restore_failed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TcpStatsSnapshot {
        let window = 60;

        let connections = self.connections.ops.sum_last(window);
        let disconnections = self.disconnections.ops.sum_last(window);

        let bytes_in = self.bytes_in.load(Ordering::Relaxed);
        let bytes_out = self.bytes_out.load(Ordering::Relaxed);

        // crude but honest: derive rates from totals
        // (can switch to rolling counters later if needed)
        TcpStatsSnapshot {
            connections_per_sec_1m: connections as f64 / window as f64,
            disconnections_per_sec_1m: disconnections as f64 / window as f64,
            total_connections: self.connections.total.load(Ordering::Relaxed),

            bytes_in_per_sec_1m: bytes_in as f64 / window as f64,
            bytes_out_per_sec_1m: bytes_out as f64 / window as f64,

            total_bytes_in: bytes_in,
            total_bytes_out: bytes_out,

            errors_total: self.errors.load(Ordering::Relaxed),

            resume_new_total: self.resume_new.load(Ordering::Relaxed),
            resume_accepted_total: self.resume_accepted.load(Ordering::Relaxed),
            resume_rejected_total: self.resume_rejected.load(Ordering::Relaxed),
            reconnect_grace_entered_total: self.reconnect_grace_entered.load(Ordering::Relaxed),
            reconnect_grace_expired_total: self.reconnect_grace_expired.load(Ordering::Relaxed),
            reconcile_requests_total: self.reconcile_requests.load(Ordering::Relaxed),
            reconcile_kept_total: self.reconcile_kept.load(Ordering::Relaxed),
            reconcile_restored_total: self.reconcile_restored.load(Ordering::Relaxed),
            reconcile_client_closed_total: self.reconcile_client_closed.load(Ordering::Relaxed),
            reconcile_server_dropped_total: self.reconcile_server_dropped.load(Ordering::Relaxed),
            reconcile_mismatched_total: self.reconcile_mismatched.load(Ordering::Relaxed),
            reconcile_restore_failed_total: self.reconcile_restore_failed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
pub struct TcpStatsSnapshot {
    pub connections_per_sec_1m: f64,
    pub disconnections_per_sec_1m: f64,
    pub total_connections: u64,

    pub bytes_in_per_sec_1m: f64,
    pub bytes_out_per_sec_1m: f64,

    pub total_bytes_in: u64,
    pub total_bytes_out: u64,

    pub errors_total: u64,

    pub resume_new_total: u64,
    pub resume_accepted_total: u64,
    pub resume_rejected_total: u64,
    pub reconnect_grace_entered_total: u64,
    pub reconnect_grace_expired_total: u64,

    pub reconcile_requests_total: u64,
    pub reconcile_kept_total: u64,
    pub reconcile_restored_total: u64,
    pub reconcile_client_closed_total: u64,
    pub reconcile_server_dropped_total: u64,
    pub reconcile_mismatched_total: u64,
    pub reconcile_restore_failed_total: u64,
}

pub struct SystemStats {
    sys: RwLock<System>,
    pid: Option<sysinfo::Pid>,
}

impl SystemStats {
    pub fn new() -> Arc<Self> {
        let pid = sysinfo::get_current_pid().ok();
        let mut sys = System::new_with_specifics(RefreshKind::nothing());
        if let Some(pid) = pid {
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                false,
                process_stats_refresh_kind(),
            );
        }
        Arc::new(Self {
            sys: RwLock::new(sys),
            pid,
        })
    }

    pub fn snapshot(&self) -> SystemSnapshot {
        let Some(pid) = self.pid else {
            return SystemSnapshot {
                rss_mb: 0.,
                cpu: 0.,
            };
        };
        self.sys.write().refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            process_stats_refresh_kind(),
        );
        let sys = self.sys.read();
        let (rss_mb, cpu) = if let Some(p) = sys.process(pid) {
            // sysinfo reports bytes; publish true megabytes.
            (p.memory() as f64 / (1024.0 * 1024.0), p.cpu_usage())
        } else {
            (0., 0.)
        };

        SystemSnapshot { rss_mb, cpu }
    }
}

fn process_stats_refresh_kind() -> ProcessRefreshKind {
    ProcessRefreshKind::nothing().with_cpu().with_memory()
}

#[derive(Serialize)]
pub struct SystemSnapshot {
    pub rss_mb: f64,
    pub cpu: f32,
}

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

pub struct MetricsHandle {
    metrics: Metrics,
    runtime: MetricsRuntime,
}

impl MetricsHandle {
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
}

struct MetricsInner {
    pub storage: Arc<StorageStats>,
    pub broker: Arc<BrokerStats>,
    pub tcp: Arc<TcpStats>,
    pub conn: Arc<ConnectionStats>,
    pub sys: Arc<SystemStats>,
    // later: protocol, client, etc
}

impl Metrics {
    pub fn new(buckets: usize) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                storage: StorageStats::new(buckets),
                broker: BrokerStats::new(buckets),
                tcp: TcpStats::new(buckets),
                conn: ConnectionStats::new(),
                sys: SystemStats::new(),
            }),
        }
    }

    pub fn start(self, config: MetricsConfig, interval: Duration) -> MetricsHandle {
        let runtime = MetricsRuntime::start(self.clone(), config, interval);
        MetricsHandle {
            metrics: self,
            runtime,
        }
    }

    pub fn storage(&self) -> Arc<StorageStats> {
        self.inner.storage.clone()
    }

    pub fn broker(&self) -> Arc<BrokerStats> {
        self.inner.broker.clone()
    }

    pub fn tcp(&self) -> Arc<TcpStats> {
        self.inner.tcp.clone()
    }

    pub fn connections(&self) -> Arc<ConnectionStats> {
        self.inner.conn.clone()
    }

    pub fn system(&self) -> Arc<SystemStats> {
        self.inner.sys.clone()
    }
}

pub struct MetricsRuntime {
    shutdown: ShutdownSignal,
    handles: Vec<JoinHandle<()>>,
}

impl MetricsRuntime {
    pub fn start(metrics: Metrics, config: MetricsConfig, interval: Duration) -> Self {
        let shutdown = ShutdownSignal::new();
        let mut handles = Vec::new();
        let dur = interval;

        if config.log_storage {
            handles.push(tokio::spawn(run_storage_logger(
                metrics.storage(),
                dur,
                shutdown.subscribe(),
            )));
        }

        if config.log_broker {
            handles.push(tokio::spawn(run_broker_logger(
                metrics.broker(),
                dur,
                shutdown.subscribe(),
            )));
        }

        if config.log_tcp {
            handles.push(tokio::spawn(run_tcp_logger(
                metrics.tcp(),
                dur,
                shutdown.subscribe(),
            )));
        }

        Self { shutdown, handles }
    }

    pub async fn shutdown(self) {
        self.shutdown.signal();
        for h in self.handles {
            let _ = h.await;
        }
    }
}

pub struct MetricsConfig {
    pub log_storage: bool,
    pub log_broker: bool,
    pub log_tcp: bool,
}

#[inline]
fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

#[inline]
fn fmt2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

pub async fn run_storage_logger(
    stats: Arc<StorageStats>,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let snap = stats.snapshot();
                tracing::info!(
                    reads_s = round1(snap.reads_per_sec_1m),
                    writes_s = round1(snap.writes_per_sec_1m),

                    read_ms = snap.avg_read_latency_ms.map(fmt2),
                    write_ms = snap.avg_write_latency_ms.map(fmt2),
                    ack_ms = snap.avg_ack_latency_ms.map(fmt2),
                    append_batch_ms = snap.avg_append_batch_latency_ms.map(fmt2),
                    ack_batch_ms = snap.avg_ack_batch_latency_ms.map(fmt2),

                    total_reads = snap.total_reads,
                    total_writes = snap.total_writes,
                    "[storage]"
                );
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

pub async fn run_broker_logger(
    stats: Arc<BrokerStats>,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    // TODO: add mark inflight batch?
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let snap = stats.snapshot();
                tracing::info!(
                    delivered_s = round1(snap.delivered_per_sec_1m),
                    published_s = round1(snap.published_per_sec_1m),
                    acked_s = round1(snap.acked_per_sec_1m),

                    pub_batch_size = snap.avg_publish_batch_size.map(fmt2),
                    ack_batch_size = snap.avg_ack_batch_size.map(fmt2),

                    redelivered_s = round1(snap.redelivered_per_sec_1m),
                    expired_s = round1(snap.expired_per_sec_1m),
                    total_published = snap.total_published,
                    total_delivered = snap.total_delivered,
                    total_acked = snap.total_acked,
                    total_expired = snap.total_expired,
                    total_redelivered = snap.total_redelivered,
                    "[broker]"
                );
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

pub async fn run_tcp_logger(
    stats: Arc<TcpStats>,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let snap = stats.snapshot();
                tracing::info!(
                    connections_s = round1(snap.connections_per_sec_1m),
                    disconnections_s = round1(snap.disconnections_per_sec_1m),
                    bytes_in_s = round1(snap.bytes_in_per_sec_1m),
                    bytes_out_s = round1(snap.bytes_out_per_sec_1m),
                    total_connections = snap.total_connections,
                    total_bytes_in = snap.total_bytes_in,
                    total_bytes_out = snap.total_bytes_out,
                    errors = snap.errors_total,
                    resume_new = snap.resume_new_total,
                    resume_accepted = snap.resume_accepted_total,
                    resume_rejected = snap.resume_rejected_total,
                    reconnect_grace_entered = snap.reconnect_grace_entered_total,
                    reconnect_grace_expired = snap.reconnect_grace_expired_total,
                    reconcile_requests = snap.reconcile_requests_total,
                    reconcile_kept = snap.reconcile_kept_total,
                    reconcile_restored = snap.reconcile_restored_total,
                    reconcile_client_closed = snap.reconcile_client_closed_total,
                    reconcile_server_dropped = snap.reconcile_server_dropped_total,
                    reconcile_mismatched = snap.reconcile_mismatched_total,
                    reconcile_restore_failed = snap.reconcile_restore_failed_total,
                    "[tcp]"
                );
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

pub struct ShutdownSignal {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }

    pub fn subscribe(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }

    pub async fn recv(&mut self) {
        while !*self.rx.borrow() {
            if self.rx.changed().await.is_err() {
                break;
            }
        }
    }

    pub fn signal(&self) {
        let _ = self.tx.send(true);
    }
}

type ConnId = Uuid;
type SubId = Uuid;
type Topic = String;
type Group = String;

struct SubInfo {
    // Duplicates the subs-map key. Kept (not read today) for the future
    // restart-reconnect reconciliation feature, which needs the identity carried
    // on the value so a SubInfo can be matched back to a reconnecting client
    // independently of the map.
    #[allow(dead_code)]
    sub_id: SubId,
    topic: Topic,
    group: Option<Group>,
    connected_at: Instant,
    auto_ack: bool,
}

struct ConnectionState {
    // Duplicates the connections-map key. Kept for the same reason as
    // SubInfo::sub_id (restart-reconnect reconciliation).
    #[allow(dead_code)]
    conn_id: ConnId,
    peer: SocketAddr,
    connected_at: Instant,
    authenticated: bool,
    subs: DashMap<SubId, SubInfo>,
    // Messages this connection published. Arc so the frame loop clones it
    // once at connection start and increments without touching the map.
    published: Arc<AtomicU64>,
}

impl ConnectionState {
    pub fn new(
        conn_id: ConnId,
        peer: SocketAddr,
        connected_at: Instant,
        authenticated: bool,
    ) -> Self {
        Self {
            conn_id,
            peer,
            connected_at,
            authenticated,
            subs: DashMap::new(),
            published: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn add_sub(
        &self,
        topic: Topic,
        group: Option<Group>,
        connected_at: Instant,
        auto_ack: bool,
    ) -> SubId {
        let sub_id = Uuid::now_v7();
        self.subs.insert(
            sub_id,
            SubInfo {
                sub_id,
                topic,
                group,
                connected_at,
                auto_ack,
            },
        );

        sub_id
    }

    pub fn remove_sub(&self, key: &SubId) -> bool {
        self.subs.remove(key).is_some()
    }
}

pub struct ConnectionStats {
    connections: DashMap<ConnId, ConnectionState>,
}

impl ConnectionStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: DashMap::new(),
        })
    }

    pub fn add_connection(
        &self,
        peer: SocketAddr,
        connected_at: Instant,
        authenticated: bool,
    ) -> ConnId {
        let conn_id = Uuid::now_v7();
        self.connections.insert(
            conn_id,
            ConnectionState::new(conn_id, peer, connected_at, authenticated),
        );

        conn_id
    }

    /// The connection's publish counter. The frame loop clones it once at
    /// connection start, so the per-publish increment is a bare atomic.
    pub fn publish_counter(&self, conn_id: &ConnId) -> Option<Arc<AtomicU64>> {
        self.connections
            .get(conn_id)
            .map(|conn| conn.published.clone())
    }

    pub fn set_connection_auth(&self, key: &Uuid, auth: bool) -> bool {
        if let Some(mut conn) = self.connections.get_mut(key) {
            conn.authenticated = auth;
            true
        } else {
            false
        }
    }

    pub fn add_sub(
        &self,
        conn_id: &ConnId,
        topic: Topic,
        group: Option<Group>,
        connected_at: Instant,
        auto_ack: bool,
    ) -> Option<SubId> {
        if let Some(conn) = self.connections.get(conn_id) {
            let key = conn.add_sub(topic, group, connected_at, auto_ack);
            return Some(key);
        }

        None
    }

    pub fn remove_connection(&self, conn_id: &ConnId) -> bool {
        self.connections.remove(conn_id).is_some()
    }

    pub fn remove_sub(&self, conn_id: &ConnId, key: &SubId) -> bool {
        if let Some(conn) = self.connections.get(conn_id) {
            return conn.remove_sub(key);
        }

        false
    }

    pub fn open_connections(&self) -> usize {
        self.connections.len()
    }

    pub fn open_subscriptions(&self) -> usize {
        self.connections.iter().map(|c| c.subs.len()).sum()
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let mut conns = Vec::new();

        for c in self.connections.iter() {
            let uptime = c.connected_at.elapsed().as_secs();
            conns.push(json!({
                "id": c.key().to_string(),
                "peer": c.peer.to_string(),
                "uptime": uptime,
                "authenticated": c.authenticated,
                "subs": c.subs.len(),
                "published": c.published.load(Ordering::Relaxed),
            }));
        }

        serde_json::Value::Array(conns)
    }

    pub fn snapshot_subs(&self) -> serde_json::Value {
        let mut subs = Vec::new();

        for c in self.connections.iter() {
            for s in c.subs.iter() {
                subs.push(json!({
                    "conn_id": c.key().to_string(),
                    "sub_id": s.key().to_string(),
                    "topic": s.topic,
                    "group": s.group,
                    "uptime": s.connected_at.elapsed().as_secs(),
                    "auto_ack": s.auto_ack
                }));
            }
        }

        serde_json::Value::Array(subs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_publish_counter_rides_the_snapshot() {
        let stats = ConnectionStats::new();
        let conn =
            stats.add_connection("127.0.0.1:9000".parse().unwrap(), Instant::now(), true);
        let counter = stats.publish_counter(&conn).expect("known connection");
        counter.fetch_add(41, Ordering::Relaxed);
        counter.fetch_add(1, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        let entry = &snapshot.as_array().expect("array")[0];
        assert_eq!(entry["published"], 42);

        // Unknown connections yield no counter.
        assert!(stats.publish_counter(&Uuid::now_v7()).is_none());
    }
}
