//! In-memory time series for the dashboard.
//!
//! The broker exposes point-in-time counters and one-minute rates, but the
//! dashboard also wants recent history: throughput sparklines and, most
//! usefully, backlog over time (the "am I draining or falling behind" signal a
//! single depth gauge cannot show). A background task samples the counters at a
//! fixed cadence into a bounded ring buffer, served from `/admin/api/history`.
//! Alongside the cluster series it keeps a small per-queue depth series, which
//! feeds the depth sparkline on each queue row.
//!
//! The rings are per-broker and live in memory only, so they start empty and
//! reset on restart. That is deliberate: this is an at-a-glance operational
//! aid, not a metrics store. Point Prometheus (`/metrics`) at the broker for
//! durable history.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::Serialize;

use crate::routes::check_auth;
use crate::server::AdminServer;

/// How often the sampler records a point, and how many points it keeps. Five
/// seconds over 360 points is the last 30 minutes.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
const CAPACITY: usize = 360;

/// Per-queue series are bounded twice: at most this many queues are tracked
/// (first come, first tracked), and each series is downsampled to
/// [`SERVE_MAX_POINTS`] on serve so a large window stays a sparkline-sized
/// payload.
const MAX_TRACKED_QUEUES: usize = 512;
const SERVE_MAX_POINTS: usize = 72;

/// One recorded instant: the rates the broker reports plus aggregate depth.
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// Unix seconds when the sample was taken.
    pub at: i64,
    pub published_per_sec: f64,
    pub delivered_per_sec: f64,
    pub completed_per_sec: f64,
    /// Ready (undelivered) messages summed across every queue partition.
    pub backlog: u64,
    /// Leased-but-not-yet-completed messages summed across partitions.
    pub inflight: u64,
    pub connections: u64,
    pub subscriptions: u64,
}

/// One per-queue instant: depth and leased work for a (topic, group) queue.
#[derive(Debug, Clone, Serialize)]
pub struct QueuePoint {
    pub at: i64,
    pub depth: u64,
    pub leased: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueHistory {
    pub topic: String,
    pub group: Option<String>,
    pub samples: Vec<QueuePoint>,
}

#[derive(Default)]
struct Rings {
    cluster: VecDeque<Sample>,
    queues: BTreeMap<(String, Option<String>), VecDeque<QueuePoint>>,
}

/// The bounded rings, shared between the sampler task and the route.
#[derive(Clone, Default)]
pub struct History {
    inner: Arc<Mutex<Rings>>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, sample: Sample, queue_points: Vec<((String, Option<String>), QueuePoint)>) {
        let mut rings = self.lock();
        if rings.cluster.len() == CAPACITY {
            rings.cluster.pop_front();
        }
        rings.cluster.push_back(sample);

        // Queues absent from this tick were deleted (or erroring); drop their
        // series rather than serving a line frozen at its last value. A queue
        // that comes back simply starts a fresh series.
        let seen: std::collections::HashSet<&(String, Option<String>)> =
            queue_points.iter().map(|(key, _)| key).collect();
        rings.queues.retain(|key, _| seen.contains(key));

        for (key, point) in queue_points {
            if !rings.queues.contains_key(&key) && rings.queues.len() >= MAX_TRACKED_QUEUES {
                continue;
            }
            let series = rings.queues.entry(key).or_default();
            if series.len() == CAPACITY {
                series.pop_front();
            }
            series.push_back(point);
        }
    }

    fn snapshot(&self) -> (Vec<Sample>, Vec<QueueHistory>) {
        let rings = self.lock();
        let cluster = rings.cluster.iter().cloned().collect();
        let queues = rings
            .queues
            .iter()
            .map(|((topic, group), series)| QueueHistory {
                topic: topic.clone(),
                group: group.clone(),
                samples: downsample(series),
            })
            .collect();
        (cluster, queues)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Rings> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Every stride-th point, chosen so the newest point is always included.
fn downsample(series: &VecDeque<QueuePoint>) -> Vec<QueuePoint> {
    let len = series.len();
    if len <= SERVE_MAX_POINTS {
        return series.iter().cloned().collect();
    }
    let stride = len.div_ceil(SERVE_MAX_POINTS);
    let offset = (len - 1) % stride;
    series
        .iter()
        .enumerate()
        .filter(|(i, _)| i % stride == offset)
        .map(|(_, point)| point.clone())
        .collect()
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

/// Record exactly one sample from the broker's live state. Depth comes from the
/// queue stats snapshot; when that read fails the depths are reported as zero
/// rather than dropping the whole sample, so the rate series stays continuous.
pub async fn run_sampler_once(server: &AdminServer) {
    let broker = server.metrics.broker().snapshot();
    let connections = server.metrics.connections();
    let at = now_unix();

    let mut backlog = 0u64;
    let mut inflight = 0u64;
    let mut queue_points = Vec::new();
    if let Ok(snapshot) = server.storage.queue_stats_snapshot().await {
        for (key, state) in &snapshot.queues {
            if let fibril_metrics::QueueStateSnapshot::Ok {
                ready_count,
                inflight_count,
            } = state
            {
                backlog += *ready_count as u64;
                inflight += *inflight_count as u64;
                queue_points.push((
                    (key.topic.clone(), key.group.clone()),
                    QueuePoint {
                        at,
                        depth: *ready_count as u64,
                        leased: *inflight_count as u64,
                    },
                ));
            }
        }
    }

    let sample = Sample {
        at,
        published_per_sec: broker.published_per_sec_1m,
        delivered_per_sec: broker.delivered_per_sec_1m,
        completed_per_sec: broker.acked_per_sec_1m,
        backlog,
        inflight,
        connections: connections.open_connections() as u64,
        subscriptions: connections.open_subscriptions() as u64,
    };
    server.history.record(sample, queue_points);
}

/// Sample the broker into the history rings forever, on a fixed cadence.
/// Spawned once when the admin server starts.
pub async fn run_sampler(server: Arc<AdminServer>) {
    let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        run_sampler_once(&server).await;
    }
}

#[derive(Serialize)]
pub struct HistoryResponse {
    pub interval_ms: u64,
    pub samples: Vec<Sample>,
    pub queues: Vec<QueueHistory>,
}

/// `GET /admin/api/history`: the recorded time series and their cadence.
pub async fn history(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Json<HistoryResponse>, StatusCode> {
    check_auth(&server, &headers).await?;
    Ok(Json(history_payload(&server)))
}

/// The history time series, shared by the GET route and the events stream.
pub(crate) fn history_payload(server: &AdminServer) -> HistoryResponse {
    let (samples, queues) = server.history.snapshot();
    HistoryResponse {
        interval_ms: SAMPLE_INTERVAL.as_millis() as u64,
        samples,
        queues,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(at: i64) -> Sample {
        Sample {
            at,
            published_per_sec: 0.0,
            delivered_per_sec: 0.0,
            completed_per_sec: 0.0,
            backlog: 0,
            inflight: 0,
            connections: 0,
            subscriptions: 0,
        }
    }

    fn point(at: i64, depth: u64) -> QueuePoint {
        QueuePoint {
            at,
            depth,
            leased: 0,
        }
    }

    fn key(topic: &str) -> (String, Option<String>) {
        (topic.to_string(), None)
    }

    #[test]
    fn ring_keeps_insertion_order_and_evicts_oldest_past_capacity() {
        let history = History::new();
        for i in 0..(CAPACITY as i64 + 10) {
            history.record(sample(i), Vec::new());
        }
        let (samples, _) = history.snapshot();
        assert_eq!(samples.len(), CAPACITY);
        // The first ten were evicted; the window holds the most recent CAPACITY,
        // oldest first.
        assert_eq!(samples.first().unwrap().at, 10);
        assert_eq!(samples.last().unwrap().at, CAPACITY as i64 + 9);
    }

    #[test]
    fn queue_series_track_present_queues_and_drop_deleted_ones() {
        let history = History::new();
        history.record(
            sample(1),
            vec![(key("orders"), point(1, 5)), (key("emails"), point(1, 2))],
        );
        history.record(sample(2), vec![(key("orders"), point(2, 7))]);

        let (_, queues) = history.snapshot();
        // emails vanished from the second tick, so its series was dropped.
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].topic, "orders");
        assert_eq!(queues[0].samples.len(), 2);
        assert_eq!(queues[0].samples[1].depth, 7);
    }

    #[test]
    fn queue_series_downsample_to_a_sparkline_sized_payload() {
        let history = History::new();
        for i in 0..CAPACITY as i64 {
            history.record(sample(i), vec![(key("orders"), point(i, i as u64))]);
        }
        let (_, queues) = history.snapshot();
        let series = &queues[0].samples;
        assert!(series.len() <= SERVE_MAX_POINTS, "{}", series.len());
        // The newest point always survives downsampling.
        assert_eq!(series.last().unwrap().at, CAPACITY as i64 - 1);
    }
}
