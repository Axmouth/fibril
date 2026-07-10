//! In-memory time series for the dashboard.
//!
//! The broker exposes point-in-time counters and one-minute rates, but the
//! dashboard also wants recent history: throughput sparklines and, most
//! usefully, backlog over time (the "am I draining or falling behind" signal a
//! single depth gauge cannot show). A background task samples the counters at a
//! fixed cadence into a bounded ring buffer, served from `/admin/api/history`.
//!
//! The ring is per-broker and lives in memory only, so it starts empty and
//! resets on restart. That is deliberate: this is an at-a-glance operational
//! aid, not a metrics store. Point Prometheus (`/metrics`) at the broker for
//! durable history.

use std::collections::VecDeque;
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

/// A bounded ring of samples, shared between the sampler task and the route.
#[derive(Clone, Default)]
pub struct History {
    inner: Arc<Mutex<VecDeque<Sample>>>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, sample: Sample) {
        let mut ring = self.lock();
        if ring.len() == CAPACITY {
            ring.pop_front();
        }
        ring.push_back(sample);
    }

    fn samples(&self) -> Vec<Sample> {
        self.lock().iter().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Sample>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

/// Compute one sample from the broker's live state. Depth comes from the queue
/// stats snapshot; when that read fails the depths are reported as zero rather
/// than dropping the whole sample, so the rate series stays continuous.
async fn take_sample(server: &AdminServer) -> Sample {
    let broker = server.metrics.broker().snapshot();
    let connections = server.metrics.connections();

    let (backlog, inflight) = match server.storage.queue_stats_snapshot().await {
        Ok(snapshot) => snapshot
            .queues
            .values()
            .fold((0u64, 0u64), |acc, state| match state {
                fibril_metrics::QueueStateSnapshot::Ok {
                    ready_count,
                    inflight_count,
                } => (acc.0 + *ready_count as u64, acc.1 + *inflight_count as u64),
                fibril_metrics::QueueStateSnapshot::Error { .. } => acc,
            }),
        Err(_) => (0, 0),
    };

    Sample {
        at: now_unix(),
        published_per_sec: broker.published_per_sec_1m,
        delivered_per_sec: broker.delivered_per_sec_1m,
        completed_per_sec: broker.acked_per_sec_1m,
        backlog,
        inflight,
        connections: connections.open_connections() as u64,
        subscriptions: connections.open_subscriptions() as u64,
    }
}

/// Record exactly one sample. Used by the sampler loop and by tests that need a
/// deterministic point without waiting on the ticker.
pub async fn run_sampler_once(server: &AdminServer) {
    let sample = take_sample(server).await;
    server.history.record(sample);
}

/// Sample the broker into the history ring forever, on a fixed cadence. Spawned
/// once when the admin server starts.
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
}

/// `GET /admin/api/history`: the recorded time series and its cadence.
pub async fn history(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Json<HistoryResponse>, StatusCode> {
    check_auth(&server, &headers).await?;
    Ok(Json(HistoryResponse {
        interval_ms: SAMPLE_INTERVAL.as_millis() as u64,
        samples: server.history.samples(),
    }))
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

    #[test]
    fn ring_keeps_insertion_order_and_evicts_oldest_past_capacity() {
        let history = History::new();
        for i in 0..(CAPACITY as i64 + 10) {
            history.record(sample(i));
        }
        let samples = history.samples();
        assert_eq!(samples.len(), CAPACITY);
        // The first ten were evicted; the window holds the most recent CAPACITY,
        // oldest first.
        assert_eq!(samples.first().unwrap().at, 10);
        assert_eq!(samples.last().unwrap().at, CAPACITY as i64 + 9);
    }
}
