//! Rate limiting for both sides of the demo. Producers pace publishes with
//! Poisson-style inter-arrival gaps (bursty up close, the target rate on
//! average). Consumers spend a bounded service time per message, so a rush
//! visibly outruns them and backlog builds instead of draining instantly.

use std::time::Duration;

/// Sleep one Poisson inter-arrival gap for `per_sec * multiplier` events/s.
/// A multiplier of zero (deep night) parks for a beat instead of dividing by
/// zero, so the loop stays responsive to the next rhythm change.
pub async fn producer_gap(per_sec: f64, multiplier: f64) {
    let rate = (per_sec * multiplier).max(0.0);
    if rate < 0.001 {
        tokio::time::sleep(Duration::from_millis(750)).await;
        return;
    }
    // Inverse-transform sampling of an exponential inter-arrival time,
    // capped so a rhythm change is picked up within a few seconds.
    let u = fastrand::f64().clamp(1e-9, 1.0 - 1e-9);
    let gap = (-(1.0 - u).ln() / rate).min(5.0);
    tokio::time::sleep(Duration::from_secs_f64(gap)).await;
}

/// Spend a jittered per-message service time, the consumer-side rate limit.
pub async fn service_time(min_ms: u64, max_ms: u64) {
    let ms = if max_ms > min_ms {
        min_ms + fastrand::u64(..(max_ms - min_ms))
    } else {
        min_ms
    };
    tokio::time::sleep(Duration::from_millis(ms)).await;
}
