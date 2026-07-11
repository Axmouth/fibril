//! The attention feed: outcomes that need an operator's eye, computed server
//! side from state the broker already tracks.
//!
//! This is the mechanism behind the dashboard's "needs attention" panel. Rather
//! than the operator scanning every gauge, the broker names the conditions that
//! usually mean something is wrong - a backlog with nobody reading it, a
//! certificate about to expire, a settings document that failed to load, a
//! quarantined partition - each with a link to where it is fixed. Rules are
//! evaluated on read; there is no background state to keep in sync.

use std::collections::HashSet;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::Serialize;
use std::sync::Arc;

use crate::routes::check_auth;
use crate::server::AdminServer;

/// Warn once a served certificate is within this window of expiring.
const CERT_EXPIRY_WARN_SECS: i64 = 30 * 24 * 60 * 60;

/// One thing worth an operator's attention. Severity orders the panel; the
/// action link points at the page where it is resolved.
#[derive(Debug, Clone, Serialize)]
pub struct AttentionItem {
    /// "critical" | "warning". Criticals sort first.
    pub severity: &'static str,
    /// Machine-readable rule id, e.g. "no_consumer" or "cert_expiring".
    pub kind: &'static str,
    /// The subject the item is about (a topic, a certificate, a node).
    pub subject: String,
    /// One plain-language sentence: what is wrong.
    pub headline: String,
    /// Where to go to resolve it.
    pub action_href: &'static str,
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "critical" => 0,
        "warning" => 1,
        _ => 2,
    }
}

/// Topics that currently have at least one live subscriber, from the connection
/// registry's subscription view.
fn subscribed_topics(server: &AdminServer) -> HashSet<String> {
    let subs = server.metrics.connections().snapshot_subs();
    let mut topics = HashSet::new();
    if let Some(array) = subs.as_array() {
        for sub in array {
            if let Some(topic) = sub.get("topic").and_then(|t| t.as_str()) {
                topics.insert(topic.to_string());
            }
        }
    }
    topics
}

/// Conditions read off one queue-stats walk: a queue holding messages no live
/// consumer is reading (work-queue messages persist until consumed or expired,
/// so an unread backlog only grows), and a queue whose state reports an error
/// short of full quarantine.
async fn queue_state_items(server: &AdminServer) -> Vec<AttentionItem> {
    let snapshot = match server.storage.queue_stats_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(_) => return Vec::new(),
    };
    let subscribed = subscribed_topics(server);

    // Sum ready messages per topic across its partitions before deciding, so a
    // multi-partition queue is judged as one queue, not one row per partition.
    let mut ready_by_topic: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut items = Vec::new();
    for (key, state) in &snapshot.queues {
        match state {
            fibril_metrics::QueueStateSnapshot::Ok { ready_count, .. } => {
                *ready_by_topic.entry(key.topic.clone()).or_default() += *ready_count as u64;
            }
            fibril_metrics::QueueStateSnapshot::Error { message } => {
                items.push(AttentionItem {
                    severity: "warning",
                    kind: "queue_state_error",
                    subject: key.topic.clone(),
                    headline: format!("Queue state reports an error: {message}"),
                    action_href: "/admin/queues",
                });
            }
        }
    }

    items.extend(
        ready_by_topic
            .into_iter()
            .filter(|(topic, ready)| *ready > 0 && !subscribed.contains(topic))
            .map(|(topic, ready)| AttentionItem {
                severity: "warning",
                kind: "no_consumer",
                headline: format!(
                    "{ready} message(s) waiting on {topic} with no consumer reading them"
                ),
                subject: topic,
                action_href: "/admin/queues",
            }),
    );
    items
}

/// The data directory's filesystem running out of room. An append-only broker
/// out of disk is its worst failure mode, so this warns early and escalates.
fn disk_items(server: &AdminServer) -> Vec<AttentionItem> {
    let Some(config) = &server.startup_config else {
        return Vec::new();
    };
    let path = std::path::Path::new(&config.data_dir);
    let (Ok(free), Ok(total)) = (fs2::available_space(path), fs2::total_space(path)) else {
        return Vec::new();
    };
    if total == 0 {
        return Vec::new();
    }
    let free_pct = (free as f64 / total as f64) * 100.0;
    let severity = if free_pct < 3.0 {
        "critical"
    } else if free_pct < 10.0 {
        "warning"
    } else {
        return Vec::new();
    };
    let free_gb = free as f64 / (1024.0 * 1024.0 * 1024.0);
    vec![AttentionItem {
        severity,
        kind: "disk_low",
        subject: config.data_dir.clone(),
        headline: format!(
            "The data directory's filesystem is {free_pct:.1}% free ({free_gb:.1} GB) - an append-only broker must not run out of disk"
        ),
        action_href: "/",
    }]
}

/// This node is draining: fine mid-restart, a parked operation if forgotten.
fn draining_items(server: &AdminServer) -> Vec<AttentionItem> {
    let Some(flag) = &server.draining_flag else {
        return Vec::new();
    };
    if !flag.load(std::sync::atomic::Ordering::Relaxed) {
        return Vec::new();
    }
    vec![AttentionItem {
        severity: "warning",
        kind: "draining",
        subject: "this broker".to_string(),
        headline: "This broker is draining - restart it to finish, or it stays parked"
            .to_string(),
        action_href: "/admin/topology",
    }]
}

/// Replication followers that are behind and not advancing (derived across
/// watcher passes): data safety eroding quietly.
fn follower_stall_items(server: &AdminServer) -> Vec<AttentionItem> {
    server
        .derived
        .stalled_followers()
        .into_iter()
        .map(|subject| AttentionItem {
            severity: "warning",
            kind: "replication_stalled",
            headline: format!(
                "Follower replication on {subject} is behind and has stopped advancing"
            ),
            subject,
            action_href: "/admin/queues",
        })
        .collect()
}

/// A queue whose backlog has grown materially across the recent history
/// window DESPITE having consumers: they are not keeping up. The no-consumer
/// rule covers the consumer-less case.
fn backlog_growth_items(server: &AdminServer) -> Vec<AttentionItem> {
    let subscribed = subscribed_topics(server);
    let (_, queues) = server.history.snapshot();
    queues
        .into_iter()
        .filter_map(|queue| {
            if !subscribed.contains(&queue.topic) {
                return None;
            }
            // Judge over up to the last ~10 minutes, requiring at least ~2
            // minutes of samples so a fresh queue is not judged on noise.
            let window = queue.samples.len().min(120);
            if window < 24 {
                return None;
            }
            let recent = &queue.samples[queue.samples.len() - window..];
            let first = recent.first()?.depth;
            let last = recent.last()?.depth;
            if !growth_is_alarming(first, last) {
                return None;
            }
            let subject = match &queue.group {
                Some(group) => format!("{} ({group})", queue.topic),
                None => queue.topic.clone(),
            };
            Some(AttentionItem {
                severity: "warning",
                kind: "backlog_rising",
                headline: format!(
                    "Backlog on {subject} grew from {first} to {last} despite live consumers - they are not keeping up"
                ),
                subject,
                action_href: "/admin/queues",
            })
        })
        .collect()
}

/// Whether a depth move counts as alarming growth: material in absolute terms
/// and clearly beyond noise relative to where it started.
fn growth_is_alarming(first: u64, last: u64) -> bool {
    last > first && last >= 100 && (last - first) >= (first / 2).max(100)
}

/// A served certificate that has expired or is close to it. Reads the optional
/// certificate-info provider the broker wires in when TLS is enabled.
fn cert_items(server: &AdminServer) -> Vec<AttentionItem> {
    let Some(provider) = &server.cert_info else {
        return Vec::new();
    };
    let info = provider();
    let Some(not_after) = info.get("not_after_unix").and_then(|v| v.as_i64()) else {
        return Vec::new();
    };
    let now = now_unix();
    let remaining = not_after - now;
    if remaining > CERT_EXPIRY_WARN_SECS {
        return Vec::new();
    }
    let subject = info
        .get("subject")
        .and_then(|v| v.as_str())
        .unwrap_or("the broker certificate")
        .to_string();
    let (severity, headline) = if remaining <= 0 {
        (
            "critical",
            "The served certificate has expired - rotate it now".to_string(),
        )
    } else {
        (
            "warning",
            format!(
                "The served certificate expires in {} day(s) - rotate the leaf before it does",
                remaining / (24 * 60 * 60)
            ),
        )
    };
    vec![AttentionItem {
        severity,
        kind: "cert_expiring",
        subject,
        headline,
        action_href: "/admin/security",
    }]
}

/// A runtime-settings document that failed to load, so the broker is running on
/// the last good version instead.
fn settings_items(server: &AdminServer) -> Vec<AttentionItem> {
    let Some(settings) = &server.runtime_settings else {
        return Vec::new();
    };
    match settings.load_issue() {
        Some(issue) => vec![AttentionItem {
            severity: "warning",
            kind: "settings_load",
            subject: format!("runtime settings v{}", issue.version),
            headline: format!("Runtime settings did not load: {}", issue.message),
            action_href: "/admin/settings",
        }],
        None => Vec::new(),
    }
}

/// Partitions parked by recovery over an unrecoverable event-log reference.
/// These are the most serious: the partition does not serve until repaired.
fn quarantine_items(server: &AdminServer) -> Vec<AttentionItem> {
    let quarantined = server.storage.quarantined_partitions();
    quarantined
        .iter()
        .filter_map(|info| serde_json::to_value(info).ok())
        .map(|value| {
            let topic = value
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string();
            let partition = value.get("partition").and_then(|v| v.as_u64()).unwrap_or(0);
            let group = value
                .get("group")
                .and_then(|v| v.as_str())
                .map(|g| format!("/{g}"))
                .unwrap_or_default();
            AttentionItem {
                severity: "critical",
                kind: "quarantined",
                subject: format!("{topic}/{partition}{group}"),
                headline: "Partition quarantined by recovery - it will not serve until repaired"
                    .to_string(),
                action_href: "/",
            }
        })
        .collect()
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

#[derive(Serialize)]
pub struct AttentionResponse {
    pub items: Vec<AttentionItem>,
}

/// `GET /admin/api/attention`: everything that currently needs an operator's
/// eye, most severe first.
pub async fn attention(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Json<AttentionResponse>, StatusCode> {
    check_auth(&server, &headers).await?;
    Ok(Json(attention_payload(&server).await))
}

/// The attention feed, shared by the GET route and the events stream.
pub(crate) async fn attention_payload(server: &AdminServer) -> AttentionResponse {
    let mut items = quarantine_items(server);
    items.extend(settings_items(server));
    items.extend(cert_items(server));
    items.extend(queue_state_items(server).await);
    items.extend(disk_items(server));
    items.extend(draining_items(server));
    items.extend(follower_stall_items(server));
    items.extend(backlog_growth_items(server));
    items.sort_by_key(|item| severity_rank(item.severity));
    AttentionResponse { items }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_heuristic_ignores_noise_and_flags_real_growth() {
        assert!(!growth_is_alarming(0, 50), "small absolute depth is noise");
        assert!(!growth_is_alarming(100, 90), "draining is never growth");
        assert!(!growth_is_alarming(1000, 1050), "within noise of the start");
        assert!(growth_is_alarming(0, 150), "fresh material backlog");
        assert!(growth_is_alarming(200, 400), "doubled and material");
    }
}
