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

/// A queue holding messages that no live consumer is reading. Work-queue
/// messages persist until consumed or expired, so an unread backlog only grows.
async fn no_consumer_items(server: &AdminServer) -> Vec<AttentionItem> {
    let snapshot = match server.storage.queue_stats_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(_) => return Vec::new(),
    };
    let subscribed = subscribed_topics(server);

    // Sum ready messages per topic across its partitions before deciding, so a
    // multi-partition queue is judged as one queue, not one row per partition.
    let mut ready_by_topic: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    for (key, state) in &snapshot.queues {
        if let fibril_metrics::QueueStateSnapshot::Ok { ready_count, .. } = state {
            *ready_by_topic.entry(key.topic.clone()).or_default() += *ready_count as u64;
        }
    }

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
        })
        .collect()
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
    let headline = if remaining <= 0 {
        "The served certificate has expired - rotate it now".to_string()
    } else {
        format!(
            "The served certificate expires in {} day(s) - rotate the leaf before it does",
            remaining / (24 * 60 * 60)
        )
    };
    vec![AttentionItem {
        severity: "warning",
        kind: "cert_expiring",
        subject,
        headline,
        action_href: "/admin/settings",
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
    items.extend(no_consumer_items(server).await);
    items.sort_by_key(|item| severity_rank(item.severity));
    AttentionResponse { items }
}
