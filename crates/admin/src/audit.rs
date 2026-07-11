//! The control-plane activity feed: a bounded in-memory ring of the things
//! operators and the cluster did to this broker (declares, deletes, drains,
//! settings changes) plus derived transitions (attention items raised and
//! resolved, membership changes). In-memory and reset on restart, like the
//! history samples: the durable trail is the broker log.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::Serialize;

use crate::routes::check_auth;
use crate::server::AdminServer;

/// Ring capacity. At a typical operator pace this holds days.
const CAPACITY: usize = 512;

/// How often the watcher diffs attention items and cluster membership into
/// feed entries. Coarser than the page tick: these transitions are rare.
pub const WATCH_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize)]
pub struct AuditEntry {
    pub at_ms: u64,
    /// Short kind slug, e.g. `queue_declared`, `drain`, `attention_raised`.
    pub kind: String,
    /// `info`, `warning`, or `critical` - drives the feed's color.
    pub severity: String,
    /// What the entry is about (a topic, a node id, a user name).
    pub subject: String,
    /// One plain sentence for the operator.
    pub detail: String,
}

/// The bounded feed. Cheap to record into from any route.
#[derive(Default)]
pub struct AuditLog {
    entries: Mutex<VecDeque<AuditEntry>>,
}

impl AuditLog {
    pub fn record(
        &self,
        kind: &str,
        severity: &str,
        subject: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let entry = AuditEntry {
            at_ms: fibril_util::unix_millis(),
            kind: kind.to_string(),
            severity: severity.to_string(),
            subject: subject.into(),
            detail: detail.into(),
        };
        let mut entries = self.entries.lock().expect("audit ring poisoned");
        if entries.len() == CAPACITY {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Newest first, ready to render.
    pub fn snapshot(&self) -> Vec<AuditEntry> {
        self.entries
            .lock()
            .expect("audit ring poisoned")
            .iter()
            .rev()
            .cloned()
            .collect()
    }
}

#[derive(Serialize)]
pub struct AuditResponse {
    pub entries: Vec<AuditEntry>,
}

pub(crate) fn audit_payload(server: &AdminServer) -> AuditResponse {
    AuditResponse {
        entries: server.audit.snapshot(),
    }
}

/// `GET /admin/api/audit`: the feed, newest first.
pub async fn audit(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Json<AuditResponse>, StatusCode> {
    check_auth(&server, &headers).await?;
    Ok(Json(audit_payload(&server)))
}

/// One watcher pass: diff the attention items and the cluster's node set
/// against the last pass and record what changed. Public as a seam so tests
/// drive passes deterministically.
pub async fn watch_once(server: &AdminServer, seen: &mut WatchState) {
    let attention = crate::attention::attention_payload(server).await;
    let current: HashSet<(String, String, String)> = attention
        .items
        .iter()
        .map(|item| {
            (
                item.kind.to_string(),
                item.subject.clone(),
                item.severity.to_string(),
            )
        })
        .collect();
    if let Some(previous) = &seen.attention {
        for (kind, subject, severity) in current.difference(previous) {
            server.audit.record(
                "attention_raised",
                severity,
                subject.clone(),
                format!("Needs attention: {}", kind.replace('_', " ")),
            );
        }
        for (kind, subject, _) in previous.difference(&current) {
            server.audit.record(
                "attention_resolved",
                "info",
                subject.clone(),
                format!("Resolved: {}", kind.replace('_', " ")),
            );
        }
    }
    seen.attention = Some(current);

    if let Some(coordination) = &server.coordination {
        let nodes: HashSet<String> = coordination
            .snapshot()
            .nodes
            .keys()
            .map(|id| id.to_string())
            .collect();
        if let Some(previous) = &seen.nodes {
            for id in nodes.difference(previous) {
                server
                    .audit
                    .record("node_joined", "info", id.clone(), "Joined the cluster");
            }
            for id in previous.difference(&nodes) {
                server.audit.record(
                    "node_left",
                    "warning",
                    id.clone(),
                    "Left the cluster view",
                );
            }
        }
        seen.nodes = Some(nodes);
    }
}

/// What the watcher saw last pass. `None` fields mean "first pass": nothing is
/// recorded then, so a broker restart does not replay every standing condition
/// as if it had just been raised.
#[derive(Default)]
pub struct WatchState {
    attention: Option<HashSet<(String, String, String)>>,
    nodes: Option<HashSet<String>>,
}

/// Drive the watcher forever. Spawned next to the history sampler.
pub async fn run_watcher(server: Arc<AdminServer>) {
    let mut seen = WatchState::default();
    let mut tick = tokio::time::interval(WATCH_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        watch_once(&server, &mut seen).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_is_bounded_and_newest_first() {
        let log = AuditLog::default();
        for i in 0..(CAPACITY + 10) {
            log.record("queue_declared", "info", format!("t{i}"), "Declared");
        }
        let entries = log.snapshot();
        assert_eq!(entries.len(), CAPACITY);
        assert_eq!(entries[0].subject, format!("t{}", CAPACITY + 9));
        assert_eq!(entries[CAPACITY - 1].subject, "t10");
    }
}
