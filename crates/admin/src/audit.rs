//! The control-plane activity feed: a bounded in-memory ring of the things
//! operators and the cluster did to this broker (declares, deletes, drains,
//! settings changes) plus derived transitions (attention items raised and
//! resolved, membership changes). In-memory and reset on restart, like the
//! history samples: the durable trail is the broker log.

use std::collections::{HashMap, HashSet, VecDeque};
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
        // Prefer the heartbeat-fresh set: registration survives a crash, so
        // diffing registered nodes would sleep through one.
        let nodes: HashSet<String> = match &server.liveness {
            Some(provider) => provider(),
            None => coordination
                .snapshot()
                .nodes
                .keys()
                .map(|id| id.to_string())
                .collect(),
        };
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
                    "Unreachable - its coordination heartbeat stopped",
                );
            }
        }
        seen.nodes = Some(nodes);
    }

    watch_followers(server, seen);
    watch_stream_lag(server, seen).await;
}

/// Track replication followers across passes: one that is behind and not
/// advancing its offsets for a few passes is stalled - data safety eroding
/// quietly. Feeds the attention rule via [`DerivedConditions`].
fn watch_followers(server: &AdminServer, seen: &mut WatchState) {
    let Some(observability) = &server.broker_queue_observability else {
        return;
    };
    let value = observability();
    let followers = value
        .get("replication_followers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut next: HashMap<String, FollowerWatch> = HashMap::new();
    let mut stalled = Vec::new();
    for follower in &followers {
        let topic = follower.get("topic").and_then(|v| v.as_str()).unwrap_or("?");
        let partition = follower
            .get("partition")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let state = follower.get("state");
        let status = state
            .and_then(|s| s.get("status"))
            .and_then(|s| s.get("status"))
            .and_then(|s| s.as_str())
            .unwrap_or("unknown");
        let cursor = state.map(follower_cursor).unwrap_or_default();
        let subject = format!("{topic}/{partition}");
        let record = advance_follower(seen.followers.get(&subject), status, cursor);
        if record.passes >= FOLLOWER_STALL_PASSES {
            stalled.push(subject.clone());
        }
        next.insert(subject, record);
    }
    seen.followers = next;
    stalled.sort();
    server.derived.set_stalled_followers(stalled);
}

/// Record a feed entry when a stream partition's lag-eviction count grows: a
/// subscriber fell behind the live buffer and went through lag recovery. An
/// event rather than a standing condition, so it lands in the feed, not the
/// attention panel.
async fn watch_stream_lag(server: &AdminServer, seen: &mut WatchState) {
    let Some(streams) = &server.streams else {
        return;
    };
    let stats = streams.stream_stats().await;
    let mut current: HashMap<String, u64> = HashMap::new();
    for entry in serde_json::to_value(&stats)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
    {
        let topic = entry.get("topic").and_then(|v| v.as_str()).unwrap_or("?");
        let partition = entry.get("partition").and_then(|v| v.as_u64()).unwrap_or(0);
        let lag = entry
            .get("lag_evictions")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let subject = format!("{topic}/{partition}");
        if let Some(previous) = seen.stream_lag.as_ref().and_then(|m| m.get(&subject))
            && lag > *previous
        {
            server.audit.record(
                "stream_lagging",
                "warning",
                subject.clone(),
                format!(
                    "A subscriber overflowed the live buffer and lag-recovered ({} total)",
                    lag
                ),
            );
        }
        current.insert(subject, lag);
    }
    seen.stream_lag = Some(current);
}

/// What the watcher saw last pass. `None` fields mean "first pass": nothing is
/// recorded then, so a broker restart does not replay every standing condition
/// as if it had just been raised.
#[derive(Default)]
pub struct WatchState {
    attention: Option<HashSet<(String, String, String)>>,
    nodes: Option<HashSet<String>>,
    /// Per follower status, cursor and passes without progress.
    followers: HashMap<String, FollowerWatch>,
    /// Per stream partition, the last lag-eviction count seen.
    stream_lag: Option<HashMap<String, u64>>,
}

/// A non-caught-up follower gets this many watcher passes (15s each) to move
/// its offsets before it counts as stalled.
const FOLLOWER_STALL_PASSES: u32 = 4;

/// Standing conditions the watcher derives across passes, read by the
/// attention rules (which are otherwise computed fresh per request).
#[derive(Default)]
pub struct DerivedConditions {
    /// Follower subjects ("topic/partition") that are behind and have not
    /// advanced for [`FOLLOWER_STALL_PASSES`] watcher passes.
    stalled_followers: Mutex<Vec<String>>,
}

impl DerivedConditions {
    pub fn stalled_followers(&self) -> Vec<String> {
        self.stalled_followers
            .lock()
            .expect("derived conditions poisoned")
            .clone()
    }

    fn set_stalled_followers(&self, subjects: Vec<String>) {
        *self
            .stalled_followers
            .lock()
            .expect("derived conditions poisoned") = subjects;
    }
}

/// What the watcher remembers about one follower between passes.
#[derive(Debug, Clone, PartialEq)]
struct FollowerWatch {
    status: String,
    /// The worker cursor fields of its state (`kind` plus one queue offset,
    /// or the stream record and cursor-event offsets), compared as a whole.
    cursor: serde_json::Value,
    passes: u32,
}

/// The cursor part of a follower worker state. Any change in it counts as
/// progress, whichever resource kind the worker follows.
fn follower_cursor(state: &serde_json::Value) -> serde_json::Value {
    const CURSOR_KEYS: [&str; 4] = ["kind", "next_offset", "message_next_offset", "event_next_offset"];
    let cursor = CURSOR_KEYS
        .iter()
        .filter_map(|key| state.get(*key).map(|value| (key.to_string(), value.clone())))
        .collect();
    serde_json::Value::Object(cursor)
}

/// Advance one follower's stall accounting: returns the updated record given
/// what this pass observed. Pure so the state machine is unit-testable.
fn advance_follower(
    previous: Option<&FollowerWatch>,
    status: &str,
    cursor: serde_json::Value,
) -> FollowerWatch {
    let passes = match previous {
        _ if status == "caught_up" => 0,
        Some(previous) if previous.cursor == cursor => previous.passes.saturating_add(1),
        _ => 0,
    };
    FollowerWatch {
        status: status.to_string(),
        cursor,
        passes,
    }
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

    fn stream(message: u64, event: u64) -> serde_json::Value {
        follower_cursor(&serde_json::json!({
            "kind": "stream",
            "message_next_offset": message,
            "event_next_offset": event,
            "status": { "status": "pending_retry" },
        }))
    }

    fn queue(next: u64) -> serde_json::Value {
        follower_cursor(&serde_json::json!({
            "kind": "queue",
            "next_offset": next,
            "status": { "status": "pending_retry" },
        }))
    }

    #[test]
    fn follower_stall_counts_passes_without_progress() {
        let start = advance_follower(None, "pending_retry", stream(10, 20));
        assert_eq!(start.passes, 0);
        let stuck = advance_follower(Some(&start), "pending_retry", stream(10, 20));
        assert_eq!(stuck.passes, 1);
        let advanced = advance_follower(Some(&stuck), "pending_retry", stream(11, 20));
        assert_eq!(advanced.passes, 0, "offset progress resets the stall count");
        let caught = advance_follower(Some(&stuck), "caught_up", stream(10, 20));
        assert_eq!(caught.passes, 0, "caught up is never stalled");
    }

    #[test]
    fn queue_follower_stall_follows_its_single_cursor() {
        assert_eq!(queue(7), serde_json::json!({ "kind": "queue", "next_offset": 7 }));
        let start = advance_follower(None, "pending_retry", queue(7));
        let stuck = advance_follower(Some(&start), "pending_retry", queue(7));
        assert_eq!(stuck.passes, 1);
        let advanced = advance_follower(Some(&stuck), "pending_retry", queue(8));
        assert_eq!(advanced.passes, 0);
    }

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
