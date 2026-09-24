//! Explicit transport suspicion feeds placement; recovery still grants authority.
use ganglion_core::CoordinationSnapshot;
use ganglion_openraft::PeerTransportFailure;
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, serde::Serialize)]
pub struct EagerSuspect {
    pub node: String,
    pub raft_id: u64,
    pub failure: String,
    pub elapsed_ms: u64,
    pub reconnect_failures: u64,
}

struct Episode {
    transport_since: Instant,
    started: Instant,
    baseline: u64,
    heartbeat: Option<String>,
    process: Option<String>,
}

#[derive(Default)]
pub(crate) struct Detector {
    policy: Option<(bool, u64)>,
    episodes: HashMap<String, Episode>,
}

impl Detector {
    pub fn suspects(
        &mut self,
        enabled: bool,
        grace_ms: u64,
        state: &CoordinationSnapshot,
        failures: &BTreeMap<u64, PeerTransportFailure>,
        local_raft_id: u64,
        now: Instant,
    ) -> Vec<EagerSuspect> {
        if self.policy != Some((enabled, grace_ms)) {
            self.episodes.clear();
            self.policy = Some((enabled, grace_ms));
        }
        if !enabled {
            self.episodes.clear();
            return vec![];
        }
        let mut ids = HashMap::<u64, usize>::new();
        for node in state.nodes.values() {
            if let Some(id) = node
                .labels
                .get(crate::RAFT_ID_LABEL)
                .and_then(|s| s.parse::<u64>().ok())
            {
                *ids.entry(id).or_default() += 1;
            }
        }
        self.episodes.retain(|node, _| {
            state.nodes.get(node).is_some_and(|n| {
                n.labels
                    .get(crate::RAFT_ID_LABEL)
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|id| failures.contains_key(&id))
            })
        });
        let mut suspects = vec![];
        for (node_id, node) in &state.nodes {
            let Some(id) = node
                .labels
                .get(crate::RAFT_ID_LABEL)
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            if id == local_raft_id || ids.get(&id) != Some(&1) {
                continue;
            }
            let Some(failure) = failures.get(&id) else {
                continue;
            };
            let heartbeat = node.labels.get(crate::HEARTBEAT_LABEL).cloned();
            let process = node.labels.get(crate::HISTORY_PROCESS_LABEL).cloned();
            let episode = self
                .episodes
                .entry(node_id.clone())
                .or_insert_with(|| Episode {
                    transport_since: failure.since,
                    started: now,
                    // Immediate mode can observe both failures together after
                    // the transport's immediate reconnect check. Default grace
                    // still requires a later failed attempt across the interval.
                    baseline: failure
                        .attempts
                        .saturating_sub(if grace_ms == 0 { 2 } else { 1 }),
                    heartbeat: heartbeat.clone(),
                    process: process.clone(),
                });
            if episode.transport_since != failure.since
                || episode.heartbeat != heartbeat
                || episode.process != process
            {
                *episode = Episode {
                    transport_since: failure.since,
                    started: now,
                    baseline: failure.attempts,
                    heartbeat,
                    process,
                };
            }
            let attempts = failure.attempts.saturating_sub(episode.baseline);
            // Require a fresh failed reconnect after the whole grace period.
            // A lone error followed by a silent/hung RPC uses the heartbeat TTL.
            if attempts >= 2
                && failure.latest.saturating_duration_since(episode.started)
                    >= Duration::from_millis(grace_ms)
                && now.saturating_duration_since(failure.latest) <= Duration::from_secs(2)
            {
                suspects.push(EagerSuspect {
                    node: node_id.clone(),
                    raft_id: id,
                    failure: format!("{:?}", failure.kind),
                    elapsed_ms: now
                        .saturating_duration_since(episode.started)
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                    reconnect_failures: attempts,
                });
            }
        }
        suspects
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn immediate_mode_accepts_coalesced_probe_but_resets_after_heartbeat() {
        let mut state = CoordinationSnapshot::default();
        let mut node = ganglion_core::NodeInfo::new("b", "127.0.0.1:1", None::<String>);
        node.labels.insert(crate::RAFT_ID_LABEL.into(), "2".into());
        state.nodes.insert("b".into(), node);
        let now = Instant::now();
        let mut failures = BTreeMap::from([(
            2,
            PeerTransportFailure {
                since: now,
                latest: now,
                attempts: 2,
                kind: std::io::ErrorKind::ConnectionRefused,
            },
        )]);
        let mut detector = Detector::default();
        assert_eq!(
            detector.suspects(true, 0, &state, &failures, 1, now).len(),
            1
        );
        state
            .nodes
            .get_mut("b")
            .unwrap()
            .labels
            .insert(crate::HEARTBEAT_LABEL.into(), "1".into());
        assert!(
            detector
                .suspects(true, 0, &state, &failures, 1, now)
                .is_empty()
        );
        failures.get_mut(&2).unwrap().attempts = 3;
        assert!(
            detector
                .suspects(true, 0, &state, &failures, 1, now)
                .is_empty()
        );
        failures.get_mut(&2).unwrap().attempts = 4;
        assert_eq!(
            detector.suspects(true, 0, &state, &failures, 1, now).len(),
            1
        );
        failures.clear();
        assert!(
            detector
                .suspects(true, 0, &state, &failures, 1, now)
                .is_empty()
        );
    }
    #[test]
    fn grace_reconnect_heartbeat_success_and_settings_changes() {
        let mut state = CoordinationSnapshot::default();
        let mut node = ganglion_core::NodeInfo::new("b", "127.0.0.1:1", None::<String>);
        node.labels.insert(crate::RAFT_ID_LABEL.into(), "2".into());
        node.labels
            .insert(crate::HEARTBEAT_LABEL.into(), "100".into());
        state.nodes.insert("b".into(), node);
        let mut detector = Detector::default();
        let now = Instant::now();
        let mut failures = BTreeMap::from([(
            2,
            PeerTransportFailure {
                since: now,
                latest: now,
                attempts: 1,
                kind: std::io::ErrorKind::ConnectionRefused,
            },
        )]);
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, now)
                .is_empty()
        );
        let later = now + Duration::from_millis(600);
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, later)
                .is_empty()
        );
        failures.get_mut(&2).unwrap().attempts = 2;
        failures.get_mut(&2).unwrap().latest = later;
        assert_eq!(
            detector
                .suspects(true, 500, &state, &failures, 1, later)
                .len(),
            1
        );
        assert!(
            detector
                .suspects(false, 500, &state, &failures, 1, later)
                .is_empty()
        );
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, later)
                .is_empty()
        );
        failures.get_mut(&2).unwrap().attempts = 4;
        failures.get_mut(&2).unwrap().latest = later + Duration::from_secs(1);
        let last = failures[&2].latest;
        assert_eq!(
            detector
                .suspects(true, 500, &state, &failures, 1, last)
                .len(),
            1
        );
        state
            .nodes
            .get_mut("b")
            .unwrap()
            .labels
            .insert(crate::HEARTBEAT_LABEL.into(), "101".into());
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, last)
                .is_empty()
        );
        failures.clear();
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, last)
                .is_empty()
        );
        assert!(detector.episodes.is_empty());
    }
    #[test]
    fn stale_errors_ambiguous_ids_and_self_cannot_trigger_exclusion() {
        let mut state = CoordinationSnapshot::default();
        let mut node = ganglion_core::NodeInfo::new("b", "127.0.0.1:1", None::<String>);
        node.labels.insert(crate::RAFT_ID_LABEL.into(), "2".into());
        state.nodes.insert("b".into(), node.clone());
        let now = Instant::now();
        let later = now + Duration::from_secs(1);
        let mut failures = BTreeMap::from([(
            2,
            PeerTransportFailure {
                since: now,
                latest: now,
                attempts: 1,
                kind: std::io::ErrorKind::ConnectionRefused,
            },
        )]);
        let mut detector = Detector::default();
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, now)
                .is_empty()
        );
        failures.get_mut(&2).unwrap().attempts = 3;
        failures.get_mut(&2).unwrap().latest = later;
        assert_eq!(
            detector
                .suspects(true, 500, &state, &failures, 1, later)
                .len(),
            1
        );
        assert!(
            detector
                .suspects(
                    true,
                    500,
                    &state,
                    &failures,
                    1,
                    later + Duration::from_secs(3)
                )
                .is_empty()
        );
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 2, later)
                .is_empty()
        );
        state.nodes.insert("duplicate".into(), node);
        assert!(
            detector
                .suspects(true, 500, &state, &failures, 1, later)
                .is_empty()
        );
        state.nodes.remove("duplicate");
        assert!(
            detector
                .suspects(true, 1000, &state, &failures, 1, later)
                .is_empty()
        );
        failures.get_mut(&2).unwrap().attempts = 5;
        failures.get_mut(&2).unwrap().latest = later + Duration::from_secs(2);
        let newest = failures[&2].latest;
        assert_eq!(
            detector
                .suspects(true, 1000, &state, &failures, 1, newest)
                .len(),
            1
        );
        state
            .nodes
            .get_mut("b")
            .unwrap()
            .labels
            .insert(crate::HISTORY_PROCESS_LABEL.into(), "new-process".into());
        assert!(
            detector
                .suspects(true, 1000, &state, &failures, 1, newest)
                .is_empty()
        );
    }
}
