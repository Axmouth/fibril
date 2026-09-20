//! Admission of explicit seal replies. A collected seal quorum still requires
//! compatible-history and dependency proof before installation or activation.

use std::collections::{BTreeMap, BTreeSet};

use fibril_broker::recovery::{BrokerSealedReplica, RecoverySealCommand};
use ganglion_core::CoordinationSnapshot;

use crate::promotion::{validate_seal_command, PendingRecovery};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealCollectionProgress {
    AwaitingSeals {
        received: usize,
        required: usize,
        missing: Vec<String>,
    },
    /// This is only the old seal-count requirement. It conveys no source choice
    /// or compatibility claim, even when every reported fingerprint is equal.
    AwaitingHistoryValidation { received: usize, required: usize },
}

/// An in-memory collection for one exact persisted transition. Reconstruction
/// after restart recollects durable seals; heartbeat labels never seed it.
/// Callers must obtain replies from authenticated peers at the contacted nodes.
#[derive(Debug)]
pub struct RecoveryWitnessSet {
    command: RecoverySealCommand,
    old_owner: String,
    old_replicas: BTreeSet<String>,
    required: usize,
    reports: BTreeMap<String, BrokerSealedReplica>,
    contradiction: Option<String>,
}

impl RecoveryWitnessSet {
    pub fn new(snapshot: &CoordinationSnapshot, pending: &PendingRecovery) -> Result<Self, String> {
        let command = pending.seal_command()?;
        validate_seal_command(snapshot, &pending.previous.owner, &command)?;
        let old_replicas: BTreeSet<_> = std::iter::once(pending.previous.owner.clone())
            .chain(pending.previous.followers.iter().cloned())
            .collect();
        if old_replicas.iter().any(|id| id.is_empty()) {
            return Err("empty old replica identity".into());
        }
        Ok(Self {
            command,
            old_owner: pending.previous.owner.clone(),
            old_replicas,
            required: pending.required_old_witnesses,
            reports: BTreeMap::new(),
            contradiction: None,
        })
    }

    fn ensure_current(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        validate_seal_command(snapshot, &self.old_owner, &self.command)?;
        if let Some(reason) = &self.contradiction {
            return Err(reason.clone());
        }
        Ok(())
    }

    pub fn command(&self) -> &RecoverySealCommand {
        &self.command
    }

    /// `contacted_node` comes from the chosen target, not from the reply. A
    /// response cannot substitute another old member or count twice under aliases.
    pub fn record(
        &mut self,
        snapshot: &CoordinationSnapshot,
        contacted_node: &str,
        report: BrokerSealedReplica,
    ) -> Result<(), String> {
        self.ensure_current(snapshot)?;
        if !self.old_replicas.contains(contacted_node) || report.node_id != contacted_node {
            return Err("seal reply does not identify the contacted old replica".into());
        }
        let seal = &report.seal;
        let history = &seal.history;
        if seal.request.transition != self.command.transition
            || seal.request.fence_epoch != self.command.fence_epoch
            || history.version != 1
            || history.message_head > history.message_next
            || history.event_head > history.event_next
            || (
                seal.message_head,
                seal.message_next,
                seal.event_head,
                seal.event_next,
            ) != (
                history.message_head,
                history.message_next,
                history.event_head,
                history.event_next,
            )
        {
            return Err("stale or malformed recovery seal reply".into());
        }
        if let Some(previous) = self.reports.get(contacted_node) {
            if previous != &report {
                let reason =
                    format!("replica {contacted_node} returned conflicting evidence for one seal");
                tracing::error!(
                    replica = contacted_node, topic = self.command.topic, group = self.command.group.as_deref(),
                    partition = self.command.partition.id(),
                    transition = %blake3::Hash::from_bytes(self.command.transition),
                    "conflicting evidence from one sealed replica; collection blocked"
                );
                self.contradiction = Some(reason.clone());
                return Err(reason);
            }
            return Ok(());
        }
        tracing::info!(
            replica = contacted_node, topic = self.command.topic, group = self.command.group.as_deref(),
            partition = self.command.partition.id(), received = self.reports.len() + 1,
            required = self.required, history = %blake3::Hash::from_bytes(history.id),
            transition = %blake3::Hash::from_bytes(self.command.transition),
            message_head = history.message_head, message_next = history.message_next,
            event_head = history.event_head, event_next = history.event_next,
            "recovery seal witness admitted; compatible history remains unproven"
        );
        self.reports.insert(contacted_node.to_owned(), report);
        Ok(())
    }

    pub fn progress(
        &self,
        snapshot: &CoordinationSnapshot,
    ) -> Result<SealCollectionProgress, String> {
        self.ensure_current(snapshot)?;
        let received = self.reports.len();
        Ok(if received < self.required {
            SealCollectionProgress::AwaitingSeals {
                received,
                required: self.required,
                missing: self
                    .old_replicas
                    .iter()
                    .filter(|id| !self.reports.contains_key(*id))
                    .cloned()
                    .collect(),
            }
        } else {
            SealCollectionProgress::AwaitingHistoryValidation {
                received,
                required: self.required,
            }
        })
    }

    /// Revalidates the exact active transition before exposing evidence to a
    /// future history verifier. The snapshot itself is not a fresh-consensus proof.
    pub fn reports(
        &self,
        snapshot: &CoordinationSnapshot,
    ) -> Result<&BTreeMap<String, BrokerSealedReplica>, String> {
        self.ensure_current(snapshot)?;
        Ok(&self.reports)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promotion::{pending_recovery_key, retain_unproven_assignments};
    use fibril_broker::recovery::{
        RecoverySealRequest, RetainedHistoryIdentity, SealedReplicaFrontiers,
    };
    use ganglion_core::{PartitionAssignment, ReplicationDurabilityPolicy, ResourceIdentity};

    fn pending(nodes: usize, writes: usize) -> (CoordinationSnapshot, PendingRecovery) {
        pending_for_kind(nodes, writes, crate::QUEUE_NAMESPACE)
    }

    fn pending_for_kind(
        nodes: usize,
        writes: usize,
        namespace: &str,
    ) -> (CoordinationSnapshot, PendingRecovery) {
        let resource = ResourceIdentity::new(namespace, "q", 0, None);
        let mut old = PartitionAssignment::new(
            resource.clone(),
            "a",
            (1..nodes).map(|i| format!("f{i}")).collect(),
            7,
        );
        old.durability = ReplicationDurabilityPolicy::ReplicaDurable { nodes: writes };
        let mut committed = CoordinationSnapshot::default();
        committed.assignments.insert(resource.clone(), old);
        let mut desired = committed.clone();
        desired.generation = 1;
        let proposed = desired.assignments.get_mut(&resource).unwrap();
        proposed.owner = "new-node".into();
        proposed.followers = vec!["f1".into()];
        proposed.durability = ReplicationDurabilityPolicy::MajorityDurable;
        retain_unproven_assignments(&committed, &mut desired).unwrap();
        let request =
            serde_json::from_str(&desired.attributes[&pending_recovery_key(&resource)]).unwrap();
        (desired, request)
    }

    fn report(set: &RecoveryWitnessSet, node: &str) -> BrokerSealedReplica {
        BrokerSealedReplica {
            node_id: node.into(),
            seal: SealedReplicaFrontiers {
                request: RecoverySealRequest {
                    transition: set.command.transition,
                    fence_epoch: set.command.fence_epoch,
                },
                history: RetainedHistoryIdentity {
                    version: 1,
                    id: [1; 32],
                    message_digest: [2; 32],
                    event_digest: [3; 32],
                    snapshot_digest: None,
                    message_head: 0,
                    message_next: 10,
                    event_head: 0,
                    event_next: 8,
                },
                message_head: 0,
                message_next: 10,
                event_head: 0,
                event_next: 8,
            },
        }
    }

    #[test]
    fn old_threshold_survives_smaller_new_configuration_and_duplicate_retries() {
        let (snapshot, request) = pending(5, 2);
        let mut set = RecoveryWitnessSet::new(&snapshot, &request).unwrap();
        for id in ["f1", "f1", "f2", "f2", "f3"] {
            set.record(&snapshot, id, report(&set, id)).unwrap();
        }
        assert_eq!(
            set.progress(&snapshot).unwrap(),
            SealCollectionProgress::AwaitingSeals {
                received: 3,
                required: 4,
                missing: vec!["a".into(), "f4".into()],
            }
        );
        set.record(&snapshot, "f4", report(&set, "f4")).unwrap();
        assert_eq!(
            set.progress(&snapshot).unwrap(),
            SealCollectionProgress::AwaitingHistoryValidation {
                received: 4,
                required: 4
            }
        );
        assert_eq!(set.reports(&snapshot).unwrap().len(), 4);
    }

    #[test]
    fn rejects_wrong_identity_transition_bounds_and_version_without_counting() {
        let (snapshot, request) = pending(3, 2);
        let mut set = RecoveryWitnessSet::new(&snapshot, &request).unwrap();
        for mutation in 0..8 {
            let mut bad = report(&set, "f1");
            match mutation {
                0 => bad.node_id = "f2".into(),
                1 => bad.seal.request.transition[0] ^= 1,
                2 => bad.seal.request.fence_epoch += 1,
                3 => bad.seal.history.version += 1,
                4 => bad.seal.history.message_head = 11,
                5 => bad.seal.history.event_head = 9,
                6 => bad.seal.message_next += 1,
                _ => bad.seal.event_next += 1,
            }
            assert!(set.record(&snapshot, "f1", bad).is_err());
            assert!(set.reports(&snapshot).unwrap().is_empty());
        }
        assert!(set
            .record(&snapshot, "new-node", report(&set, "new-node"))
            .is_err());
    }

    #[test]
    fn changed_reply_from_one_sealed_replica_latches_a_contradiction() {
        let (snapshot, request) = pending(3, 2);
        let mut set = RecoveryWitnessSet::new(&snapshot, &request).unwrap();
        let first = report(&set, "f1");
        set.record(&snapshot, "f1", first.clone()).unwrap();
        let mut changed = first.clone();
        changed.seal.history.id[0] ^= 1;
        assert!(set.record(&snapshot, "f1", changed).is_err());
        assert!(set.record(&snapshot, "f1", first).is_err());
        assert!(set.progress(&snapshot).is_err());
        assert!(set.reports(&snapshot).is_err());
    }

    #[test]
    fn distinct_histories_remain_unclassified_and_metadata_changes_invalidate_collection() {
        let (snapshot, request) = pending(3, 2);
        let mut set = RecoveryWitnessSet::new(&snapshot, &request).unwrap();
        set.record(&snapshot, "f1", report(&set, "f1")).unwrap();
        let mut other = report(&set, "f2");
        other.seal.history.id[0] ^= 1;
        other.seal.history.message_head = 5;
        other.seal.message_head = 5;
        set.record(&snapshot, "f2", other).unwrap();
        assert_eq!(
            set.progress(&snapshot).unwrap(),
            SealCollectionProgress::AwaitingHistoryValidation {
                received: 2,
                required: 2
            }
        );
        let mut unrelated = snapshot.clone();
        unrelated.generation += 1;
        assert!(set.progress(&unrelated).is_ok());
        let mut activated = snapshot.clone();
        activated
            .assignments
            .insert(request.previous.resource.clone(), request.proposed.clone());
        assert!(set.progress(&activated).is_err());
        assert!(set.reports(&activated).is_err());
        assert!(RecoveryWitnessSet::new(&activated, &request).is_err());
        let mut replaced = snapshot.clone();
        replaced
            .attributes
            .remove(&pending_recovery_key(&request.previous.resource));
        assert!(set.progress(&replaced).is_err());
    }
    #[test]
    fn every_small_write_policy_keeps_its_old_intersection_requirement() {
        for nodes in 1..=7 {
            for writes in 1..=nodes {
                let (snapshot, request) = pending(nodes, writes);
                let mut set = RecoveryWitnessSet::new(&snapshot, &request).unwrap();
                let replicas: Vec<_> = set.old_replicas.iter().cloned().collect();
                for (index, replica) in replicas.iter().enumerate() {
                    set.record(&snapshot, replica, report(&set, replica))
                        .unwrap();
                    let progress = set.progress(&snapshot).unwrap();
                    assert_eq!(
                        matches!(
                            progress,
                            SealCollectionProgress::AwaitingHistoryValidation { .. }
                        ),
                        index + 1 >= nodes - writes + 1,
                        "N={nodes}, W={writes}, R={}",
                        index + 1
                    );
                }
            }
        }
    }
    #[test]
    fn stream_threshold_uses_its_all_assigned_copies_confirmation_contract() {
        let (snapshot, request) = pending_for_kind(3, 1, crate::STREAM_NAMESPACE);
        assert_eq!(request.previous_write_nodes, 3);
        let mut set = RecoveryWitnessSet::new(&snapshot, &request).unwrap();
        assert!(set.command().stream);
        set.record(&snapshot, "f2", report(&set, "f2")).unwrap();
        assert_eq!(
            set.progress(&snapshot).unwrap(),
            SealCollectionProgress::AwaitingHistoryValidation {
                received: 1,
                required: 1,
            }
        );
    }
}
