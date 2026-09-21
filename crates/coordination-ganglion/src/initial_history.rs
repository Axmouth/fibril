//! Explicit consensus preparation of a fresh history. Preparing a baseline does
//! not activate a writer or establish an installed quorum. Ordinary startup does
//! not invoke these APIs while recovery readmission remains incomplete.

use fibril_broker::queue_engine::{
    PartitionKind, PreparedStorageHistory, StorageHistoryBinding, StromaEngine,
};
use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

use crate::{
    history_identity::{resource_incarnation, ResourceIncarnation},
    promotion, GanglionCoordination,
};

const PREFIX: &str = "fibril/initial-history/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InitialHistoryPhase {
    Preparing,
}

/// Immutable initial decision. Membership, policy and owner process are fixed
/// through retries. It authorizes preparation only, never ordinary writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialHistoryDecision {
    pub version: u32,
    pub phase: InitialHistoryPhase,
    pub incarnation: ResourceIncarnation,
    pub assignment: PartitionAssignment,
    pub owner_process: [u8; 16],
    pub binding: StorageHistoryBinding,
    pub required_write_nodes: usize,
}

/// Produced after fresh consensus authorization and durable local preparation.
/// Remote transport must authenticate the reporting node before admitting this
/// evidence to a future quorum-installation record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialHistoryLocalReceipt {
    pub decision: [u8; 32],
    pub node_id: String,
    pub replica_process: [u8; 16],
    pub storage: PreparedStorageHistory,
}

/// Receipt counts describe prepared storage only. Activation requires a fresh
/// persisted quorum decision and the remaining writer-admission protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitialHistoryProgress {
    AwaitingPreparation {
        received: usize,
        required: usize,
        owner_prepared: bool,
    },
    PreparedQuorum {
        received: usize,
        required: usize,
    },
}

/// Explicit, bounded-by-membership collection. Callers must obtain reports from
/// authenticated targets; the contacted identity does not come from the report.
pub struct InitialHistoryReceiptSet {
    decision: InitialHistoryDecision,
    reports: std::collections::BTreeMap<String, InitialHistoryLocalReceipt>,
    contradiction: bool,
}

impl InitialHistoryReceiptSet {
    pub fn new(
        snapshot: &CoordinationSnapshot,
        decision: InitialHistoryDecision,
    ) -> Result<Self, String> {
        validate_committed(snapshot, &decision)?;
        Ok(Self {
            decision,
            reports: Default::default(),
            contradiction: false,
        })
    }

    pub fn record(
        &mut self,
        snapshot: &CoordinationSnapshot,
        contacted_node: &str,
        receipt: InitialHistoryLocalReceipt,
    ) -> Result<(), String> {
        validate_committed(snapshot, &self.decision)?;
        if self.contradiction {
            return Err("contradictory initial history receipts require recovery".into());
        }
        let decision = &self.decision;
        let resource = &decision.incarnation.resource;
        let owner = contacted_node == decision.assignment.owner;
        if (!owner
            && !decision
                .assignment
                .followers
                .iter()
                .any(|node| node == contacted_node))
            || receipt.node_id != contacted_node
            || receipt.decision != decision.digest()?
            || receipt.replica_process == [0; 16]
            || receipt.storage.storage_instance == [0; 16]
            || receipt.storage.topic != resource.name
            || u64::from(receipt.storage.partition) != resource.partition
            || receipt.storage.group != resource.group
            || receipt.storage.stream != (resource.namespace == crate::STREAM_NAMESPACE)
            || receipt.storage.binding != decision.binding
        {
            return Err(
                "initial history receipt does not match the contacted replica and decision".into(),
            );
        }
        if owner && receipt.replica_process != decision.owner_process {
            self.contradiction = true;
            tracing::warn!(
                node_id = contacted_node,
                topic = resource.name,
                partition = resource.partition,
                "owner process changed during initial history preparation"
            );
            return Err("owner process changed during initial preparation".into());
        }
        if let Some(existing) = self.reports.get(contacted_node) {
            if existing != &receipt {
                self.contradiction = true;
                tracing::warn!(node_id = contacted_node, topic = resource.name,
                    partition = resource.partition, "replica changed process or storage instance during initial history preparation");
                return Err(
                    "replica changed its process or storage instance during preparation".into(),
                );
            }
            return Ok(());
        }
        self.reports.insert(contacted_node.into(), receipt);
        Ok(())
    }

    pub fn progress(
        &self,
        snapshot: &CoordinationSnapshot,
    ) -> Result<InitialHistoryProgress, String> {
        validate_committed(snapshot, &self.decision)?;
        if self.contradiction {
            return Err("contradictory initial history receipts require recovery".into());
        }
        let received = self.reports.len();
        let required = self.decision.required_write_nodes;
        let owner_prepared = self.reports.contains_key(&self.decision.assignment.owner);
        if owner_prepared && received >= required {
            Ok(InitialHistoryProgress::PreparedQuorum { received, required })
        } else {
            Ok(InitialHistoryProgress::AwaitingPreparation {
                received,
                required,
                owner_prepared,
            })
        }
    }
}

fn validate_committed(
    snapshot: &CoordinationSnapshot,
    decision: &InitialHistoryDecision,
) -> Result<(), String> {
    decision.validate(snapshot)?;
    let raw = snapshot
        .attributes
        .get(&key(&decision.incarnation))
        .ok_or("initial history is not committed")?;
    if serde_json::from_str::<InitialHistoryDecision>(raw).map_err(|e| e.to_string())? != *decision
    {
        return Err("initial history differs from the committed decision".into());
    }
    Ok(())
}

fn key(incarnation: &ResourceIncarnation) -> String {
    // Recreated resources have a different key; an old decision remains evidence.
    format!(
        "{PREFIX}{}",
        serde_json::to_string(incarnation).expect("incarnation serializes")
    )
}

fn error(message: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(message.to_string())
}

fn validate_resource(resource: &ResourceIdentity) -> Result<(), String> {
    if !matches!(
        resource.namespace.as_str(),
        crate::QUEUE_NAMESPACE | crate::STREAM_NAMESPACE
    ) || resource.name.is_empty()
        || resource.partition > u32::MAX as u64
        || resource
            .group
            .as_deref()
            .is_some_and(|group| group.is_empty() || group == "default")
        || resource.namespace == crate::STREAM_NAMESPACE && resource.group.is_some()
    {
        return Err("initial history requires a canonical queue or stream resource".into());
    }
    Ok(())
}

impl InitialHistoryDecision {
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-initial-history-v1\0");
        hash.update(&serde_json::to_vec(self).map_err(|e| e.to_string())?);
        Ok(*hash.finalize().as_bytes())
    }

    /// A snapshot check has no freshness guarantee. Public operations also make
    /// a generation-guarded consensus write before preparing local storage.
    pub fn validate(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        let resource = &self.incarnation.resource;
        validate_resource(resource)?;
        if self.version != 1
            || self.owner_process == [0; 16]
            || self.binding.accepted_history == [0; 16]
            || self.binding.writer_session == [0; 16]
            || self.binding.resource_incarnation != self.incarnation.id
            || self.assignment.resource != *resource
            || self.assignment.owner.is_empty()
            || self.assignment.followers.iter().any(String::is_empty)
            || !snapshot.resources.contains(resource)
            || resource_incarnation(snapshot, resource)?.as_ref() != Some(&self.incarnation)
            || snapshot.assignments.get(resource) != Some(&self.assignment)
            || snapshot
                .attributes
                .contains_key(&promotion::pending_recovery_key(resource))
            || promotion::write_requirement(&self.assignment)? != self.required_write_nodes
        {
            return Err("initial history no longer matches its incarnation, assignment or confirmation policy".into());
        }
        Ok(())
    }
}

fn proposed_decision(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
    node: &str,
    process: [u8; 16],
) -> Result<InitialHistoryDecision, String> {
    validate_resource(resource)?;
    let incarnation = resource_incarnation(snapshot, resource)?
        .ok_or("legacy resource needs a verified baseline before initial history")?;
    let assignment = snapshot
        .assignments
        .get(resource)
        .ok_or("initial history requires a committed assignment")?
        .clone();
    if assignment.owner != node {
        return Err("only the assigned owner may prepare an initial history decision".into());
    }
    let decision = if let Some(raw) = snapshot.attributes.get(&key(&incarnation)) {
        let existing = serde_json::from_str::<InitialHistoryDecision>(raw)
            .map_err(|e| format!("invalid initial history: {e}"))?;
        if existing.incarnation != incarnation || existing.assignment != assignment {
            return Err(
                "initial history is stored under the wrong incarnation or assignment".into(),
            );
        }
        existing
    } else {
        InitialHistoryDecision {
            version: 1,
            phase: InitialHistoryPhase::Preparing,
            binding: StorageHistoryBinding {
                resource_incarnation: incarnation.id,
                accepted_history: *uuid::Uuid::now_v7().as_bytes(),
                writer_session: *uuid::Uuid::now_v7().as_bytes(),
            },
            required_write_nodes: promotion::write_requirement(&assignment)?,
            incarnation,
            assignment,
            owner_process: process,
        }
    };
    decision.validate(snapshot)?;
    if decision.owner_process != process || decision.assignment.owner != node {
        return Err(
            "initial history belongs to another owner process; recovery readmission required"
                .into(),
        );
    }
    Ok(decision)
}

impl GanglionCoordination {
    /// Persist one initial-history preparation decision, or resume this provider's
    /// exact decision. A replacement provider cannot reuse an old owner grant.
    pub async fn prepare_initial_history(
        &self,
        resource: &ResourceIdentity,
    ) -> Result<InitialHistoryDecision, OpenraftAdapterError> {
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            let decision =
                proposed_decision(&snapshot, resource, &self.node_id, self.history_process)
                    .map_err(error)?;
            let key = key(&decision.incarnation);
            let expected = snapshot.attributes.get(&key).cloned();
            let command = MetadataRaftCommand::CompareAndSetAttributeGuarded {
                expected_generation: snapshot.generation,
                key,
                expected,
                value: serde_json::to_string(&decision).map_err(error)?,
            };
            match self.forward_command(command).await {
                Ok(response) => {
                    decision.validate(&response.snapshot).map_err(error)?;
                    return Ok(decision);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(error(
            "initial history preparation raced metadata changes; retry with backoff",
        ))
    }

    /// Prepare this replica's empty storage against a freshly committed decision.
    /// Assignment/incarnation changes reject before local I/O. Changes after
    /// authorization can leave inert preparation, but cannot activate a writer.
    pub async fn prepare_local_initial_history(
        &self,
        decision: &InitialHistoryDecision,
        engine: &StromaEngine,
    ) -> Result<InitialHistoryLocalReceipt, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        decision.validate(&snapshot).map_err(error)?;
        let owner = decision.assignment.owner == self.node_id;
        if !owner && !decision.assignment.followers.contains(&self.node_id) {
            return Err(error(
                "initial history preparation requires assigned replica membership",
            ));
        }
        if owner && decision.owner_process != self.history_process {
            return Err(error("restarted owner requires recovery readmission"));
        }
        let key = key(&decision.incarnation);
        let raw = snapshot
            .attributes
            .get(&key)
            .ok_or_else(|| error("initial history decision is not committed"))?;
        if serde_json::from_str::<InitialHistoryDecision>(raw).map_err(error)? != *decision {
            return Err(error("initial history differs from the committed decision"));
        }
        let response = self
            .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                expected_generation: snapshot.generation,
                key,
                expected: Some(raw.clone()),
                value: raw.clone(),
            })
            .await?;
        decision.validate(&response.snapshot).map_err(error)?;
        let resource = &decision.incarnation.resource;
        let kind = if resource.namespace == crate::STREAM_NAMESPACE {
            PartitionKind::Stream
        } else {
            PartitionKind::Queue
        };
        let storage = engine
            .prepare_empty_storage_history(
                &resource.name,
                resource.partition as u32,
                resource.group.as_deref(),
                kind,
                decision.binding.clone(),
            )
            .await
            .map_err(error)?;
        Ok(InitialHistoryLocalReceipt {
            decision: decision.digest().map_err(error)?,
            node_id: self.node_id.clone(),
            replica_process: self.history_process,
            storage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_core::ReplicationDurabilityPolicy;
    use std::collections::BTreeMap;

    fn fixture(stream: bool) -> (CoordinationSnapshot, ResourceIdentity) {
        let resource = ResourceIdentity::new(
            if stream {
                crate::STREAM_NAMESPACE
            } else {
                crate::QUEUE_NAMESPACE
            },
            "q",
            0,
            None::<String>,
        );
        let mut assignment =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 7);
        assignment.durability = ReplicationDurabilityPolicy::MajorityDurable;
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.resources.insert(resource.clone());
        snapshot.assignments.insert(resource.clone(), assignment);
        let identity = ResourceIncarnation {
            version: 1,
            resource: resource.clone(),
            id: [1; 16],
        };
        snapshot.attributes.insert(
            crate::history_identity::key(&resource),
            serde_json::to_string(&identity).unwrap(),
        );
        (snapshot, resource)
    }

    #[test]
    fn preparation_is_stable_and_binds_process_incarnation_membership_and_policy() {
        let (mut snapshot, resource) = fixture(false);
        let decision = proposed_decision(&snapshot, &resource, "a", [2; 16]).unwrap();
        assert_eq!(decision.required_write_nodes, 2);
        snapshot.attributes.insert(
            key(&decision.incarnation),
            serde_json::to_string(&decision).unwrap(),
        );
        assert_eq!(
            proposed_decision(&snapshot, &resource, "a", [2; 16]).unwrap(),
            decision
        );
        assert!(proposed_decision(&snapshot, &resource, "a", [3; 16]).is_err());
        assert!(proposed_decision(&snapshot, &resource, "b", [2; 16]).is_err());
        for mutation in 0..7 {
            let mut changed = snapshot.clone();
            match mutation {
                0 => {
                    changed.assignments.get_mut(&resource).unwrap().epoch += 1;
                }
                1 => {
                    changed.assignments.get_mut(&resource).unwrap().owner = "b".into();
                }
                2 => {
                    changed.assignments.get_mut(&resource).unwrap().followers = vec!["b".into()];
                }
                3 => {
                    changed.assignments.get_mut(&resource).unwrap().durability =
                        ReplicationDurabilityPolicy::LocalDurable;
                }
                4 => {
                    changed
                        .attributes
                        .remove(&crate::history_identity::key(&resource));
                }
                5 => {
                    changed.resources.remove(&resource);
                }
                _ => {
                    changed
                        .attributes
                        .insert(promotion::pending_recovery_key(&resource), "pending".into());
                }
            }
            assert!(decision.validate(&changed).is_err(), "mutation {mutation}");
        }
        let mut wrong = decision.clone();
        wrong.binding.writer_session = [3; 16];
        assert_ne!(decision.digest().unwrap(), wrong.digest().unwrap());
        let (stream, resource) = fixture(true);
        assert_eq!(
            proposed_decision(&stream, &resource, "a", [2; 16])
                .unwrap()
                .required_write_nodes,
            3
        );
    }

    #[test]
    fn prepared_quorum_requires_owner_and_unique_consistent_replica_receipts() {
        let (mut snapshot, resource) = fixture(false);
        let decision = proposed_decision(&snapshot, &resource, "a", [2; 16]).unwrap();
        assert!(InitialHistoryReceiptSet::new(&snapshot, decision.clone()).is_err());
        snapshot.attributes.insert(
            key(&decision.incarnation),
            serde_json::to_string(&decision).unwrap(),
        );
        let receipt = |node: &str| InitialHistoryLocalReceipt {
            decision: decision.digest().unwrap(),
            node_id: node.into(),
            replica_process: [2; 16],
            storage: PreparedStorageHistory {
                topic: "q".into(),
                partition: 0,
                group: None,
                stream: false,
                binding: decision.binding.clone(),
                storage_instance: [3; 16],
            },
        };
        let mut set = InitialHistoryReceiptSet::new(&snapshot, decision.clone()).unwrap();
        set.record(&snapshot, "b", receipt("b")).unwrap();
        set.record(&snapshot, "b", receipt("b")).unwrap();
        assert_eq!(
            set.progress(&snapshot).unwrap(),
            InitialHistoryProgress::AwaitingPreparation {
                received: 1,
                required: 2,
                owner_prepared: false
            }
        );
        set.record(&snapshot, "c", receipt("c")).unwrap();
        assert!(matches!(
            set.progress(&snapshot).unwrap(),
            InitialHistoryProgress::AwaitingPreparation {
                received: 2,
                owner_prepared: false,
                ..
            }
        ));
        assert!(set
            .record(&snapshot, "outside", receipt("outside"))
            .is_err());
        for mutation in 1..5 {
            let mut bad = receipt("a");
            match mutation {
                1 => bad.storage.binding.writer_session = [8; 16],
                2 => bad.decision = [8; 32],
                3 => bad.storage.stream = true,
                _ => bad.storage.partition = 1,
            }
            assert!(set.record(&snapshot, "a", bad).is_err());
        }
        set.record(&snapshot, "a", receipt("a")).unwrap();
        assert!(matches!(
            set.progress(&snapshot).unwrap(),
            InitialHistoryProgress::PreparedQuorum {
                received: 3,
                required: 2
            }
        ));
        let mut owner_set = InitialHistoryReceiptSet::new(&snapshot, decision.clone()).unwrap();
        owner_set.record(&snapshot, "a", receipt("a")).unwrap();
        owner_set.record(&snapshot, "b", receipt("b")).unwrap();
        let mut restarted_owner = receipt("a");
        restarted_owner.replica_process = [8; 16];
        assert!(owner_set.record(&snapshot, "a", restarted_owner).is_err());
        assert!(owner_set.progress(&snapshot).is_err());
        let mut changed = snapshot.clone();
        changed.attributes.remove(&key(&decision.incarnation));
        assert!(set.progress(&changed).is_err());
        let mut conflicting = receipt("b");
        conflicting.storage.storage_instance = [4; 16];
        assert!(set.record(&snapshot, "b", conflicting).is_err());
        assert!(set.progress(&snapshot).is_err());
        assert!(
            set.record(&snapshot, "b", receipt("b")).is_err(),
            "contradiction must remain sticky"
        );
    }

    #[test]
    fn initial_decision_cannot_be_reused_under_another_resource_key() {
        let (mut snapshot, resource) = fixture(false);
        let decision = proposed_decision(&snapshot, &resource, "a", [2; 16]).unwrap();
        let other = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "other", 0, None::<String>);
        let incarnation = ResourceIncarnation {
            version: 1,
            resource: other.clone(),
            id: [8; 16],
        };
        snapshot.resources.insert(other.clone());
        snapshot.assignments.insert(
            other.clone(),
            PartitionAssignment::new(other.clone(), "a", vec![], 7),
        );
        snapshot.attributes.insert(
            crate::history_identity::key(&other),
            serde_json::to_string(&incarnation).unwrap(),
        );
        snapshot
            .attributes
            .insert(key(&incarnation), serde_json::to_string(&decision).unwrap());
        assert!(proposed_decision(&snapshot, &other, "a", [2; 16]).is_err());
    }

    #[test]
    fn preparation_rejects_legacy_malformed_and_noncanonical_resources() {
        let (mut snapshot, resource) = fixture(false);
        let decision = proposed_decision(&snapshot, &resource, "a", [2; 16]).unwrap();
        snapshot
            .attributes
            .insert(key(&decision.incarnation), "{torn".into());
        assert!(proposed_decision(&snapshot, &resource, "a", [2; 16]).is_err());
        snapshot
            .attributes
            .remove(&crate::history_identity::key(&resource));
        assert!(proposed_decision(&snapshot, &resource, "a", [2; 16]).is_err());
        for bad in [
            ResourceIdentity::new(
                crate::QUEUE_NAMESPACE,
                "q",
                u32::MAX as u64 + 1,
                None::<String>,
            ),
            ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, Some("default".into())),
            ResourceIdentity::new(crate::STREAM_NAMESPACE, "q", 0, Some("g".into())),
        ] {
            assert!(validate_resource(&bad).is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn consensus_preparation_survives_restart_without_granting_writer_permission() {
        use fibril_broker::{
            coordination::QueueIdentity,
            queue_engine::{
                KeratinConfig, QueueEngine, SnapshotConfig, StromaError, StromaKeratinConfig,
            },
            Partition,
        };
        use ganglion_openraft::{default_raft_config, InProcessRouter, RaftMetadataNode};
        use std::time::Duration;

        let root = std::env::temp_dir().join(format!("initial-history-{}", uuid::Uuid::now_v7()));
        let router = InProcessRouter::new();
        let node = RaftMetadataNode::start_durable(
            1,
            default_raft_config().unwrap(),
            &router,
            root.join("metadata"),
        )
        .await
        .unwrap();
        node.initialize(BTreeMap::from([(
            1,
            ganglion_openraft::openraft::BasicNode::new("n1"),
        )]))
        .await
        .unwrap();
        node.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let provider = GanglionCoordination::new("a", node);
        let queue = QueueIdentity::new("q", Partition::new(0), None);
        provider.register_queue(&queue).await.unwrap();
        let resource = crate::to_ganglion_resource(&queue);
        let mut snapshot = provider.node.committed_snapshot();
        let generation = snapshot.generation;
        let mut assignment =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 7);
        assignment.durability = ReplicationDurabilityPolicy::MajorityDurable;
        snapshot.assignments.insert(resource.clone(), assignment);
        snapshot.generation += 1;
        provider
            .node
            .write_snapshot_guarded(generation, snapshot)
            .await
            .unwrap();

        let (left, right) = tokio::join!(
            provider.prepare_initial_history(&resource),
            provider.prepare_initial_history(&resource)
        );
        let decision = left.unwrap();
        assert_eq!(
            decision,
            right.unwrap(),
            "concurrent calls must retain the committed IDs"
        );
        assert_eq!(decision.required_write_nodes, 2);
        let engine = StromaEngine::open(
            root.join("storage"),
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let receipt = provider
            .prepare_local_initial_history(&decision, &engine)
            .await
            .unwrap();
        assert_eq!(
            receipt,
            provider
                .prepare_local_initial_history(&decision, &engine)
                .await
                .unwrap()
        );
        assert_eq!(receipt.storage.binding, decision.binding);
        assert_eq!(receipt.decision, decision.digest().unwrap());
        assert!(matches!(
            engine.ensure_queue_owner_epoch("q", 0, None, Some(7)).await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        let mut forged = decision.clone();
        forged.binding.accepted_history = [8; 16];
        assert!(provider
            .prepare_local_initial_history(&forged, &engine)
            .await
            .is_err());
        engine.shutdown().await.unwrap();
        provider.node.shutdown().await.unwrap();
        provider.forwarder.abort();
        drop(provider);
        drop(router);

        let router = InProcessRouter::new();
        let node = RaftMetadataNode::start_durable(
            1,
            default_raft_config().unwrap(),
            &router,
            root.join("metadata"),
        )
        .await
        .unwrap();
        node.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let reopened = GanglionCoordination::new("a", node);
        let snapshot = reopened.node.committed_snapshot();
        assert_eq!(
            serde_json::from_str::<InitialHistoryDecision>(
                &snapshot.attributes[&key(&decision.incarnation)]
            )
            .unwrap(),
            decision
        );
        assert!(
            reopened.prepare_initial_history(&resource).await.is_err(),
            "same node and epoch are insufficient after process replacement"
        );
        let engine = StromaEngine::open(
            root.join("storage"),
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        assert!(reopened
            .prepare_local_initial_history(&decision, &engine)
            .await
            .is_err());
        assert_eq!(
            engine.storage_history_binding("q", 0, None).unwrap(),
            Some(decision.binding)
        );
        engine.shutdown().await.unwrap();
        reopened.node.shutdown().await.unwrap();
        reopened.forwarder.abort();
        drop(reopened);
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
    }
}
