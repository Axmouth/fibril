//! Explicit consensus preparation of a fresh history. Preparing a baseline does
//! not activate a writer or establish an installed quorum. Ordinary startup does
//! not invoke these APIs while recovery readmission remains incomplete.

use fibril_broker::queue_engine::{PartitionKind, StorageHistoryBinding, StromaEngine};
use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

use crate::{
    GanglionCoordination,
    history_identity::{ResourceIncarnation, resource_incarnation},
    promotion,
};

const PREFIX: &str = "fibril/initial-history/";
const QUORUM_PREFIX: &str = "fibril/initial-history-prepared-quorum/";

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

pub use fibril_broker::initial_history::InitialHistoryLocalReceipt;
use fibril_broker::initial_history::{InitialHistoryAuthorization, InitialHistoryPrepareCommand};

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

/// Durable evidence of an exact preparation round. These are historical
/// receipts, not proof that their processes still exist or permission to write.
/// Activation must freshly authorize the same local storage/process instances.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialHistoryPreparedQuorum {
    pub version: u32,
    pub decision: [u8; 32],
    pub reports: std::collections::BTreeMap<String, InitialHistoryLocalReceipt>,
}

impl InitialHistoryPreparedQuorum {
    pub fn validate(
        &self,
        snapshot: &CoordinationSnapshot,
        decision: &InitialHistoryDecision,
    ) -> Result<(), String> {
        if self.version != 1 || self.decision != decision.digest()? {
            return Err("prepared quorum does not identify the initial decision".into());
        }
        let mut receipts = InitialHistoryReceiptSet::new(snapshot, decision.clone())?;
        for (node, receipt) in &self.reports {
            receipts.record(snapshot, node, receipt.clone())?;
        }
        receipts.prepared_quorum(snapshot)?;
        Ok(())
    }
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
                tracing::warn!(
                    node_id = contacted_node,
                    topic = resource.name,
                    partition = resource.partition,
                    "replica changed process or storage instance during initial history preparation"
                );
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

    fn prepared_quorum(
        &self,
        snapshot: &CoordinationSnapshot,
    ) -> Result<InitialHistoryPreparedQuorum, String> {
        if !matches!(
            self.progress(snapshot)?,
            InitialHistoryProgress::PreparedQuorum { .. }
        ) {
            return Err("initial history requires the owner and a prepared write quorum".into());
        }
        Ok(InitialHistoryPreparedQuorum {
            version: 1,
            decision: self.decision.digest()?,
            reports: self.reports.clone(),
        })
    }
}

pub(crate) fn validate_committed(
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

pub(crate) fn key(incarnation: &ResourceIncarnation) -> String {
    // Recreated resources have a different key; an old decision remains evidence.
    format!(
        "{PREFIX}{}",
        serde_json::to_string(incarnation).expect("incarnation serializes")
    )
}

pub(crate) fn quorum_key(incarnation: &ResourceIncarnation) -> String {
    format!(
        "{QUORUM_PREFIX}{}",
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
    pub fn prepare_command(
        &self,
        replica_id: &str,
    ) -> Result<InitialHistoryPrepareCommand, String> {
        if replica_id != self.assignment.owner
            && !self.assignment.followers.iter().any(|id| id == replica_id)
        {
            return Err("initial preparation target is not an assigned replica".into());
        }
        let resource = &self.incarnation.resource;
        validate_resource(resource)?;
        Ok(InitialHistoryPrepareCommand {
            replica_id: replica_id.into(),
            topic: resource.name.clone(),
            partition: fibril_broker::Partition::new(resource.partition as u32),
            group: resource.group.clone(),
            stream: resource.namespace == crate::STREAM_NAMESPACE,
            decision: self.digest()?,
            binding: self.binding.clone(),
        })
    }

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
            || self.incarnation.version != 2
            || self.incarnation.retired
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
    pub(crate) async fn authorize_initial_history_command(
        &self,
        command: &InitialHistoryPrepareCommand,
    ) -> Result<InitialHistoryAuthorization, String> {
        if command.replica_id != self.node_id {
            return Err("initial preparation was addressed to another replica".into());
        }
        let resource = ResourceIdentity::new(
            if command.stream {
                crate::STREAM_NAMESPACE
            } else {
                crate::QUEUE_NAMESPACE
            },
            command.topic.clone(),
            u64::from(command.partition.id()),
            command.group.clone(),
        );
        validate_resource(&resource)?;
        let snapshot = self.node.committed_snapshot();
        let incarnation = resource_incarnation(&snapshot, &resource)?
            .ok_or("initial preparation requires enrolled metadata")?;
        let key = key(&incarnation);
        let raw = snapshot
            .attributes
            .get(&key)
            .ok_or("initial history decision is not committed")?;
        let decision: InitialHistoryDecision =
            serde_json::from_str(raw).map_err(|e| e.to_string())?;
        validate_committed(&snapshot, &decision)?;
        if decision.prepare_command(&self.node_id)? != *command {
            return Err("initial preparation does not match the exact committed decision".into());
        }
        if decision.assignment.owner == self.node_id
            && decision.owner_process != self.history_process
        {
            return Err("restarted owner requires recovery readmission".into());
        }
        let response = self
            .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                expected_generation: snapshot.generation,
                key,
                expected: Some(raw.clone()),
                value: raw.clone(),
            })
            .await
            .map_err(|e| e.to_string())?;
        validate_committed(&response.snapshot, &decision)?;
        Ok(InitialHistoryAuthorization {
            node_id: self.node_id.clone(),
            replica_process: self.history_process,
        })
    }

    /// Read persisted preparation evidence. Validation against the local snapshot
    /// establishes identity consistency only; this is not a fresh admission check.
    pub fn prepared_initial_history_quorum(
        &self,
        decision: &InitialHistoryDecision,
    ) -> Result<Option<InitialHistoryPreparedQuorum>, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        validate_committed(&snapshot, decision).map_err(error)?;
        let Some(raw) = snapshot.attributes.get(&quorum_key(&decision.incarnation)) else {
            return Ok(None);
        };
        let quorum: InitialHistoryPreparedQuorum = serde_json::from_str(raw).map_err(error)?;
        quorum.validate(&snapshot, decision).map_err(error)?;
        Ok(Some(quorum))
    }

    /// Commit exactly the authenticated receipts collected for this owner round.
    /// Retrying a lost reply keeps the first record; a different receipt set
    /// cannot replace it. No storage admission or serving route is published.
    pub async fn persist_initial_history_quorum(
        &self,
        receipts: &InitialHistoryReceiptSet,
    ) -> Result<InitialHistoryPreparedQuorum, OpenraftAdapterError> {
        let decision = &receipts.decision;
        if decision.assignment.owner != self.node_id
            || decision.owner_process != self.history_process
        {
            return Err(error(
                "only the original owner process may persist its prepared quorum",
            ));
        }
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            let quorum = receipts.prepared_quorum(&snapshot).map_err(error)?;
            let key = quorum_key(&decision.incarnation);
            let expected = snapshot.attributes.get(&key).cloned();
            if let Some(raw) = &expected {
                let existing: InitialHistoryPreparedQuorum =
                    serde_json::from_str(raw).map_err(error)?;
                existing.validate(&snapshot, decision).map_err(error)?;
                if existing != quorum {
                    tracing::warn!(
                        node_id = self.node_id,
                        topic = decision.incarnation.resource.name,
                        partition = decision.incarnation.resource.partition,
                        "initial history receipt set conflicts with persisted quorum; recovery required"
                    );
                    return Err(error(
                        "prepared quorum differs from the persisted receipts; recovery required",
                    ));
                }
            }
            let value = serde_json::to_string(&quorum).map_err(error)?;
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    key: key.clone(),
                    expected,
                    value,
                })
                .await
            {
                Ok(response) => {
                    quorum
                        .validate(&response.snapshot, decision)
                        .map_err(error)?;
                    let stored: InitialHistoryPreparedQuorum =
                        serde_json::from_str(response.snapshot.attributes.get(&key).ok_or_else(
                            || error("prepared quorum missing from consensus response"),
                        )?)
                        .map_err(error)?;
                    if stored != quorum {
                        return Err(error("consensus response has different prepared receipts"));
                    }
                    return Ok(quorum);
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
            "prepared quorum raced metadata changes; retry with backoff",
        ))
    }

    /// Explicit opt-in for a new resource. Enrollment and incarnation are one
    /// atomic creation attribute; an existing declaration or retiring assignment
    /// can never be upgraded into an empty origin. Enrolled resources remain
    /// absent from ordinary broker/replication/topology serving projections.
    pub async fn register_initial_history_resource(
        &self,
        resource: &ResourceIdentity,
    ) -> Result<ResourceIncarnation, OpenraftAdapterError> {
        validate_resource(resource).map_err(error)?;
        // Avoid reviving a known retired catalogue entry on an invalid retry.
        // The atomic registration still decides origin eligibility if this
        // local view races another writer; its returned identity is rechecked.
        let snapshot = self.node.committed_snapshot();
        let observed = resource_incarnation(&snapshot, resource).map_err(error)?;
        if (snapshot.resources.contains(resource) || snapshot.assignments.contains_key(resource))
            && !observed
                .as_ref()
                .is_some_and(|identity| identity.version == 2 && !identity.retired)
        {
            return Err(error(
                "existing or retiring resource requires a verified recovery baseline; fresh enrollment refused",
            ));
        }
        let response = self
            .forward_command(crate::history_identity::enrolled_registration(
                resource.clone(),
            )?)
            .await?;
        let incarnation = resource_incarnation(&response.snapshot, resource)
            .map_err(error)?
            .filter(|identity| identity.version == 2 && !identity.retired)
            .ok_or_else(|| error("existing or retiring resource requires a verified recovery baseline; fresh enrollment refused"))?;
        Ok(incarnation)
    }

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
        let command = decision.prepare_command(&self.node_id).map_err(error)?;
        self.authorize_initial_history_command(&command)
            .await
            .map_err(error)?;
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
    use fibril_broker::queue_engine::PreparedStorageHistory;
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
            version: 2,
            resource: resource.clone(),
            id: [1; 16],
            retired: false,
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
        assert!(
            set.record(&snapshot, "outside", receipt("outside"))
                .is_err()
        );
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
        let quorum = set.prepared_quorum(&snapshot).unwrap();
        quorum.validate(&snapshot, &decision).unwrap();
        for mutation in 0..5 {
            let mut invalid = quorum.clone();
            match mutation {
                0 => {
                    invalid.reports.remove("a");
                }
                1 => {
                    invalid.reports.remove("b");
                    invalid.reports.remove("c");
                }
                2 => invalid.decision = [9; 32],
                3 => invalid.version = 2,
                _ => invalid.reports.get_mut("b").unwrap().node_id = "a".into(),
            }
            assert!(
                invalid.validate(&snapshot, &decision).is_err(),
                "quorum mutation {mutation}"
            );
        }
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
            version: 2,
            resource: other.clone(),
            id: [8; 16],
            retired: false,
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
        let mut legacy = snapshot.clone();
        let mut incarnation = resource_incarnation(&legacy, &resource).unwrap().unwrap();
        incarnation.version = 1;
        legacy.attributes.insert(
            crate::history_identity::key(&resource),
            serde_json::to_string(&incarnation).unwrap(),
        );
        assert!(
            proposed_decision(&legacy, &resource, "a", [2; 16]).is_err(),
            "a catalogue ID does not prove creation-time enrollment"
        );
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

    #[test]
    fn serving_projection_withholds_enrolled_and_corrupt_queue_and_stream_assignments() {
        for stream in [false, true] {
            let (mut snapshot, resource) = fixture(stream);
            let hidden = crate::to_fibril_snapshot(&snapshot);
            assert!(hidden.assignments.is_empty());
            assert!(hidden.stream_assignments.is_empty());
            let placement = crate::project_snapshot(&snapshot, false);
            assert_eq!(
                placement.assignments.len() + placement.stream_assignments.len(),
                1
            );
            snapshot
                .attributes
                .insert(crate::history_identity::key(&resource), "{torn".into());
            let hidden = crate::to_fibril_snapshot(&snapshot);
            assert!(hidden.assignments.is_empty());
            assert!(hidden.stream_assignments.is_empty());
            snapshot
                .attributes
                .remove(&crate::history_identity::key(&resource));
            let legacy = crate::to_fibril_snapshot(&snapshot);
            assert_eq!(
                legacy.assignments.len() + legacy.stream_assignments.len(),
                1
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn consensus_preparation_survives_restart_without_granting_writer_permission() {
        use fibril_broker::{
            Partition,
            coordination::QueueIdentity,
            queue_engine::{
                KeratinConfig, QueueEngine, SnapshotConfig, StromaError, StromaKeratinConfig,
            },
        };
        use ganglion_openraft::{InProcessRouter, RaftMetadataNode, default_raft_config};
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
        let resource = crate::to_ganglion_resource(&queue);
        let (left, right) = tokio::join!(
            provider.register_initial_history_resource(&resource),
            provider.register_initial_history_resource(&resource),
        );
        let enrolled = left.unwrap();
        assert_eq!(enrolled, right.unwrap());
        assert_eq!(enrolled.version, 2);
        // An ordinary registration retry cannot downgrade enrollment.
        provider.register_queue(&queue).await.unwrap();
        assert_eq!(
            provider
                .register_initial_history_resource(&resource)
                .await
                .unwrap(),
            enrolled
        );
        let legacy_queue = QueueIdentity::new("legacy", Partition::new(0), None);
        provider.register_queue(&legacy_queue).await.unwrap();
        let legacy_resource = crate::to_ganglion_resource(&legacy_queue);
        assert!(
            provider
                .register_initial_history_resource(&legacy_resource)
                .await
                .is_err()
        );
        let stream = ResourceIdentity::new(crate::STREAM_NAMESPACE, "events", 0, None::<String>);
        provider
            .register_initial_history_resource(&stream)
            .await
            .unwrap();
        provider
            .register_self(&crate::NodeInfo {
                node_id: "a".into(),
                broker_addr: "127.0.0.1:9000".into(),
                admin_addr: None,
            })
            .await
            .unwrap();
        let live = provider.live_nodes(Duration::from_secs(30));
        for _ in 0..2 {
            let serving = provider
                .control_iteration(
                    &fibril_broker::coordination::DeterministicPartitionPlacement,
                    &provider.registered_queues(),
                    &crate::DeterministicStreamPlacement,
                    &provider.registered_streams(),
                    0,
                    0,
                    crate::ReplicationDurabilityPolicy::LocalDurable,
                    &live,
                    8,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(!serving.assignments.contains_key(&queue));
            assert!(serving.assignments.contains_key(&legacy_queue));
            assert!(serving.stream_assignments.is_empty());
        }
        let placed = provider.node.committed_snapshot();
        assert_eq!(placed.assignments.len(), 3);
        assert!(
            provider.pending_recoveries().unwrap().is_empty(),
            "serving quarantine must not make placement rediscover or reassign a resource"
        );
        let mut watch = crate::Coordination::watch(&provider);
        tokio::time::timeout(Duration::from_secs(10), async {
            while watch.borrow_and_update().generation < placed.generation {
                watch.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(!crate::Coordination::owns_queue(
            &provider,
            "q",
            Partition::new(0),
            None
        ));
        assert!(crate::Coordination::owns_queue(
            &provider,
            "legacy",
            Partition::new(0),
            None
        ));
        assert!(!fibril_broker::broker::StreamOwnership::owns_stream(
            &provider,
            "events",
            Partition::new(0)
        ));
        let mut snapshot = provider.node.committed_snapshot();
        let generation = snapshot.generation;
        let mut assignment =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 7);
        assignment.durability = ReplicationDurabilityPolicy::MajorityDurable;
        snapshot.assignments.insert(resource.clone(), assignment);
        let retiring = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "retiring", 0, None::<String>);
        snapshot.assignments.insert(
            retiring.clone(),
            PartitionAssignment::new(retiring.clone(), "a", vec![], 1),
        );
        snapshot.generation += 1;
        provider
            .node
            .write_snapshot_guarded(generation, snapshot)
            .await
            .unwrap();
        assert!(
            provider
                .register_initial_history_resource(&retiring)
                .await
                .is_err()
        );
        assert!(
            resource_incarnation(&provider.node.committed_snapshot(), &retiring)
                .unwrap()
                .is_none()
        );
        assert!(
            provider
                .prepare_initial_history(&legacy_resource)
                .await
                .is_err()
        );

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
        let mut partial =
            InitialHistoryReceiptSet::new(&provider.node.committed_snapshot(), decision.clone())
                .unwrap();
        partial
            .record(&provider.node.committed_snapshot(), "a", receipt)
            .unwrap();
        assert!(
            provider
                .persist_initial_history_quorum(&partial)
                .await
                .is_err()
        );
        assert!(
            provider
                .prepared_initial_history_quorum(&decision)
                .unwrap()
                .is_none()
        );

        // A real local preparation suffices for this owner-only stream. Queue
        // majority/owner requirements are checked above and in the collector test.
        let stream_decision = provider.prepare_initial_history(&stream).await.unwrap();
        assert_eq!(stream_decision.required_write_nodes, 1);
        let stream_receipt = provider
            .prepare_local_initial_history(&stream_decision, &engine)
            .await
            .unwrap();
        let mut complete = InitialHistoryReceiptSet::new(
            &provider.node.committed_snapshot(),
            stream_decision.clone(),
        )
        .unwrap();
        complete
            .record(
                &provider.node.committed_snapshot(),
                "a",
                stream_receipt.clone(),
            )
            .unwrap();
        let persisted = provider
            .persist_initial_history_quorum(&complete)
            .await
            .unwrap();
        assert_eq!(
            provider
                .persist_initial_history_quorum(&complete)
                .await
                .unwrap(),
            persisted
        );
        assert_eq!(
            provider
                .prepared_initial_history_quorum(&stream_decision)
                .unwrap(),
            Some(persisted.clone())
        );
        let mut changed = InitialHistoryReceiptSet::new(
            &provider.node.committed_snapshot(),
            stream_decision.clone(),
        )
        .unwrap();
        let mut changed_receipt = stream_receipt;
        changed_receipt.storage.storage_instance = [9; 16];
        changed
            .record(&provider.node.committed_snapshot(), "a", changed_receipt)
            .unwrap();
        assert!(
            provider
                .persist_initial_history_quorum(&changed)
                .await
                .is_err(),
            "a replacement receipt cannot overwrite the original quorum"
        );
        assert_eq!(
            provider
                .prepared_initial_history_quorum(&stream_decision)
                .unwrap(),
            Some(persisted.clone())
        );
        assert!(
            crate::to_fibril_snapshot(&provider.node.committed_snapshot())
                .stream_assignments
                .is_empty(),
            "persisted preparation evidence cannot expose serving routes"
        );
        assert!(matches!(
            engine
                .ensure_queue_owner_epoch("events", 0, None, Some(stream_decision.assignment.epoch))
                .await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        assert!(matches!(
            engine.ensure_queue_owner_epoch("q", 0, None, Some(7)).await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        let mut forged = decision.clone();
        forged.binding.accepted_history = [8; 16];
        assert!(
            provider
                .prepare_local_initial_history(&forged, &engine)
                .await
                .is_err()
        );
        assert!(
            provider
                .commit_initial_history_activation(&decision, &engine)
                .await
                .is_err(),
            "an insufficient prepared quorum cannot authorize activation"
        );
        assert!(
            provider
                .admit_local_initial_history(&stream_decision, &engine)
                .await
                .is_err(),
            "preparation is not activation authority"
        );
        let activation = provider
            .commit_initial_history_activation(&stream_decision, &engine)
            .await
            .unwrap();
        assert_eq!(
            provider
                .commit_initial_history_activation(&stream_decision, &engine)
                .await
                .unwrap(),
            activation
        );
        assert!(
            matches!(
                engine
                    .ensure_queue_owner_epoch(
                        "events",
                        0,
                        None,
                        Some(stream_decision.assignment.epoch)
                    )
                    .await,
                Err(StromaError::HistoryAdmissionRequired { .. })
            ),
            "metadata activation alone cannot open storage"
        );
        let activation_snapshot = provider.node.committed_snapshot();
        for mutation in 0..3 {
            let mut bad = activation.clone();
            match mutation {
                0 => bad.version = 2,
                1 => bad.decision[0] ^= 1,
                _ => bad.prepared_quorum[0] ^= 1,
            }
            assert!(
                bad.validate(&activation_snapshot, &stream_decision)
                    .is_err()
            );
        }
        let mut changed_receipts = activation_snapshot.clone();
        let mut changed_quorum = persisted.clone();
        changed_quorum
            .reports
            .get_mut("a")
            .unwrap()
            .storage
            .storage_instance = [8; 16];
        changed_receipts.attributes.insert(
            quorum_key(&stream_decision.incarnation),
            serde_json::to_string(&changed_quorum).unwrap(),
        );
        assert!(
            activation
                .validate(&changed_receipts, &stream_decision)
                .is_err()
        );
        let admitted = provider
            .admit_local_initial_history(&stream_decision, &engine)
            .await
            .unwrap();
        assert_eq!(admitted, persisted.reports["a"].storage);
        assert_eq!(
            provider
                .admit_local_initial_history(&stream_decision, &engine)
                .await
                .unwrap(),
            admitted
        );
        assert_eq!(
            provider
                .commit_initial_history_activation(&stream_decision, &engine)
                .await
                .unwrap(),
            activation
        );
        assert_eq!(
            crate::to_fibril_snapshot(&provider.node.committed_snapshot())
                .stream_assignments
                .len(),
            1
        );
        assert!(
            crate::local_serving_snapshot(&provider.node.committed_snapshot(), "a", [9; 16])
                .streams_owned_by("a").is_empty(),
            "a different process cannot use the activated assignment"
        );
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
            reopened
                .initial_history_activation(&stream_decision)
                .unwrap(),
            Some(activation)
        );
        assert_eq!(
            reopened
                .prepared_initial_history_quorum(&stream_decision)
                .unwrap(),
            Some(persisted)
        );
        assert!(
            reopened
                .persist_initial_history_quorum(&complete)
                .await
                .is_err(),
            "persisted receipts do not authorize a replacement owner instance"
        );
        assert!(
            crate::local_serving_snapshot(&snapshot, "a", reopened.history_process)
                .streams_owned_by("a").is_empty()
        );
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
        assert!(
            reopened
                .commit_initial_history_activation(&stream_decision, &engine)
                .await
                .is_err()
        );
        assert!(
            reopened
                .admit_local_initial_history(&stream_decision, &engine)
                .await
                .is_err()
        );
        assert!(
            reopened
                .prepare_local_initial_history(&decision, &engine)
                .await
                .is_err()
        );
        assert_eq!(
            engine.storage_history_binding("q", 0, None).unwrap(),
            Some(decision.binding)
        );
        engine.shutdown().await.unwrap();
        let stream_identity = crate::StreamIdentity::new("events", Partition::new(0));
        reopened.deregister_stream(&stream_identity).await.unwrap();
        let retired_snapshot = reopened.node.committed_snapshot();
        let retired_identity = resource_incarnation(&retired_snapshot, &stream)
            .unwrap()
            .unwrap();
        assert!(retired_identity.retired);
        assert!(!retired_snapshot.resources.contains(&stream));
        assert!(retired_snapshot.assignments.contains_key(&stream));
        assert!(
            crate::to_fibril_snapshot(&retired_snapshot)
                .stream_assignments
                .is_empty()
        );
        assert!(
            reopened
                .register_initial_history_resource(&stream)
                .await
                .is_err()
        );
        assert!(
            !reopened
                .node
                .committed_snapshot()
                .resources
                .contains(&stream)
        );
        assert!(
            reopened
                .prepared_initial_history_quorum(&stream_decision)
                .is_err()
        );
        // Once placement has removed the old assignment, recreation receives a
        // different origin. The old conditional delete cannot erase that origin.
        let mut removed = retired_snapshot.clone();
        removed.assignments.remove(&stream);
        removed.generation += 1;
        reopened
            .node
            .write_snapshot_guarded(retired_snapshot.generation, removed)
            .await
            .unwrap();
        let replacement = reopened
            .register_initial_history_resource(&stream)
            .await
            .unwrap();
        assert_ne!(replacement.id, retired_identity.id);
        assert!(!replacement.retired);
        let stale_delete = reopened
            .forward_command(MetadataRaftCommand::DeregisterResourceReplacingAttribute {
                resource: stream.clone(),
                key: crate::history_identity::key(&stream),
                expected: Some(serde_json::to_string(&retired_identity).unwrap()),
                value: serde_json::to_string(&retired_identity).unwrap(),
            })
            .await;
        assert!(matches!(
            stale_delete,
            Err(OpenraftAdapterError::AttributeMismatch { .. })
        ));
        assert_eq!(
            resource_incarnation(&reopened.node.committed_snapshot(), &stream).unwrap(),
            Some(replacement)
        );
        reopened.node.shutdown().await.unwrap();
        reopened.forwarder.abort();
        drop(reopened);
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
    }
}
