//! Initial activation authority binds the immutable preparation decision to the
//! exact persisted quorum. Ordinary startup remains gated until recovery
//! readmission is integrated.
use crate::{
    GanglionCoordination,
    initial_history::{self, InitialHistoryDecision, InitialHistoryPreparedQuorum},
};
use fibril_broker::queue_engine::{PreparedStorageHistory, StromaEngine};
use ganglion_core::CoordinationSnapshot;
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

const PREFIX: &str = "fibril/initial-history-activation/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialHistoryActivation {
    pub version: u32,
    pub decision: [u8; 32],
    pub prepared_quorum: [u8; 32],
}

fn error(message: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(message.to_string())
}

fn key(decision: &InitialHistoryDecision) -> String {
    format!(
        "{PREFIX}{}",
        serde_json::to_string(&decision.incarnation).expect("incarnation serializes")
    )
}

fn prepared_quorum(
    snapshot: &CoordinationSnapshot,
    decision: &InitialHistoryDecision,
) -> Result<InitialHistoryPreparedQuorum, String> {
    initial_history::validate_committed(snapshot, decision)?;
    let raw = snapshot
        .attributes
        .get(&initial_history::quorum_key(&decision.incarnation))
        .ok_or("initial activation requires persisted prepared-quorum evidence")?;
    let quorum: InitialHistoryPreparedQuorum =
        serde_json::from_str(raw).map_err(|e| e.to_string())?;
    quorum.validate(snapshot, decision)?;
    Ok(quorum)
}

fn quorum_digest(quorum: &InitialHistoryPreparedQuorum) -> Result<[u8; 32], String> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-initial-prepared-quorum-v1\0");
    hash.update(&serde_json::to_vec(quorum).map_err(|e| e.to_string())?);
    Ok(*hash.finalize().as_bytes())
}

impl InitialHistoryActivation {
    pub fn validate(
        &self,
        snapshot: &CoordinationSnapshot,
        decision: &InitialHistoryDecision,
    ) -> Result<(), String> {
        let quorum = prepared_quorum(snapshot, decision)?;
        if self.version != 1
            || self.decision != decision.digest()?
            || self.prepared_quorum != quorum_digest(&quorum)?
        {
            return Err(
                "initial activation does not identify the exact persisted decision and quorum"
                    .into(),
            );
        }
        Ok(())
    }
}

impl GanglionCoordination {
    /// Read historical activation authority without granting local admission.
    pub fn initial_history_activation(
        &self,
        decision: &InitialHistoryDecision,
    ) -> Result<Option<InitialHistoryActivation>, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        initial_history::validate_committed(&snapshot, decision).map_err(error)?;
        let Some(raw) = snapshot.attributes.get(&key(decision)) else {
            return Ok(None);
        };
        let activation: InitialHistoryActivation = serde_json::from_str(raw).map_err(error)?;
        activation.validate(&snapshot, decision).map_err(error)?;
        Ok(Some(activation))
    }

    /// Persist activation only for the original owner and its exact live storage
    /// instance. A lost reply is retryable even after local admission and writes.
    /// This publishes the accepted assignment; local storage admission is separate.
    pub async fn commit_initial_history_activation(
        &self,
        decision: &InitialHistoryDecision,
        engine: &StromaEngine,
    ) -> Result<InitialHistoryActivation, OpenraftAdapterError> {
        if decision.assignment.owner != self.node_id
            || decision.owner_process != self.history_process
        {
            return Err(error(
                "initial activation requires the original owner process",
            ));
        }
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            let quorum = prepared_quorum(&snapshot, decision).map_err(error)?;
            let owner = quorum
                .reports
                .get(&self.node_id)
                .ok_or_else(|| error("prepared owner receipt is missing"))?;
            engine
                .verify_prepared_storage_history(&owner.storage)
                .map_err(error)?;
            let activation = InitialHistoryActivation {
                version: 1,
                decision: decision.digest().map_err(error)?,
                prepared_quorum: quorum_digest(&quorum).map_err(error)?,
            };
            let key = key(decision);
            let expected = snapshot.attributes.get(&key).cloned();
            if let Some(raw) = &expected {
                let existing: InitialHistoryActivation =
                    serde_json::from_str(raw).map_err(error)?;
                existing.validate(&snapshot, decision).map_err(error)?;
                if existing != activation {
                    return Err(error(
                        "initial activation conflicts with persisted authority",
                    ));
                }
            }
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    key,
                    expected,
                    value: serde_json::to_string(&activation).map_err(error)?,
                })
                .await
            {
                Ok(response) => {
                    activation
                        .validate(&response.snapshot, decision)
                        .map_err(error)?;
                    return Ok(activation);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        Err(error(
            "initial activation raced metadata changes; retry with backoff",
        ))
    }

    /// Obtain fresh admission authority for one exact live replica. The stored
    /// quorum alone is insufficient: provider and storage instances must match.
    /// Ordinary startup must wait for automatic recovery readmission integration.
    pub async fn admit_local_initial_history(
        &self,
        decision: &InitialHistoryDecision,
        engine: &StromaEngine,
    ) -> Result<PreparedStorageHistory, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        let quorum = prepared_quorum(&snapshot, decision).map_err(error)?;
        let receipt = quorum.reports.get(&self.node_id).ok_or_else(|| error("replica is outside the activated preparation quorum; recovery installation required"))?;
        if receipt.replica_process != self.history_process {
            return Err(error(
                "replacement replica process requires recovery readmission",
            ));
        }
        engine
            .verify_prepared_storage_history(&receipt.storage)
            .map_err(error)?;
        let key = key(decision);
        let raw = snapshot
            .attributes
            .get(&key)
            .ok_or_else(|| error("initial activation is not committed"))?;
        let activation: InitialHistoryActivation = serde_json::from_str(raw).map_err(error)?;
        activation.validate(&snapshot, decision).map_err(error)?;
        let response = self
            .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                expected_generation: snapshot.generation,
                key,
                expected: Some(raw.clone()),
                value: raw.clone(),
            })
            .await?;
        activation
            .validate(&response.snapshot, decision)
            .map_err(error)?;
        engine
            .admit_prepared_storage_history(receipt.storage.clone())
            .await
            .map_err(error)?;
        Ok(receipt.storage.clone())
    }
}

/// Read the exact currently accepted initial history. Missing activation and
/// changed/pending assignments fail closed. Recovery histories will extend this
/// accessor once installation can establish their continuation authority.
pub(crate) fn accepted_history(
    snapshot: &CoordinationSnapshot,
    resource: &ganglion_core::ResourceIdentity,
) -> Result<fibril_broker::history_replication::AcceptedHistory, String> {
    if let Some(history) = crate::recovery_activation::accepted(snapshot, resource)? {
        return Ok(history);
    }
    let incarnation = crate::history_identity::resource_incarnation(snapshot, resource)?
        .ok_or("resource has no enrolled history")?;
    let raw = snapshot
        .attributes
        .get(&initial_history::key(&incarnation))
        .ok_or("initial decision is absent")?;
    let decision: InitialHistoryDecision = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if decision.incarnation != incarnation {
        return Err("initial decision belongs to a different resource incarnation".into());
    }
    let quorum = prepared_quorum(snapshot, &decision)?;
    let raw = snapshot
        .attributes
        .get(&key(&decision))
        .ok_or("history activation is absent")?;
    let activation: InitialHistoryActivation =
        serde_json::from_str(raw).map_err(|e| e.to_string())?;
    activation.validate(snapshot, &decision)?;
    let mut digest = blake3::Hasher::new();
    digest.update(b"fibril-initial-activation-v1\0");
    digest.update(&serde_json::to_vec(&activation).map_err(|e| e.to_string())?);
    Ok(fibril_broker::history_replication::AcceptedHistory {
        activation: *digest.finalize().as_bytes(),
        binding: decision.binding,
        blocked_local_replica: None,
        owner: decision.assignment.owner,
        replicas: quorum
            .reports
            .into_iter()
            .map(|(id, receipt)| {
                (
                    id,
                    fibril_broker::history_replication::ReplicaHistoryInstance {
                        process: receipt.replica_process,
                        storage: receipt.storage.storage_instance,
                    },
                )
            })
            .collect(),
    })
}

impl GanglionCoordination {
    pub(crate) fn validate_history_replication(
        &self,
        session: &fibril_broker::history_replication::HistoryReplicationSession,
        owner_is_receiver: bool,
    ) -> Result<(), String> {
        if session.receiver != self.node_id
            || session.receiver_instance.process != self.history_process
        {
            return Err("replication history targets another replica process".into());
        }
        let resource = ganglion_core::ResourceIdentity::new(
            if session.stream {
                crate::STREAM_NAMESPACE
            } else {
                crate::QUEUE_NAMESPACE
            },
            session.topic.clone(),
            u64::from(session.partition.id()),
            session.group.clone(),
        );
        let snapshot = self.node.committed_snapshot();
        let accepted = accepted_history(&snapshot, &resource)?;
        if accepted.owner.as_str()
            != if owner_is_receiver {
                session.receiver.as_str()
            } else {
                session.sender.as_str()
            }
            || session.sender == session.receiver
            || accepted.session(
                &session.topic,
                session.partition,
                session.group.as_deref(),
                session.stream,
                &session.sender,
                &session.receiver,
            )? != *session
        {
            return Err(
                "replication history differs from the activated replica identities or direction"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Heartbeats can request a recovery fence when a known prepared process has
/// been replaced, even if placement is unchanged. They never authorize the new
/// process, choose its history, or clear an existing recovery decision.
pub(crate) fn replaced_initial_processes(
    snapshot: &CoordinationSnapshot,
    resource: &ganglion_core::ResourceIdentity,
) -> Result<Vec<String>, String> {
    if let Some(history) = crate::recovery_activation::accepted(snapshot, resource)? {
        return Ok(history.replicas.into_iter().filter_map(|(id, previous)| {
            let current = snapshot.nodes.get(&id)?.labels.get(crate::HISTORY_PROCESS_LABEL)?;
            let current = uuid::Uuid::parse_str(current).ok()?;
            (!current.is_nil() && current.as_bytes() != &previous.process).then_some(id)
        }).collect());
    }
    let Some(incarnation) = crate::history_identity::resource_incarnation(snapshot, resource)?
    else {
        return Ok(Vec::new());
    };
    if incarnation.version != 2 || incarnation.retired {
        return Ok(Vec::new());
    }
    let Some(raw) = snapshot.attributes.get(&initial_history::key(&incarnation)) else {
        return Ok(Vec::new());
    };
    let decision: InitialHistoryDecision = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if decision.incarnation != incarnation {
        return Err("initial decision has wrong incarnation".into());
    }
    initial_history::validate_committed(snapshot, &decision)?;
    let mut expected = std::collections::BTreeMap::from([(
        decision.assignment.owner.clone(),
        decision.owner_process,
    )]);
    if let Some(raw) = snapshot
        .attributes
        .get(&initial_history::quorum_key(&incarnation))
    {
        let quorum: InitialHistoryPreparedQuorum =
            serde_json::from_str(raw).map_err(|e| e.to_string())?;
        quorum.validate(snapshot, &decision)?;
        expected.extend(
            quorum
                .reports
                .into_iter()
                .map(|(id, report)| (id, report.replica_process)),
        );
    }
    Ok(expected
        .into_iter()
        .filter_map(|(id, previous)| {
            let current = snapshot
                .nodes
                .get(&id)?
                .labels
                .get(crate::HISTORY_PROCESS_LABEL)?;
            let current = uuid::Uuid::parse_str(current).ok()?;
            (!current.is_nil() && current.as_bytes() != &previous).then_some(id)
        })
        .collect())
}

/// Recover the immutable authority of the previous assignment. A pending fence
/// closes serving, but must not erase the certificate needed to prove ancestry.
pub(crate) fn previous_initial_history(
    snapshot: &CoordinationSnapshot,
    resource: &ganglion_core::ResourceIdentity,
) -> Result<Option<fibril_broker::history_replication::AcceptedHistory>, String> {
    let Some(incarnation) = crate::history_identity::resource_incarnation(snapshot, resource)?
    else {
        return Ok(None);
    };
    if incarnation.version != 2 || incarnation.retired {
        return Ok(None);
    }
    let Some(raw) = snapshot.attributes.get(&initial_history::key(&incarnation)) else {
        return Ok(None);
    };
    let decision: InitialHistoryDecision = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if decision.incarnation != incarnation {
        return Err("initial decision has wrong incarnation".into());
    }
    if !snapshot.attributes.contains_key(&key(&decision)) {
        return Ok(None);
    }
    let mut previous = snapshot.clone();
    previous
        .attributes
        .remove(&crate::promotion::pending_recovery_key(resource));
    accepted_history(&previous, resource).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        history_identity::ResourceIncarnation,
        initial_history::{InitialHistoryLocalReceipt, InitialHistoryPhase},
        promotion,
        recovery_witnesses::{RecoveryWitnessSet, SealCollectionProgress},
    };
    use fibril_broker::{
        queue_engine::StorageHistoryBinding,
        recovery::{
            BrokerSealedReplica, RecoverySealRequest, RetainedHistoryIdentity,
            SealedReplicaFrontiers,
        },
    };
    use ganglion_core::{PartitionAssignment, ReplicationDurabilityPolicy, ResourceIdentity};
    use std::collections::BTreeMap;

    include!("recovery_selection_tests.rs");

    fn activated() -> (
        CoordinationSnapshot,
        InitialHistoryDecision,
        InitialHistoryPreparedQuorum,
    ) {
        let resource = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, None::<String>);
        let incarnation = ResourceIncarnation {
            version: 2,
            resource: resource.clone(),
            id: [1; 16],
            retired: false,
        };
        let mut assignment =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 7);
        assignment.durability = ReplicationDurabilityPolicy::MajorityDurable;
        let decision = InitialHistoryDecision {
            version: 2,
            phase: InitialHistoryPhase::Preparing,
            incarnation: incarnation.clone(),
            assignment: assignment.clone(),
            owner_process: [2; 16],
            binding: StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [3; 16],
                writer_session: [4; 16],
            },
            required_write_nodes: 2,
        };
        let reports = [("a", [2; 16], [5; 16]), ("b", [6; 16], [7; 16])]
            .into_iter()
            .map(|(node, process, storage)| {
                (
                    node.into(),
                    InitialHistoryLocalReceipt {
                        decision: decision.digest().unwrap(),
                        node_id: node.into(),
                        replica_process: process,
                        storage: PreparedStorageHistory {
                            topic: "q".into(),
                            partition: 0,
                            group: None,
                            stream: false,
                            binding: decision.binding.clone(),
                            storage_instance: storage,
                        },
                    },
                )
            })
            .collect();
        let quorum = InitialHistoryPreparedQuorum {
            version: 1,
            decision: decision.digest().unwrap(),
            reports,
        };
        let activation = InitialHistoryActivation {
            version: 1,
            decision: decision.digest().unwrap(),
            prepared_quorum: quorum_digest(&quorum).unwrap(),
        };
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.resources.insert(resource.clone());
        snapshot.assignments.insert(resource.clone(), assignment);
        snapshot.attributes = BTreeMap::from([
            (
                crate::history_identity::key(&resource),
                serde_json::to_string(&incarnation).unwrap(),
            ),
            (
                initial_history::key(&incarnation),
                serde_json::to_string(&decision).unwrap(),
            ),
            (
                initial_history::quorum_key(&incarnation),
                serde_json::to_string(&quorum).unwrap(),
            ),
            (key(&decision), serde_json::to_string(&activation).unwrap()),
        ]);
        activation.validate(&snapshot, &decision).unwrap();
        (snapshot, decision, quorum)
    }

    #[test]
    fn recovery_uses_exact_eligible_replicas_and_keeps_the_configured_write_threshold() {
        let (snapshot, decision, quorum) = activated();
        let resource = &decision.incarnation.resource;
        let history = accepted_history(&snapshot, resource).unwrap();
        assert_eq!(
            promotion::write_requirement(&decision.assignment).unwrap(),
            2
        );
        assert_eq!(history.replicas.len(), 2);
        let mut recovering = snapshot.clone();
        recovering.generation += 1;
        recovering.assignments.get_mut(resource).unwrap().owner = "b".into();
        recovering.assignments.get_mut(resource).unwrap().followers = vec!["a".into(), "c".into()];
        assert_eq!(
            promotion::retain_unproven_assignments(&snapshot, &mut recovering).unwrap(),
            1
        );
        let pending: promotion::PendingRecovery = serde_json::from_str(
            &recovering.attributes[&promotion::pending_recovery_key(resource)],
        )
        .unwrap();
        assert_eq!(pending.previous_activation, Some(history.activation));
        // Both a and b were needed for every confirm. Either surviving sealed
        // copy intersects that exact write set; unprepared c supplies no proof.
        assert_eq!(pending.required_old_witnesses, 1);
        assert!(accepted_history(&recovering, resource).is_err());
        assert_eq!(
            previous_initial_history(&recovering, resource).unwrap(),
            Some(history.clone())
        );
        let mut witnesses = RecoveryWitnessSet::new(&recovering, &pending).unwrap();
        let report = BrokerSealedReplica {
            node_id: "b".into(),
            seal: SealedReplicaFrontiers {
                request: RecoverySealRequest {
                    transition: pending.transition_digest().unwrap(),
                    fence_epoch: 8,
                },
                history: RetainedHistoryIdentity {
                    version: 2,
                    storage_history: Some(quorum.reports["b"].storage.clone()),
                    id: [9; 32],
                    message_digest: [10; 32],
                    event_digest: [11; 32],
                    snapshot_digest: None,
                    message_head: 0,
                    message_next: 5,
                    event_head: 0,
                    event_next: 7,
                },
                message_head: 0,
                message_next: 5,
                event_head: 0,
                event_next: 7,
            },
        };
        for mutation in 0..5 {
            let mut wrong = report.clone();
            match mutation {
                0 => wrong.seal.history.storage_history = None,
                1 => {
                    wrong
                        .seal
                        .history
                        .storage_history
                        .as_mut()
                        .unwrap()
                        .binding
                        .writer_session = [8; 16]
                }
                2 => {
                    wrong
                        .seal
                        .history
                        .storage_history
                        .as_mut()
                        .unwrap()
                        .storage_instance = [8; 16]
                }
                3 => wrong.seal.history.storage_history.as_mut().unwrap().topic = "other".into(),
                _ => wrong.node_id = "c".into(),
            }
            assert!(witnesses.record(&recovering, "b", wrong).is_err());
        }
        let mut outsider = report.clone();
        outsider.node_id = "c".into();
        assert!(witnesses.record(&recovering, "c", outsider).is_err());
        witnesses.record(&recovering, "b", report).unwrap();
        assert_eq!(
            witnesses.progress(&recovering).unwrap(),
            SealCollectionProgress::AwaitingHistoryValidation {
                received: 1,
                required: 1
            }
        );
        assert_eq!(
            witnesses.accepted_history(&recovering).unwrap(),
            Some(&history)
        );
        let mut forged = pending.clone();
        forged.previous_activation = Some([8; 32]);
        let mut forged_snapshot = recovering.clone();
        forged_snapshot.attributes.insert(
            promotion::pending_recovery_key(resource),
            serde_json::to_string(&forged).unwrap(),
        );
        assert!(RecoveryWitnessSet::new(&forged_snapshot, &forged).is_err());
    }
}
