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
