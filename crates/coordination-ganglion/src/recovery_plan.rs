//! Consensus-persisted installation intent. A plan fixes the selected baseline
//! and the replacement history before storage work. It never grants serving.
use std::collections::BTreeMap;

use fibril_broker::{
    queue_engine::StorageHistoryBinding,
    recovery::{
        BrokerSealedReplica, RecoverySealRequest, RetainedHistoryIdentity, SealedReplicaFrontiers,
        replay::RecoveryQueueStateArtifact,
    },
};
use ganglion_core::CoordinationSnapshot;
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

use crate::{
    GanglionCoordination,
    promotion::PendingRecovery,
    recovery_selection::{QueueRecoverySelection, RecordedQueueSelection},
    recovery_witnesses::{RecoveryWitnessSet, SealCollectionProgress},
};

const PREFIX: &str = "fibril/queue-recovery-plan/";

/// Historical, payload-free intent. Deserialization alone conveys no authority:
/// installation must verify this exact record through fresh consensus first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryPlan {
    version: u32,
    pending: PendingRecovery,
    selected: RecordedQueueSelection,
    witnesses: BTreeMap<String, RetainedHistoryIdentity>,
    binding: StorageHistoryBinding,
}

fn error(message: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(message.to_string())
}

pub(crate) fn key(pending: &PendingRecovery) -> Result<String, String> {
    Ok(format!(
        "{PREFIX}{}",
        blake3::Hash::from_bytes(pending.transition_digest()?)
    ))
}

impl QueueRecoveryPlan {
    pub(crate) fn stage_spec(
        &self,
    ) -> Result<fibril_broker::queue_engine::QueueRecoveryStageSpec, String> {
        let assignment = &self.pending.proposed;
        let s = &self.selected;
        let source = self.source_history().ok_or("recovery source is missing")?;
        let spec = fibril_broker::queue_engine::QueueRecoveryStageSpec {
            plan: self.digest()?,
            topic: assignment.resource.name.clone(),
            partition: u32::try_from(assignment.resource.partition).map_err(|e| e.to_string())?,
            group: assignment.resource.group.clone(),
            binding: self.binding.clone(),
            fence_epoch: assignment.epoch,
            source_history: s.source_history,
            message_head: s.message_head,
            message_next: s.message_next,
            event_next: s.event_next,
            message_digest: source.message_digest,
            snapshot_digest: s.snapshot_digest,
            state_digest: s.state_digest,
            live_payload_digest: s.live_payload_digest,
        };
        Ok(spec)
    }
    pub(crate) fn validate_activated(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        let resource = &self.pending.proposed.resource;
        if self.version != 1
            || resource.namespace != crate::QUEUE_NAMESPACE
            || !snapshot.resources.contains(resource)
            || self.pending.resource_incarnation
                != crate::history_identity::resource_incarnation(snapshot, resource)?
            || snapshot.assignments.get(resource) != Some(&self.pending.proposed)
            || snapshot
                .attributes
                .contains_key(&crate::promotion::pending_recovery_key(resource))
            || crate::promotion::write_requirement(&self.pending.proposed)?
                != self.pending.proposed_write_nodes
            || self.pending.previous_activation.is_none()
            || snapshot
                .attributes
                .get(&key(&self.pending)?)
                .map(|raw| serde_json::from_str::<Self>(raw).map_err(|e| e.to_string()))
                .transpose()?
                .as_ref()
                != Some(self)
        {
            return Err("activated recovery plan differs from current resource authority".into());
        }
        Ok(())
    }
    pub fn pending(&self) -> &PendingRecovery {
        &self.pending
    }
    pub fn binding(&self) -> &StorageHistoryBinding {
        &self.binding
    }
    pub fn source_node(&self) -> &str {
        &self.selected.source_node
    }
    pub fn source_history(&self) -> Option<&RetainedHistoryIdentity> {
        self.witnesses.get(&self.selected.source_node)
    }
    pub fn event_next(&self) -> u64 {
        self.selected.event_next
    }
    pub fn message_next(&self) -> u64 {
        self.selected.message_next
    }
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut h = blake3::Hasher::new();
        h.update(b"fibril-queue-recovery-plan-v1\0");
        h.update(&serde_json::to_vec(self).map_err(|e| e.to_string())?);
        Ok(*h.finalize().as_bytes())
    }

    /// A resumed transfer must reproduce the originally selected state, rather
    /// than silently choosing a newer checkpoint or another sealed history.
    pub fn verify_artifact(&self, artifact: &RecoveryQueueStateArtifact) -> Result<(), String> {
        let evidence = artifact.evidence();
        if evidence.version != 2
            || evidence.history_id != self.selected.source_history
            || evidence.event_next != self.event_next()
            || evidence.message_next != self.message_next()
            || artifact.message_head() != self.selected.message_head
            || artifact.snapshot_digest() != self.selected.snapshot_digest
            || evidence.lease_normalized_state_digest != self.selected.state_digest
            || evidence.live_payload_digest != Some(self.selected.live_payload_digest)
        {
            return Err("recovery artifact differs from the persisted source and state".into());
        }
        Ok(())
    }

    pub(crate) fn proposed(
        snapshot: &CoordinationSnapshot,
        pending: &PendingRecovery,
        witnesses: &RecoveryWitnessSet,
        selection: &QueueRecoverySelection,
    ) -> Result<Self, String> {
        if witnesses.command() != &pending.seal_command()? {
            return Err("recovery witnesses belong to another transition".into());
        }
        let incarnation = pending
            .resource_incarnation
            .as_ref()
            .ok_or("recovery installation requires an enrolled incarnation")?;
        let plan = Self {
            version: 1,
            pending: pending.clone(),
            selected: selection.0.clone(),
            witnesses: witnesses
                .reports(snapshot)?
                .iter()
                .map(|(node, report)| (node.clone(), report.seal.history.clone()))
                .collect(),
            binding: StorageHistoryBinding {
                resource_incarnation: incarnation.id,
                accepted_history: *uuid::Uuid::now_v7().as_bytes(),
                writer_session: *uuid::Uuid::now_v7().as_bytes(),
            },
        };
        plan.validate(snapshot)?;
        Ok(plan)
    }

    fn validate(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        if self.version != 1 || self.pending.previous.resource.namespace != crate::QUEUE_NAMESPACE {
            return Err("unsupported queue recovery plan".into());
        }
        let mut witnesses = RecoveryWitnessSet::new(snapshot, &self.pending)?;
        for (node, history) in &self.witnesses {
            witnesses.record(
                snapshot,
                node,
                BrokerSealedReplica {
                    node_id: node.clone(),
                    seal: SealedReplicaFrontiers {
                        request: RecoverySealRequest {
                            transition: self.pending.transition_digest()?,
                            fence_epoch: self.pending.proposed.epoch,
                        },
                        message_head: history.message_head,
                        message_next: history.message_next,
                        event_head: history.event_head,
                        event_next: history.event_next,
                        history: history.clone(),
                    },
                },
            )?;
        }
        if !matches!(
            witnesses.progress(snapshot)?,
            SealCollectionProgress::AwaitingHistoryValidation { .. }
        ) {
            return Err("recovery plan is missing the old witness intersection".into());
        }
        let accepted = witnesses
            .accepted_history(snapshot)?
            .ok_or("recovery plan requires accepted history authority")?;
        let incarnation = self
            .pending
            .resource_incarnation
            .as_ref()
            .ok_or("recovery plan requires an incarnation")?;
        let decision: crate::initial_history::InitialHistoryDecision = serde_json::from_str(
            snapshot
                .attributes
                .get(&crate::initial_history::key(incarnation))
                .ok_or("recovery plan requires durable-timer origin")?,
        )
        .map_err(|e| e.to_string())?;
        if decision.version != 2 {
            return Err("recovery plan requires durable-timer origin".into());
        }
        let selected = &self.selected;
        let source = self
            .witnesses
            .get(&selected.source_node)
            .ok_or("selected recovery source is absent from witnesses")?;
        if selected.transition != self.pending.transition_digest()?
            || selected.previous_activation != accepted.activation
            || selected.source_history != source.id
            || selected.message_head != source.message_head
            || selected.message_next != source.message_next
            || selected.event_next != source.event_next
            || selected.witnesses
                != self
                    .witnesses
                    .iter()
                    .map(|(node, history)| (node.clone(), history.id))
                    .collect()
            || self.witnesses.values().any(|history| {
                history.event_next > selected.event_next
                    || history.message_next > selected.message_next
            })
        {
            return Err("recovery plan source differs from the verified witness selection".into());
        }
        if self.binding.resource_incarnation != incarnation.id
            || self.binding.accepted_history == [0; 16]
            || self.binding.writer_session == [0; 16]
            || self.binding.accepted_history == accepted.binding.accepted_history
            || self.binding.writer_session == accepted.binding.writer_session
        {
            return Err("recovery plan must start a fresh history and writer session".into());
        }
        Ok(())
    }

    pub(crate) fn validate_committed(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        self.validate(snapshot)?;
        let raw = snapshot
            .attributes
            .get(&key(&self.pending)?)
            .ok_or("queue recovery plan is not committed")?;
        if serde_json::from_str::<Self>(raw).map_err(|e| e.to_string())? != *self {
            return Err("queue recovery plan differs from committed intent".into());
        }
        Ok(())
    }
}

impl GanglionCoordination {
    /// Freshly authorize non-serving staging on one proposed replica. A saved
    /// local metadata view or a deserialized plan is insufficient. The returned
    /// stage cannot replace active data or grant writer/replication admission.
    pub async fn open_local_queue_recovery_stage(
        &self,
        plan: &QueueRecoveryPlan,
        engine: &fibril_broker::queue_engine::StromaEngine,
        artifact: &RecoveryQueueStateArtifact,
        limits: fibril_broker::queue_engine::RecoveryStageLimits,
    ) -> Result<fibril_broker::queue_engine::QueueRecoveryStage, OpenraftAdapterError> {
        plan.verify_artifact(artifact).map_err(error)?;
        let spec = self.authorize_local_queue_recovery_stage(plan).await?;
        engine
            .open_queue_recovery_stage(spec, artifact.state_snapshot().to_vec(), limits)
            .await
            .map_err(error)
    }

    /// Receive the exact snapshot from another completed stage after the old
    /// source disappears. Storage verifies the plan's state and payload digests.
    pub async fn open_local_queue_recovery_stage_from_snapshot(
        &self,
        plan: &QueueRecoveryPlan,
        engine: &fibril_broker::queue_engine::StromaEngine,
        snapshot: Vec<u8>,
        limits: fibril_broker::queue_engine::RecoveryStageLimits,
    ) -> Result<fibril_broker::queue_engine::QueueRecoveryStage, OpenraftAdapterError> {
        let spec = self.authorize_local_queue_recovery_stage(plan).await?;
        engine
            .open_queue_recovery_stage(spec, snapshot, limits)
            .await
            .map_err(error)
    }

    /// Resume an existing stage from its durable snapshot without contacting
    /// the old source. Fresh consensus still gates the exact pending plan.
    pub async fn resume_local_queue_recovery_stage(
        &self,
        plan: &QueueRecoveryPlan,
        engine: &fibril_broker::queue_engine::StromaEngine,
        limits: fibril_broker::queue_engine::RecoveryStageLimits,
    ) -> Result<fibril_broker::queue_engine::QueueRecoveryStage, OpenraftAdapterError> {
        let spec = self.authorize_local_queue_recovery_stage(plan).await?;
        engine
            .resume_queue_recovery_stage(spec, limits)
            .await
            .map_err(error)
    }

    pub(crate) async fn authorize_local_queue_recovery_stage(
        &self,
        plan: &QueueRecoveryPlan,
    ) -> Result<fibril_broker::queue_engine::QueueRecoveryStageSpec, OpenraftAdapterError> {
        let assignment = &plan.pending.proposed;
        if assignment.owner != self.node_id && !assignment.followers.contains(&self.node_id) {
            return Err(error("recovery staging requires a proposed replica"));
        }
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            plan.validate_committed(&snapshot).map_err(error)?;
            let key = key(&plan.pending).map_err(error)?;
            let raw = snapshot.attributes[&key].clone();
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    key,
                    expected: Some(raw.clone()),
                    value: raw,
                })
                .await
            {
                Ok(response) => {
                    plan.validate_committed(&response.snapshot).map_err(error)?;
                    return plan.stage_spec().map_err(error);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
                Err(e) => return Err(e),
            }
        }
        Err(error(
            "recovery authorization raced metadata changes; retry with backoff",
        ))
    }

    /// Read an existing intent after restart. This is a historical read, not
    /// fresh permission for storage mutation or writer admission.
    pub fn queue_recovery_plan(
        &self,
        pending: &PendingRecovery,
    ) -> Result<Option<QueueRecoveryPlan>, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        // Even a missing plan must not disguise an obsolete transition.
        RecoveryWitnessSet::new(&snapshot, pending).map_err(error)?;
        let Some(raw) = snapshot.attributes.get(&key(pending).map_err(error)?) else {
            return Ok(None);
        };
        let plan: QueueRecoveryPlan = serde_json::from_str(raw).map_err(error)?;
        plan.validate_committed(&snapshot).map_err(error)?;
        Ok(Some(plan))
    }

    /// Commit the locally verified selection before staging replacement data.
    /// The first committed plan wins. Retries preserve its IDs and evidence;
    /// conflicting proposals cannot replace it. The pending barrier stays set.
    pub async fn persist_queue_recovery_plan(
        &self,
        pending: &PendingRecovery,
        witnesses: &RecoveryWitnessSet,
        selection: &QueueRecoverySelection,
    ) -> Result<QueueRecoveryPlan, OpenraftAdapterError> {
        if pending.proposed.owner != self.node_id {
            return Err(error(
                "only the proposed owner may persist queue recovery intent",
            ));
        }
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            let proposed = QueueRecoveryPlan::proposed(&snapshot, pending, witnesses, selection)
                .map_err(error)?;
            let key = key(pending).map_err(error)?;
            let expected = snapshot.attributes.get(&key).cloned();
            let plan = if let Some(raw) = &expected {
                let existing: QueueRecoveryPlan = serde_json::from_str(raw).map_err(error)?;
                existing.validate_committed(&snapshot).map_err(error)?;
                if existing.pending != proposed.pending
                    || existing.selected != proposed.selected
                    || existing.witnesses != proposed.witnesses
                {
                    return Err(error(
                        "queue recovery selection conflicts with persisted intent",
                    ));
                }
                existing
            } else {
                proposed
            };
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    key,
                    expected,
                    value: serde_json::to_string(&plan).map_err(error)?,
                })
                .await
            {
                Ok(response) => {
                    plan.validate_committed(&response.snapshot).map_err(error)?;
                    tracing::info!(topic = pending.previous.resource.name,
                        partition = pending.previous.resource.partition,
                        group = pending.previous.resource.group.as_deref(),
                        plan = %blake3::Hash::from_bytes(plan.digest().map_err(error)?),
                        source = plan.source_node(), event_next = plan.event_next(),
                        message_next = plan.message_next(),
                        "queue recovery intent committed; replicas remain fenced during installation");
                    return Ok(plan);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e),
            }
        }
        Err(error(
            "queue recovery plan raced metadata changes; retry with backoff",
        ))
    }
}
