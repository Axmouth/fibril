//! One bounded metadata record coordinates healthy checkpoint work. Payloads and
//! snapshots stay on their replica. Each node publishes only its own disk proof.
use super::*;
use crate::GanglionCoordination;
use fibril_broker::queue_engine::QueueEngine;
use fibril_broker::queue_engine::{
    PreparedStorageHistory, QueueCheckpointBuildLimits, QueueCheckpointPin, QueueCheckpointTarget,
    StromaEngine,
};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use std::collections::BTreeSet;

const MAX_METADATA: usize = 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct State {
    active: Option<QueueCheckpointEvidence>,
    #[serde(default)]
    cleaned: BTreeSet<String>,
    attempt: Option<Attempt>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    context: QueueCheckpointProposal,
    started_ms: u64,
    pins: BTreeMap<String, QueueCheckpointPin>,
    target: Option<QueueCheckpointTarget>,
    proposal: Option<QueueCheckpointProposal>,
    receipts: BTreeMap<String, QueueCheckpointReceipt>,
    installed: BTreeSet<String>,
    // Contradictory immutable receipts cannot be erased by a retry.
    failed: Option<String>,
    committed: bool,
}
impl Attempt {
    fn id(&self) -> Result<[u8; 32], String> {
        self.context.digest()
    }
    fn validate(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        self.context.validate(snapshot)?;
        let resource = &self.context.incarnation.resource;
        let epoch = snapshot
            .assignments
            .get(resource)
            .ok_or("checkpoint assignment absent")?
            .epoch;
        for (node, pin) in &self.pins {
            let instance = self
                .context
                .replicas
                .get(node)
                .ok_or("checkpoint pin node not admitted")?;
            if pin.attempt != self.id()?
                || pin.storage != storage(&self.context, instance)?
                || pin.event_epoch != epoch
                || pin.message_epoch != epoch
            {
                return Err(format!(
                    "checkpoint pin differs from accepted storage or epoch: node={node}, epochs=({},{}), expected={epoch}, attempt_matches={}, storage_matches={}",
                    pin.message_epoch,
                    pin.event_epoch,
                    pin.attempt == self.id()?,
                    pin.storage == storage(&self.context, instance)?
                ));
            }
        }
        if let Some(target) = &self.target {
            if self.pins.len() != self.context.replicas.len()
                || self.pins.values().any(|p| {
                    target.event_next < p.event_next
                        || target.message_next < p.message_next
                        || target.message_head < p.message_head
                })
            {
                return Err("checkpoint cut does not cover every pinned base".into());
            }
        }
        if let Some(proposal) = &self.proposal {
            proposal.validate(snapshot)?;
            if proposal.candidate != self.context.candidate
                || Some(proposal.contents.target()) != self.target
            {
                return Err("checkpoint proposal differs from selected cut".into());
            }
            for (node, receipt) in &self.receipts {
                proposal.validate_receipt(node, receipt)?;
            }
        } else if !self.receipts.is_empty() || self.committed || !self.installed.is_empty() {
            return Err("checkpoint receipt has no proposal".into());
        }
        if self
            .installed
            .iter()
            .any(|node| !self.context.replicas.contains_key(node))
        {
            return Err("checkpoint installation by an excluded replica".into());
        }
        Ok(())
    }
}
fn key(resource: &ResourceIdentity) -> Result<String, String> {
    Ok(format!(
        "fibril/queue-checkpoint/{}",
        serde_json::to_string(resource).map_err(|e| e.to_string())?
    ))
}
fn state(snapshot: &CoordinationSnapshot, resource: &ResourceIdentity) -> Result<State, String> {
    snapshot
        .attributes
        .get(&key(resource)?)
        .map(|raw| {
            if raw.len() > MAX_METADATA {
                return Err("checkpoint metadata exceeds limit".into());
            }
            serde_json::from_str(raw).map_err(|e| e.to_string())
        })
        .unwrap_or_else(|| Ok(State::default()))
}
fn storage(
    context: &QueueCheckpointProposal,
    instance: &ReplicaHistoryInstance,
) -> Result<PreparedStorageHistory, String> {
    let resource = &context.incarnation.resource;
    Ok(PreparedStorageHistory {
        topic: resource.name.clone(),
        partition: u32::try_from(resource.partition).map_err(|e| e.to_string())?,
        group: resource.group.clone(),
        stream: false,
        binding: context.binding.clone(),
        storage_instance: instance.storage,
    })
}
fn empty_contents() -> QueueCheckpointContents {
    QueueCheckpointContents {
        event_next: 0,
        message_head: 0,
        message_next: 0,
        required_message_next: 0,
        snapshot_digest: [0; 32],
        state_digest: [0; 32],
        message_digest: [0; 32],
        live_payload_digest: [0; 32],
    }
}

impl GanglionCoordination {
    /// Eligible local replicas only. Invalid/uninitialized histories are skipped;
    /// their ordinary admission/recovery workers remain responsible for repair.
    pub fn queue_checkpoint_work(&self, create: bool) -> Vec<ResourceIdentity> {
        let snapshot = self.node.committed_snapshot();
        let resources: Vec<ResourceIdentity> = if create {
            snapshot
                .resources
                .iter()
                .filter(|r| r.namespace == crate::QUEUE_NAMESPACE)
                .cloned()
                .collect()
        } else {
            snapshot
                .attributes
                .keys()
                .filter_map(|key| key.strip_prefix("fibril/queue-checkpoint/"))
                .filter_map(|raw| serde_json::from_str(raw).ok())
                .collect()
        };
        resources
            .into_iter()
            .filter(|r| {
                current_history(&snapshot, r).is_ok_and(|(_, h)| {
                    h.replicas
                        .get(&self.node_id)
                        .is_some_and(|i| i.process == self.history_process)
                })
            })
            .collect()
    }

    async fn checkpoint_cas(
        &self,
        resource: &ResourceIdentity,
        before: &State,
        after: &State,
    ) -> Result<(), String> {
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            current_history(&snapshot, resource)?;
            if state(&snapshot, resource)? != *before {
                return Err("checkpoint attempt advanced concurrently; retry".into());
            }
            if let Some(attempt) = &after.attempt {
                attempt.validate(&snapshot)?;
            }
            if let Some(active) = &after.active {
                active.validate_agreement(&snapshot)?;
            }
            let k = key(resource)?;
            let value = serde_json::to_string(after).map_err(|e| e.to_string())?;
            if value.len() > MAX_METADATA {
                return Err("checkpoint metadata exceeds limit".into());
            }
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    key: k.clone(),
                    expected: snapshot.attributes.get(&k).cloned(),
                    value,
                })
                .await
            {
                Ok(result) => {
                    // Fresh committed authorization is a prerequisite for every
                    // local mutation. Wait for this node's applied metadata too.
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while self.node.committed_snapshot().generation < result.snapshot.generation
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .map_err(|_| "checkpoint metadata has not applied locally")?;
                    return Ok(());
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e.to_string()),
            }
        }
        Err("checkpoint publication raced metadata changes; retry".into())
    }

    /// Progress one phase without holding storage locks across consensus or peer
    /// waits. `start` controls cadence only; an existing attempt always progresses.
    /// Returns true when this replica has installed the current certificate.
    pub async fn queue_checkpoint_step(
        &self,
        resource: &ResourceIdentity,
        engine: &StromaEngine,
        start: bool,
    ) -> Result<bool, String> {
        let _operation = self.checkpoint_operation.lock().await;
        let snapshot = self.node.committed_snapshot();
        let (_, history) = current_history(&snapshot, resource)?;
        if history
            .replicas
            .get(&self.node_id)
            .is_none_or(|i| i.process != self.history_process)
        {
            return Err("checkpoint worker is not the admitted process".into());
        }
        if !engine.is_materialized(
            &resource.name,
            u32::try_from(resource.partition).map_err(|e| e.to_string())?,
            resource.group.as_deref(),
        ) {
            return Ok(false);
        }
        let before = state(&snapshot, resource)?;
        let compatible = before
            .attempt
            .as_ref()
            .is_some_and(|a| a.context.validate(&snapshot).is_ok());
        let finished = before
            .attempt
            .as_ref()
            .is_some_and(|a| a.committed && a.installed.len() == a.context.replicas.len());
        if before.attempt.is_none() || !compatible || (finished && start) {
            if !start || history.owner != self.node_id {
                if before.attempt.is_none() {
                    let context = QueueCheckpointProposal::new(
                        &snapshot,
                        resource,
                        [1; 16],
                        empty_contents(),
                        None,
                    )?;
                    let local = storage(&context, &history.replicas[&self.node_id])?;
                    // Only do cleanup when a checkpoint record exists; default-off
                    // queues incur no metadata writes or disk work.
                    if snapshot.attributes.contains_key(&key(resource)?)
                        && !before.cleaned.contains(&self.node_id)
                    {
                        self.checkpoint_cas(resource, &before, &before).await?;
                        engine
                            .reconcile_queue_checkpoint_pins(
                                local,
                                None,
                                before
                                    .active
                                    .as_ref()
                                    .map(QueueCheckpointEvidence::digest)
                                    .transpose()?,
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                        let mut cleaned = before.clone();
                        cleaned.cleaned.insert(self.node_id.clone());
                        self.checkpoint_cas(resource, &before, &cleaned).await?;
                    }
                }
                return Ok(finished);
            }
            let context = QueueCheckpointProposal::new(
                &snapshot,
                resource,
                *uuid::Uuid::now_v7().as_bytes(),
                empty_contents(),
                None,
            )?;
            let active = before
                .active
                .clone()
                .filter(|a| a.validate_agreement(&snapshot).is_ok());
            let after = State {
                active,
                cleaned: BTreeSet::new(),
                attempt: Some(Attempt {
                    context,
                    started_ms: crate::unix_millis_now(),
                    pins: BTreeMap::new(),
                    target: None,
                    proposal: None,
                    receipts: BTreeMap::new(),
                    installed: BTreeSet::new(),
                    failed: None,
                    committed: false,
                }),
            };
            self.checkpoint_cas(resource, &before, &after).await?;
            return Ok(false);
        }
        let attempt = before.attempt.as_ref().unwrap();
        attempt.validate(&snapshot)?;
        let instance = attempt.context.replicas[&self.node_id].clone();
        let local = storage(&attempt.context, &instance)?;
        engine
            .verify_admitted_storage_history(&local)
            .map_err(|e| e.to_string())?;
        engine
            .verify_prepared_storage_history(&local)
            .map_err(|e| e.to_string())?;
        if let Some(failure) = &attempt.failed {
            self.checkpoint_cas(resource, &before, &before).await?;
            engine
                .reconcile_queue_checkpoint_pins(
                    local,
                    None,
                    before
                        .active
                        .as_ref()
                        .map(QueueCheckpointEvidence::digest)
                        .transpose()?,
                )
                .await
                .map_err(|e| e.to_string())?;
            return Err(format!("checkpoint agreement stopped: {failure}"));
        }
        if !attempt.committed
            && history.owner == self.node_id
            && crate::unix_millis_now().saturating_sub(attempt.started_ms) > 120_000
        {
            let after = State {
                active: before.active.clone(),
                attempt: None,
                cleaned: BTreeSet::new(),
            };
            self.checkpoint_cas(resource, &before, &after).await?;
            engine
                .reconcile_queue_checkpoint_pins(
                    local,
                    None,
                    after
                        .active
                        .as_ref()
                        .map(QueueCheckpointEvidence::digest)
                        .transpose()?,
                )
                .await
                .map_err(|e| e.to_string())?;
            tracing::warn!(
                topic = resource.name,
                partition = resource.partition,
                "checkpoint attempt expired; retained last accepted checkpoint"
            );
            return Ok(false);
        }
        let mut after = before.clone();
        let next = after.attempt.as_mut().unwrap();
        if !attempt.pins.contains_key(&self.node_id) {
            self.checkpoint_cas(resource, &before, &before).await?;
            engine
                .reconcile_queue_checkpoint_pins(
                    local.clone(),
                    Some(attempt.id()?),
                    before
                        .active
                        .as_ref()
                        .map(QueueCheckpointEvidence::digest)
                        .transpose()?,
                )
                .await
                .map_err(|e| e.to_string())?;
            let base = engine
                .begin_queue_checkpoint_pin(local, attempt.id()?)
                .await
                .map_err(|e| e.to_string())?;
            next.pins.insert(self.node_id.clone(), base.pin);
        } else if attempt.committed {
            if attempt.installed.contains(&self.node_id) {
                return Ok(true);
            }
            let active = before
                .active
                .as_ref()
                .ok_or("committed checkpoint lost certificate")?;
            if active.proposal
                != *attempt
                    .proposal
                    .as_ref()
                    .ok_or("committed checkpoint lost proposal")?
                || active.receipts != attempt.receipts
            {
                return Err("committed checkpoint differs from receipts".into());
            }
            active.validate_agreement(&snapshot)?;
            self.checkpoint_cas(resource, &before, &before).await?;
            let receipt = &active.receipts[&self.node_id];
            engine
                .accept_queue_checkpoint(
                    attempt.pins[&self.node_id].clone(),
                    active.digest()?,
                    receipt.capsule,
                    active.proposal.previous,
                )
                .await
                .map_err(|e| e.to_string())?;
            next.installed.insert(self.node_id.clone());
        } else if attempt.target.is_none() {
            if history.owner != self.node_id || attempt.pins.len() != history.replicas.len() {
                return Ok(false);
            }
            let event = attempt.pins.values().map(|p| p.event_next).max().unwrap();
            let head = attempt.pins.values().map(|p| p.message_head).max().unwrap();
            let tail = attempt.pins.values().map(|p| p.message_next).max().unwrap();
            next.target = Some(
                engine
                    .queue_checkpoint_target(local, event, head, tail)
                    .await
                    .map_err(|e| e.to_string())?,
            );
            if before.active.as_ref().is_some_and(|active| {
                next.target.as_ref().unwrap().event_next <= active.proposal.contents.event_next
            }) {
                let idle = State {
                    active: before.active.clone(),
                    attempt: None,
                    cleaned: BTreeSet::new(),
                };
                self.checkpoint_cas(resource, &before, &idle).await?;
                return Ok(true);
            }
        } else if attempt.proposal.is_none() {
            if history.owner != self.node_id {
                return Ok(false);
            }
            let capsule = engine
                .build_queue_checkpoint_capsule(
                    attempt.pins[&self.node_id].clone(),
                    attempt.target.clone().unwrap(),
                    QueueCheckpointBuildLimits::default(),
                )
                .await
                .map_err(|e| e.to_string())?;
            let proposal = QueueCheckpointProposal::new(
                &snapshot,
                resource,
                attempt.context.candidate,
                capsule.contents.clone(),
                before.active.as_ref(),
            )?;
            next.receipts.insert(
                self.node_id.clone(),
                QueueCheckpointReceipt {
                    proposal: proposal.digest()?,
                    node: self.node_id.clone(),
                    instance,
                    contents: capsule.contents.clone(),
                    capsule: capsule.digest().map_err(|e| e.to_string())?,
                },
            );
            next.proposal = Some(proposal);
        } else if !attempt.receipts.contains_key(&self.node_id) {
            let proposal = attempt.proposal.as_ref().unwrap();
            let capsule = engine
                .build_queue_checkpoint_capsule(
                    attempt.pins[&self.node_id].clone(),
                    attempt.target.clone().unwrap(),
                    QueueCheckpointBuildLimits::default(),
                )
                .await
                .map_err(|e| e.to_string())?;
            if capsule.contents != proposal.contents {
                next.failed = Some(format!(
                    "replica {} reconstructed different checkpoint contents",
                    self.node_id
                ));
            } else {
                next.receipts.insert(
                    self.node_id.clone(),
                    QueueCheckpointReceipt {
                        proposal: proposal.digest()?,
                        node: self.node_id.clone(),
                        instance,
                        contents: capsule.contents.clone(),
                        capsule: capsule.digest().map_err(|e| e.to_string())?,
                    },
                );
            }
        } else if history.owner == self.node_id && attempt.receipts.len() == history.replicas.len()
        {
            let mut agreement =
                QueueCheckpointAgreement::new(&snapshot, attempt.proposal.clone().unwrap())?;
            for (node, receipt) in &attempt.receipts {
                agreement.record(&snapshot, node, receipt.clone())?;
            }
            after.active = Some(agreement.finish(&snapshot, before.active.as_ref())?);
            next.committed = true;
        } else {
            return Ok(false);
        }
        self.checkpoint_cas(resource, &before, &after).await?;
        let published = after.attempt.as_ref().unwrap();
        if published.committed && !attempt.committed {
            tracing::info!(
                topic = resource.name,
                partition = resource.partition,
                event_next = published.proposal.as_ref().unwrap().contents.event_next,
                replicas = published.receipts.len(),
                "queue checkpoint committed by all admitted replicas"
            );
        }
        if published.installed.contains(&self.node_id) && !attempt.installed.contains(&self.node_id)
        {
            tracing::info!(
                topic = resource.name,
                partition = resource.partition,
                node = self.node_id,
                event_next = published.proposal.as_ref().unwrap().contents.event_next,
                "queue checkpoint installed with durable retention"
            );
        }

        Ok(after
            .attempt
            .as_ref()
            .unwrap()
            .installed
            .contains(&self.node_id))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueCheckpointStatus {
    pub event_next: Option<u64>,
    pub certificate: Option<[u8; 32]>,
    pub pins: usize,
    pub receipts: usize,
    pub installed: usize,
    pub admitted: usize,
    pub failed: Option<String>,
}
impl GanglionCoordination {
    pub fn queue_checkpoint_status(
        &self,
        resource: &ResourceIdentity,
    ) -> Result<QueueCheckpointStatus, String> {
        let snapshot = self.node.committed_snapshot();
        let (_, history) = current_history(&snapshot, resource)?;
        let state = state(&snapshot, resource)?;
        let attempt = state
            .attempt
            .as_ref()
            .filter(|a| a.context.validate(&snapshot).is_ok());
        let active = state
            .active
            .as_ref()
            .filter(|a| a.validate_agreement(&snapshot).is_ok());
        Ok(QueueCheckpointStatus {
            event_next: active.map(|a| a.proposal.contents.event_next),
            certificate: active.map(QueueCheckpointEvidence::digest).transpose()?,
            pins: attempt.map_or(0, |a| a.pins.len()),
            receipts: attempt.map_or(0, |a| a.receipts.len()),
            installed: attempt.map_or(0, |a| a.installed.len()),
            admitted: history.replicas.len(),
            failed: attempt.and_then(|a| a.failed.clone()),
        })
    }
}
