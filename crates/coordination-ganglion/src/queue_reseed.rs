//! Revocation and re-admission of an existing replica within one queue history.
//! The original electorate is retained for recovery quorum intersection.
use crate::{GanglionCoordination, history_activation, promotion::pending_recovery_key};
use fibril_broker::{
    history_replication::{AcceptedHistory, HistoryReplicationSession, ReplicaHistoryInstance},
    queue_engine::{PreparedStorageHistory, StorageHistoryBinding, StromaEngine},
};
use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

fn error(e: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(e.to_string())
}
fn key(activation: [u8; 32], node: &str) -> String {
    format!(
        "fibril/reseed/{}/{}",
        blake3::Hash::from_bytes(activation),
        serde_json::to_string(node).unwrap()
    )
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReseedPhase {
    Revoking,
    Ready,
    Admitted,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReseed {
    version: u32,
    id: [u8; 16],
    pub assignment: PartitionAssignment,
    pub activation: [u8; 32],
    pub binding: StorageHistoryBinding,
    pub node: String,
    pub instance: ReplicaHistoryInstance,
    owner_instance: ReplicaHistoryInstance,
    pub phase: ReseedPhase,
    completed_cut: Option<(u64, u64)>,
}
impl QueueReseed {
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut h = blake3::Hasher::new();
        h.update(b"fibril-queue-reseed-v1\0");
        h.update(&self.activation);
        h.update(&self.id);
        h.update(self.node.as_bytes());
        Ok(*h.finalize().as_bytes())
    }
    fn validate_base(
        &self,
        s: &CoordinationSnapshot,
        base: &AcceptedHistory,
    ) -> Result<(), String> {
        if self.version != 1
            || self.id == [0; 16]
            || self.assignment.resource.namespace != crate::QUEUE_NAMESPACE
            || s.assignments.get(&self.assignment.resource) != Some(&self.assignment)
            || base.activation != self.activation
            || base.binding != self.binding
            || base.owner != self.assignment.owner
            || self.node == base.owner
            || !self.assignment.followers.contains(&self.node)
            || base.replicas.get(&self.node) != Some(&self.instance)
            || base.replicas.get(&base.owner) != Some(&self.owner_instance)
            || (self.phase == ReseedPhase::Admitted) != self.completed_cut.is_some()
        {
            return Err("reseed differs from the accepted history or exact replica".into());
        }
        Ok(())
    }
    fn validate_live(&self, s: &CoordinationSnapshot) -> Result<(), String> {
        let base = unprojected(s, &self.assignment.resource)?;
        self.validate_base(s, &base)?;
        if s.attributes
            .contains_key(&pending_recovery_key(&self.assignment.resource))
        {
            return Err("reseed is fenced by recovery".into());
        }
        let saved = read(s, self.activation, &self.node)?.ok_or("reseed intent absent")?;
        if saved != *self {
            return Err("reseed intent changed".into());
        }
        for (node, process) in [
            (&self.node, self.instance.process),
            (&base.owner, self.owner_instance.process),
        ] {
            let registered = s
                .nodes
                .get(node)
                .and_then(|n| n.labels.get(crate::HISTORY_PROCESS_LABEL))
                .and_then(|v| uuid::Uuid::parse_str(v).ok());
            if registered.as_ref().map(uuid::Uuid::as_bytes) != Some(&process) {
                return Err("reseed replica process changed".into());
            }
        }
        Ok(())
    }
    pub fn receipt(&self) -> PreparedStorageHistory {
        let r = &self.assignment.resource;
        PreparedStorageHistory {
            topic: r.name.clone(),
            partition: r.partition as u32,
            group: r.group.clone(),
            stream: false,
            binding: self.binding.clone(),
            storage_instance: self.instance.storage,
        }
    }
    pub fn session(&self, s: &CoordinationSnapshot) -> Result<HistoryReplicationSession, String> {
        self.validate_live(s)?;
        if self.phase != ReseedPhase::Ready {
            return Err("reseed owner has not acknowledged revocation".into());
        }
        let r = &self.assignment.resource;
        Ok(HistoryReplicationSession {
            topic: r.name.clone(),
            partition: fibril_broker::Partition::new(
                u32::try_from(r.partition).map_err(|e| e.to_string())?,
            ),
            group: r.group.clone(),
            stream: false,
            activation: self.digest()?,
            binding: self.binding.clone(),
            sender: self.node.clone(),
            sender_instance: self.instance.clone(),
            receiver: self.assignment.owner.clone(),
            receiver_instance: self.owner_instance.clone(),
        })
    }
}
fn read(
    s: &CoordinationSnapshot,
    activation: [u8; 32],
    node: &str,
) -> Result<Option<QueueReseed>, String> {
    s.attributes
        .get(&key(activation, node))
        .map(|v| serde_json::from_str(v).map_err(|e| e.to_string()))
        .transpose()
}
fn unprojected(s: &CoordinationSnapshot, r: &ResourceIdentity) -> Result<AcceptedHistory, String> {
    crate::queue_learner::extend(s, r, history_activation::base_history(s, r)?)
}
pub(crate) fn project(
    s: &CoordinationSnapshot,
    r: &ResourceIdentity,
    mut history: AcceptedHistory,
) -> Result<AcceptedHistory, String> {
    for node in history.replicas.keys().cloned().collect::<Vec<_>>() {
        if let Some(reseed) = read(s, history.activation, &node)? {
            reseed.validate_base(s, &history)?;
            if &reseed.assignment.resource != r {
                return Err("reseed resource mismatch".into());
            }
            history
                .replica_generations
                .insert(node.clone(), reseed.digest()?);
            if reseed.phase != ReseedPhase::Admitted {
                history.suspended_replicas.insert(node);
            }
        }
    }
    Ok(history)
}
impl GanglionCoordination {
    async fn store_reseed(
        &self,
        s: &CoordinationSnapshot,
        value: &QueueReseed,
    ) -> Result<(), OpenraftAdapterError> {
        let k = key(value.activation, &value.node);
        self.forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
            expected_generation: s.generation,
            expected: s.attributes.get(&k).cloned(),
            key: k,
            value: serde_json::to_string(value).map_err(error)?,
        })
        .await?;
        Ok(())
    }
    pub async fn request_queue_reseed(
        &self,
        r: &ResourceIdentity,
        node: &str,
    ) -> Result<(), OpenraftAdapterError> {
        let s = self.node.committed_snapshot();
        let base = unprojected(&s, r).map_err(error)?;
        let a = s
            .assignments
            .get(r)
            .ok_or_else(|| error("reseed assignment absent"))?;
        if s.attributes.contains_key(&pending_recovery_key(r))
            || r.namespace != crate::QUEUE_NAMESPACE
            || !a.followers.contains(&node.to_owned())
            || (self.node_id != node && self.node_id != a.owner)
        {
            return Err(error(
                "reseed requester is not the current owner or target follower",
            ));
        }
        let own = base
            .replicas
            .get(&self.node_id)
            .ok_or_else(|| error("requester not admitted"))?;
        if own.process != self.history_process {
            return Err(error("reseed requester process changed"));
        }
        if let Some(old) = read(&s, base.activation, node).map_err(error)? {
            old.validate_base(&s, &base).map_err(error)?;
            if old.phase != ReseedPhase::Admitted {
                return Ok(());
            }
        }
        let value = QueueReseed {
            version: 1,
            id: *uuid::Uuid::now_v7().as_bytes(),
            assignment: a.clone(),
            activation: base.activation,
            binding: base.binding,
            node: node.to_owned(),
            instance: base
                .replicas
                .get(node)
                .ok_or_else(|| error("reseed target not admitted"))?
                .clone(),
            owner_instance: base.replicas[&a.owner].clone(),
            phase: ReseedPhase::Revoking,
            completed_cut: None,
        };
        self.store_reseed(&s, &value).await
    }
    pub fn queue_reseed_work(&self) -> Result<Vec<QueueReseed>, String> {
        let s = self.node.committed_snapshot();
        let mut result = Vec::new();
        for (r, a) in &s.assignments {
            if r.namespace != crate::QUEUE_NAMESPACE
                || s.attributes.contains_key(&pending_recovery_key(r))
            {
                continue;
            }
            let Some(base) = history_activation::previous_initial_history(&s, r)? else {
                continue;
            };
            for node in &a.followers {
                if let Some(intent) = read(&s, base.activation, node)? {
                    if intent.phase != ReseedPhase::Admitted
                        && (self.node_id == a.owner || self.node_id == *node)
                    {
                        intent.validate_live(&s)?;
                        result.push(intent);
                    }
                }
            }
        }
        Ok(result)
    }
    /// Called only after the owner's broker applied the revocation projection.
    pub async fn acknowledge_queue_reseed(
        &self,
        intent: &QueueReseed,
        broker: &fibril_broker::broker::Broker<StromaEngine>,
    ) -> Result<(), OpenraftAdapterError> {
        let s = self.node.committed_snapshot();
        intent.validate_live(&s).map_err(error)?;
        if intent.assignment.owner != self.node_id
            || intent.owner_instance.process != self.history_process
            || intent.phase != ReseedPhase::Revoking
            || !broker.queue_reseed_revocation_applied(
                &intent.assignment.resource.name,
                intent.assignment.resource.partition as u32,
                intent.assignment.resource.group.as_deref(),
                &intent.node,
                intent.digest().map_err(error)?,
            )
        {
            return Err(error("owner has not applied this exact reseed revocation"));
        }
        let mut next = intent.clone();
        next.phase = ReseedPhase::Ready;
        self.store_reseed(&s, &next).await
    }
    pub async fn authorize_queue_reseed(
        &self,
        intent: &QueueReseed,
    ) -> Result<(), OpenraftAdapterError> {
        let s = self.node.committed_snapshot();
        intent.validate_live(&s).map_err(error)?;
        if intent.node != self.node_id
            || intent.instance.process != self.history_process
            || intent.phase != ReseedPhase::Ready
        {
            return Err(error("reseed requires the exact ready target process"));
        }
        self.store_reseed(&s, intent).await
    }
    pub async fn prepare_queue_reseed(
        &self,
        intent: &QueueReseed,
        engine: &StromaEngine,
    ) -> Result<StromaEngine, OpenraftAdapterError> {
        self.authorize_queue_reseed(intent).await?;
        engine
            .prepare_queue_reseed_storage(intent.receipt(), intent.digest().map_err(error)?)
            .await
            .map_err(error)
    }
    pub async fn admit_queue_reseed(
        &self,
        intent: &QueueReseed,
        engine: &StromaEngine,
        message: u64,
        event: u64,
    ) -> Result<(), OpenraftAdapterError> {
        self.authorize_queue_reseed(intent).await?;
        let r = &intent.assignment.resource;
        engine
            .install_queue_reseed_storage(
                intent.receipt(),
                intent.digest().map_err(error)?,
                intent.assignment.epoch,
                message,
                event,
            )
            .await
            .map_err(error)?;
        engine
            .verify_admitted_storage_history(&intent.receipt())
            .map_err(error)?;
        engine
            .verify_queue_learner_caught_up(
                &r.name,
                r.partition as u32,
                r.group.as_deref(),
                intent.assignment.epoch,
                message,
                event,
            )
            .await
            .map_err(error)?;
        let s = self.node.committed_snapshot();
        intent.validate_live(&s).map_err(error)?;
        let mut next = intent.clone();
        next.phase = ReseedPhase::Admitted;
        next.completed_cut = Some((message, event));
        self.store_reseed(&s, &next).await
    }
    pub(crate) fn validate_reseed_read(
        &self,
        session: &HistoryReplicationSession,
        owner_is_receiver: bool,
    ) -> Result<(), String> {
        if !owner_is_receiver
            || session.stream
            || session.receiver != self.node_id
            || session.receiver_instance.process != self.history_process
        {
            return Err("invalid reseed read direction".into());
        }
        let s = self.node.committed_snapshot();
        let r = ResourceIdentity::new(
            crate::QUEUE_NAMESPACE,
            session.topic.clone(),
            u64::from(session.partition.id()),
            session.group.clone(),
        );
        let base = unprojected(&s, &r)?;
        let intent = read(&s, base.activation, &session.sender)?.ok_or("reseed intent absent")?;
        if intent.session(&s)? != *session {
            return Err("reseed read authority changed".into());
        }
        Ok(())
    }
}
