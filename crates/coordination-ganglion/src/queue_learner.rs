//! Non-voting catch-up and additive admission within an unchanged queue history.
use crate::{GanglionCoordination, history_activation, promotion::pending_recovery_key};
use fibril_broker::{
    history_replication::{AcceptedHistory, HistoryReplicationSession, ReplicaHistoryInstance},
    queue_engine::{PreparedStorageHistory, StromaEngine},
};
use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

fn error(e: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(e.to_string())
}
fn key(activation: [u8; 32], node: &str) -> String {
    format!(
        "fibril/learner/{}/{}",
        blake3::Hash::from_bytes(activation),
        serde_json::to_string(node).unwrap()
    )
}
fn admissions_key(activation: [u8; 32]) -> String {
    format!(
        "fibril/learner-admissions/{}",
        blake3::Hash::from_bytes(activation)
    )
}
fn receipt_key(intent: &QueueLearner) -> Result<String, String> {
    Ok(format!(
        "fibril/learner-prepared/{}",
        blake3::Hash::from_bytes(intent.digest()?)
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueLearner {
    version: u32,
    id: [u8; 16],
    pub assignment: PartitionAssignment,
    pub activation: [u8; 32],
    pub binding: fibril_broker::queue_engine::StorageHistoryBinding,
    pub node: String,
    pub process: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    intent: QueueLearner,
    storage: PreparedStorageHistory,
    /// Least queue log end the learner had to reach.
    target: u64,
    /// Queue log end the learner verified at admission.
    next: u64,
}
/// Queues whose identity the learner records accept. Named default groups are
/// normalized away before they reach coordination.
pub(crate) fn supports(resource: &ResourceIdentity) -> bool {
    resource.namespace == crate::QUEUE_NAMESPACE
        && !resource
            .group
            .as_deref()
            .is_some_and(|g| g.is_empty() || g == "default")
}

/// Whether `node` is outside the accepted write set for `process`: either not
/// eligible at all, or eligible under an earlier process that restarted.
fn excluded(base: &AcceptedHistory, node: &str, process: [u8; 16]) -> bool {
    base.replicas.get(node).is_none_or(|r| r.process != process)
}

impl QueueLearner {
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut h = blake3::Hasher::new();
        h.update(b"fibril-queue-learner-v1\0");
        h.update(&serde_json::to_vec(self).map_err(|e| e.to_string())?);
        Ok(*h.finalize().as_bytes())
    }
    /// Everything but the live intent record. Admitted evidence embeds its
    /// intent, so a later intent from a restarted process cannot void it.
    fn validate_shape(
        &self,
        snapshot: &CoordinationSnapshot,
        base: &AcceptedHistory,
    ) -> Result<(), String> {
        if self.version != 1
            || self.id == [0; 16]
            || self.process == [0; 16]
            || !supports(&self.assignment.resource)
            || snapshot.assignments.get(&self.assignment.resource) != Some(&self.assignment)
            || snapshot
                .attributes
                .contains_key(&pending_recovery_key(&self.assignment.resource))
            || base.activation != self.activation
            || base.binding != self.binding
            || base.owner != self.assignment.owner
            || self.node == base.owner
            || !self.assignment.followers.contains(&self.node)
            || !excluded(base, &self.node, self.process)
        {
            return Err("learner differs from the active history or is already eligible".into());
        }
        Ok(())
    }
    fn validate_base(
        &self,
        snapshot: &CoordinationSnapshot,
        base: &AcceptedHistory,
    ) -> Result<(), String> {
        self.validate_shape(snapshot, base)?;
        let raw = snapshot
            .attributes
            .get(&key(self.activation, &self.node))
            .ok_or("learner intent absent")?;
        if serde_json::from_str::<Self>(raw).map_err(|e| e.to_string())? != *self {
            return Err("learner intent was replaced".into());
        }
        Ok(())
    }
    fn validate_processes(
        &self,
        snapshot: &CoordinationSnapshot,
        base: &AcceptedHistory,
    ) -> Result<(), String> {
        for (node, process) in [
            (&self.node, self.process),
            (&base.owner, base.replicas[&base.owner].process),
        ] {
            let registered = snapshot
                .nodes
                .get(node)
                .and_then(|n| n.labels.get(crate::HISTORY_PROCESS_LABEL))
                .and_then(|s| uuid::Uuid::parse_str(s).ok());
            if registered.as_ref().map(uuid::Uuid::as_bytes) != Some(&process) {
                return Err("learner or owner process registration changed".into());
            }
        }
        Ok(())
    }
    fn validate(&self, snapshot: &CoordinationSnapshot) -> Result<AcceptedHistory, String> {
        let base = history_activation::accepted_history(snapshot, &self.assignment.resource)?;
        self.validate_base(snapshot, &base)?;
        self.validate_processes(snapshot, &base)?;
        Ok(base)
    }
    fn receipt(&self, snapshot: &CoordinationSnapshot) -> Result<PreparedStorageHistory, String> {
        let receipt: PreparedStorageHistory = serde_json::from_str(
            snapshot
                .attributes
                .get(&receipt_key(self)?)
                .ok_or("learner storage receipt absent")?,
        )
        .map_err(|e| e.to_string())?;
        let r = &self.assignment.resource;
        if receipt.topic != r.name
            || u64::from(receipt.partition) != r.partition
            || receipt.group != r.group
            || receipt.stream
            || receipt.binding != self.binding
            || receipt.storage_instance == [0; 16]
        {
            return Err("learner receipt does not identify the exact storage history".into());
        }
        Ok(receipt)
    }
    pub fn session(
        &self,
        snapshot: &CoordinationSnapshot,
    ) -> Result<HistoryReplicationSession, String> {
        let base = self.validate(snapshot)?;
        let receipt = self.receipt(snapshot)?;
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
            sender_instance: ReplicaHistoryInstance {
                process: self.process,
                storage: receipt.storage_instance,
            },
            receiver: base.owner.clone(),
            receiver_instance: base.replicas[&base.owner].clone(),
        })
    }
}

/// The base history/session remains stable for existing peers. The additive
/// evidence is fixed before any recovery fence can be committed; recovery reads
/// the complete eligible set and increases its intersection threshold accordingly.
pub(crate) fn extend(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
    mut base: AcceptedHistory,
) -> Result<AcceptedHistory, String> {
    let Some(raw) = snapshot.attributes.get(&admissions_key(base.activation)) else {
        return Ok(base);
    };
    let admissions: BTreeMap<String, Admission> = serde_json::from_str(raw).map_err(|e| {
        format!("learner admissions are unreadable, records from an older build need a fresh cluster: {e}")
    })?;
    let original = base.clone();
    for (node, a) in admissions {
        a.intent.validate_shape(snapshot, &original)?;
        // A restarted process may have replaced the live intent, but some
        // intent for this node must remain.
        if !snapshot.attributes.contains_key(&key(a.intent.activation, &node)) {
            return Err("learner admission lost its intent record".into());
        }
        if a.intent.assignment.resource != *resource
            || a.intent.node != node
            || a.intent.receipt(snapshot)? != a.storage
            || a.next < a.target
        {
            return Err(
                "learner admission differs from its prepared history and verified cut".into(),
            );
        }
        base.replicas.insert(
            node,
            ReplicaHistoryInstance {
                process: a.intent.process,
                storage: a.storage.storage_instance,
            },
        );
    }
    Ok(base)
}

/// Rejoining an eligible replica rewrites its local storage receipt, so its
/// old copy stops being a valid recovery seal witness until admission records
/// the new instance. If the owner fails meanwhile, recovery still needs
/// E - W + 1 seals, so at most W - 1 eligible replicas may rejoin at once.
/// Every intent from an eligible node that is not yet admitted counts, even
/// one whose process restarted again, since its receipt may already be gone.
fn rejoin_slot_available(
    snapshot: &CoordinationSnapshot,
    history: &AcceptedHistory,
    assignment: &PartitionAssignment,
    node: &str,
) -> Result<(), String> {
    let writes = crate::promotion::write_requirement(assignment)?;
    let mut rejoining = 0;
    for (id, instance) in &history.replicas {
        if id == node {
            continue;
        }
        if let Some(raw) = snapshot.attributes.get(&key(history.activation, id)) {
            let intent: QueueLearner = serde_json::from_str(raw).map_err(|e| e.to_string())?;
            if intent.process != instance.process {
                rejoining += 1;
            }
        }
    }
    if rejoining + 1 >= writes {
        return Err(format!(
            "{rejoining} other replica(s) are rejoining and W is {writes}; waiting so a recovery could still collect enough seals"
        ));
    }
    Ok(())
}

impl GanglionCoordination {
    async fn await_learner_metadata(&self, generation: u64) -> Result<(), OpenraftAdapterError> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while self.node.committed_snapshot().generation < generation {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| error("learner metadata has not applied locally; retry"))
    }
    pub fn queue_learner_work(&self) -> Result<Vec<ResourceIdentity>, String> {
        let s = self.node.committed_snapshot();
        let mut work = vec![];
        for (r, a) in &s.assignments {
            if r.namespace != crate::QUEUE_NAMESPACE
                || !a.followers.contains(&self.node_id)
                || s.attributes.contains_key(&pending_recovery_key(r))
            {
                continue;
            }
            let Some(h) = history_activation::previous_initial_history(&s, r)? else {
                continue;
            };
            // A restarted follower rejoins the same way as a new one.
            if supports(r) && excluded(&h, &self.node_id, self.history_process) {
                work.push(r.clone());
            }
        }
        Ok(work)
    }
    pub async fn begin_queue_learner(
        &self,
        resource: &ResourceIdentity,
    ) -> Result<QueueLearner, OpenraftAdapterError> {
        for _ in 0..8 {
            let s = self.node.committed_snapshot();
            let history = history_activation::accepted_history(&s, resource).map_err(error)?;
            let assignment = s
                .assignments
                .get(resource)
                .ok_or_else(|| error("learner assignment absent"))?
                .clone();
            if !excluded(&history, &self.node_id, self.history_process)
                || history.owner == self.node_id
                || !assignment.followers.contains(&self.node_id)
                || !supports(resource)
            {
                return Err(error("only an excluded queue follower may learn"));
            }
            if history.replicas.contains_key(&self.node_id) {
                rejoin_slot_available(&s, &history, &assignment, &self.node_id).map_err(error)?;
            }
            let k = key(history.activation, &self.node_id);
            let expected = s.attributes.get(&k).cloned();
            let old = expected
                .as_ref()
                .map(|r| serde_json::from_str::<QueueLearner>(r).map_err(error))
                .transpose()?;
            let intent = match old {
                Some(old)
                    if old.process == self.history_process
                        && old.assignment == assignment
                        && old.binding == history.binding =>
                {
                    old
                }
                _ => QueueLearner {
                    version: 1,
                    id: *uuid::Uuid::now_v7().as_bytes(),
                    assignment,
                    activation: history.activation,
                    binding: history.binding,
                    node: self.node_id.clone(),
                    process: self.history_process,
                },
            };
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: s.generation,
                    key: k,
                    expected,
                    value: serde_json::to_string(&intent).map_err(error)?,
                })
                .await
            {
                Ok(r) => {
                    intent.validate(&r.snapshot).map_err(error)?;
                    self.await_learner_metadata(r.snapshot.generation).await?;
                    return Ok(intent);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e),
            }
        }
        Err(error("learner intent raced metadata changes; retry"))
    }
    pub async fn authorize_queue_learner(
        &self,
        intent: &QueueLearner,
    ) -> Result<(), OpenraftAdapterError> {
        if intent.node != self.node_id || intent.process != self.history_process {
            return Err(error("learner belongs to another process"));
        }
        let s = self.node.committed_snapshot();
        intent.validate(&s).map_err(error)?;
        let k = key(intent.activation, &intent.node);
        let raw = s.attributes[&k].clone();
        let r = self
            .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                expected_generation: s.generation,
                key: k,
                expected: Some(raw.clone()),
                value: raw,
            })
            .await?;
        intent.validate(&r.snapshot).map_err(error)?;
        Ok(())
    }
    pub async fn prepare_queue_learner(
        &self,
        intent: &QueueLearner,
        engine: &StromaEngine,
    ) -> Result<PreparedStorageHistory, OpenraftAdapterError> {
        self.authorize_queue_learner(intent).await?;
        let r = &intent.assignment.resource;
        let receipt = engine
            .prepare_queue_learner_storage(
                &r.name,
                u32::try_from(r.partition).map_err(error)?,
                r.group.as_deref(),
                intent.binding.clone(),
                intent.digest().map_err(error)?,
            )
            .await
            .map_err(error)?;
        for _ in 0..8 {
            let s = self.node.committed_snapshot();
            intent.validate(&s).map_err(error)?;
            engine
                .verify_admitted_storage_history(&receipt)
                .map_err(error)?;
            let k = receipt_key(intent).map_err(error)?;
            let expected = s.attributes.get(&k).cloned();
            if let Some(raw) = &expected {
                if serde_json::from_str::<PreparedStorageHistory>(raw).map_err(error)? != receipt {
                    return Err(error(
                        "learner storage changed within the same process; restart required",
                    ));
                }
            }
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: s.generation,
                    key: k,
                    expected,
                    value: serde_json::to_string(&receipt).map_err(error)?,
                })
                .await
            {
                Ok(r) => {
                    intent.validate(&r.snapshot).map_err(error)?;
                    self.await_learner_metadata(r.snapshot.generation).await?;
                    return Ok(receipt);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e),
            }
        }
        Err(error("learner preparation raced metadata changes; retry"))
    }
    pub(crate) fn validate_learner_read(
        &self,
        session: &HistoryReplicationSession,
        owner_is_receiver: bool,
    ) -> Result<(), String> {
        if !owner_is_receiver
            || session.stream
            || session.receiver != self.node_id
            || session.receiver_instance.process != self.history_process
        {
            return Err("learner authority only permits reading from the exact owner".into());
        }
        let s = self.node.committed_snapshot();
        let resource = ResourceIdentity::new(
            crate::QUEUE_NAMESPACE,
            session.topic.clone(),
            u64::from(session.partition.id()),
            session.group.clone(),
        );
        let base = history_activation::accepted_history(&s, &resource)?;
        let intent: QueueLearner = serde_json::from_str(
            s.attributes
                .get(&key(base.activation, &session.sender))
                .ok_or("learner intent absent")?,
        )
        .map_err(|e| e.to_string())?;
        if intent.session(&s)? != *session {
            return Err("learner read identity differs from committed authority".into());
        }
        Ok(())
    }
    pub async fn admit_queue_learner(
        &self,
        intent: &QueueLearner,
        engine: &StromaEngine,
        target: u64,
    ) -> Result<(), OpenraftAdapterError> {
        if intent.node != self.node_id || intent.process != self.history_process {
            return Err(error("only the learner process may request admission"));
        }
        let r = &intent.assignment.resource;
        let ready = engine
            .verify_queue_learner_caught_up(
                &r.name,
                u32::try_from(r.partition).map_err(error)?,
                r.group.as_deref(),
                intent.assignment.epoch,
                target,
            )
            .await
            .map_err(error)?;
        for _ in 0..8 {
            let s = self.node.committed_snapshot();
            let base = history_activation::base_history(&s, r).map_err(error)?;
            intent.validate_base(&s, &base).map_err(error)?;
            intent.validate_processes(&s, &base).map_err(error)?;
            let storage = intent.receipt(&s).map_err(error)?;
            engine
                .verify_admitted_storage_history(&storage)
                .map_err(error)?;
            let k = admissions_key(intent.activation);
            let expected = s.attributes.get(&k).cloned();
            let mut admissions: BTreeMap<String, Admission> = expected
                .as_ref()
                .map(|raw| serde_json::from_str(raw).map_err(error))
                .transpose()?
                .unwrap_or_default();
            let admission = Admission {
                intent: intent.clone(),
                storage,
                target,
                next: ready.next,
            };
            match admissions.get(&self.node_id) {
                // Same process: a retry of a stored admission.
                Some(old) if old.intent.process == intent.process => {
                    if old.intent != *intent || old.storage != admission.storage {
                        return Err(error(
                            "learner admission conflicts with persisted authority",
                        ));
                    }
                }
                // Absent, or admitted under a process that has since restarted.
                _ => {
                    admissions.insert(self.node_id.clone(), admission);
                }
            }
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: s.generation,
                    key: k,
                    expected,
                    value: serde_json::to_string(&admissions).map_err(error)?,
                })
                .await
            {
                Ok(r) => {
                    history_activation::accepted_history(&r.snapshot, &intent.assignment.resource)
                        .map_err(error)?;
                    return Ok(());
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e),
            }
        }
        Err(error("learner admission raced metadata changes; retry"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_learner_evidence_survives_restart_but_fresh_authority_does_not() {
        let resource = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, None::<String>);
        let assignment =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 1);
        let binding = fibril_broker::queue_engine::StorageHistoryBinding {
            resource_incarnation: [1; 16],
            accepted_history: [2; 16],
            writer_session: [3; 16],
        };
        let instance = ReplicaHistoryInstance {
            process: [1; 16],
            storage: [2; 16],
        };
        let base = AcceptedHistory {
            activation: [1; 32],
            binding: binding.clone(),
            owner: "a".into(),
            replicas: BTreeMap::from([("a".into(), instance.clone()), ("b".into(), instance)]),
            blocked_local_replica: None,
            suspended_replicas: Default::default(),
            replica_generations: Default::default(),
        };
        let intent = QueueLearner {
            version: 1,
            id: [4; 16],
            assignment: assignment.clone(),
            activation: base.activation,
            binding: binding.clone(),
            node: "c".into(),
            process: [5; 16],
        };
        let storage = PreparedStorageHistory {
            topic: "q".into(),
            partition: 0,
            group: None,
            stream: false,
            binding,
            storage_instance: [6; 16],
        };
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.assignments.insert(resource.clone(), assignment);
        for (id, process) in [("a", [1; 16]), ("c", [5; 16])] {
            let mut node = ganglion_core::NodeInfo::new(id, "127.0.0.1:1", None::<String>);
            node.labels.insert(
                crate::HISTORY_PROCESS_LABEL.into(),
                uuid::Uuid::from_bytes(process).to_string(),
            );
            snapshot.nodes.insert(id.into(), node);
        }
        snapshot.attributes.insert(
            key(base.activation, "c"),
            serde_json::to_string(&intent).unwrap(),
        );
        snapshot.attributes.insert(
            receipt_key(&intent).unwrap(),
            serde_json::to_string(&storage).unwrap(),
        );
        intent.validate_base(&snapshot, &base).unwrap();
        intent.validate_processes(&snapshot, &base).unwrap();
        let admission = Admission {
            intent: intent.clone(),
            storage,
            target: 5,
            next: 5,
        };
        snapshot.attributes.insert(
            admissions_key(base.activation),
            serde_json::to_string(&BTreeMap::from([("c", admission.clone())])).unwrap(),
        );
        let joined = extend(&snapshot, &resource, base.clone()).unwrap();
        assert_eq!(joined.replicas.len(), 3);
        assert_eq!(joined.activation, base.activation);
        snapshot.nodes.get_mut("c").unwrap().labels.insert(
            crate::HISTORY_PROCESS_LABEL.into(),
            uuid::Uuid::from_bytes([9; 16]).to_string(),
        );
        assert!(intent.validate_processes(&snapshot, &base).is_err());
        // Historical evidence must remain intact so recovery can fence/count it.
        assert_eq!(extend(&snapshot, &resource, base.clone()).unwrap(), joined);
        for mutation in 0..5 {
            let mut bad = snapshot.clone();
            match mutation {
                0 => {
                    bad.attributes.remove(&key(base.activation, "c"));
                }
                1 => {
                    bad.attributes
                        .insert(pending_recovery_key(&resource), "pending".into());
                }
                2 => {
                    bad.assignments.get_mut(&resource).unwrap().epoch += 1;
                }
                3 => {
                    bad.attributes.remove(&receipt_key(&intent).unwrap());
                }
                _ => {
                    let mut short = admission.clone();
                    short.next = 4;
                    bad.attributes.insert(
                        admissions_key(base.activation),
                        serde_json::to_string(&BTreeMap::from([("c", short)])).unwrap(),
                    );
                }
            }
            assert!(extend(&bad, &resource, base.clone()).is_err());
        }
    }

    fn label(snapshot: &mut CoordinationSnapshot, id: &str, process: [u8; 16]) {
        let node = snapshot
            .nodes
            .entry(id.into())
            .or_insert_with(|| ganglion_core::NodeInfo::new(id, "127.0.0.1:1", None::<String>));
        node.labels.insert(
            crate::HISTORY_PROCESS_LABEL.into(),
            uuid::Uuid::from_bytes(process).to_string(),
        );
    }

    /// Owner a with eligible followers b and c, each with its own process.
    fn eligible_three(
        durability: ganglion_core::ReplicationDurabilityPolicy,
    ) -> (CoordinationSnapshot, AcceptedHistory, PartitionAssignment) {
        let resource = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, None::<String>);
        let mut assignment =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 1);
        assignment.durability = durability;
        let base = AcceptedHistory {
            activation: [1; 32],
            binding: fibril_broker::queue_engine::StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
            owner: "a".into(),
            replicas: [("a", 1u8), ("b", 2), ("c", 3)]
                .into_iter()
                .map(|(id, n)| {
                    (id.into(), ReplicaHistoryInstance { process: [n; 16], storage: [n + 10; 16] })
                })
                .collect(),
            blocked_local_replica: None,
            suspended_replicas: Default::default(),
            replica_generations: Default::default(),
        };
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.assignments.insert(resource, assignment.clone());
        for (id, instance) in &base.replicas {
            label(&mut snapshot, id, instance.process);
        }
        (snapshot, base, assignment)
    }

    fn rejoin(
        snapshot: &mut CoordinationSnapshot,
        base: &AcceptedHistory,
        assignment: &PartitionAssignment,
        node: &str,
        process: [u8; 16],
    ) -> QueueLearner {
        let intent = QueueLearner {
            version: 1,
            id: [process[0]; 16],
            assignment: assignment.clone(),
            activation: base.activation,
            binding: base.binding.clone(),
            node: node.into(),
            process,
        };
        snapshot.attributes.insert(
            key(base.activation, node),
            serde_json::to_string(&intent).unwrap(),
        );
        intent
    }

    fn admit(
        snapshot: &mut CoordinationSnapshot,
        intent: &QueueLearner,
        storage: [u8; 16],
        admissions: &mut BTreeMap<String, Admission>,
    ) {
        let r = &intent.assignment.resource;
        let receipt = PreparedStorageHistory {
            topic: r.name.clone(),
            partition: r.partition as u32,
            group: None,
            stream: false,
            binding: intent.binding.clone(),
            storage_instance: storage,
        };
        snapshot.attributes.insert(
            receipt_key(intent).unwrap(),
            serde_json::to_string(&receipt).unwrap(),
        );
        admissions.insert(
            intent.node.clone(),
            Admission { intent: intent.clone(), storage: receipt, target: 9, next: 9 },
        );
        snapshot.attributes.insert(
            admissions_key(intent.activation),
            serde_json::to_string(admissions).unwrap(),
        );
    }

    #[test]
    fn restarted_eligible_follower_rejoins_as_a_learner_under_its_new_process() {
        let (mut snapshot, base, assignment) =
            eligible_three(ganglion_core::ReplicationDurabilityPolicy::MajorityDurable);
        let resource = assignment.resource.clone();
        // The current process of an eligible follower cannot learn again.
        let current = rejoin(&mut snapshot, &base, &assignment, "c", [3; 16]);
        assert!(current.validate_base(&snapshot, &base).is_err());

        label(&mut snapshot, "c", [4; 16]);
        let first = rejoin(&mut snapshot, &base, &assignment, "c", [4; 16]);
        first.validate_base(&snapshot, &base).unwrap();
        first.validate_processes(&snapshot, &base).unwrap();
        // The owner never rejoins as a learner.
        let mut owner = first.clone();
        owner.node = "a".into();
        assert!(owner.validate_shape(&snapshot, &base).is_err());

        let mut admissions = BTreeMap::new();
        admit(&mut snapshot, &first, [40; 16], &mut admissions);
        let joined = extend(&snapshot, &resource, base.clone()).unwrap();
        assert_eq!(joined.replicas.len(), 3, "the slot is replaced, not added");
        assert_eq!(joined.replicas["c"], ReplicaHistoryInstance { process: [4; 16], storage: [40; 16] });
        assert_eq!(joined.replicas["b"], base.replicas["b"]);
        assert!(first.validate_base(&snapshot, &joined).is_err(), "admitted process is eligible");

        // c restarts again. Its new intent replaces the live record, but the
        // admitted evidence stays valid until the next admission replaces it.
        label(&mut snapshot, "c", [5; 16]);
        let second = rejoin(&mut snapshot, &base, &assignment, "c", [5; 16]);
        assert_eq!(extend(&snapshot, &resource, base.clone()).unwrap(), joined);
        assert!(first.validate_base(&snapshot, &base).is_err());
        second.validate_base(&snapshot, &joined).unwrap();
        admit(&mut snapshot, &second, [50; 16], &mut admissions);
        let rejoined = extend(&snapshot, &resource, base.clone()).unwrap();
        assert_eq!(rejoined.replicas.len(), 3);
        assert_eq!(rejoined.replicas["c"], ReplicaHistoryInstance { process: [5; 16], storage: [50; 16] });
    }

    #[test]
    fn concurrent_rejoins_leave_enough_seal_witnesses_for_recovery() {
        use ganglion_core::ReplicationDurabilityPolicy as P;
        // Three copies, W = 2: recovery needs 2 of 3 seals, so one rejoin at a time.
        let (mut snapshot, base, assignment) = eligible_three(P::MajorityDurable);
        rejoin_slot_available(&snapshot, &base, &assignment, "b").unwrap();
        label(&mut snapshot, "c", [4; 16]);
        let c = rejoin(&mut snapshot, &base, &assignment, "c", [4; 16]);
        assert!(rejoin_slot_available(&snapshot, &base, &assignment, "b").is_err());
        // Its own earlier intent never blocks a node.
        rejoin_slot_available(&snapshot, &base, &assignment, "c").unwrap();
        // c restarting again still holds the slot: its receipt may be gone.
        label(&mut snapshot, "c", [5; 16]);
        assert!(rejoin_slot_available(&snapshot, &base, &assignment, "b").is_err());
        // Once admitted, c seals with its new instance and frees the slot.
        label(&mut snapshot, "c", [4; 16]);
        let mut admissions = BTreeMap::new();
        admit(&mut snapshot, &c, [40; 16], &mut admissions);
        let joined = extend(&snapshot, &assignment.resource, base.clone()).unwrap();
        rejoin_slot_available(&snapshot, &joined, &assignment, "b").unwrap();

        // Five copies, W = 3: two rejoins may overlap, not three.
        let (mut snapshot, mut base, mut assignment) = eligible_three(P::MajorityDurable);
        assignment.followers.extend(["d".to_string(), "e".to_string()]);
        snapshot.assignments.insert(assignment.resource.clone(), assignment.clone());
        for (id, n) in [("d", 6u8), ("e", 7)] {
            base.replicas.insert(id.into(), ReplicaHistoryInstance { process: [n; 16], storage: [n; 16] });
            label(&mut snapshot, id, [n; 16]);
        }
        rejoin(&mut snapshot, &base, &assignment, "b", [20; 16]);
        rejoin_slot_available(&snapshot, &base, &assignment, "c").unwrap();
        rejoin(&mut snapshot, &base, &assignment, "c", [30; 16]);
        assert!(rejoin_slot_available(&snapshot, &base, &assignment, "d").is_err());
    }
}
