//! Accepted history identity for live replication. An epoch alone cannot
//! distinguish recreated resources or replacement storage instances.
use crate::{
    Partition,
    broker::{Broker, BrokerError},
    queue_engine::{PreparedStorageHistory, StorageHistoryBinding, StromaEngine},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaHistoryInstance {
    pub process: [u8; 16],
    pub storage: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedHistory {
    pub activation: [u8; 32],
    pub binding: StorageHistoryBinding,
    pub owner: String,
    pub replicas: BTreeMap<String, ReplicaHistoryInstance>,
    /// Local serving projection only; never part of the persisted certificate.
    /// Keep remote routing visible while withholding this process's role.
    pub blocked_local_replica: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryReplicationSession {
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    pub stream: bool,
    pub activation: [u8; 32],
    pub binding: StorageHistoryBinding,
    pub sender: String,
    pub sender_instance: ReplicaHistoryInstance,
    pub receiver: String,
    pub receiver_instance: ReplicaHistoryInstance,
}

impl AcceptedHistory {
    pub fn permits_role(&self, node: &str) -> bool {
        self.blocked_local_replica.as_deref() != Some(node) && self.replicas.contains_key(node)
    }

    pub fn session(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        stream: bool,
        sender: &str,
        receiver: &str,
    ) -> Result<HistoryReplicationSession, String> {
        if !self.permits_role(sender) || !self.permits_role(receiver) {
            return Err("replica is outside the admitted history projection".into());
        }
        Ok(HistoryReplicationSession {
            topic: topic.into(),
            partition,
            group: group.map(str::to_owned),
            stream,
            activation: self.activation,
            binding: self.binding.clone(),
            sender: sender.into(),
            receiver: receiver.into(),
            sender_instance: self
                .replicas
                .get(sender)
                .ok_or("sender is outside the activated history")?
                .clone(),
            receiver_instance: self
                .replicas
                .get(receiver)
                .ok_or("receiver is outside the activated history")?
                .clone(),
        })
    }
}

impl Broker<StromaEngine> {
    /// Check both current metadata authority and this admitted storage instance.
    /// The direction comes from the decoded operation, not a peer-supplied flag.
    pub fn authorize_history_replication(
        &self,
        session: &HistoryReplicationSession,
        owner_is_receiver: bool,
    ) -> Result<(), BrokerError> {
        self.ownership
            .authorize_history_replication(session, owner_is_receiver)
            .map_err(BrokerError::InvalidArgument)?;
        let prepared = PreparedStorageHistory {
            topic: session.topic.clone(),
            partition: session.partition.id(),
            group: session.group.clone(),
            stream: session.stream,
            binding: session.binding.clone(),
            storage_instance: session.receiver_instance.storage,
        };
        self.engine
            .verify_admitted_storage_history(&prepared)
            .map_err(|e| BrokerError::InvalidArgument(e.to_string()))
    }

    pub fn authorize_legacy_replication(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<(), BrokerError> {
        if !self
            .ownership
            .permits_legacy_replication(topic, partition, group)
        {
            return Err(BrokerError::InvalidArgument(
                "replication requires accepted history identity".into(),
            ));
        }
        Ok(())
    }
}
