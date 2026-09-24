//! Agreement evidence for a future durable queue checkpoint protocol.
//!
//! These types validate identity and unanimous agreement only. They do not
//! attest to disk persistence, grant serving, authorize compaction, or replace
//! sealed recovery verification. Runtime publication must additionally verify
//! durable local capsules and commit their receipts through fresh consensus.
use std::collections::BTreeMap;

use fibril_broker::{
    history_replication::{AcceptedHistory, ReplicaHistoryInstance},
    queue_engine::StorageHistoryBinding,
};
use ganglion_core::{CoordinationSnapshot, ResourceIdentity};
use serde::{Deserialize, Serialize};

use crate::history_identity::ResourceIncarnation;

/// Shared content at one exact, fully applied exclusive event boundary.
///
/// The complete retained payload digest includes pending/orphan payloads, not
/// only messages currently visible in the queue. A later enqueue may refer to a
/// payload below `message_next`. The eventual capsule/retention implementation
/// must preserve those dependencies as well as older live messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointContents {
    pub event_next: u64,
    pub message_head: u64,
    pub message_next: u64,
    pub required_message_next: u64,
    pub snapshot_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub message_digest: [u8; 32],
    pub live_payload_digest: [u8; 32],
}
impl QueueCheckpointContents {
    fn validate(&self) -> Result<(), String> {
        if self.message_head > self.message_next || self.required_message_next > self.message_next {
            return Err("checkpoint payload bounds do not cover state dependencies".into());
        }
        Ok(())
    }
}

/// Immutable agreement proposal. Deserialization conveys no authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointProposal {
    version: u32,
    candidate: [u8; 16],
    incarnation: ResourceIncarnation,
    activation: [u8; 32],
    binding: StorageHistoryBinding,
    owner: String,
    replicas: BTreeMap<String, ReplicaHistoryInstance>,
    previous: Option<[u8; 32]>,
    contents: QueueCheckpointContents,
}

/// An authenticated replica's claim that its immutable capsule is durable.
/// The future receipt publisher must check that claim against local storage;
/// a peer-supplied hash or a deserialized record is not such a check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointReceipt {
    pub proposal: [u8; 32],
    pub node: String,
    pub instance: ReplicaHistoryInstance,
    pub contents: QueueCheckpointContents,
    /// Digest of the local capsule manifest, including retention obligations.
    /// Local manifests may differ; the shared content above must agree exactly.
    pub capsule: [u8; 32],
}

/// Complete report set, ready for durable-authority validation and publication.
/// It is deliberately named evidence: this is not a committed certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointEvidence {
    proposal: QueueCheckpointProposal,
    receipts: BTreeMap<String, QueueCheckpointReceipt>,
}

fn digest<T: Serialize>(domain: &[u8], value: &T) -> Result<[u8; 32], String> {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(&serde_json::to_vec(value).map_err(|e| e.to_string())?);
    Ok(*hash.finalize().as_bytes())
}

fn current_history(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> Result<(ResourceIncarnation, AcceptedHistory), String> {
    if resource.namespace != crate::QUEUE_NAMESPACE
        || !snapshot.resources.contains(resource)
        || snapshot
            .attributes
            .contains_key(&crate::promotion::pending_recovery_key(resource))
    {
        return Err("checkpoint requires a current queue outside recovery".into());
    }
    let incarnation = crate::history_identity::resource_incarnation(snapshot, resource)?
        .ok_or("checkpoint requires an enrolled resource")?;
    if incarnation.version != 2 || incarnation.retired {
        return Err("checkpoint requires a live enrolled incarnation".into());
    }
    let history = crate::history_activation::accepted_history(snapshot, resource)?;
    if history.replicas.is_empty()
        || !history.replicas.contains_key(&history.owner)
        || history.blocked_local_replica.is_some()
        || history
            .replicas
            .values()
            .any(|instance| instance.process == [0; 16] || instance.storage == [0; 16])
    {
        return Err("checkpoint requires exact admitted owner and replica identities".into());
    }
    Ok((incarnation, history))
}

impl QueueCheckpointProposal {
    /// `previous` must ultimately be the current committed certificate's
    /// evidence, checked by the runtime publisher. This method checks its
    /// agreement/lineage and monotonicity; it cannot prove publication.
    pub fn new(
        snapshot: &CoordinationSnapshot,
        resource: &ResourceIdentity,
        candidate: [u8; 16],
        contents: QueueCheckpointContents,
        previous: Option<&QueueCheckpointEvidence>,
    ) -> Result<Self, String> {
        let (incarnation, history) = current_history(snapshot, resource)?;
        let proposal = Self {
            version: 1,
            candidate,
            incarnation,
            activation: history.activation,
            binding: history.binding,
            owner: history.owner,
            replicas: history.replicas,
            previous: previous.map(QueueCheckpointEvidence::digest).transpose()?,
            contents,
        };
        proposal.validate(snapshot)?;
        proposal.validate_predecessor(snapshot, previous)?;
        Ok(proposal)
    }

    pub fn digest(&self) -> Result<[u8; 32], String> {
        digest(b"fibril-queue-checkpoint-proposal-v1\0", self)
    }

    pub fn contents(&self) -> &QueueCheckpointContents {
        &self.contents
    }

    pub fn replicas(&self) -> &BTreeMap<String, ReplicaHistoryInstance> {
        &self.replicas
    }

    pub fn validate(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        let (incarnation, history) = current_history(snapshot, &self.incarnation.resource)?;
        if self.version != 1
            || self.candidate == [0; 16]
            || self.incarnation != incarnation
            || self.activation != history.activation
            || self.binding != history.binding
            || self.owner != history.owner
            || self.replicas != history.replicas
        {
            return Err("checkpoint proposal differs from current accepted history".into());
        }
        self.contents.validate()
    }

    fn validate_predecessor(
        &self,
        snapshot: &CoordinationSnapshot,
        previous: Option<&QueueCheckpointEvidence>,
    ) -> Result<(), String> {
        let Some(previous) = previous else {
            return if self.previous.is_none() {
                Ok(())
            } else {
                Err("checkpoint predecessor evidence is missing".into())
            };
        };
        // Only the direct predecessor is needed here. The committed-certificate
        // lookup must authenticate its historical chain before publication.
        previous.validate_agreement(snapshot)?;
        let old = &previous.proposal;
        if self.previous != Some(previous.digest()?)
            || self.incarnation != old.incarnation
            || self.activation != old.activation
            || self.binding != old.binding
            || self.owner != old.owner
            || self.replicas != old.replicas
            || self.candidate == old.candidate
            || self.contents.event_next <= old.contents.event_next
            || self.contents.message_next < old.contents.message_next
            || self.contents.message_head < old.contents.message_head
        {
            return Err("checkpoint replacement regresses or changes its accepted lineage".into());
        }
        Ok(())
    }

    fn validate_receipt(
        &self,
        contacted_node: &str,
        receipt: &QueueCheckpointReceipt,
    ) -> Result<(), String> {
        if receipt.node != contacted_node
            || self.replicas.get(contacted_node) != Some(&receipt.instance)
            || receipt.proposal != self.digest()?
            || receipt.contents != self.contents
        {
            return Err(
                "checkpoint receipt differs from exact replica, proposal or content".into(),
            );
        }
        Ok(())
    }
}

impl QueueCheckpointEvidence {
    pub fn proposal(&self) -> &QueueCheckpointProposal {
        &self.proposal
    }

    pub fn receipts(&self) -> &BTreeMap<String, QueueCheckpointReceipt> {
        &self.receipts
    }

    pub fn digest(&self) -> Result<[u8; 32], String> {
        digest(b"fibril-queue-checkpoint-evidence-v1\0", self)
    }

    fn validate_agreement(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        self.proposal.validate(snapshot)?;
        if self.receipts.len() != self.proposal.replicas.len() {
            return Err("checkpoint requires every admitted replica receipt".into());
        }
        for (node, receipt) in &self.receipts {
            self.proposal.validate_receipt(node, receipt)?;
        }
        Ok(())
    }

    /// Revalidate deserialized evidence and its direct predecessor against fresh
    /// metadata. Successful validation alone permits no recovery shortcut.
    pub fn validate(
        &self,
        snapshot: &CoordinationSnapshot,
        previous: Option<&Self>,
    ) -> Result<(), String> {
        self.validate_agreement(snapshot)?;
        self.proposal.validate_predecessor(snapshot, previous)
    }
}

/// Attempt-local collector. A contradictory authenticated report permanently
/// poisons the attempt; retries cannot erase it with a later matching report.
#[derive(Debug)]
pub struct QueueCheckpointAgreement {
    proposal: QueueCheckpointProposal,
    receipts: BTreeMap<String, QueueCheckpointReceipt>,
    contradiction: bool,
}
impl QueueCheckpointAgreement {
    pub fn new(
        snapshot: &CoordinationSnapshot,
        proposal: QueueCheckpointProposal,
    ) -> Result<Self, String> {
        proposal.validate(snapshot)?;
        Ok(Self {
            proposal,
            receipts: BTreeMap::new(),
            contradiction: false,
        })
    }

    /// `contacted_node` comes from the authenticated transport, never from a
    /// field in the report. Exact duplicate replies are idempotent.
    pub fn record(
        &mut self,
        snapshot: &CoordinationSnapshot,
        contacted_node: &str,
        receipt: QueueCheckpointReceipt,
    ) -> Result<(), String> {
        self.proposal.validate(snapshot)?;
        if self.contradiction {
            return Err("checkpoint attempt contains contradictory evidence".into());
        }
        if let Err(error) = self.proposal.validate_receipt(contacted_node, &receipt) {
            self.contradiction = true;
            return Err(error);
        }
        if let Some(previous) = self.receipts.get(contacted_node) {
            if previous != &receipt {
                self.contradiction = true;
                return Err("replica changed its checkpoint receipt within one attempt".into());
            }
        } else {
            self.receipts.insert(contacted_node.to_owned(), receipt);
        }
        Ok(())
    }

    pub fn finish(
        self,
        snapshot: &CoordinationSnapshot,
        previous: Option<&QueueCheckpointEvidence>,
    ) -> Result<QueueCheckpointEvidence, String> {
        if self.contradiction {
            return Err("checkpoint attempt contains contradictory evidence".into());
        }
        let evidence = QueueCheckpointEvidence {
            proposal: self.proposal,
            receipts: self.receipts,
        };
        evidence.validate(snapshot, previous)?;
        Ok(evidence)
    }
}
