//! Source choice for one accepted initial queue history. This produces a proposal;
//! persisted installation and fresh activation are separate required authorities.
use std::collections::BTreeMap;

use fibril_broker::recovery::{
    inspection::{RecoveryOverlap, RecoveryPairInspection},
    replay::RecoveryQueueStateArtifact,
};
use ganglion_core::{CoordinationSnapshot, ResourceIdentity};
use serde::Serialize;

use crate::{
    initial_history::InitialHistoryDecision,
    recovery_witnesses::{RecoveryWitnessSet, SealCollectionProgress},
};

/// Immutable proposal created from locally verified evidence. It deliberately
/// cannot be deserialized as a proof or used directly to grant writer permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueRecoverySelection {
    transition: [u8; 32],
    previous_activation: [u8; 32],
    source_node: String,
    source_history: [u8; 32],
    event_next: u64,
    message_head: u64,
    message_next: u64,
    snapshot_digest: [u8; 32],
    state_digest: [u8; 32],
    live_payload_digest: [u8; 32],
    witnesses: BTreeMap<String, [u8; 32]>,
}
impl QueueRecoverySelection {
    pub fn source_node(&self) -> &str {
        &self.source_node
    }
    pub fn event_next(&self) -> u64 {
        self.event_next
    }
    pub fn message_next(&self) -> u64 {
        self.message_next
    }
    pub fn snapshot_digest(&self) -> [u8; 32] {
        self.snapshot_digest
    }
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-queue-recovery-selection-v1\0");
        hash.update(&serde_json::to_vec(self).map_err(|e| e.to_string())?);
        Ok(*hash.finalize().as_bytes())
    }
}

impl RecoveryWitnessSet {
    /// Choose a complete source that covers both observed tails. All supplied
    /// witnesses must be compared with that source; contradictory overlaps block
    /// selection. Artifacts and comparisons must come from local sealed-source
    /// verification, not peer-supplied assertions or heartbeat labels.
    ///
    /// Accepted single-writer lineage justifies compacted, disjoint prefixes;
    /// matching bytes alone cannot. Crossed incomplete tails require composite
    /// reconstruction and are refused here rather than independently maximized.
    pub fn select_queue_source(
        &self,
        snapshot: &CoordinationSnapshot,
        artifacts: &BTreeMap<String, RecoveryQueueStateArtifact>,
        comparisons: &[RecoveryPairInspection],
    ) -> Result<QueueRecoverySelection, String> {
        if !matches!(
            self.progress(snapshot)?,
            SealCollectionProgress::AwaitingHistoryValidation { .. }
        ) {
            return Err("source selection requires the fixed old witness threshold".into());
        }
        let command = self.command();
        if command.stream {
            return Err("queue source selection does not prove stream state".into());
        }
        let accepted = self
            .accepted_history(snapshot)?
            .ok_or("legacy history requires a verified baseline")?;
        let resource = ResourceIdentity::new(
            crate::QUEUE_NAMESPACE,
            &command.topic,
            command.partition.id() as u64,
            command.group.clone(),
        );
        let incarnation = crate::history_identity::resource_incarnation(snapshot, &resource)?
            .ok_or("source selection requires an accepted incarnation")?;
        let decision: InitialHistoryDecision = serde_json::from_str(
            snapshot
                .attributes
                .get(&crate::initial_history::key(&incarnation))
                .ok_or("initial history decision is missing")?,
        )
        .map_err(|e| e.to_string())?;
        // The witness set already validated this exact activation/decision. A
        // legacy writer could omit delayed activation and lose retry semantics.
        if decision.version != 2 {
            return Err(
                "previous history predates durable timer replay; verified baseline required".into(),
            );
        }
        let reports = self.reports(snapshot)?;
        let event_next = reports
            .values()
            .map(|r| r.seal.event_next)
            .max()
            .ok_or("no sealed witnesses")?;
        let message_next = reports
            .values()
            .map(|r| r.seal.message_next)
            .max()
            .ok_or("no sealed witnesses")?;
        for (node, artifact) in artifacts {
            let seal = &reports
                .get(node)
                .ok_or("artifact is not from a collected witness")?
                .seal;
            let evidence = artifact.evidence();
            if evidence.version != 2
                || evidence.history_id != seal.history.id
                || evidence.event_next != seal.event_next
                || evidence.message_next != seal.message_next
                || artifact.message_head() != seal.message_head
                || evidence.live_payload_digest.is_none()
            {
                return Err("artifact does not cover its complete sealed source".into());
            }
        }
        let (source_node, source) = artifacts
            .iter()
            .find(|(_, artifact)| {
                artifact.evidence().event_next == event_next
                    && artifact.evidence().message_next == message_next
            })
            .ok_or(
                "no verified single source covers both observed tails; reconstruction required",
            )?;
        let source_seal = &reports[source_node].seal;
        for (node, report) in reports {
            if node == source_node {
                continue;
            }
            let mut pairs = comparisons
                .iter()
                .filter(|p| {
                    p.history_ids == [source_seal.history.id, report.seal.history.id]
                        || p.history_ids == [report.seal.history.id, source_seal.history.id]
                })
                .peekable();
            if pairs.peek().is_none() {
                return Err("source has not been compared with every collected witness".into());
            }
            for pair in pairs {
                let (left, right) = if pair.history_ids[0] == source_seal.history.id {
                    (source_seal, &report.seal)
                } else {
                    (&report.seal, source_seal)
                };
                if pair.transition != command.transition
                    || pair.messages.left_range != (left.message_head, left.message_next)
                    || pair.messages.right_range != (right.message_head, right.message_next)
                    || pair.events.left_range != (left.event_head, left.event_next)
                    || pair.events.right_range != (right.event_head, right.event_next)
                {
                    return Err("comparison does not identify the exact sealed witnesses".into());
                }
                if matches!(pair.messages.overlap, RecoveryOverlap::Divergent { .. })
                    || matches!(pair.events.overlap, RecoveryOverlap::Divergent { .. })
                {
                    return Err(
                        "authoritative sealed histories diverge; source selection blocked".into(),
                    );
                }
                if let Some([a, b]) = &pair.queue_replay {
                    if a.event_next != b.event_next
                        || a.lease_normalized_state_digest != b.lease_normalized_state_digest
                        || a.live_payload_digest != b.live_payload_digest
                    {
                        return Err(
                            "compared queue states disagree at a common applied boundary".into(),
                        );
                    }
                }
            }
            if let Some(other) = artifacts.get(node) {
                let a = source.evidence();
                let b = other.evidence();
                if a.event_next == b.event_next
                    && (a.lease_normalized_state_digest != b.lease_normalized_state_digest
                        || a.live_payload_digest != b.live_payload_digest)
                {
                    return Err("sealed states disagree at the same applied boundary".into());
                }
            }
        }
        let selection = QueueRecoverySelection {
            transition: command.transition,
            previous_activation: accepted.activation,
            source_node: source_node.clone(),
            source_history: source_seal.history.id,
            event_next,
            message_head: source.message_head(),
            message_next,
            snapshot_digest: source.snapshot_digest(),
            state_digest: source.evidence().lease_normalized_state_digest,
            live_payload_digest: source.evidence().live_payload_digest.unwrap(),
            witnesses: reports
                .iter()
                .map(|(node, report)| (node.clone(), report.seal.history.id))
                .collect(),
        };
        tracing::info!(
            topic = command.topic,
            partition = command.partition.id(),
            group = command.group.as_deref(),
            source = source_node,
            event_next,
            message_next,
            witnesses = reports.len(),
            "verified queue recovery source selected; installation and activation still required"
        );
        Ok(selection)
    }
}
