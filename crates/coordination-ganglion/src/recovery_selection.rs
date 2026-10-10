//! Source choice for one accepted initial queue history. This produces a proposal;
//! persisted installation and fresh activation are separate required authorities.
use std::collections::BTreeMap;

use fibril_broker::recovery::{
    inspection::{RecoveryOverlap, RecoveryPairInspection},
    replay::{RecoveryQueueStateArtifact, RecoveryQueueStateSummary},
};
use ganglion_core::{CoordinationSnapshot, ResourceIdentity};
use serde::{Deserialize, Serialize};

use crate::{
    initial_history::InitialHistoryDecision,
    recovery_witnesses::{RecoveryWitnessSet, SealCollectionProgress},
};

/// Immutable proposal created from locally verified evidence. It deliberately
/// cannot be deserialized as a proof or used directly to grant writer permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct QueueRecoverySelection(pub(crate) RecordedQueueSelection);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordedQueueSelection {
    pub(crate) transition: [u8; 32],
    pub(crate) previous_activation: [u8; 32],
    pub(crate) source_node: String,
    pub(crate) source_history: [u8; 32],
    /// Selected applied boundary, equal to the source seal log tail.
    pub(crate) next: u64,
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) snapshot_bytes: u64,
    pub(crate) state_digest: [u8; 32],
    pub(crate) live_payload_digest: [u8; 32],
    pub(crate) witnesses: BTreeMap<String, [u8; 32]>,
}
impl QueueRecoverySelection {
    pub fn source_node(&self) -> &str {
        &self.0.source_node
    }
    pub fn next(&self) -> u64 {
        self.0.next
    }
    pub fn snapshot_digest(&self) -> [u8; 32] {
        self.0.snapshot_digest
    }
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-queue-recovery-selection-v2\0");
        hash.update(&serde_json::to_vec(self).map_err(|e| e.to_string())?);
        Ok(*hash.finalize().as_bytes())
    }
}

impl RecoveryWitnessSet {
    /// Choose a complete source that covers the longest observed tail. All supplied
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
        let summaries = artifacts
            .iter()
            .map(|(node, artifact)| (node.clone(), artifact.summary()))
            .collect();
        self.select_queue_source_summaries(snapshot, &summaries, comparisons)
    }

    /// Select using verified summaries so replica count does not multiply retained
    /// snapshot memory. The selected bytes are reopened from their exact phase.
    pub fn select_queue_source_summaries(
        &self,
        snapshot: &CoordinationSnapshot,
        artifacts: &BTreeMap<String, RecoveryQueueStateSummary>,
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
        // Every witness must be a queue history before any tail is compared,
        // so a stream history can never slip into the single-log maximum.
        let logs = reports
            .iter()
            .map(|(node, r)| {
                r.seal
                    .history
                    .queue_log()
                    .map(|log| (node.clone(), *log))
                    .ok_or("queue source selection received a stream history")
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let next = logs
            .values()
            .map(|log| log.next)
            .max()
            .ok_or("no sealed witnesses")?;
        for (node, artifact) in artifacts {
            let seal = &reports
                .get(node)
                .ok_or("artifact is not from a collected witness")?
                .seal;
            let log = logs
                .get(node)
                .ok_or("artifact is not from a collected witness")?;
            let evidence = artifact.evidence();
            // Evidence must replay its whole sealed log, so its applied
            // boundary is that seal's tail.
            if evidence.log != *log
                || evidence.version != 2
                || evidence.history_id != seal.history.id
                || evidence.next != log.next
                || evidence.live_payload_digest.is_none()
            {
                return Err("artifact does not cover its complete sealed source".into());
            }
        }
        let (source_node, source) = artifacts
            .iter()
            .find(|(_, artifact)| artifact.evidence().next == next)
            .ok_or(
                "no verified single source covers the longest observed tail; reconstruction required",
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
                let range = |seal: &fibril_broker::recovery::SealedReplicaFrontiers| {
                    seal.history.queue_log().map(|log| (log.head, log.next))
                };
                if pair.transition != command.transition
                    || Some(pair.log.left_range) != range(left)
                    || Some(pair.log.right_range) != range(right)
                {
                    return Err("comparison does not identify the exact sealed witnesses".into());
                }
                // Payloads are records of the one queue log, so one overlap
                // check covers both payload and state history.
                if matches!(pair.log.overlap, RecoveryOverlap::Divergent { .. }) {
                    return Err(
                        "authoritative sealed histories diverge; source selection blocked".into(),
                    );
                }
                if let Some([a, b]) = &pair.queue_replay {
                    if a.next != b.next
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
                if a.next == b.next
                    && (a.lease_normalized_state_digest != b.lease_normalized_state_digest
                        || a.live_payload_digest != b.live_payload_digest)
                {
                    return Err("sealed states disagree at the same applied boundary".into());
                }
            }
        }
        let selection = QueueRecoverySelection(RecordedQueueSelection {
            transition: command.transition,
            previous_activation: accepted.activation,
            source_node: source_node.clone(),
            source_history: source_seal.history.id,
            next,
            snapshot_digest: source.snapshot_digest(),
            snapshot_bytes: source.snapshot_bytes(),
            state_digest: source.evidence().lease_normalized_state_digest,
            live_payload_digest: source.evidence().live_payload_digest.unwrap(),
            witnesses: reports
                .iter()
                .map(|(node, report)| (node.clone(), report.seal.history.id))
                .collect(),
        });
        tracing::info!(
            topic = command.topic,
            partition = command.partition.id(),
            group = command.group.as_deref(),
            source = source_node,
            next,
            witnesses = reports.len(),
            "verified queue recovery source selected; installation and activation still required"
        );
        Ok(selection)
    }
}
