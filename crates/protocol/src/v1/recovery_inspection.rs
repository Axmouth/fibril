//! Explicit, read-only comparison of two previously collected sealed witnesses.
//! This diagnostic path never selects a source, installs state or activates it.

use super::replication::{request_recovery_read, ProtocolOwnerPeerResolverConfig};
use fibril_broker::{
    broker::BrokerError,
    recovery::{
        inspection::{
            RecoveryInspectionLimits, RecoveryOverlap, RecoveryPairInspection,
            RecoveryPairInspector, RecoverySide,
        },
        replay::{RecoveryQueueStateArtifact, RecoveryReplayLimits},
        BrokerSealedReplica, RecoveryReadPage, RecoveryRecord, RecoverySealCommand,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolRecoveryPairInspection {
    pub left_node: String,
    pub right_node: String,
    pub evidence: RecoveryPairInspection,
}

/// Inputs come from the caller's transition-bound witness set. Membership/count
/// and fresh consensus authority must be rechecked before any later recovery
/// decision. Equal shared records alone cannot authorize such a decision.
pub async fn inspect_recovery_pair(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    deadline: std::time::Duration,
) -> Result<ProtocolRecoveryPairInspection, BrokerError> {
    inspect_pair(config, command, left, right, limits, None, deadline).await
}

/// Reconstruct both fully retained queue histories at the same exclusive event
/// boundary. This produces evidence only; compacted origins need a separately
/// proven checkpoint. Full sealed contents are still verified after the target.
pub async fn inspect_recovery_pair_with_queue_replay(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    event_next: u64,
    replay_limits: RecoveryReplayLimits,
    deadline: std::time::Duration,
) -> Result<ProtocolRecoveryPairInspection, BrokerError> {
    inspect_pair(
        config,
        command,
        left,
        right,
        limits,
        Some((event_next, replay_limits, None)),
        deadline,
    )
    .await
}

/// Compare queues replayed from each sealed replica's exact checkpoint. Raw
/// snapshots, event suffixes and live payloads share the inspection budgets and
/// deadline. Matching evidence still does not authorize source activation.
pub async fn inspect_recovery_pair_with_checkpoints(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    event_next: u64,
    replay_limits: RecoveryReplayLimits,
    max_checkpoint_bytes: u32,
    deadline: std::time::Duration,
) -> Result<ProtocolRecoveryPairInspection, BrokerError> {
    inspect_pair(
        config,
        command,
        left,
        right,
        limits,
        Some((event_next, replay_limits, Some(max_checkpoint_bytes))),
        deadline,
    )
    .await
}

/// Reconstruct a single complete sealed queue source into bounded state bytes.
/// The source is inspected against itself; it still contributes only one witness
/// to recovery. This verifies content and replay, never quorum or lineage.
pub async fn inspect_recovery_source_artifact(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    source: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    replay_limits: RecoveryReplayLimits,
    max_checkpoint_bytes: u32,
    max_artifact_bytes: usize,
    deadline: std::time::Duration,
) -> Result<RecoveryQueueStateArtifact, BrokerError> {
    if max_artifact_bytes == 0 || max_artifact_bytes > 16 * 1024 * 1024 {
        return Err(BrokerError::InvalidArgument(
            "invalid recovery artifact limit".into(),
        ));
    }
    let (_, [artifact, _]) = inspect_pair_inner(
        config,
        command,
        source,
        source,
        limits,
        Some((
            source.seal.event_next,
            replay_limits,
            Some(max_checkpoint_bytes),
        )),
        deadline,
        true,
        move |inspector| inspector.finish_with_queue_artifacts(max_artifact_bytes),
    )
    .await?;
    Ok(artifact)
}

// Admission remains held by owned CPU work if its caller times out. This bounds
// concurrent sorting/replay even when requests disconnect or deadlines expire.
static INSPECTION_CPU_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

async fn inspect_pair(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    replay: Option<(u64, RecoveryReplayLimits, Option<u32>)>,
    deadline: std::time::Duration,
) -> Result<ProtocolRecoveryPairInspection, BrokerError> {
    inspect_pair_inner(
        config,
        command,
        left,
        right,
        limits,
        replay,
        deadline,
        false,
        |inspector| inspector.finish().map(|report| (report, ())),
    )
    .await
    .map(|(report, ())| report)
}

async fn inspect_pair_inner<T: Send + 'static>(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    replay: Option<(u64, RecoveryReplayLimits, Option<u32>)>,
    deadline: std::time::Duration,
    allow_same_source: bool,
    finish: impl FnOnce(RecoveryPairInspector) -> Result<(RecoveryPairInspection, T), String>
        + Send
        + 'static,
) -> Result<(ProtocolRecoveryPairInspection, T), BrokerError> {
    if left.node_id.is_empty()
        || right.node_id.is_empty()
        || (left.node_id == right.node_id && (!allow_same_source || left != right))
    {
        return Err(BrokerError::InvalidArgument(
            "recovery inspection requires distinct contacted replicas".into(),
        ));
    }
    for replica in [left, right] {
        if replica.seal.request.transition != command.transition
            || replica.seal.request.fence_epoch != command.fence_epoch
        {
            return Err(BrokerError::InvalidArgument(
                "recovery inspection transition mismatch".into(),
            ));
        }
    }
    let mut inspector = RecoveryPairInspector::new(
        &command.topic,
        command.partition.id(),
        command.group.as_deref(),
        command.stream,
        left.seal.clone(),
        right.seal.clone(),
        limits,
    )
    .map_err(BrokerError::InvalidArgument)?;
    if let Some((target, replay_limits, checkpoint_bytes)) = replay {
        inspector = match checkpoint_bytes {
            Some(max_bytes) => {
                inspector.with_queue_checkpoint_replay(target, replay_limits, max_bytes)
            }
            None => inspector.with_queue_replay(target, replay_limits),
        }
        .map_err(BrokerError::InvalidArgument)?;
    }
    let inspect = async {
        while let Some((side, request)) = inspector
            .next_read()
            .map_err(BrokerError::InvalidArgument)?
        {
            let replica = match side {
                RecoverySide::Left => left,
                RecoverySide::Right => right,
            };
            let page = request_recovery_read(config, command, replica, &request, deadline).await?;
            let page = RecoveryReadPage {
                history_id: page.history_id,
                source: request.source,
                from: page.from,
                next: page.next,
                end: page.end,
                snapshot_bytes: page.snapshot_bytes,
                records: page
                    .records
                    .into_iter()
                    .map(|record| RecoveryRecord {
                        offset: record.offset,
                        flags: record.flags,
                        headers: record.headers,
                        payload: record.payload,
                    })
                    .collect(),
            };
            let permit = INSPECTION_CPU_SLOTS
                .acquire()
                .await
                .map_err(|_| BrokerError::Unknown("inspection CPU admission closed".into()))?;
            inspector = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                inspector.accept_page(side, page)?;
                Ok::<_, String>(inspector)
            })
            .await
            .map_err(|e| BrokerError::Unknown(format!("recovery inspection worker failed: {e}")))?
            .map_err(BrokerError::InvalidArgument)?;
        }
        let permit = INSPECTION_CPU_SLOTS
            .acquire()
            .await
            .map_err(|_| BrokerError::Unknown("inspection CPU admission closed".into()))?;
        let (evidence, artifact) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            finish(inspector)
        })
        .await
        .map_err(|e| BrokerError::Unknown(format!("recovery digest worker failed: {e}")))?
        .map_err(BrokerError::InvalidArgument)?;
        for (source, log) in [
            ("messages", &evidence.messages),
            ("events", &evidence.events),
        ] {
            if let RecoveryOverlap::Divergent { first, .. } = &log.overlap {
                let hex = |id: &[u8; 32]| {
                    id.iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                };
                tracing::warn!(left_replica=left.node_id, right_replica=right.node_id,
                    topic=command.topic, partition=command.partition.id(), group=command.group.as_deref(),
                    source, offset=first.offset, left_record_id=%hex(&first.left_id), right_record_id=%hex(&first.right_id),
                    "sealed histories differ at a shared offset; authority remains unresolved");
            }
        }
        tracing::info!(left_replica=left.node_id,right_replica=right.node_id,
            topic=command.topic,partition=command.partition.id(),group=command.group.as_deref(),
            messages=?evidence.messages,events=?evidence.events,references=?evidence.references,queue_replay=?evidence.queue_replay,remaining_proofs=?evidence.remaining_proofs,
            pages=evidence.pages,records=evidence.records,bytes=evidence.bytes,
            "sealed history inspection complete; unresolved proofs remain explicit");
        Ok((
            ProtocolRecoveryPairInspection {
                left_node: left.node_id.clone(),
                right_node: right.node_id.clone(),
                evidence,
            },
            artifact,
        ))
    };
    tokio::time::timeout(deadline, inspect).await.map_err(|_| {
        BrokerError::Unknown(
            "recovery pair inspection deadline elapsed; partial evidence discarded".into(),
        )
    })?
}
