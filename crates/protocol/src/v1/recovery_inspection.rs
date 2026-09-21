//! Explicit, read-only comparison of two previously collected sealed witnesses.
//! This diagnostic path never selects a source, installs state or activates it.

use super::replication::{ProtocolOwnerPeerResolverConfig, request_recovery_read};
use fibril_broker::{
    broker::BrokerError,
    recovery::{
        BrokerSealedReplica, RecoveryReadPage, RecoveryRecord, RecoverySealCommand,
        inspection::{
            RecoveryInspectionLimits, RecoveryOverlap, RecoveryPairInspection,
            RecoveryPairInspector, RecoverySide,
        },
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
    if left.node_id.is_empty() || right.node_id.is_empty() || left.node_id == right.node_id {
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
            inspector
                .accept_page(
                    side,
                    RecoveryReadPage {
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
                    },
                )
                .map_err(BrokerError::InvalidArgument)?;
        }
        let evidence = inspector.finish().map_err(BrokerError::InvalidArgument)?;
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
            messages=?evidence.messages,events=?evidence.events,references=?evidence.references,remaining_proofs=?evidence.remaining_proofs,
            pages=evidence.pages,records=evidence.records,bytes=evidence.bytes,
            "sealed history inspection complete; ancestry and state proof remain required");
        Ok(ProtocolRecoveryPairInspection {
            left_node: left.node_id.clone(),
            right_node: right.node_id.clone(),
            evidence,
        })
    };
    tokio::time::timeout(deadline, inspect).await.map_err(|_| {
        BrokerError::Unknown(
            "recovery pair inspection deadline elapsed; partial evidence discarded".into(),
        )
    })?
}
