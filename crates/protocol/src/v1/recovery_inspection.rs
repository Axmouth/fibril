//! Explicit, read-only comparison of two previously collected sealed witnesses.
//! This diagnostic path never selects a source, installs state or activates it.

use super::replication::{
    ProtocolOwnerPeerResolverConfig, request_recovery_read_sequential_reusing,
};
use fibril_broker::{
    broker::BrokerError,
    recovery::{
        BrokerSealedReplica, RecoveryReadPage, RecoveryRecord, RecoverySealCommand,
        inspection::{
            RecoveryInspectionLimits, RecoveryOverlap, RecoveryPairInspection,
            RecoveryPairInspector, RecoverySide,
        },
        journal::{RecoveryInspectionJournal, RecoveryJournalIdentity},
        phase::{RecoveryPhaseKind, RecoveryPhaseRecipe, RecoveryPhaseStore},
        replay::{RecoveryQueueStateArtifact, RecoveryReplayLimits},
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

/// Inspect exact contacted witnesses using a durable local page journal. Cached
/// pages are fully revalidated and never count as another independent witness.
pub async fn inspect_recovery_pair_with_journal(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    deadline: std::time::Duration,
    journal_root: &std::path::Path,
) -> Result<ProtocolRecoveryPairInspection, BrokerError> {
    inspect_pair_inner(
        config,
        command,
        left,
        right,
        limits,
        None,
        deadline,
        false,
        Some(journal_root),
        |inspector| inspector.finish().map(|report| (report, ())),
    )
    .await
    .map(|(report, ())| report)
}

fn phase_recipe(
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    kind: RecoveryPhaseKind,
) -> Result<RecoveryPhaseRecipe, BrokerError> {
    for replica in [left, right] {
        if replica.seal.request.transition != command.transition
            || replica.seal.request.fence_epoch != command.fence_epoch
        {
            return Err(BrokerError::InvalidArgument(
                "phase transition differs from requested recovery".into(),
            ));
        }
    }
    Ok(RecoveryPhaseRecipe {
        sources: [left, right].map(|replica| RecoveryJournalIdentity {
            topic: command.topic.clone(),
            partition: command.partition.id(),
            group: command.group.clone(),
            stream: command.stream,
            replica: replica.node_id.clone(),
            seal: replica.seal.clone(),
        }),
        limits,
        state_entries: 1_000_000,
        kind,
    })
}

/// Persist completed local comparisons, retaining tentative pages on interruption.
/// A restored result still requires current witness/lineage and plan authority.
pub async fn inspect_recovery_pair_resumable(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    left: &BrokerSealedReplica,
    right: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    deadline: std::time::Duration,
    root: &std::path::Path,
) -> Result<ProtocolRecoveryPairInspection, BrokerError> {
    let recipe = phase_recipe(command, left, right, limits, RecoveryPhaseKind::Comparison)?;
    let path = root.to_path_buf();
    let (store, saved) = inspection_work(deadline, "completed comparison read", move || {
        let store = RecoveryPhaseStore::open(&path, recipe).map_err(|e| e.to_string())?;
        let saved = store.load().map_err(|e| e.to_string())?;
        Ok((store, saved))
    })
    .await?;
    if let Some(saved) = saved {
        tracing::info!(
            left_replica = left.node_id,
            right_replica = right.node_id,
            "reused completed local recovery comparison"
        );
        return Ok(ProtocolRecoveryPairInspection {
            left_node: left.node_id.clone(),
            right_node: right.node_id.clone(),
            evidence: saved.report,
        });
    }
    inspect_pair_inner(config, command, left, right, limits, None, deadline, false,
        Some(root), move |inspector| {
            let report = inspector.finish()?;
            // Publication belongs to the admitted finish task. A caller timing
            // out while this task finishes must not discard completed work.
            store.save(&report, None).map_err(|e| e.to_string())?;
            Ok((report, ()))
        }).await.map(|(report, ())| report)
}

/// Restore a completed locally verified source phase, or continue its pages.
pub async fn inspect_recovery_source_artifact_resumable(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    source: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    replay_limits: RecoveryReplayLimits,
    max_checkpoint_bytes: u32,
    max_artifact_bytes: usize,
    deadline: std::time::Duration,
    root: &std::path::Path,
) -> Result<RecoveryQueueStateArtifact, BrokerError> {
    let recipe = phase_recipe(
        command,
        source,
        source,
        limits,
        RecoveryPhaseKind::SourceArtifact {
            replay: replay_limits,
            checkpoint_bytes: max_checkpoint_bytes,
            artifact_bytes: max_artifact_bytes,
        },
    )?;
    let path = root.to_path_buf();
    let (store, saved) = inspection_work(deadline, "completed source read", move || {
        let store = RecoveryPhaseStore::open(&path, recipe).map_err(|e| e.to_string())?;
        let saved = store.load().map_err(|e| e.to_string())?;
        Ok((store, saved))
    })
    .await?;
    if let Some(saved) = saved {
        tracing::info!(
            replica = source.node_id,
            "reused completed local recovery inspection"
        );
        return saved.artifact.ok_or_else(|| {
            BrokerError::InvalidArgument("completed source has no artifact".into())
        });
    }
    let (_, artifact) = inspect_pair_inner(
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
        Some(root),
        move |inspector| {
            let (report, [artifact, _]) = inspector.finish_with_queue_artifacts(max_artifact_bytes)?;
            store.save(&report, Some(&artifact)).map_err(|e| e.to_string())?;
            Ok((report, artifact))
        },
    )
    .await?;
    Ok(artifact)
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
    inspect_source_artifact(
        config,
        command,
        source,
        limits,
        replay_limits,
        max_checkpoint_bytes,
        max_artifact_bytes,
        deadline,
        None,
    )
    .await
}

/// Retain tentative pages across retries while preserving all sealed validation.
pub async fn inspect_recovery_source_artifact_with_journal(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    source: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    replay_limits: RecoveryReplayLimits,
    max_checkpoint_bytes: u32,
    max_artifact_bytes: usize,
    deadline: std::time::Duration,
    journal_root: &std::path::Path,
) -> Result<RecoveryQueueStateArtifact, BrokerError> {
    inspect_source_artifact(
        config,
        command,
        source,
        limits,
        replay_limits,
        max_checkpoint_bytes,
        max_artifact_bytes,
        deadline,
        Some(journal_root),
    )
    .await
}

async fn inspect_source_artifact(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &RecoverySealCommand,
    source: &BrokerSealedReplica,
    limits: RecoveryInspectionLimits,
    replay_limits: RecoveryReplayLimits,
    max_checkpoint_bytes: u32,
    max_artifact_bytes: usize,
    deadline: std::time::Duration,
    journal_root: Option<&std::path::Path>,
) -> Result<RecoveryQueueStateArtifact, BrokerError> {
    if max_artifact_bytes == 0
        || max_artifact_bytes > fibril_broker::recovery::snapshot::MAX_RECOVERY_SNAPSHOT_BYTES
    {
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
        journal_root,
        move |inspector| inspector.finish_with_queue_artifacts(max_artifact_bytes),
    )
    .await?;
    Ok(artifact)
}

// Admission remains held by owned CPU work if its caller times out. This bounds
// concurrent sorting/replay even when requests disconnect or deadlines expire.
static INSPECTION_CPU_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

// Each bounded blocking operation owns admission through caller cancellation.
// A progressing journal inspection has per-operation deadlines, rather than one
// deadline proportional to the entire sealed history.
async fn inspection_work<T: Send + 'static>(
    deadline: std::time::Duration,
    operation: &'static str,
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, BrokerError> {
    let permit = tokio::time::timeout(deadline, INSPECTION_CPU_SLOTS.acquire())
        .await
        .map_err(|_| BrokerError::Unknown(format!("{operation} admission timed out")))?
        .map_err(|_| BrokerError::Unknown("inspection CPU admission closed".into()))?;
    tokio::time::timeout(
        deadline,
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        }),
    )
    .await
    .map_err(|_| {
        BrokerError::Unknown(format!(
            "{operation} timed out, durable downloaded pages retained"
        ))
    })?
    .map_err(|e| BrokerError::Unknown(format!("{operation} worker failed: {e}")))?
    .map_err(BrokerError::InvalidArgument)
}

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
        None,
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
    journal_root: Option<&std::path::Path>,
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
    if journal_root.is_some() {
        inspector = inspector
            .with_incremental_work(1_000_000)
            .map_err(BrokerError::InvalidArgument)?;
    }
    if allow_same_source {
        inspector = inspector
            .with_single_source()
            .map_err(BrokerError::InvalidArgument)?;
    }
    // Keep the hash/replay state at one heap address while pages cross the
    // blocking-worker boundary. Moving the full inspector as each task's
    // input and output inflates nested async frames, especially in debug builds.
    let mut inspector = Box::new(inspector);
    let inspect = async {
        let mut left_connection = None;
        let mut right_connection = None;
        let mut journals: [Option<RecoveryInspectionJournal>; 2] = [None, None];
        let mut cached_pages = 0u64;
        let mut fetched_pages = 0u64;
        while let Some((side, request)) = inspector
            .next_read()
            .map_err(BrokerError::InvalidArgument)?
        {
            let (replica, connection) = match side {
                RecoverySide::Left => (left, &mut left_connection),
                RecoverySide::Right => (right, &mut right_connection),
            };
            let index = if side == RecoverySide::Left { 0 } else { 1 };
            let cached = if let Some(root) = journal_root {
                let root = root.to_path_buf();
                let identity = RecoveryJournalIdentity {
                    topic: command.topic.clone(),
                    partition: command.partition.id(),
                    group: command.group.clone(),
                    stream: command.stream,
                    replica: replica.node_id.clone(),
                    seal: replica.seal.clone(),
                };
                let journal = journals[index].take();
                let read_request = request.clone();
                let (journal, page) =
                    inspection_work(deadline, "inspection journal read", move || {
                        let journal = match journal {
                            Some(journal) => journal,
                            None => RecoveryInspectionJournal::open(&root, identity)
                                .map_err(|e| e.to_string())?,
                        };
                        let page = journal.read(&read_request).map_err(|e| e.to_string())?;
                        Ok((journal, page))
                    })
                    .await?;
                journals[index] = Some(journal);
                page
            } else {
                None
            };
            let cache_hit = cached.is_some();
            let page = if let Some(page) = cached {
                cached_pages += 1;
                page
            } else {
                fetched_pages += 1;
                let page = request_recovery_read_sequential_reusing(
                    config, command, replica, &request, deadline, connection,
                )
                .await?;
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
                }
            };
            let journal = journals[index].take();
            let (next_inspector, journal) =
                inspection_work(deadline, "inspection page", move || {
                    if !cache_hit {
                        if let Some(journal) = &journal {
                            journal
                                .append(&request, &page)
                                .map_err(|e| format!("inspection journal append failed: {e}"))?;
                        }
                    }
                    inspector.accept_page(side, page)?;
                    Ok((inspector, journal))
                })
                .await?;
            inspector = next_inspector;
            journals[index] = journal;
        }
        let (evidence, artifact) =
            inspection_work(deadline, "inspection finish", move || finish(*inspector)).await?;
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
            pages=evidence.pages,records=evidence.records,bytes=evidence.bytes,cached_pages,fetched_pages,
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
    if journal_root.is_some() {
        inspect.await
    } else {
        tokio::time::timeout(deadline, inspect).await.map_err(|_| {
            BrokerError::Unknown(
                "recovery pair inspection deadline elapsed, partial evidence discarded".into(),
            )
        })?
    }
}
