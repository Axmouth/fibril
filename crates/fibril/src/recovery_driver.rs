//! Bounded creation and recovery for enrolled queue histories. Legacy origins,
//! streams and crossed tails remain fenced with an actionable error.
use fibril_broker::{
    broker::{Broker, QueueOwnership},
    queue_engine::StromaEngine,
    recovery::{
        BrokerSealedReplica, RecoveryReadRequest, RecoverySealRequest, SealedReplicaFrontiers,
        inspection::RecoveryInspectionLimits,
    },
    recovery_transfer::{
        QueueRecoveryCommand, QueueRecoveryOperation as Operation, QueueRecoveryReply as Reply,
        QueueRecoveryRequest,
    },
};
use fibril_coordination_ganglion::{
    GanglionCoordination, promotion::PendingRecovery, recovery_plan::QueueRecoveryPlan,
    recovery_witnesses::RecoveryWitnessSet,
};
use fibril_protocol::v1::{
    recovery_inspection::{
        inspect_recovery_pair_resumable, inspect_recovery_source_artifact_resumable,
    },
    recovery_transfer::request_transfer,
    replication::{ProtocolOwnerPeerResolverConfig, request_recovery_read, request_recovery_seal},
};
use futures::{StreamExt, stream};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
const RPC: Duration = Duration::from_secs(10);
const MAX_REPLICAS: usize = 16;
// Each copying target retains at most an append page and one look-ahead page,
// each limited to 16 MiB. Bound aggregate page retention and disk pressure;
// wire encoding and storage buffers are additional allocations.
const PARALLEL_REPLICAS: usize = 2;
// Sequential inspection verifies each complete retained history. Use the wire limits
// to amortize scans. The resumable path bounds retained state and individual pages.
fn automatic_inspection_limits() -> RecoveryInspectionLimits {
    RecoveryInspectionLimits {
        page_records: 4096,
        page_bytes: 16 * 1024 * 1024,
        ..Default::default()
    }
}

fn err(e: impl ToString) -> String {
    e.to_string()
}
fn command(plan: &QueueRecoveryPlan, node: &str) -> Result<QueueRecoveryCommand, String> {
    let r = &plan.pending().proposed.resource;
    Ok(QueueRecoveryCommand {
        replica_id: node.into(),
        topic: r.name.clone(),
        partition: u32::try_from(r.partition).map_err(err)?,
        group: r.group.clone(),
        plan: plan.digest()?,
    })
}
async fn transfer(
    config: &ProtocolOwnerPeerResolverConfig,
    plan: &QueueRecoveryPlan,
    node: &str,
    operation: Operation,
) -> Result<Reply, String> {
    request_transfer(
        config,
        &QueueRecoveryRequest {
            command: command(plan, node)?,
            operation,
        },
        RPC,
    )
    .await
    .map_err(err)
}
async fn completed_snapshot(
    config: &ProtocolOwnerPeerResolverConfig,
    plan: &QueueRecoveryPlan,
    node: &str,
) -> Result<Vec<u8>, String> {
    use fibril_broker::recovery::snapshot::{MAX_RECOVERY_SNAPSHOT_BYTES, SNAPSHOT_CHUNK_BYTES};
    let total = plan.snapshot_bytes();
    if total == 0 || total > MAX_RECOVERY_SNAPSHOT_BYTES as u64 {
        return Err("selected snapshot exceeds retained-state byte admission".into());
    }
    let bytes = if total <= 16 * 1024 * 1024 {
        let Reply::Snapshot(bytes) = transfer(config, plan, node, Operation::Snapshot).await?
        else {
            return Err("invalid completed snapshot reply".into());
        };
        bytes
    } else {
        let mut bytes = Vec::with_capacity(total as usize);
        while (bytes.len() as u64) < total {
            let from = bytes.len() as u64;
            let Reply::SnapshotChunk {
                from: actual,
                bytes: chunk,
            } = transfer(
                config,
                plan,
                node,
                Operation::SnapshotRead {
                    from,
                    max_bytes: SNAPSHOT_CHUNK_BYTES as u32,
                },
            )
            .await?
            else {
                return Err("invalid completed snapshot chunk reply".into());
            };
            if actual != from
                || chunk.is_empty()
                || chunk.len() > SNAPSHOT_CHUNK_BYTES
                || from + chunk.len() as u64 > total
            {
                return Err("completed snapshot chunk differs from selected bounds".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        bytes
    };
    plan.verify_snapshot(&bytes)?;
    Ok(bytes)
}

async fn begin_target(
    config: &ProtocolOwnerPeerResolverConfig,
    plan: &QueueRecoveryPlan,
    node: &str,
    snapshot: &[u8],
) -> Result<Reply, String> {
    use fibril_broker::recovery::snapshot::SNAPSHOT_CHUNK_BYTES;
    plan.verify_snapshot(snapshot)?;
    if snapshot.len() <= 16 * 1024 * 1024 {
        return transfer(
            config,
            plan,
            node,
            Operation::Begin {
                snapshot: snapshot.to_vec(),
            },
        )
        .await;
    }
    let Reply::SnapshotProgress(mut from) =
        transfer(config, plan, node, Operation::SnapshotProgress).await?
    else {
        return Err("invalid snapshot transfer progress reply".into());
    };
    if from > plan.snapshot_bytes() {
        return Err("snapshot transfer progress exceeds selected length".into());
    }
    while from < plan.snapshot_bytes() {
        let end = (from as usize + SNAPSHOT_CHUNK_BYTES).min(snapshot.len());
        let Reply::SnapshotProgress(next) = transfer(
            config,
            plan,
            node,
            Operation::SnapshotAppend {
                from,
                bytes: snapshot[from as usize..end].to_vec(),
            },
        )
        .await?
        else {
            return Err("invalid snapshot append progress reply".into());
        };
        if next != end as u64 {
            return Err("snapshot append made unexpected progress".into());
        }
        from = next;
    }
    transfer(config, plan, node, Operation::BeginTransferredSnapshot).await
}

fn source_report(plan: &QueueRecoveryPlan) -> Result<BrokerSealedReplica, String> {
    let h = plan.source_history().ok_or("plan source absent")?.clone();
    Ok(BrokerSealedReplica {
        node_id: plan.source_node().into(),
        seal: SealedReplicaFrontiers {
            request: RecoverySealRequest {
                transition: plan.pending().transition_digest()?,
                fence_epoch: plan.pending().proposed.epoch,
            },
            message_head: h.message_head,
            message_next: h.message_next,
            event_head: h.event_head,
            event_next: h.event_next,
            history: h,
        },
    })
}

/// Resume completed inspection phases, the immutable plan and staged offsets.
/// Each network/storage step has its own admission and deadline. The automatic
/// worker keeps progressing histories independent of a whole-attempt stopwatch.
pub async fn recover_queue_once(
    provider: &GanglionCoordination,
    broker: &Broker<StromaEngine>,
    config: &ProtocolOwnerPeerResolverConfig,
    pending: &PendingRecovery,
) -> Result<(), String> {
    let mut timing = crate::recovery_timing::RecoveryTiming::new(
        pending,
        provider.recovery_diagnostics().clone(),
    )?;
    let result = recover_queue_attempt(provider, broker, config, pending, &timing).await;
    timing.finish_result(&result);
    result
}

async fn recover_queue_attempt(
    provider: &GanglionCoordination,
    broker: &Broker<StromaEngine>,
    config: &ProtocolOwnerPeerResolverConfig,
    pending: &PendingRecovery,
    timing: &crate::recovery_timing::RecoveryTiming,
) -> Result<(), String> {
    if broker.is_shutting_down() {
        return Err("broker is shutting down".into());
    }
    let local = provider
        .replication_node_id()
        .ok_or("local node identity absent")?;
    let candidate = provider.queue_recovery_candidate(pending)?;
    if candidate.owner != local {
        return Err("only the current recovery candidate drives recovery".into());
    }
    if pending.proposed.resource.namespace != "fibril/queue"
        || pending.previous_activation.is_none()
    {
        return Err("recovery requires an accepted queue origin; legacy/stream reconstruction needs operator attention".into());
    }
    if pending.previous.replica_set_size() > MAX_REPLICAS
        || pending.proposed.replica_set_size() > MAX_REPLICAS
    {
        return Err(fibril_broker::recovery::RecoveryBudgetExceeded::message(
            fibril_broker::recovery::RecoveryBudget::ReplicaCount,
            MAX_REPLICAS as u64,
            0,
            pending
                .previous
                .replica_set_size()
                .max(pending.proposed.replica_set_size()) as u64,
        ));
    }
    let seal_command = pending.seal_command()?;
    let journal_root = broker.engine().recovery_inspection_journal_root().join(
        pending
            .transition_digest()?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    );
    let state_bytes = fibril_broker::recovery::snapshot::MAX_RECOVERY_SNAPSHOT_BYTES;
    let plan = if let Some(plan) = provider.queue_recovery_plan(pending).map_err(err)? {
        plan
    } else {
        let snapshot = provider.consensus_node().committed_snapshot();
        let mut witnesses = RecoveryWitnessSet::new(&snapshot, pending)?;
        let eligible: Vec<_> = witnesses
            .accepted_history(&snapshot)?
            .ok_or("accepted history absent")?
            .replicas
            .keys()
            .cloned()
            .collect();
        // Each source may be inspected as soon as its seal is available. Keep
        // the entire pipeline bounded and collect all evidence before selection.
        let inspected = stream::iter(
            eligible
                .iter()
                .map(|node| {
                    let seal_command = &seal_command;
                    let journal_root = &journal_root;
                    async move {
                        let report = timing
                            .stage(
                                "witness_seal",
                                node,
                                request_recovery_seal(config, node, seal_command, RPC),
                            )
                            .await;
                        let artifact = match &report {
                            Ok(report) => Some(
                                timing
                                    .stage(
                                        "inspect_source",
                                        node,
                                        inspect_recovery_source_artifact_resumable(
                                            config,
                                            seal_command,
                                            report,
                                            automatic_inspection_limits(),
                                            Default::default(),
                                            state_bytes as u32,
                                            state_bytes,
                                            RPC,
                                            journal_root,
                                        ),
                                    )
                                    .await
                                    .map(|artifact| artifact.summary()),
                            ),
                            Err(_) => None,
                        };
                        (node, report, artifact)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .buffer_unordered(PARALLEL_REPLICAS)
        .collect::<Vec<_>>()
        .await;
        let mut reports = vec![];
        let mut artifacts = BTreeMap::new();
        for (node, report, artifact) in inspected {
            match report {
                Ok(report) => {
                    witnesses.record(
                        &provider.consensus_node().committed_snapshot(),
                        node,
                        report.clone(),
                    )?;
                    match artifact.expect("a sealed source was inspected") {
                        Ok(artifact) => {
                            artifacts.insert(node.clone(), artifact);
                        }
                        Err(error) => {
                            // Preserve the witness even if another source must
                            // supply payloads missing from this replica.
                            tracing::warn!(node, message_next=report.seal.message_next,
                                event_next=report.seal.event_next, %error,
                                "sealed replica cannot supply a complete recovery artifact");
                        }
                    }
                    reports.push(report);
                }
                Err(error) => tracing::debug!(node, %error, "recovery witness unavailable"),
            }
        }
        // Completion order must not alter deterministic source selection.
        reports.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        let mut comparisons = vec![];
        for (i, left) in reports.iter().enumerate() {
            for right in &reports[i + 1..] {
                let pair = timing
                    .stage(
                        "compare_pair",
                        &format!("{}+{}", left.node_id, right.node_id),
                        inspect_recovery_pair_resumable(
                            config,
                            &seal_command,
                            left,
                            right,
                            automatic_inspection_limits(),
                            RPC,
                            &journal_root,
                        ),
                    )
                    .await
                    .map_err(err)?;
                comparisons.push(pair.evidence);
            }
        }
        let selection = witnesses.select_queue_source_summaries(
            &provider.consensus_node().committed_snapshot(),
            &artifacts,
            &comparisons,
        )?;
        let plan = timing
            .stage(
                "commit_plan",
                local,
                provider.persist_queue_recovery_plan(pending, &witnesses, &selection),
            )
            .await
            .map_err(err)?;
        plan
    };
    let members: Vec<_> = std::iter::once(&candidate.owner)
        .chain(candidate.followers.iter())
        .cloned()
        .collect();
    // Prefer a completed transferred copy; it survives loss of the old source.
    let mut completed_source = None;
    let mut snapshot = None;
    for node in &members {
        if let Ok(bytes) = timing
            .stage(
                "probe_snapshot",
                node,
                completed_snapshot(config, &plan, node),
            )
            .await
        {
            completed_source = Some(node.clone());
            snapshot = Some(bytes);
            break;
        }
    }
    if snapshot.is_none() {
        let artifact = timing
            .stage(
                "reinspect_source",
                plan.source_node(),
                inspect_recovery_source_artifact_resumable(
                    config,
                    &seal_command,
                    &source_report(&plan)?,
                    automatic_inspection_limits(),
                    Default::default(),
                    state_bytes as u32,
                    state_bytes,
                    RPC,
                    &journal_root,
                ),
            )
            .await
            .map_err(err)?;
        plan.verify_artifact(&artifact)?;
        snapshot = Some(artifact.state_snapshot().to_vec());
    }
    let snapshot = snapshot.unwrap();
    let mut errors = vec![];
    let source_reads = tokio::sync::Mutex::new(());
    let prepared = stream::iter(
        members
            .iter()
            .filter(|node| completed_source.as_ref() != Some(*node))
            .map(|node| {
                let plan = &plan;
                let snapshot = &snapshot;
                let source = completed_source.as_deref();
                let source_reads = &source_reads;
                async move {
                    (
                        node,
                        prepare_target(
                            config,
                            plan,
                            pending,
                            node,
                            snapshot,
                            source,
                            source_reads,
                            timing,
                        )
                        .await,
                    )
                }
            })
            .collect::<Vec<_>>(),
    )
    .buffer_unordered(PARALLEL_REPLICAS)
    .collect::<Vec<_>>()
    .await;
    let mut ready = vec![];
    for (node, result) in prepared {
        match result {
            Ok(()) => ready.push(node),
            Err(e) => errors.push(format!("{node}: {e}")),
        }
    }
    if let Some(source) = &completed_source {
        match prepare_target(
            config,
            &plan,
            pending,
            source,
            &snapshot,
            Some(source),
            &source_reads,
            timing,
        )
        .await
        {
            Ok(()) => ready.push(source),
            Err(e) => errors.push(format!("{source}: {e}")),
        }
    }
    // If too few targets completed, preserve the old source for retry. With an
    // exact completed quorum, failed targets are excluded by activation; all
    // in-flight reads have finished before any source replacement starts.
    if !errors.is_empty()
        && (ready.len() < pending.proposed_write_nodes
            || !ready.iter().any(|n| n.as_str() == local))
    {
        return Err(format!(
            "recovery staging incomplete: {}",
            errors.join("; ")
        ));
    }
    let installations = stream::iter(
        ready
            .into_iter()
            .map(|node| {
                let plan = &plan;
                async move {
                    (
                        node,
                        timing
                            .stage(
                                "install_target",
                                node,
                                transfer(config, plan, node, Operation::Install),
                            )
                            .await,
                    )
                }
            })
            .collect::<Vec<_>>(),
    )
    .buffer_unordered(PARALLEL_REPLICAS)
    .collect::<Vec<_>>()
    .await;
    let mut installed = 0usize;
    let mut owner_ready = false;
    for (node, result) in installations {
        match result {
            Ok(Reply::Installed(_)) => {
                installed += 1;
                owner_ready |= node == local;
            }
            Ok(_) => errors.push(format!("{node}: invalid installed response")),
            Err(e) => errors.push(format!("{node}: {e}")),
        }
    }
    if !owner_ready || installed < pending.proposed_write_nodes {
        return Err(format!(
            "recovery has {installed}/{} installed replicas, owner_ready={owner_ready}: {}",
            pending.proposed_write_nodes,
            errors.join("; ")
        ));
    }
    let activation = timing
        .stage(
            "activate",
            local,
            provider.activate_queue_recovery(&plan, &broker.engine()),
        )
        .await
        .map_err(err)?;
    for node in activation.reports().keys() {
        if let Err(e) = timing
            .stage(
                "admit",
                node,
                transfer(config, &plan, node, Operation::Admit),
            )
            .await
        {
            tracing::warn!(node,error=%e,"recovery activated; exact local admission will retry");
        }
    }
    tracing::info!(
        topic = pending.proposed.resource.name,
        partition = pending.proposed.resource.partition,
        epoch = pending.proposed.epoch,
        replicas = activation.reports().len(),
        message_next = plan.message_next(),
        event_next = plan.event_next(),
        "queue recovery activated"
    );
    Ok(())
}

// Preparing a private stage does not replace the frozen source. All page reads
// finish before any installation starts, so concurrent targets cannot invalidate
// a source still needed by another target. A completed-stage source is left alone
// until its readers finish because transfer RPCs share a per-broker stage lock.
async fn prepare_target(
    config: &ProtocolOwnerPeerResolverConfig,
    plan: &QueueRecoveryPlan,
    pending: &PendingRecovery,
    node: &str,
    snapshot: &[u8],
    completed_source: Option<&str>,
    source_reads: &tokio::sync::Mutex<()>,
    timing: &crate::recovery_timing::RecoveryTiming,
) -> Result<(), String> {
    let seal_command = pending.seal_command()?;
    let payload_log = plan
        .source_history()
        .ok_or("recovery source is missing")?
        .payload_log();
    timing
        .stage("prepare_target", node, async {
            // Existing old storage must be durably fenced before replacement.
            // Already installed targets reject this old seal and resume below.
            if pending.previous.owner == node
                || pending.previous.followers.iter().any(|n| n == node)
            {
                let _ = timing
                    .stage(
                        "target_seal",
                        node,
                        request_recovery_seal(config, node, &seal_command, RPC),
                    )
                    .await;
            }
            let Reply::Progress(mut next) = timing
                .stage(
                    "begin_target",
                    node,
                    begin_target(config, plan, node, snapshot),
                )
                .await?
            else {
                return Err("invalid staging progress response".into());
            };
            if next < payload_log.head || next > payload_log.next {
                return Err("staging progress is outside the selected log".into());
            }
            timing
                .stage("copy_pages", node, async {
                    let mut prefetched = None;
                    while next < payload_log.next {
                        let page = match prefetched.take() {
                            Some(page) => page,
                            None => {
                                read_source_page(
                                    config,
                                    plan,
                                    &seal_command,
                                    completed_source,
                                    source_reads,
                                    next,
                                )
                                .await?
                            }
                        };
                        if page.next <= next || page.next > payload_log.next {
                            return Err("recovery page made no valid progress".into());
                        }
                        let expected_next = page.next;
                        let append = transfer(config, &plan, node, Operation::Append { page });
                        // Own both futures through completion: a failed append
                        // must not leave an unbounded/detached source read behind.
                        let (appended, ahead) = tokio::join!(append, async {
                            if expected_next < payload_log.next {
                                read_source_page(
                                    config,
                                    plan,
                                    &seal_command,
                                    completed_source,
                                    source_reads,
                                    expected_next,
                                )
                                .await
                                .map(Some)
                            } else {
                                Ok(None)
                            }
                        });
                        let Reply::Progress(progress) = appended? else {
                            return Err("invalid append response".into());
                        };
                        if progress <= next || progress > payload_log.next {
                            return Err("invalid staged progress".into());
                        }
                        // A resumed target can already be ahead. Discard a page
                        // fetched for a different frontier and read the exact one.
                        if progress == expected_next {
                            prefetched = ahead?;
                        }
                        next = progress;
                    }
                    Ok::<_, String>(())
                })
                .await?;
            if !matches!(
                timing
                    .stage(
                        "finish_target",
                        node,
                        transfer(config, &plan, node, Operation::Finish)
                    )
                    .await?,
                Reply::Complete(_)
            ) {
                return Err("invalid completion response".into());
            }
            Ok(())
        })
        .await
}

async fn read_source_page(
    config: &ProtocolOwnerPeerResolverConfig,
    plan: &QueueRecoveryPlan,
    seal_command: &fibril_broker::recovery::RecoverySealCommand,
    completed_source: Option<&str>,
    source_reads: &tokio::sync::Mutex<()>,
    next: u64,
) -> Result<fibril_broker::recovery::RecoveryReadPage, String> {
    // Completed-stage reads share one per-broker stage lock.
    // Serialize source reads while target appends still overlap.
    let _read = source_reads.lock().await;
    let source_kind = plan
        .source_history()
        .ok_or("recovery source is missing")?
        .payload_log()
        .source;
    Ok(if let Some(source) = completed_source {
        let Reply::Page(page) = transfer(
            config,
            &plan,
            source,
            Operation::Read {
                from: next,
                max_records: 4096,
                max_bytes: 16 * 1024 * 1024,
            },
        )
        .await?
        else {
            return Err("invalid completed-source page".into());
        };
        page
    } else {
        let report = source_report(&plan)?;
        let page = request_recovery_read(
            config,
            &seal_command,
            &report,
            &RecoveryReadRequest {
                seal: report.seal.request.clone(),
                history_id: report.seal.history.id,
                source: source_kind,
                from: next,
                max_records: 4096,
                max_bytes: 16 * 1024 * 1024,
            },
            RPC,
        )
        .await
        .map_err(err)?;
        fibril_broker::recovery::RecoveryReadPage {
            history_id: page.history_id,
            source: source_kind,
            from: page.from,
            next: page.next,
            end: page.end,
            records: page
                .records
                .into_iter()
                .map(|r| fibril_broker::recovery::RecoveryRecord {
                    offset: r.offset,
                    flags: r.flags,
                    headers: r.headers,
                    payload: r.payload,
                })
                .collect(),
            snapshot_bytes: page.snapshot_bytes,
        }
    })
}

/// Start bounded recovery and exact local readmission for enrolled histories.
pub fn spawn(
    provider: Arc<GanglionCoordination>,
    broker: Arc<Broker<StromaEngine>>,
    mut config: ProtocolOwnerPeerResolverConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut initial_failures: HashMap<
            ganglion_core::ResourceIdentity,
            (tokio::time::Instant, u64, String),
        > = HashMap::new();
        let _checkpoints = crate::queue_checkpoint_driver::spawn(provider.clone(), broker.clone());
        let _learners =
            crate::queue_learner_driver::spawn(provider.clone(), broker.clone(), config.clone());
        let mut failures: HashMap<[u8; 32], (tokio::time::Instant, u64, String)> = HashMap::new();
        // One admitted queue recovery preserves the per-node stage cache. Its
        // bounded RPC/inspection steps run separately from metadata admission,
        // so a long but progressing history does not block those control loops.
        let mut jobs: tokio::task::JoinSet<(PendingRecovery, Result<(), String>)> =
            tokio::task::JoinSet::new();
        let mut running: Option<(PendingRecovery, tokio::task::AbortHandle)> = None;
        let mut cleanup_state = Some(fibril_broker::recovery::cache::RecoveryCacheSweep::default());
        let mut cleanup_job: Option<
            tokio::task::JoinHandle<(
                fibril_broker::recovery::cache::RecoveryCacheSweep,
                Result<fibril_broker::recovery::cache::RecoveryCacheSweepStep, String>,
            )>,
        > = None;
        let mut cleanup_at = tokio::time::Instant::now();
        let mut wake =
            crate::recovery_wake::RecoveryWake::new(provider.consensus_node().watch_committed());
        loop {
            let state = wake.next().await;
            if broker.is_shutting_down() {
                jobs.abort_all();
                break;
            }
            while let Some(outcome) = jobs.try_join_next() {
                match outcome {
                    Ok((pending, outcome)) => {
                        let id = pending.transition_digest().unwrap_or_default();
                        match outcome {
                            Ok(()) => {
                                failures.remove(&id);
                            }
                            Err(error) => {
                                let previous = failures.get(&id);
                                let delay = previous.map_or(1, |(_, delay, _)| (delay * 2).min(30));
                                if previous.is_none_or(|(_, _, old)| old != &error) {
                                    tracing::warn!(topic=pending.proposed.resource.name,partition=pending.proposed.resource.partition,retry_seconds=delay,%error,"queue remains fenced, recovery will resume verified phases");
                                }
                                failures.insert(
                                    id,
                                    (
                                        tokio::time::Instant::now() + Duration::from_secs(delay),
                                        delay,
                                        error,
                                    ),
                                );
                            }
                        }
                    }
                    Err(error) if !error.is_cancelled() => {
                        tracing::error!(%error, "recovery worker failed, durable phases remain available");
                        if let Some((pending, _)) = &running {
                            failures.insert(
                                pending.transition_digest().unwrap_or_default(),
                                (
                                    tokio::time::Instant::now() + Duration::from_secs(1),
                                    1,
                                    error.to_string(),
                                ),
                            );
                        }
                    }
                    Err(_) => {}
                }
                running = None;
            }
            if cleanup_job.as_ref().is_some_and(|job| job.is_finished()) {
                match cleanup_job.take().unwrap().await {
                    Ok((sweep, result)) => {
                        cleanup_state = Some(sweep);
                        let complete = match result {
                            Ok(step) => {
                                if step.cleanup.removed_files != 0 {
                                    tracing::debug!(
                                        files = step.cleanup.removed_files,
                                        bytes = step.cleanup.removed_bytes,
                                        "reclaimed retired recovery inspection cache"
                                    );
                                }
                                step.cycle_complete
                            }
                            Err(error) => {
                                tracing::warn!(%error,"recovery inspection cleanup will retry");
                                true
                            }
                        };
                        cleanup_at = tokio::time::Instant::now()
                            + if complete {
                                Duration::from_secs(30)
                            } else {
                                Duration::ZERO
                            };
                    }
                    Err(error) => {
                        tracing::warn!(%error,"recovery inspection cleanup worker failed");
                        cleanup_state = Some(Default::default());
                        cleanup_at = tokio::time::Instant::now() + Duration::from_secs(30);
                    }
                }
            }
            config.nodes = state
                .nodes
                .iter()
                .map(|(id, n)| (id.clone(), n.endpoint.clone()))
                .collect();
            // Activation can commit before the owner returns a reply. Every
            // exact prepared process retries its own admission independently.
            match provider.local_initial_history_admissions() {
                Ok(admissions) => {
                    for (decision, prepared) in admissions {
                        if broker
                            .engine()
                            .verify_admitted_storage_history(&prepared)
                            .is_ok()
                        {
                            continue;
                        }
                        if let Err(e) = provider
                            .admit_local_initial_history(&decision, &broker.engine())
                            .await
                        {
                            tracing::debug!(error=%e, "initial history admission will retry");
                        } else {
                            broker.notify_history_admitted();
                        }
                    }
                }
                Err(e) => tracing::warn!(error=%e, "cannot inspect initial history admissions"),
            }
            match provider.initial_queue_work() {
                Ok(work) => {
                    let active: std::collections::HashSet<_> = work.iter().collect();
                    initial_failures.retain(|resource, _| active.contains(resource));
                    for resource in work {
                        if initial_failures
                            .get(&resource)
                            .is_some_and(|(at, _, _)| *at > tokio::time::Instant::now())
                        {
                            continue;
                        }
                        let outcome = tokio::time::timeout(
                            Duration::from_secs(120),
                            crate::initial_history_driver::prepare_queue_once(
                                &provider, &broker, &config, &resource,
                            ),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            Err("initial preparation exceeded work budget; retry".into())
                        });
                        match outcome {
                            Ok(()) => {
                                initial_failures.remove(&resource);
                            }
                            Err(e) => {
                                let previous = initial_failures.get(&resource);
                                let delay = previous.map_or(1, |(_, d, _)| (d * 2).min(30));
                                if previous.is_none_or(|(_, _, old)| old != &e) {
                                    tracing::warn!(topic=resource.name, partition=resource.partition, retry_seconds=delay, error=%e,
                                        "queue remains fenced during initial preparation");
                                }
                                initial_failures.insert(
                                    resource,
                                    (
                                        tokio::time::Instant::now() + Duration::from_secs(delay),
                                        delay,
                                        e,
                                    ),
                                );
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error=%e, "cannot inspect initial queue work"),
            }
            // Admission is independently retried on every target, so the owner
            // disappearing after activation cannot strand a successfully installed copy.
            match provider.local_queue_recovery_admissions() {
                Ok(commands) => {
                    for (command, prepared) in commands {
                        if broker
                            .engine()
                            .verify_admitted_storage_history(&prepared.storage)
                            .is_ok()
                        {
                            continue;
                        }
                        if let Err(e) = broker
                            .recovery_transfer(QueueRecoveryRequest {
                                command,
                                operation: Operation::Admit,
                            })
                            .await
                        {
                            tracing::debug!(error=%e,"recovery admission will retry");
                        }
                    }
                }
                Err(e) => tracing::warn!(error=%e,"cannot inspect local recovery admissions"),
            }
            let requests = match provider.pending_recoveries() {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error=%e,"cannot inspect pending recoveries");
                    continue;
                }
            };
            let active: std::collections::HashSet<_> = requests
                .iter()
                .filter_map(|p| p.transition_digest().ok())
                .collect();
            failures.retain(|id, _| active.contains(id));
            if cleanup_job.is_none() && tokio::time::Instant::now() >= cleanup_at {
                if let Some(mut sweep) = cleanup_state.take() {
                    let root = broker.engine().recovery_inspection_journal_root();
                    let cleanup_provider = provider.clone();
                    cleanup_job = Some(tokio::task::spawn_blocking(move || {
                        let result = sweep
                            .step(&root, |id| {
                                // Observe consensus after finding this cache. A
                                // transition created since the sweep started
                                // must not be retired from a stale active set.
                                let pending = cleanup_provider.pending_recoveries()
                                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                                for item in pending {
                                    let digest = item.transition_digest()
                                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                                    let encoded: String = digest.iter()
                                        .map(|byte| format!("{byte:02x}")).collect();
                                    if encoded == id { return Ok(true); }
                                }
                                Ok(false)
                            }, 32, 128)
                            .map_err(|e| e.to_string());
                        (sweep, result)
                    }));
                }
            }
            if let Some((work, task)) = &running {
                let current = requests
                    .iter()
                    .find(|p| p.transition_digest().ok() == work.transition_digest().ok());
                let still_owned = if let Some(current) = current {
                    provider
                        .queue_recovery_candidate(current)
                        .ok()
                        .is_some_and(|candidate| {
                            provider.replication_node_id() == Some(candidate.owner.as_str())
                        })
                } else {
                    // Activation removes the pending barrier before admission
                    // replies finish. Let that successful work finish normally.
                    state
                        .assignments
                        .get(&work.proposed.resource)
                        .is_some_and(|assignment| {
                            assignment.epoch == work.proposed.epoch
                                && provider.replication_node_id() == Some(assignment.owner.as_str())
                        })
                };
                if !still_owned {
                    task.abort();
                }
            }
            for pending in requests {
                // Let the original driver report malformed/stale authority via
                // the existing bounded error path instead of silently skipping it.
                // recover_queue_once always validates the candidate again.
                let candidate = provider
                    .queue_recovery_candidate(&pending)
                    .unwrap_or_else(|_| pending.proposed.clone());
                if provider.replication_node_id() != Some(candidate.owner.as_str())
                    || pending.previous_activation.is_none()
                {
                    continue;
                }
                let Ok(id) = pending.transition_digest() else {
                    continue;
                };
                if failures
                    .get(&id)
                    .is_some_and(|(at, _, _)| *at > tokio::time::Instant::now())
                {
                    continue;
                }
                if !jobs.is_empty() {
                    break;
                }
                let work_provider = provider.clone();
                let work_broker = broker.clone();
                let work_config = config.clone();
                let work_pending = pending.clone();
                let task = jobs.spawn(async move {
                    let outcome =
                        recover_queue_once(&work_provider, &work_broker, &work_config, &pending)
                            .await;
                    (pending, outcome)
                });
                running = Some((work_pending, task));
                break;
            }
        }
    })
}
