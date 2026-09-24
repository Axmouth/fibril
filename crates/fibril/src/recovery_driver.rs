//! Bounded creation and recovery for enrolled queue histories. Legacy origins,
//! streams and crossed tails remain fenced with an actionable error.
use fibril_broker::{
    broker::{Broker, QueueOwnership},
    queue_engine::StromaEngine,
    recovery::{
        BrokerSealedReplica, RecoveryReadRequest, RecoveryReadSource, RecoverySealRequest,
        SealedReplicaFrontiers, inspection::RecoveryInspectionLimits,
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
    recovery_inspection::{inspect_recovery_pair, inspect_recovery_source_artifact},
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
// Each transfer can retain a 16 MiB page. Bound aggregate memory and disk pressure.
const PARALLEL_REPLICAS: usize = 2;
// Sequential inspection verifies each complete retained history. Use the wire limits
// to amortize those scans, while keeping total inspection budgets unchanged.
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

/// One bounded attempt. Repeating it resumes the same immutable plan and staged
/// offsets. Callers supply authenticated peer addresses and impose a total deadline.
pub async fn recover_queue_once(
    provider: &GanglionCoordination,
    broker: &Broker<StromaEngine>,
    config: &ProtocolOwnerPeerResolverConfig,
    pending: &PendingRecovery,
) -> Result<(), String> {
    let mut timing = crate::recovery_timing::RecoveryTiming::new(pending)?;
    let result = recover_queue_attempt(provider, broker, config, pending, &timing).await;
    timing.finish(result.is_ok());
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
        return Err("automatic recovery exceeds the 16-replica work budget".into());
    }
    let seal_command = pending.seal_command()?;
    // Attempt-local only: a resumed persisted plan reconstructs its source as
    // before. Keep the selected replay result alive across plan persistence.
    let mut selected_artifact = None;
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
        let mut reports = vec![];
        let sealed = stream::iter(
            eligible
                .iter()
                .map(|node| {
                    let seal_command = &seal_command;
                    async move {
                        (
                            node,
                            timing
                                .stage(
                                    "witness_seal",
                                    node,
                                    request_recovery_seal(config, node, seal_command, RPC),
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
        for (node, result) in sealed {
            match result {
                Ok(report) => {
                    witnesses.record(
                        &provider.consensus_node().committed_snapshot(),
                        node,
                        report.clone(),
                    )?;
                    reports.push(report);
                }
                Err(e) => tracing::debug!(node,error=%e,"recovery witness unavailable"),
            }
        }
        // Completion order must not alter deterministic source selection.
        reports.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        let inspected = stream::iter(
            reports
                .iter()
                .map(|report| {
                    let seal_command = &seal_command;
                    async move {
                        let result = timing
                            .stage(
                                "inspect_source",
                                &report.node_id,
                                inspect_recovery_source_artifact(
                                    config,
                                    &seal_command,
                                    report,
                                    automatic_inspection_limits(),
                                    Default::default(),
                                    16 * 1024 * 1024,
                                    16 * 1024 * 1024,
                                    RPC,
                                ),
                            )
                            .await;
                        (report, result)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .buffer_unordered(PARALLEL_REPLICAS)
        .collect::<Vec<_>>()
        .await;
        let mut artifacts = BTreeMap::new();
        for (report, result) in inspected {
            match result {
                Ok(artifact) => {
                    artifacts.insert(report.node_id.clone(), artifact);
                }
                Err(e) => {
                    // Keep the witness: another replica may supply missing payloads.
                    tracing::warn!(node=report.node_id,message_next=report.seal.message_next,event_next=report.seal.event_next,error=%e,
                        "sealed replica cannot supply a complete recovery artifact");
                }
            }
        }
        let mut comparisons = vec![];
        for (i, left) in reports.iter().enumerate() {
            for right in &reports[i + 1..] {
                let pair = timing
                    .stage(
                        "compare_pair",
                        &format!("{}+{}", left.node_id, right.node_id),
                        inspect_recovery_pair(
                            config,
                            &seal_command,
                            left,
                            right,
                            automatic_inspection_limits(),
                            RPC,
                        ),
                    )
                    .await
                    .map_err(err)?;
                comparisons.push(pair.evidence);
            }
        }
        let selection = witnesses.select_queue_source(
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
        selected_artifact = artifacts.remove(plan.source_node());
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
        if let Ok(Reply::Snapshot(bytes)) = timing
            .stage(
                "probe_snapshot",
                node,
                transfer(config, &plan, node, Operation::Snapshot),
            )
            .await
        {
            completed_source = Some(node.clone());
            snapshot = Some(bytes);
            break;
        }
    }
    if snapshot.is_none() {
        let artifact = if let Some(artifact) = selected_artifact.take() {
            artifact
        } else {
            timing
                .stage(
                    "reinspect_source",
                    plan.source_node(),
                    inspect_recovery_source_artifact(
                        config,
                        &seal_command,
                        &source_report(&plan)?,
                        automatic_inspection_limits(),
                        Default::default(),
                        16 * 1024 * 1024,
                        16 * 1024 * 1024,
                        RPC,
                    ),
                )
                .await
                .map_err(err)?
        };
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
                    transfer(
                        config,
                        &plan,
                        node,
                        Operation::Begin {
                            snapshot: snapshot.to_vec(),
                        },
                    ),
                )
                .await?
            else {
                return Err("invalid staging progress response".into());
            };
            timing
                .stage("copy_pages", node, async {
                    while next < plan.message_next() {
                        let page = {
                            // Completed-stage reads share one per-broker stage lock.
                            // Serialize source reads while target appends still overlap.
                            let _read = source_reads.lock().await;
                            if let Some(source) = completed_source {
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
                                        source: RecoveryReadSource::Messages,
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
                                    source: RecoveryReadSource::Messages,
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
                            }
                        };
                        if page.next <= next || page.next > plan.message_next() {
                            return Err("recovery page made no valid progress".into());
                        }
                        let Reply::Progress(progress) =
                            transfer(config, &plan, node, Operation::Append { page }).await?
                        else {
                            return Err("invalid append response".into());
                        };
                        if progress <= next || progress > plan.message_next() {
                            return Err("invalid staged progress".into());
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
        let mut wake =
            crate::recovery_wake::RecoveryWake::new(provider.consensus_node().watch_committed());
        loop {
            let state = wake.next().await;
            if broker.is_shutting_down() {
                break;
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
                let outcome = tokio::time::timeout(
                    Duration::from_secs(120),
                    recover_queue_once(&provider, &broker, &config, &pending),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(
                        "recovery attempt exceeded two-minute work budget; durable stages retained"
                            .into(),
                    )
                });
                match outcome {
                    Ok(()) => {
                        failures.remove(&id);
                    }
                    Err(e) => {
                        let previous = failures.get(&id);
                        let delay = previous.map_or(1, |(_, d, _)| (d * 2).min(30));
                        if previous.is_none_or(|(_, _, old)| old != &e) {
                            tracing::warn!(topic=pending.proposed.resource.name,partition=pending.proposed.resource.partition,retry_seconds=delay,error=%e,"queue remains fenced; recovery will retry within verified bounds");
                        }
                        failures.insert(
                            id,
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
    })
}
