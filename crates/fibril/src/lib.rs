//! Server wiring extracted from the `fibril-server` binary so it can be unit
//! tested. Per the fibril/ganglion split, this holds fibril-specific glue
//! (protocol<->coordination adapter bridges, config->settings mapping); the
//! reusable coordination primitives live in ganglion.

use std::collections::BTreeMap;
use std::num::ParseIntError;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use fibril_admin::{
    AdminConfig, AdminServer, AdminServerError, CoordinationMembershipManager,
    RuntimeSettingsClusterStore, RuntimeSettingsClusterUpdateOutcome, StartupConfigSummary,
};
use fibril_broker::{
    broker::{Broker, BrokerConfig, FollowerReplicationWorkerConfig, OwnAllQueues, QueueOwnership},
    coordination::{
        ClusterCohortController, ConsumerGroupKey, Coordination, DeterministicPartitionPlacement,
        NodeInfo, QueueIdentity, ReplicationDurabilityPolicy, StaticCoordination,
        StickyConsumerGroupAssignor,
    },
    queue_engine::{
        KeratinConfig, QueueEngine as _, SnapshotConfig, StromaEngine, StromaError,
        StromaKeratinConfig,
    },
    runtime_settings::{
        ConnectionRuntimeSettings as BrokerConnectionRuntimeSettings, ConsumerGroupRuntimeSettings,
        DeliveryRuntimeSettings, IdleQueueCleanupRuntimeSettings, PartitioningRuntimeSettings,
        ReplicationRuntimeSettings, RuntimeSettings, RuntimeSettingsError, RuntimeSettingsLocks,
        RuntimeSettingsManager, RuntimeSettingsSnapshot,
    },
};
use fibril_config::{
    CoordinationMode, GanglionAssignmentDurabilityMode, GanglionAssignmentDurabilitySection,
    ServerConfig,
};
use fibril_coordination_ganglion::{
    ClientTopology, ClusterRuntimeSettingsUpdateOutcome, ControllerStatus, ForwardedWritePolicy,
    GanglionCoordination,
};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::{
    ClientTopologySource, ConnectionRuntimeSettings as ProtocolConnectionRuntimeSettings,
    ConnectionSettings, ProtocolServerError, QueueDeclareCoordinator,
    run_server as run_protocol_server,
};
use fibril_protocol::v1::{Partition, QueueTopologyEntry, TopologyOk};
use fibril_util::StaticAuthHandler;
use ganglion_openraft::{
    FileRaftLogStore, GanglionRaftConfig, TcpNetworkFactory, TcpRaftServer, WireFormat,
    WireFormatParseError,
    openraft::{BasicNode, RaftNetworkFactory, storage::RaftLogStorage},
};

/// Map the startup config's runtime-seed section into the broker's
/// `RuntimeSettings` (the initial cluster document before replicated overrides).
pub fn runtime_seed_from_config(config: &ServerConfig) -> RuntimeSettings {
    RuntimeSettings {
        delivery: DeliveryRuntimeSettings {
            inflight_ttl_ms: config.runtime_seed.delivery.inflight_ttl_ms,
            expiry_poll_min_ms: config.runtime_seed.delivery.expiry_poll_min_ms,
            expiry_batch_max: config.runtime_seed.delivery.expiry_batch_max,
            delivery_poll_max_ms: config.runtime_seed.delivery.delivery_poll_max_ms,
        },
        idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
            enabled: config.runtime_seed.idle_queue_cleanup.enabled,
            evict_after_ms: config.runtime_seed.idle_queue_cleanup.evict_after_ms,
            sweep_interval_ms: config.runtime_seed.idle_queue_cleanup.sweep_interval_ms,
            publisher_idle_timeout_ms: config
                .runtime_seed
                .idle_queue_cleanup
                .publisher_idle_timeout_ms,
        },
        connection: BrokerConnectionRuntimeSettings {
            reconnect_grace_ms: config.runtime_seed.connection.reconnect_grace_ms,
        },
        replication: ReplicationRuntimeSettings {
            confirm_timeout_ms: config.runtime_seed.replication.confirm_timeout_ms,
            caught_up_poll_ms: config.runtime_seed.replication.caught_up_poll_ms,
            retry_poll_ms: config.runtime_seed.replication.retry_poll_ms,
            checkpoint_retry_poll_ms: config.runtime_seed.replication.checkpoint_retry_poll_ms,
            max_messages_per_read: config.runtime_seed.replication.max_messages_per_read,
            max_events_per_read: config.runtime_seed.replication.max_events_per_read,
            max_bytes_per_read: config.runtime_seed.replication.max_bytes_per_read,
            max_iterations_per_tick: config.runtime_seed.replication.max_iterations_per_tick,
            min_in_sync_replicas: config.runtime_seed.replication.min_in_sync_replicas,
            isr_timeout_ms: config.runtime_seed.replication.isr_timeout_ms,
            stream_enabled: config.runtime_seed.replication.stream_enabled,
            stream_apply_linger_us: config.runtime_seed.replication.stream_apply_linger_us,
        },
        partitioning: PartitioningRuntimeSettings {
            default_partition_count: config.runtime_seed.partitioning.default_partition_count,
        },
        consumer_groups: ConsumerGroupRuntimeSettings {
            default_target_per_consumer: config
                .runtime_seed
                .consumer_groups
                .default_target_per_consumer,
        },
    }
}

/// Production Ganglion coordination provider used by `fibril-server`.
pub type TcpGanglionCoordination = GanglionCoordination<FileRaftLogStore, TcpNetworkFactory>;

/// Started Ganglion coordination pieces for the TCP-backed server path.
///
/// The coordination server handle must be kept alive for as long as the broker serves.
/// The controller status is shared with the admin topology endpoint.
pub struct TcpGanglionParts {
    pub coordination: Arc<TcpGanglionCoordination>,
    pub consensus_server: Arc<TcpRaftServer>,
    pub controller_task: tokio::task::JoinHandle<()>,
    pub controller_status: Arc<RwLock<ControllerStatus>>,
}

/// Background handles returned by cluster task wiring.
///
/// The binary currently lets these tasks run until process shutdown. Tests can
/// keep the handles and abort them explicitly.
pub struct GanglionBrokerTaskHandles {
    pub heartbeat: tokio::task::JoinHandle<()>,
    pub cohort_controller: tokio::task::JoinHandle<()>,
    pub cohort_owner_watcher: tokio::task::JoinHandle<()>,
    pub repartition_watcher: tokio::task::JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
pub enum FibrilServerError {
    #[error("failed to open storage engine: {0}")]
    Storage(#[source] StromaError),
    #[error("failed to load runtime settings: {0}")]
    RuntimeSettings(#[from] RuntimeSettingsError),
    #[error("coordination.ganglion.wire_format: {0}")]
    CoordinationWireFormat(#[source] WireFormatParseError),
    #[error("embedded coordinator failed to start: {0}")]
    EmbeddedCoordinatorStart(String),
    #[error("peer id `{id}` is not a u64: {source}")]
    InvalidPeerId {
        id: String,
        #[source]
        source: ParseIntError,
    },
    #[error("ganglion consensus config: {0}")]
    GanglionConsensusConfig(String),
    #[error(transparent)]
    AssignmentDurability(#[from] AssignmentDurabilityConfigError),
    #[error("admin.auth.password must be set when admin auth is enabled")]
    MissingAdminPassword,
    #[error("broker listener failed: {0}")]
    BrokerListener(#[source] ProtocolServerError),
    #[error("admin listener failed: {0}")]
    AdminListener(#[source] AdminServerError),
}

/// Live-repartition trigger for the admin endpoint: decides grow vs shrink from
/// the queue's current count and calls the matching coordination operation.
pub struct GanglionQueueRepartitionManager {
    coordination: Arc<TcpGanglionCoordination>,
}

impl GanglionQueueRepartitionManager {
    pub fn new(coordination: Arc<TcpGanglionCoordination>) -> Self {
        Self { coordination }
    }
}

#[async_trait]
impl fibril_admin::QueueRepartitionManager for GanglionQueueRepartitionManager {
    async fn repartition(
        &self,
        topic: String,
        group: Option<String>,
        partition_count: u32,
    ) -> Result<serde_json::Value, String> {
        // Serialize repartitions per (topic, group): refuse to start a new one
        // while a transition is still in flight. This keeps a fresh grow from
        // overlapping a shrink whose merged-away partitions are still being
        // drained and retired, so a recreate never races a retirement.
        if self
            .coordination
            .repartition_transition(&topic, group.as_deref())
            .is_some()
        {
            return Err(format!(
                "queue `{topic}` (group {group:?}) already has a repartition in progress"
            ));
        }
        let current = self
            .coordination
            .queue_partitioning(&topic, group.as_deref())
            .map(|p| p.partition_count);
        let result = match current {
            Some(current) if partition_count < current => self
                .coordination
                .shrink_queue(&topic, group.as_deref(), partition_count)
                .await
                .map_err(|error| error.to_string())?,
            _ => self
                .coordination
                .grow_queue(&topic, group.as_deref(), partition_count)
                .await
                .map_err(|error| error.to_string())?,
        };
        serde_json::to_value(result).map_err(|error| error.to_string())
    }
}

pub struct GanglionCoordinationMembershipManager {
    coordination: Arc<TcpGanglionCoordination>,
}

impl GanglionCoordinationMembershipManager {
    pub fn new(coordination: Arc<TcpGanglionCoordination>) -> Self {
        Self { coordination }
    }

    fn topology_json(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self.coordination.raft_node().topology())
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl CoordinationMembershipManager for GanglionCoordinationMembershipManager {
    async fn add_voting_member(&self, id: u64, addr: String) -> Result<serde_json::Value, String> {
        self.coordination
            .raft_node()
            .add_learner(id, BasicNode::new(addr), true)
            .await
            .map_err(|error| error.to_string())?;

        let mut voters = self.coordination.raft_node().topology().voters;
        if !voters.contains(&id) {
            voters.push(id);
            voters.sort_unstable();
        }

        self.coordination
            .raft_node()
            .change_membership(voters, false)
            .await
            .map_err(|error| error.to_string())?;

        self.topology_json()
    }

    async fn remove_voting_member(&self, id: u64) -> Result<serde_json::Value, String> {
        let voters = self
            .coordination
            .raft_node()
            .topology()
            .voters
            .into_iter()
            .filter(|voter| *voter != id)
            .collect::<Vec<_>>();
        if voters.is_empty() {
            return Err("refusing to remove the last coordination voting member".to_string());
        }

        self.coordination
            .raft_node()
            .change_membership(voters, false)
            .await
            .map_err(|error| error.to_string())?;

        self.topology_json()
    }
}

/// Start TCP-backed Ganglion coordination if `[coordination].mode = "ganglion"`.
///
/// This owns only the coordination process and embedded placement controller.
/// Broker-specific wiring is separate so tests can build the broker around the
/// returned ownership provider.
pub async fn open_tcp_ganglion_parts(
    config: &ServerConfig,
) -> Result<Option<TcpGanglionParts>, FibrilServerError> {
    match config.coordination.mode {
        CoordinationMode::Static => Ok(None),
        CoordinationMode::Ganglion => {
            let section = &config.coordination.ganglion;
            let data_dir = if section.data_dir.as_os_str().is_empty() {
                config.server.data_dir.join("coordination")
            } else {
                section.data_dir.clone()
            };

            let consensus_config = consensus_config_from_config(config)?;
            let wire_format: WireFormat = section
                .wire_format
                .parse()
                .map_err(FibrilServerError::CoordinationWireFormat)?;
            let (node, consensus_server) =
                ganglion_openraft::RaftMetadataNode::start_durable_tcp_with_format(
                    section.raft_node_id,
                    consensus_config,
                    section.listen,
                    &data_dir,
                    wire_format,
                )
                .await
                .map_err(|error| FibrilServerError::EmbeddedCoordinatorStart(error.to_string()))?;
            let consensus_server = Arc::new(consensus_server);

            if section.bootstrap {
                let members: std::collections::BTreeMap<u64, BasicNode> = section
                    .peers
                    .iter()
                    .map(|(id, addr)| {
                        Ok((
                            id.parse::<u64>().map_err(|source| {
                                FibrilServerError::InvalidPeerId {
                                    id: id.clone(),
                                    source,
                                }
                            })?,
                            BasicNode::new(addr.clone()),
                        ))
                    })
                    .collect::<Result<_, FibrilServerError>>()?;
                // Re-running initialize after first boot is rejected by coordination.
                // That is the expected restart path, not a startup failure.
                if let Err(error) = node.initialize(members).await {
                    tracing::info!("coordinator initialize skipped: {error}");
                }
            }

            let coordination = Arc::new(GanglionCoordination::new_with_forwarded_write_policy(
                config.coordination.node_id.clone(),
                node,
                wire_format,
                forwarded_write_policy_from_config(config),
            ));
            let (controller_task, controller_status) = coordination.spawn_controller(
                Arc::new(DeterministicPartitionPlacement),
                fibril_coordination_ganglion::ControllerConfig {
                    target_followers: section.target_followers,
                    default_durability: assignment_durability_from_config(
                        &section.assignment_durability,
                    )?,
                    tick: Duration::from_millis(section.controller_tick_ms),
                    liveness_ttl: Duration::from_millis(section.liveness_ttl_ms),
                    max_cas_retries: 8,
                },
            );

            Ok(Some(TcpGanglionParts {
                coordination,
                consensus_server,
                controller_task,
                controller_status,
            }))
        }
    }
}

fn consensus_config_from_config(
    config: &ServerConfig,
) -> Result<Arc<ganglion_openraft::openraft::Config>, FibrilServerError> {
    let section = &config.coordination.ganglion.raft;
    let mut consensus_config = ganglion_openraft::default_raft_config()
        .map_err(|error| FibrilServerError::GanglionConsensusConfig(error.to_string()))?
        .as_ref()
        .clone();
    consensus_config.heartbeat_interval = section.heartbeat_interval_ms;
    consensus_config.election_timeout_min = section.election_timeout_min_ms;
    consensus_config.election_timeout_max = section.election_timeout_max_ms;
    consensus_config
        .validate()
        .map(Arc::new)
        .map_err(|error| FibrilServerError::GanglionConsensusConfig(error.to_string()))
}

fn forwarded_write_policy_from_config(config: &ServerConfig) -> ForwardedWritePolicy {
    let section = &config.coordination.ganglion.forwarded_write;
    ForwardedWritePolicy {
        redirect_limit: section.redirect_limit,
        no_leader_retries: section.no_leader_retries,
        no_leader_base_backoff: Duration::from_millis(section.no_leader_base_backoff_ms),
        no_leader_max_backoff: Duration::from_millis(section.no_leader_max_backoff_ms),
    }
}

fn assignment_durability_from_config(
    section: &GanglionAssignmentDurabilitySection,
) -> Result<ReplicationDurabilityPolicy, AssignmentDurabilityConfigError> {
    match section.mode {
        GanglionAssignmentDurabilityMode::LocalDurable => {
            Ok(ReplicationDurabilityPolicy::LocalDurable)
        }
        GanglionAssignmentDurabilityMode::ReplicaAccepted => {
            Ok(ReplicationDurabilityPolicy::ReplicaAccepted {
                nodes: section.nodes.ok_or(
                    AssignmentDurabilityConfigError::MissingReplicaNodes {
                        mode: "replica_accepted",
                    },
                )?,
            })
        }
        GanglionAssignmentDurabilityMode::ReplicaDurable => {
            Ok(ReplicationDurabilityPolicy::ReplicaDurable {
                nodes: section.nodes.ok_or(
                    AssignmentDurabilityConfigError::MissingReplicaNodes {
                        mode: "replica_durable",
                    },
                )?,
            })
        }
        GanglionAssignmentDurabilityMode::MajorityDurable => {
            Ok(ReplicationDurabilityPolicy::MajorityDurable)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AssignmentDurabilityConfigError {
    #[error("coordination.ganglion.assignment_durability.nodes is required for {mode}")]
    MissingReplicaNodes { mode: &'static str },
}

/// Ownership provider used by the broker: cluster assignments in Ganglion mode,
/// own-all standalone behavior otherwise.
pub fn queue_ownership_for_ganglion(parts: Option<&TcpGanglionParts>) -> Arc<dyn QueueOwnership> {
    match parts {
        Some(parts) => parts.coordination.clone() as Arc<dyn QueueOwnership>,
        None => Arc::new(OwnAllQueues),
    }
}

/// Static single-node coordination view for admin topology in standalone mode.
pub fn single_node_admin_coordination(config: &ServerConfig) -> Arc<dyn Coordination> {
    Arc::new(StaticCoordination::single_node(
        config.coordination.node_id.clone(),
        config.broker.listener.bind,
    ))
}

/// Register local on-disk queues in the cluster catalogue on a retrying loop.
pub fn spawn_ganglion_catalogue_sync(
    parts: &TcpGanglionParts,
    engine: StromaEngine,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    parts.coordination.spawn_catalogue_sync(
        move || {
            let engine = engine.clone();
            async move {
                match engine.queue_stats_snapshot().await {
                    Ok(snapshot) => snapshot
                        .queues
                        .keys()
                        .map(|key| {
                            // Queue stats key by (topic, group) only.
                            // Partition 0 always exists; declare registers
                            // the full partition set.
                            QueueIdentity::new(
                                key.topic.clone(),
                                Partition::ZERO,
                                key.group.as_deref(),
                            )
                        })
                        .collect(),
                    Err(_) => Vec::new(),
                }
            }
        },
        interval,
    )
}

/// Replicate cluster runtime settings into the local runtime manager.
pub fn spawn_ganglion_runtime_settings_sync(
    parts: &TcpGanglionParts,
    runtime_settings: Arc<RuntimeSettingsManager>,
) -> tokio::task::JoinHandle<()> {
    parts
        .coordination
        .spawn_runtime_settings_sync(runtime_settings)
}

/// Build advisory heartbeat labels from local broker state.
pub fn broker_heartbeat_labels(broker: &Broker<StromaEngine>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    for worker in broker
        .sparse_queue_observability_report()
        .replication_followers
    {
        if let Some(state) = worker.state {
            let queue = QueueIdentity::new(worker.topic, worker.partition, worker.group.as_deref());
            labels.insert(
                fibril_coordination_ganglion::applied_tail_label(&queue),
                format!("{}:{}", state.message_next_offset, state.event_next_offset),
            );
        }
    }

    let cohorts = broker.local_cohort_membership();
    if !cohorts.is_empty() {
        labels.insert(
            fibril_coordination_ganglion::COHORT_MEMBERSHIP_LABEL.to_string(),
            fibril_coordination_ganglion::encode_cohort_membership(&cohorts),
        );
    }

    let drained = broker.repartition_drained_reports();
    if !drained.is_empty() {
        let reports: Vec<fibril_coordination_ganglion::RepartitionDrainedReport> = drained
            .into_iter()
            .map(
                |status| fibril_coordination_ganglion::RepartitionDrainedReport {
                    topic: status.topic,
                    group: status.group,
                    version: status.version,
                    drained: status.drained,
                },
            )
            .collect();
        labels.insert(
            fibril_coordination_ganglion::REPARTITION_DRAINED_LABEL.to_string(),
            fibril_coordination_ganglion::encode_repartition_drained(&reports),
        );
    }
    labels
}

/// Wire the broker-side Ganglion tasks: assignment watcher, heartbeat labels,
/// cohort controller, and owner-side cohort plan application.
pub fn spawn_ganglion_broker_tasks(
    parts: &TcpGanglionParts,
    broker: Arc<Broker<StromaEngine>>,
    config: &ServerConfig,
    runtime: &RuntimeSettings,
) -> GanglionBrokerTaskHandles {
    let resolver = Arc::new(
        fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::with_config(
            parts.coordination.clone(),
            fibril_protocol::v1::replication::ProtocolOwnerPeerResolverConfig::new(
                std::collections::HashMap::new(),
            )
            .with_reporter(config.coordination.node_id.clone())
            .with_auth("fibril", "fibril"),
        ),
    );
    broker.spawn_assignment_watcher_with_follower_replication(
        parts.coordination.clone(),
        resolver,
        FollowerReplicationWorkerConfig {
            allow_checkpoint_install: true,
            follow_runtime_settings: true,
            stream_enabled: false,
            ..Default::default()
        },
    );

    let tails_broker = broker.clone();
    let heartbeat = parts.coordination.spawn_heartbeat_with_labels(
        NodeInfo {
            node_id: config.coordination.node_id.clone(),
            broker_addr: config.broker.listener.bind,
            admin_addr: Some(config.admin.listener.bind),
        },
        Duration::from_millis(config.coordination.ganglion.heartbeat_interval_ms),
        move || broker_heartbeat_labels(&tails_broker),
    );

    let coordination = parts.coordination.clone();
    let default_target = runtime.consumer_groups.default_target_per_consumer;
    let tick_ms = config.coordination.ganglion.heartbeat_interval_ms;
    let cohort_controller = tokio::spawn(async move {
        let mut controller =
            ClusterCohortController::new(Arc::new(StickyConsumerGroupAssignor), default_target);
        loop {
            tokio::time::sleep(Duration::from_millis(tick_ms)).await;
            let count_of = |key: &ConsumerGroupKey| {
                coordination
                    .queue_partitioning(&key.topic, key.group.as_deref())
                    .map(|partitioning| partitioning.partition_count)
                    .unwrap_or(1)
            };
            if let Err(error) = coordination
                .run_cohort_controller_tick(&mut controller, count_of)
                .await
            {
                tracing::debug!(%error, "cohort controller tick deferred; will retry");
            }
        }
    });

    let coordination = parts.coordination.clone();
    let watch_broker = broker.clone();
    let mut snapshot = coordination.watch();
    let cohort_owner_watcher = tokio::spawn(async move {
        loop {
            for cohort in watch_broker.local_cohort_membership() {
                let key = ConsumerGroupKey::new(
                    cohort.topic.clone(),
                    cohort.group.as_deref(),
                    cohort.consumer_group.clone(),
                );
                if let Some(doc) = coordination.cohort_assignment_doc(&key) {
                    let plan: std::collections::HashMap<_, _> = doc
                        .entries
                        .into_iter()
                        .map(|entry| (entry.partition, entry.member))
                        .collect();
                    if !plan.is_empty() {
                        watch_broker.apply_exclusive_assignment(
                            &cohort.topic,
                            cohort.group.as_deref(),
                            &cohort.consumer_group,
                            doc.generation,
                            plan,
                        );
                    }
                }
            }
            if snapshot.changed().await.is_err() {
                break;
            }
        }
    });

    // Repartition watcher: drives the per-partition drain gate during a grow.
    // Runs on a timer (drain progresses as messages are acked, which is not a
    // coordination event), holding new partitions until their source old
    // partition drains, reporting this owner's drained partitions, and (as
    // leader) clearing the transition marker once every old partition has drained.
    let coordination = parts.coordination.clone();
    let repartition_broker = broker.clone();
    let repartition_tick_ms = config.coordination.ganglion.heartbeat_interval_ms;
    let repartition_watcher = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(repartition_tick_ms)).await;
            let active = coordination.active_repartition_transitions();
            // Drop local state for any grow whose marker is gone (completed).
            let active_keys: std::collections::HashSet<(String, Option<String>)> = active
                .iter()
                .map(|(topic, group, _)| (topic.clone(), group.clone()))
                .collect();
            for (topic, group) in repartition_broker.active_repartition_queues() {
                if !active_keys.contains(&(topic.clone(), group.clone())) {
                    repartition_broker.clear_repartition_transition(&topic, group.as_deref());
                }
            }
            for (topic, group, doc) in active {
                repartition_broker.apply_repartition_transition(
                    &topic,
                    group.as_deref(),
                    doc.version,
                    doc.n_old,
                    doc.n_new,
                );
                repartition_broker
                    .refresh_repartition_drain(&topic, group.as_deref())
                    .await;
                let drained =
                    coordination.global_repartition_drained(&topic, group.as_deref(), doc.version);
                repartition_broker.apply_repartition_drained(
                    &topic,
                    group.as_deref(),
                    drained.clone(),
                );
                // Once every old partition has drained cluster-wide, the shrink's
                // merged-away partitions (>= n_new) can be retired.
                let complete = (0..doc.n_old).all(|r| drained.contains(&r));
                if complete {
                    let is_leader = coordination.is_leader().await;
                    if doc.n_new < doc.n_old {
                        // Still-retired fence: only touch indices that are still
                        // outside the live partition count. If a later grow has
                        // already brought an index back, partition_count covers it
                        // again and we must not destroy the fresh incarnation.
                        // Retire runs only while this shrink's marker is active
                        // (we are iterating it), so a recreate cannot have started
                        // for a still-retired index.
                        let live_count = coordination
                            .queue_partitioning(&topic, group.as_deref())
                            .map(|partitioning| partitioning.partition_count)
                            .unwrap_or(doc.n_new);
                        for p in doc.n_new..doc.n_old {
                            if p < live_count {
                                // A grow re-added this index; leave it alone.
                                continue;
                            }
                            // The leader removes it from the catalogue (idempotent)
                            // so it is no longer assigned or routed to.
                            if is_leader {
                                let queue = QueueIdentity::new(
                                    topic.clone(),
                                    Partition::new(p),
                                    group.as_deref(),
                                );
                                let _ = coordination.deregister_queue(&queue).await;
                            }
                            // Every node frees its own on-disk storage for the
                            // retired index. No-op where this node holds nothing.
                            let _ = repartition_broker
                                .retire_partition(&topic, p, group.as_deref())
                                .await;
                        }
                    }
                    // The leader clears the marker last, ending the transition.
                    if is_leader {
                        let _ = coordination
                            .clear_repartition_transition(&topic, group.as_deref())
                            .await;
                    }
                }
            }
        }
    });

    GanglionBrokerTaskHandles {
        heartbeat,
        cohort_controller,
        cohort_owner_watcher,
        repartition_watcher,
    }
}

/// Protocol topology source for Ganglion-backed routing.
pub fn topology_source_for_ganglion(parts: &TcpGanglionParts) -> Arc<dyn ClientTopologySource> {
    let coordination = parts.coordination.clone();
    Arc::new(CoordinationTopologySource {
        fetch: Arc::new(move || coordination.client_topology()),
    }) as Arc<dyn ClientTopologySource>
}

/// Protocol declare coordinator for Ganglion-backed partition metadata and
/// catalogue registration.
pub fn declare_coordinator_for_ganglion(
    parts: &TcpGanglionParts,
) -> Arc<dyn QueueDeclareCoordinator> {
    let coordination = parts.coordination.clone();
    Arc::new(CoordinationDeclareCoordinator {
        declare: Arc::new(move |topic, group, count| {
            let coordination = coordination.clone();
            Box::pin(async move {
                let partitioning = coordination
                    .declare_queue_partitioning(&topic, group.as_deref(), count)
                    .await
                    .map_err(|error| error.to_string())?;
                for partition in 0..partitioning.partition_count {
                    let queue = QueueIdentity::new(
                        topic.clone(),
                        Partition::new(partition),
                        group.as_deref(),
                    );
                    coordination
                        .register_queue(&queue)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                Ok(partitioning.partition_count)
            })
        }),
    }) as Arc<dyn QueueDeclareCoordinator>
}

/// Run a Fibril broker/admin server from an already loaded config.
///
/// The binary is intentionally a thin wrapper around this function. Keeping the
/// server composition here lets integration tests reuse production wiring
/// without shelling out to `fibril-server`.
pub async fn run_server_from_config(config: ServerConfig) -> Result<(), FibrilServerError> {
    let metrics = Metrics::new(3 * 60 * 60);
    let keratin_default = KeratinConfig::default();
    let keratin_message_cfg = KeratinConfig {
        fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
        batch_linger_ms: config.storage.keratin.batch_linger_ms,
        segment_max_bytes: config.storage.keratin.message_log.segment_max_bytes,
        ..keratin_default
    };
    let keratin_event_cfg = KeratinConfig {
        fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
        batch_linger_ms: config.storage.keratin.batch_linger_ms,
        segment_max_bytes: config.storage.keratin.event_log.segment_max_bytes,
        flush_target_bytes: keratin_default.flush_target_bytes / 8,
        max_batch_bytes: keratin_default.max_batch_bytes / 8,
        index_stride_bytes: keratin_default.index_stride_bytes / 8,
        ..keratin_default
    };
    let engine = StromaEngine::open(
        &config.server.data_dir,
        StromaKeratinConfig {
            message_log: keratin_message_cfg,
            event_log: keratin_event_cfg,
        },
        SnapshotConfig::default(),
    )
    .await
    .map_err(FibrilServerError::Storage)?;

    let runtime_seed = runtime_seed_from_config(&config);
    let runtime_settings = Arc::new(
        RuntimeSettingsManager::load_from_stroma_engine(
            &engine,
            runtime_seed,
            RuntimeSettingsLocks {
                idle_queue_cleanup: config.runtime_locks.idle_queue_cleanup,
            },
        )
        .await?,
    );
    let runtime_snapshot = runtime_settings.current();
    let runtime = &runtime_snapshot.settings;
    let broker_cfg = BrokerConfig::from_runtime_settings(runtime);

    let ganglion_parts = open_tcp_ganglion_parts(&config).await?;
    if let Some(parts) = &ganglion_parts {
        spawn_ganglion_catalogue_sync(
            parts,
            engine.clone(),
            Duration::from_millis(config.coordination.ganglion.heartbeat_interval_ms),
        );
        spawn_ganglion_runtime_settings_sync(parts, runtime_settings.clone());
    }

    let ownership = queue_ownership_for_ganglion(ganglion_parts.as_ref());
    let broker = Broker::new_with_ownership(
        engine.clone(),
        broker_cfg,
        Some(metrics.broker()),
        ownership,
    );

    let _ganglion_broker_tasks = ganglion_parts
        .as_ref()
        .map(|parts| spawn_ganglion_broker_tasks(parts, broker.clone(), &config, runtime));

    let connection_settings = ConnectionSettings::new(None)
        .with_publisher_cache_idle_timeout_ms(runtime.idle_queue_cleanup.publisher_idle_timeout_ms)
        .with_reconnect_grace_ms(runtime.connection.reconnect_grace_ms);
    {
        let broker = broker.clone();
        let connection_settings = connection_settings.clone();
        let mut runtime_updates = runtime_settings.subscribe();
        tokio::spawn(async move {
            while runtime_updates.changed().await.is_ok() {
                let snapshot = runtime_updates.borrow().clone();
                broker.update_config(BrokerConfig::from_runtime_settings(&snapshot.settings));
                connection_settings.update_runtime(ProtocolConnectionRuntimeSettings {
                    publisher_cache_idle_timeout_ms: snapshot
                        .settings
                        .idle_queue_cleanup
                        .publisher_idle_timeout_ms,
                    reconnect_grace_ms: snapshot.settings.connection.reconnect_grace_ms,
                });
            }
        });
    }

    let auth_handler = StaticAuthHandler::new("fibril".to_string(), "fibril".to_string());
    let admin_auth_handler = if config.admin.auth.enabled {
        let password = config
            .admin
            .auth
            .password
            .clone()
            .ok_or(FibrilServerError::MissingAdminPassword)?;
        Some(StaticAuthHandler::new(
            config.admin.auth.username.clone(),
            password,
        ))
    } else {
        None
    };

    let stroma_metrics = broker.stroma_metrics();
    let topology_source = ganglion_parts.as_ref().map(topology_source_for_ganglion);
    let declare_coordinator = ganglion_parts
        .as_ref()
        .map(declare_coordinator_for_ganglion);

    let broker_observability = {
        let broker = broker.clone();
        Arc::new(move || {
            serde_json::to_value(broker.sparse_queue_observability_report()).unwrap_or_default()
        })
    };

    let admin = AdminServer::new(
        metrics.clone(),
        stroma_metrics,
        AdminConfig {
            bind: config.admin.listener.bind.to_string(),
            auth: admin_auth_handler,
        },
        Some(StartupConfigSummary {
            data_dir: config.server.data_dir.display().to_string(),
            broker_bind: config.broker.listener.bind.to_string(),
            admin_bind: config.admin.listener.bind.to_string(),
            admin_auth_enabled: config.admin.auth.enabled,
            keratin_fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
            keratin_message_log_segment_max_bytes: config
                .storage
                .keratin
                .message_log
                .segment_max_bytes,
            keratin_event_log_segment_max_bytes: config.storage.keratin.event_log.segment_max_bytes,
            coordination_heartbeat_interval_ms: config.coordination.ganglion.heartbeat_interval_ms,
            coordination_liveness_ttl_ms: config.coordination.ganglion.liveness_ttl_ms,
        }),
        Arc::new(engine.clone()),
        Some(broker_observability),
        Some(runtime_settings.clone()),
    );

    // The coordination listener handle must outlive the server futures.
    let mut _consensus_server = None;
    let admin = match ganglion_parts {
        None => admin.with_coordination(single_node_admin_coordination(&config)),
        Some(parts) => {
            _consensus_server = Some(parts.consensus_server.clone());
            let topology_source = parts.coordination.clone();
            let consensus_server = parts.consensus_server;
            let controller_status = parts.controller_status;
            admin
                .with_coordination(parts.coordination.clone())
                .with_consensus_topology(Arc::new(move || {
                    let mut value = serde_json::to_value(topology_source.raft_node().topology())
                        .unwrap_or(serde_json::Value::Null);
                    if let Some(object) = value.as_object_mut() {
                        object.insert(
                            "healthy".into(),
                            serde_json::Value::Bool(topology_source.coordination_healthy()),
                        );
                        object.insert(
                            "listener_serving".into(),
                            serde_json::Value::Bool(consensus_server.is_serving()),
                        );
                        if let Ok(status) = controller_status.read() {
                            object.insert(
                                "controller".into(),
                                serde_json::to_value(&*status).unwrap_or(serde_json::Value::Null),
                            );
                        }
                    }
                    value
                }))
                .with_coordination_membership(Arc::new(GanglionCoordinationMembershipManager::new(
                    parts.coordination.clone(),
                )))
                .with_queue_repartition(Arc::new(GanglionQueueRepartitionManager::new(
                    parts.coordination.clone(),
                )))
                .with_runtime_settings_cluster(Arc::new(GanglionRuntimeSettingsStore::new(
                    parts.coordination,
                )))
        }
    };

    let tcp_metrics = metrics.tcp();
    let connection_metrics = metrics.connections();
    metrics.start(
        MetricsConfig {
            log_broker: true,
            log_storage: true,
            log_tcp: true,
        },
        Duration::from_secs(10),
    );

    let broker_server_fut = async move {
        run_protocol_server(
            config.broker.listener.bind,
            broker.clone(),
            tcp_metrics,
            connection_metrics,
            Some(auth_handler.clone()),
            connection_settings,
            topology_source,
            declare_coordinator,
        )
        .await
        .map_err(FibrilServerError::BrokerListener)
    };
    let admin_server_fut =
        async move { admin.run().await.map_err(FibrilServerError::AdminListener) };

    tokio::try_join!(broker_server_fut, admin_server_fut)?;

    Ok(())
}

pub struct GanglionRuntimeSettingsStore<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    coordination: Arc<GanglionCoordination<LS, NF>>,
}

impl<LS, NF> GanglionRuntimeSettingsStore<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    pub fn new(coordination: Arc<GanglionCoordination<LS, NF>>) -> Self {
        Self { coordination }
    }
}

#[async_trait]
impl<LS, NF> RuntimeSettingsClusterStore for GanglionRuntimeSettingsStore<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig> + 'static,
    NF: RaftNetworkFactory<GanglionRaftConfig> + 'static,
{
    async fn current_runtime_settings(&self) -> Result<Option<RuntimeSettingsSnapshot>, String> {
        self.coordination
            .runtime_settings_document()
            .map(|document| {
                document.map(|document| RuntimeSettingsSnapshot {
                    version: document.cluster_version,
                    settings: document.settings,
                })
            })
            .map_err(|error| error.to_string())
    }

    async fn update_runtime_settings(
        &self,
        expected_version: u64,
        settings: RuntimeSettings,
    ) -> Result<RuntimeSettingsClusterUpdateOutcome, String> {
        match self
            .coordination
            .update_runtime_settings(expected_version, &settings)
            .await
            .map_err(|error| error.to_string())?
        {
            ClusterRuntimeSettingsUpdateOutcome::Stored(document) => Ok(
                RuntimeSettingsClusterUpdateOutcome::Stored(RuntimeSettingsSnapshot {
                    version: document.cluster_version,
                    settings: document.settings,
                }),
            ),
            ClusterRuntimeSettingsUpdateOutcome::Conflict(Some(document)) => Ok(
                RuntimeSettingsClusterUpdateOutcome::Conflict(RuntimeSettingsSnapshot {
                    version: document.cluster_version,
                    settings: document.settings,
                }),
            ),
            ClusterRuntimeSettingsUpdateOutcome::Conflict(None) => Ok(
                RuntimeSettingsClusterUpdateOutcome::Conflict(RuntimeSettingsSnapshot {
                    version: 0,
                    settings,
                }),
            ),
        }
    }
}

/// Bridges the coordination provider's client topology into the protocol
/// handler's `ClientTopologySource`, keeping coordination-ganglion free of a
/// protocol dependency. The closure fetches a fresh snapshot each call.
pub struct CoordinationTopologySource {
    pub fetch: Arc<dyn Fn() -> ClientTopology + Send + Sync>,
}

impl ClientTopologySource for CoordinationTopologySource {
    fn topology(&self) -> TopologyOk {
        let topology = (self.fetch)();
        TopologyOk {
            generation: topology.generation,
            queues: topology
                .queues
                .into_iter()
                .map(|queue| QueueTopologyEntry {
                    topic: queue.topic,
                    partition: queue.partition,
                    group: queue.group,
                    owner_endpoint: queue.owner_endpoint,
                    partitioning_version: queue.partitioning_version,
                    partition_count: queue.partition_count,
                })
                .collect(),
        }
    }

    fn owner_endpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<(String, u64)> {
        (self.fetch)()
            .queues
            .into_iter()
            .find(|queue| {
                queue.topic == topic
                    && queue.partition == partition
                    && queue.group.as_deref() == group
            })
            .and_then(|queue| {
                queue
                    .owner_endpoint
                    .map(|endpoint| (endpoint, queue.partitioning_version))
            })
    }
}

/// Boxed future returned by the declare bridge.
pub type DeclareFut = futures::future::BoxFuture<'static, Result<u32, String>>;

/// Bridges queue-declare partitioning writes to the coordination provider. The
/// boxed-future closure captures the provider, avoiding naming its generic type
/// and keeping coordination-ganglion free of a protocol dependency.
pub struct CoordinationDeclareCoordinator {
    pub declare: Arc<dyn Fn(String, Option<String>, u32) -> DeclareFut + Send + Sync>,
}

impl QueueDeclareCoordinator for CoordinationDeclareCoordinator {
    fn declare_partitioning<'a>(
        &'a self,
        topic: &'a str,
        group: Option<&'a str>,
        partition_count: u32,
    ) -> futures::future::BoxFuture<'a, Result<u32, String>> {
        (self.declare)(
            topic.to_string(),
            group.map(str::to_string),
            partition_count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_seed_maps_config_defaults() {
        let config = ServerConfig::default();
        let seed = runtime_seed_from_config(&config);

        // Spot-check each section maps through (this mapping has regressed
        // before when fields were added).
        assert_eq!(
            seed.delivery.inflight_ttl_ms,
            config.runtime_seed.delivery.inflight_ttl_ms
        );
        assert_eq!(
            seed.replication.confirm_timeout_ms,
            config.runtime_seed.replication.confirm_timeout_ms
        );
        assert_eq!(
            seed.replication.min_in_sync_replicas,
            config.runtime_seed.replication.min_in_sync_replicas
        );
        assert_eq!(
            seed.partitioning.default_partition_count,
            config.runtime_seed.partitioning.default_partition_count
        );
        assert_eq!(
            seed.idle_queue_cleanup.sweep_interval_ms,
            config.runtime_seed.idle_queue_cleanup.sweep_interval_ms
        );
    }

    #[test]
    fn runtime_seed_carries_non_default_values() {
        let mut config = ServerConfig::default();
        config.runtime_seed.partitioning.default_partition_count = 7;
        config.runtime_seed.replication.min_in_sync_replicas = 3;
        let seed = runtime_seed_from_config(&config);
        assert_eq!(seed.partitioning.default_partition_count, 7);
        assert_eq!(seed.replication.min_in_sync_replicas, 3);
    }

    fn temp_root(tag: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "fibril-{tag}-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&root).expect("temp root");
        root
    }

    fn free_loopback_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
        listener.local_addr().expect("local addr")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_startup_fails_fast_when_admin_bind_fails() {
        let root = temp_root("admin-bind-fails-fast");
        let occupied_admin =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind occupied admin port");
        let occupied_admin_addr = occupied_admin.local_addr().expect("occupied admin addr");

        let mut config = ServerConfig::default();
        config.server.data_dir = root.join("data");
        config.broker.listener.bind = free_loopback_addr();
        config.admin.listener.bind = occupied_admin_addr;

        let result = tokio::time::timeout(Duration::from_secs(5), run_server_from_config(config))
            .await
            .expect("startup should fail instead of serving broker forever")
            .expect_err("occupied admin port should fail startup");

        let error = format!("{result:#}");
        assert!(error.contains("admin listener failed"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_ganglion_bootstrap_exposes_declare_coordinator() {
        let root = temp_root("tcp-ganglion-bootstrap");
        let raft_addr = free_loopback_addr();
        let mut config = ServerConfig::default();
        config.server.data_dir = root.join("data");
        config.coordination.mode = CoordinationMode::Ganglion;
        config.coordination.node_id = "broker-a".into();
        config.coordination.ganglion.raft_node_id = 1;
        config.coordination.ganglion.listen = raft_addr;
        config
            .coordination
            .ganglion
            .peers
            .insert("1".into(), raft_addr.to_string());
        config.coordination.ganglion.bootstrap = true;
        config.coordination.ganglion.data_dir = root.join("coordination");
        config.coordination.ganglion.heartbeat_interval_ms = 50;
        config.coordination.ganglion.controller_tick_ms = 50;
        config.coordination.ganglion.liveness_ttl_ms = 200;

        let parts = open_tcp_ganglion_parts(&config)
            .await
            .expect("start ganglion")
            .expect("ganglion parts");
        parts
            .coordination
            .raft_node()
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("leader");

        let declare = declare_coordinator_for_ganglion(&parts);
        let count = declare
            .declare_partitioning("jobs", None, 2)
            .await
            .expect("declare partitioning");
        assert_eq!(count, 2);

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let queues = parts.coordination.registered_queues();
                let has_zero = queues.contains(&QueueIdentity::new("jobs", Partition::ZERO, None));
                let has_one = queues.contains(&QueueIdentity::new("jobs", Partition::new(1), None));
                if has_zero && has_one {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("catalogue registrations");

        parts.controller_task.abort();
        parts
            .coordination
            .raft_node()
            .shutdown()
            .await
            .expect("shutdown");
    }
}
