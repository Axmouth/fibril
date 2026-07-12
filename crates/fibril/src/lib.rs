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
    AdminConfig, AdminServer, AdminServerError, AdminUserInfo, CoordinationMembershipManager,
    RuntimeSettingsClusterStore, RuntimeSettingsClusterUpdateOutcome, StartupConfigSummary,
    UserAdmin,
};
use fibril_broker::{
    auth_store::{NODE_PRINCIPAL, StoreAuthHandler, UserStoreError, UserStoreManager},
    broker::{
        Broker, BrokerConfig, FollowerReplicationWorkerConfig, OwnAllQueues, OwnAllStreams,
        QueueOwnership, StreamOwnership,
    },
    coordination::{
        ClusterCohortController, ConsumerGroupKey, Coordination, DeterministicPartitionPlacement,
        NodeInfo, QueueIdentity, ReplicationDurabilityPolicy, StaticCoordination,
        StickyConsumerGroupAssignor, StreamIdentity,
    },
    queue_engine::{
        KeratinConfig, MessageContentType, MessageHeaders, QueueEngine as _,
        RecoveryMismatchPolicy, SnapshotConfig, StromaEngine, StromaError, StromaKeratinConfig,
    },
    runtime_settings::{
        ConnectionRuntimeSettings as BrokerConnectionRuntimeSettings, ConsumerGroupRuntimeSettings,
        DeliveryRuntimeSettings, IdleQueueCleanupRuntimeSettings, PartitioningRuntimeSettings,
        ReplicationRuntimeSettings, RuntimeSettings, RuntimeSettingsError, RuntimeSettingsLocks,
        RuntimeSettingsManager, RuntimeSettingsSnapshot, StreamRuntimeSettings,
    },
};
use fibril_config::{
    ConfigError, CoordinationMode, GanglionAssignmentDurabilityMode,
    GanglionAssignmentDurabilitySection, RecoveryMismatchMode, ServerConfig,
};
use fibril_coordination_ganglion::{
    ClientTopology, ClusterRuntimeSettingsUpdateOutcome, ControllerStatus, ForwardedWritePolicy,
    GanglionCoordination, StreamRetentionConfig,
};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::{
    ClientTopologySource, ConnectionRuntimeSettings as ProtocolConnectionRuntimeSettings,
    ConnectionSettings, DeclareCoordinator, ProtocolServerError, TopologyAdoptionTracker,
    run_server as run_protocol_server,
};
use fibril_protocol::v1::{
    AdvertisedAddress, Partition, QueueTopologyEntry, StreamRetention, StreamTopologyEntry,
    TopologyOk,
};
use fibril_util::StaticAuthHandler;
use ganglion::{TcpRaftServer, WireFormat, WireFormatParseError, openraft::BasicNode};

pub use fibril_tls as tls;

pub mod raft_tls;

use fibril_tls::{TlsMaterialSource, TlsSetupError};

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
            drain_handoff_timeout_ms: None,
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
            read_timeout_slack_ms: config.runtime_seed.replication.read_timeout_slack_ms,
            owner_connect_timeout_ms: config.runtime_seed.replication.owner_connect_timeout_ms,
            stream_enabled: config.runtime_seed.replication.stream_enabled,
            stream_apply_linger_us: config.runtime_seed.replication.stream_apply_linger_us,
            stream_apply_max_merge_bytes: config
                .runtime_seed
                .replication
                .stream_apply_max_merge_bytes,
            stream_buffer_batches: config.runtime_seed.replication.stream_buffer_batches,
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
        // Stream knobs (cursor-commit microbatch, idle eviction) are
        // runtime-tunable via the cluster settings document; the static seed
        // uses the defaults.
        stream: StreamRuntimeSettings::default(),
    }
}

/// Production Ganglion coordination provider used by `fibril-server`.
///
/// `GanglionCoordination` is not generic over the log store or network factory
/// (the raft handle is type-erased), so this is a plain alias kept for naming
/// continuity at the server call sites.
pub type TcpGanglionCoordination = GanglionCoordination;

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
    #[error("tls configuration: {0}")]
    TlsConfig(#[source] ConfigError),
    #[error(
        "tls.inter_broker is enabled without server TLS material: the coordination and \
         replication listeners need tls.enabled with a certificate to serve"
    )]
    InterBrokerTlsWithoutMaterial,
    #[error(
        "coordination.mode = ganglion requires a cluster secret for node-to-node \
         authentication. Generate one with fibrilctl secret generate and give every \
         node the same value via FIBRIL_CLUSTER_SECRET, coordination.secret_path, or \
         <data_dir>/cluster.secret"
    )]
    MissingClusterSecret,
    #[error("user store: {0}")]
    UserStore(#[from] UserStoreError),
    #[error(transparent)]
    TlsSetup(#[from] TlsSetupError),
}

/// Drain controller for the admin endpoint: announces a planned restart to
/// connected clients through the shared connection registry.
pub struct ConnectionSettingsDrainController {
    connection_settings: ConnectionSettings,
    handoff: Option<DrainHandoff>,
}

/// Coordinated-mode drain context: the flag the heartbeat labels publish,
/// and the committed assignment view the bounded wait polls.
struct DrainHandoff {
    coordination: Arc<TcpGanglionCoordination>,
    node_id: String,
    draining: Arc<std::sync::atomic::AtomicBool>,
    runtime_settings: tokio::sync::watch::Receiver<RuntimeSettingsSnapshot>,
}

impl ConnectionSettingsDrainController {
    pub fn new(connection_settings: ConnectionSettings) -> Self {
        Self {
            connection_settings,
            handoff: None,
        }
    }

    /// Enable the coordinated handoff: mark this node draining in its
    /// heartbeat labels and wait (bounded) for the controller to move its
    /// partitions before the drain call returns.
    pub fn with_handoff(
        mut self,
        coordination: Arc<TcpGanglionCoordination>,
        node_id: String,
        draining: Arc<std::sync::atomic::AtomicBool>,
        runtime_settings: tokio::sync::watch::Receiver<RuntimeSettingsSnapshot>,
    ) -> Self {
        self.handoff = Some(DrainHandoff {
            coordination,
            node_id,
            draining,
            runtime_settings,
        });
        self
    }
}

/// Partitions (queues and streams) `node_id` owns in the committed snapshot.
fn owned_partition_count(
    snapshot: &fibril_broker::coordination::CoordinationSnapshot,
    node_id: &str,
) -> usize {
    snapshot
        .assignments
        .values()
        .filter(|assignment| assignment.is_owned_by(node_id))
        .count()
        + snapshot
            .stream_assignments
            .values()
            .filter(|assignment| assignment.owner == node_id)
            .count()
}

/// Whether the drain wait is over: everything moved, or the bounded wait is
/// spent. Pure so the bound is unit-testable.
pub fn drain_handoff_complete(
    owned_remaining: usize,
    elapsed: Duration,
    timeout: Duration,
) -> bool {
    owned_remaining == 0 || elapsed >= timeout
}

#[async_trait]
impl fibril_admin::BrokerDrainController for ConnectionSettingsDrainController {
    async fn announce_drain(&self, grace_ms: u64, message: String) -> fibril_admin::DrainOutcome {
        let connections_notified = self
            .connection_settings
            .announce_going_away(grace_ms, &message)
            .await;

        // Standalone brokers have nowhere to move ownership: announce only,
        // exactly the pre-handoff behavior.
        let Some(handoff) = &self.handoff else {
            return fibril_admin::DrainOutcome {
                connections_notified,
                owned_partitions_remaining: 0,
                handoff_complete: false,
                handoff_waited_ms: 0,
            };
        };

        handoff
            .draining
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let timeout = Duration::from_millis(
            handoff
                .runtime_settings
                .borrow()
                .settings
                .connection
                .drain_handoff_timeout_ms
                .unwrap_or(fibril_broker::runtime_settings::DEFAULT_DRAIN_HANDOFF_TIMEOUT_MS),
        );
        let started = std::time::Instant::now();
        let mut remaining =
            owned_partition_count(&handoff.coordination.snapshot(), &handoff.node_id);
        while !drain_handoff_complete(remaining, started.elapsed(), timeout) {
            tokio::time::sleep(Duration::from_millis(250)).await;
            remaining = owned_partition_count(&handoff.coordination.snapshot(), &handoff.node_id);
            tracing::info!(
                remaining,
                "drain handoff waiting for partition ownership to move"
            );
        }
        fibril_admin::DrainOutcome {
            connections_notified,
            owned_partitions_remaining: remaining,
            handoff_complete: remaining == 0,
            handoff_waited_ms: started.elapsed().as_millis() as u64,
        }
    }
}

/// Admin test publish: one operator message through the broker's real publish
/// path (partition pick, durable confirm, delivery wakeup), so the dashboard
/// can verify a queue end to end without a client.
struct AdminTestPublisher {
    broker: Arc<Broker<StromaEngine>>,
    engine: StromaEngine,
}

impl AdminTestPublisher {
    /// Test-publish into a Plexus stream through the same channel path the
    /// publish handler takes: durability confirm first, then the assignment's
    /// replication policy, exactly like a client publish.
    async fn publish_stream_text(
        &self,
        topic: &str,
        local_partition_count: Option<u32>,
        text: String,
    ) -> Result<fibril_admin::TestPublishOutcome, String> {
        // Without a locally open channel the count is unknown - try partition
        // zero and let the ownership check say who serves this stream.
        let mut partitions: Vec<u32> = (0..local_partition_count.unwrap_or(1).max(1)).collect();
        fastrand::shuffle(&mut partitions);
        let mut last_error = String::new();
        for part in partitions {
            if let Err(e) = self.broker.ensure_stream_owner(topic, part) {
                last_error = e.to_string();
                continue;
            }
            let Some(channel) = self.broker.route_stream(topic, part).await else {
                last_error = format!("stream partition {part} is not open on this broker");
                continue;
            };
            let now = fibril_util::unix_millis();
            let mut extra = std::collections::HashMap::new();
            extra.insert("fibril.test".to_string(), "admin".to_string());
            if channel.durability() == fibril_broker::stream::StreamDurability::Speculative {
                extra.insert(
                    fibril_protocol::v1::HEADER_SPECULATIVE.to_string(),
                    "1".to_string(),
                );
            }
            let headers = MessageHeaders {
                published: now,
                publish_received: now,
                content_type: Some(MessageContentType::Text),
                extra,
            };
            let confirm = channel
                .publish_pipelined(headers, text.clone().into_bytes())
                .await
                .map_err(|e| format!("stream publish failed: {e}"))?;
            let offset = match tokio::time::timeout(Duration::from_secs(10), confirm).await {
                Ok(Ok(Ok(offset))) => offset,
                Ok(Ok(Err(e))) => return Err(format!("stream publish failed: {e}")),
                Ok(Err(_)) => return Err("publish confirmation dropped".to_string()),
                Err(_) => return Err("timed out waiting for the durable confirm".to_string()),
            };
            self.broker
                .await_replication_confirm(
                    topic,
                    fibril_broker::storage::Partition::new(part),
                    None,
                    offset,
                )
                .await
                .map_err(|e| format!("stream replication confirm failed: {e}"))?;
            return Ok(fibril_admin::TestPublishOutcome {
                partition: part,
                kind: "stream".to_string(),
            });
        }
        Err(format!(
            "no publishable stream partition on this broker: {last_error}"
        ))
    }
}

#[async_trait]
impl fibril_admin::BrokerTestPublisher for AdminTestPublisher {
    async fn publish_text(
        &self,
        topic: &str,
        group: Option<&str>,
        text: String,
    ) -> Result<fibril_admin::TestPublishOutcome, String> {
        // Streams resolve first, mirroring the publish handler's routing: a
        // topic declared as a stream must never grow queue state as a side
        // effect of a test message.
        let stream_partitions = self
            .broker
            .stream_partition_counts()
            .into_iter()
            .find(|(tp, _)| tp == topic)
            .map(|(_, count)| count);
        if stream_partitions.is_some() || self.broker.stream_declared_in_coordination(topic) {
            if group.is_some() {
                return Err("streams have no consumer groups - leave group empty".to_string());
            }
            return self.publish_stream_text(topic, stream_partitions, text).await;
        }

        let mut partitions: Vec<u32> = self
            .engine
            .list_partitions()
            .await
            .map_err(|e| format!("could not list partitions: {e}"))?
            .into_iter()
            .filter(|(tp, _, grp)| tp == topic && grp.as_deref() == group)
            .map(|(_, part, _)| part)
            .collect();
        if partitions.is_empty() {
            return Err(format!(
                "no queue or stream named '{topic}' on this broker"
            ));
        }

        // Random starting partition, then try the rest in turn: in cluster mode
        // this node may own only some of them and get_publisher refuses the rest.
        fastrand::shuffle(&mut partitions);
        let group: Option<fibril_broker::storage::Group> = group.map(str::to_string);
        let mut last_error = String::new();
        for part in partitions {
            let handle = match self
                .broker
                .get_publisher(topic, fibril_broker::storage::Partition::new(part), &group)
                .await
            {
                Ok(handle) => handle,
                Err(e) => {
                    last_error = e.to_string();
                    continue;
                }
            };
            let now = fibril_util::unix_millis();
            // Mark the message in the reserved broker header namespace so
            // consumers can recognize operator test traffic. Clients cannot
            // set fibril.* headers themselves, so the marker is trustworthy.
            let mut extra = std::collections::HashMap::new();
            extra.insert("fibril.test".to_string(), "admin".to_string());
            let confirm = handle
                .publish(
                    text.clone().into_bytes(),
                    now,
                    now,
                    Some(MessageContentType::Text),
                    extra,
                    None,
                )
                .await
                .map_err(|e| format!("publish failed: {e}"))?;
            return match tokio::time::timeout(Duration::from_secs(10), confirm).await {
                Ok(Ok(Ok(_))) => Ok(fibril_admin::TestPublishOutcome {
                    partition: part,
                    kind: "queue".to_string(),
                }),
                Ok(Ok(Err(e))) => Err(format!("publish failed: {e}")),
                Ok(Err(_)) => Err("publish confirmation dropped".to_string()),
                Err(_) => Err("timed out waiting for the durable confirm".to_string()),
            };
        }
        Err(format!("no publishable partition on this broker: {last_error}"))
    }
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
        serde_json::to_value(self.coordination.consensus_node().topology())
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl CoordinationMembershipManager for GanglionCoordinationMembershipManager {
    async fn add_voting_member(&self, id: u64, addr: String) -> Result<serde_json::Value, String> {
        self.coordination
            .consensus_node()
            .add_learner(id, BasicNode::new(addr), true)
            .await
            .map_err(|error| error.to_string())?;

        let mut voters = self.coordination.consensus_node().topology().voters;
        if !voters.contains(&id) {
            voters.push(id);
            voters.sort_unstable();
        }

        self.coordination
            .consensus_node()
            .change_membership(voters, false)
            .await
            .map_err(|error| error.to_string())?;

        self.topology_json()
    }

    async fn remove_voting_member(&self, id: u64) -> Result<serde_json::Value, String> {
        let voters = self
            .coordination
            .consensus_node()
            .topology()
            .voters
            .into_iter()
            .filter(|voter| *voter != id)
            .collect::<Vec<_>>();
        if voters.is_empty() {
            return Err("refusing to remove the last coordination voting member".to_string());
        }

        self.coordination
            .consensus_node()
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
/// The TLS connector broker-to-broker dials use: peer trust from the
/// configured peer CA (or the generated CA, then OS roots), presenting this
/// node's own serving certificate as its client identity. Presenting costs
/// nothing when the peer does not request a certificate, so dials stay
/// compatible with peers at any `tls.client_auth` setting.
fn peer_connector_from_config(
    config: &ServerConfig,
) -> Result<fibril_tls::TlsConnector, FibrilServerError> {
    let identity = match config.tls.mode().map_err(FibrilServerError::TlsConfig)? {
        fibril_config::TlsMode::Provided {
            cert_path,
            key_path,
        } => Some((cert_path, key_path)),
        fibril_config::TlsMode::AutoSelfSigned => {
            let dir = config.server.data_dir.join(tls::GENERATED_TLS_DIR);
            Some((dir.join("server.pem"), dir.join("server.key")))
        }
        fibril_config::TlsMode::Disabled => None,
    };
    Ok(fibril_tls::build_peer_connector_with_identity(
        config.tls.peer_ca_path.as_deref(),
        &config.server.data_dir,
        identity
            .as_ref()
            .map(|(cert, key)| (cert.as_path(), key.as_path())),
    )?)
}

/// TLS transport for the embedded coordinator's raft channel: the acceptor
/// serves the broker's certificate on the raft listener, the connector
/// verifies peers against the peer CA.
pub struct RaftTlsTransport {
    pub connector: fibril_tls::TlsConnector,
    pub acceptor: fibril_tls::RustlsAcceptor,
}

pub async fn open_tcp_ganglion_parts(
    config: &ServerConfig,
    raft_tls: Option<RaftTlsTransport>,
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
            let (node, consensus_server) = match raft_tls {
                Some(transport) => {
                    ganglion::RaftMetadataNode::start_durable_tcp_with_transport(
                        section.raft_node_id,
                        consensus_config,
                        section.listen,
                        &data_dir,
                        wire_format,
                        raft_tls::TlsRaftDialer::new(transport.connector),
                        raft_tls::TlsRaftAcceptor::new(transport.acceptor),
                    )
                    .await
                }
                None => {
                    ganglion::RaftMetadataNode::start_durable_tcp_with_format(
                        section.raft_node_id,
                        consensus_config,
                        section.listen,
                        &data_dir,
                        wire_format,
                    )
                    .await
                }
            }
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
                    stream_replication_factor: section.stream_replication_factor,
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
) -> Result<Arc<ganglion::openraft::Config>, FibrilServerError> {
    let section = &config.coordination.ganglion.raft;
    let mut consensus_config = ganglion::default_raft_config()
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

/// Stream-ownership provider used by the broker: cluster assignments in Ganglion
/// mode (the same coordination provider as queues), own-all standalone.
pub fn stream_ownership_for_ganglion(parts: Option<&TcpGanglionParts>) -> Arc<dyn StreamOwnership> {
    match parts {
        Some(parts) => parts.coordination.clone() as Arc<dyn StreamOwnership>,
        None => Arc::new(OwnAllStreams),
    }
}

/// Static single-node coordination view for admin topology in standalone mode.
pub fn single_node_admin_coordination(config: &ServerConfig) -> Arc<dyn Coordination> {
    Arc::new(StaticCoordination::single_node(
        config.coordination.node_id.clone(),
        config
            .broker_advertise_addresses()
            .into_iter()
            .next()
            .unwrap_or_else(|| config.broker.listener.bind.to_string()),
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

/// Whether a drained repartition transition may finalize (retire shrunk-away
/// partitions and clear the marker). Proceeds once the fleet has adopted the new
/// routing - the cluster-wide acked topology generation has reached the
/// transition's `adoption_generation` - or the adoption wait has timed out. A
/// transition with no `adoption_generation` predates adoption fencing and is not
/// gated. Publish version-fencing is the correctness backstop, so the timeout is a
/// safe upper bound. Pure so the gate decision is unit tested directly.
pub fn repartition_adoption_satisfied(
    adoption_generation: Option<u64>,
    global_adoption: Option<u64>,
    elapsed: Duration,
    timeout: Duration,
) -> bool {
    let adopted = match adoption_generation {
        Some(target) => global_adoption.is_some_and(|acked| acked >= target),
        None => true,
    };
    adopted || elapsed >= timeout
}

/// Turns the monotonic publish/deliver counters into per-second rates
/// between heartbeat ticks, so the heartbeat can carry coarse load labels.
/// The first tick has no baseline and reports zero.
pub struct HeartbeatRateTracker {
    prev: Option<(std::time::Instant, u64, u64)>,
}

impl HeartbeatRateTracker {
    pub fn new() -> Self {
        Self { prev: None }
    }

    pub fn tick(&mut self, published: u64, delivered: u64) -> (u64, u64) {
        let now = std::time::Instant::now();
        let rates = match self.prev {
            Some((at, prev_pub, prev_dlv)) => {
                let secs = now.duration_since(at).as_secs_f64().max(0.001);
                (
                    (published.saturating_sub(prev_pub) as f64 / secs).round() as u64,
                    (delivered.saturating_sub(prev_dlv) as f64 / secs).round() as u64,
                )
            }
            None => (0, 0),
        };
        self.prev = Some((now, published, delivered));
        rates
    }
}

impl Default for HeartbeatRateTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Build advisory heartbeat labels from local broker state. `topology_adoption`,
/// when present, contributes the node's lowest acked topology generation so the
/// repartition controller can fence a cutover on cluster-wide client adoption.
/// `rates` is this node's (published/s, delivered/s) since its previous beat.
pub fn broker_heartbeat_labels(
    broker: &Broker<StromaEngine>,
    topology_adoption: Option<&TopologyAdoptionTracker>,
    advertise: &[String],
    draining: bool,
    raft_node_id: u64,
    rates: (u64, u64),
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(
        fibril_coordination_ganglion::RAFT_ID_LABEL.to_string(),
        raft_node_id.to_string(),
    );
    labels.insert(
        fibril_coordination_ganglion::RATE_PUB_LABEL.to_string(),
        rates.0.to_string(),
    );
    labels.insert(
        fibril_coordination_ganglion::RATE_DLV_LABEL.to_string(),
        rates.1.to_string(),
    );
    if draining {
        labels.insert(
            fibril_coordination_ganglion::DRAINING_LABEL.to_string(),
            "1".to_string(),
        );
    }

    // The broker's full advertise list, so clients learn every reachable endpoint
    // of an owner (not just the single endpoint registered in the node table).
    if !advertise.is_empty() {
        labels.insert(
            fibril_coordination_ganglion::ADVERTISE_LABEL.to_string(),
            fibril_coordination_ganglion::encode_advertise(advertise),
        );
    }
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

    // Report the lowest topology generation this node's clients have acked, so the
    // repartition controller can fence a cutover on cluster-wide adoption. Absent
    // when no connected client has acked (no signal -> the controller leans on the
    // adoption timeout).
    if let Some(min_acked) = topology_adoption.and_then(|a| a.min_acked_generation()) {
        labels.insert(
            fibril_coordination_ganglion::TOPOLOGY_ADOPTION_LABEL.to_string(),
            min_acked.to_string(),
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
    topology_adoption: Option<Arc<TopologyAdoptionTracker>>,
    cluster_secret: &str,
    peer_tls: Option<fibril_tls::TlsConnector>,
    draining: Arc<std::sync::atomic::AtomicBool>,
    broker_stats: Arc<fibril_metrics::BrokerStats>,
) -> GanglionBrokerTaskHandles {
    let mut resolver_cfg = fibril_protocol::v1::replication::ProtocolOwnerPeerResolverConfig::new(
        std::collections::HashMap::new(),
    )
    .with_reporter(config.coordination.node_id.clone())
    .with_auth(NODE_PRINCIPAL, cluster_secret.to_string())
    .with_timeouts(
        runtime.replication.read_timeout_slack_ms,
        runtime.replication.owner_connect_timeout_ms,
    );
    if let Some(connector) = peer_tls {
        resolver_cfg = resolver_cfg.with_tls(connector);
    }
    let resolver = Arc::new(
        fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::with_config(
            parts.coordination.clone(),
            resolver_cfg,
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
    let labels_adoption = topology_adoption.clone();
    // Advertise the configured/derived addresses (routable host:port, possibly
    // service names) so peers and clients can dial back even when bind is 0.0.0.0.
    // The node table holds the primary (first) endpoint; the full priority list
    // rides a heartbeat label so clients can try them in order. Fall back to the
    // bind address when nothing else is known.
    let advertise_list = config.broker_advertise_addresses();
    let advertise_primary = advertise_list
        .first()
        .cloned()
        .unwrap_or_else(|| config.broker.listener.bind.to_string());
    let heartbeat_advertise = advertise_list.clone();
    let heartbeat_raft_id = config.coordination.ganglion.raft_node_id;
    // Cold path (one lock per heartbeat), so a Mutex around the rate
    // baseline is fine under the Fn bound.
    let rate_tracker = std::sync::Mutex::new(HeartbeatRateTracker::new());
    let heartbeat = parts.coordination.spawn_heartbeat_with_labels(
        NodeInfo {
            node_id: config.coordination.node_id.clone(),
            broker_addr: advertise_primary,
            admin_addr: Some(config.admin.listener.bind),
        },
        Duration::from_millis(config.coordination.ganglion.heartbeat_interval_ms),
        move || {
            let rates = match rate_tracker.lock() {
                Ok(mut tracker) => tracker.tick(
                    broker_stats
                        .published
                        .total
                        .load(std::sync::atomic::Ordering::Relaxed),
                    broker_stats
                        .delivered
                        .total
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                Err(_) => (0, 0),
            };
            broker_heartbeat_labels(
                &tails_broker,
                labels_adoption.as_deref(),
                &heartbeat_advertise,
                draining.load(std::sync::atomic::Ordering::Relaxed),
                heartbeat_raft_id,
                rates,
            )
        },
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
    let adoption_timeout =
        Duration::from_millis(config.coordination.ganglion.repartition_adoption_timeout_ms);
    // Only live nodes' adoption labels count toward the cutover gate; a dead
    // node's frozen label must not stall it. Same liveness window the controller
    // uses to detect dead brokers.
    let adoption_liveness_ttl = Duration::from_millis(config.coordination.ganglion.liveness_ttl_ms);
    let repartition_watcher = tokio::spawn(async move {
        // When each drained-complete transition first became eligible to finalize,
        // keyed by (topic, group, version). Used to bound the wait for client
        // adoption so a silent or stuck client cannot stall a cutover forever.
        let mut drain_complete_since: std::collections::HashMap<
            (String, Option<String>, u64),
            std::time::Instant,
        > = std::collections::HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_millis(repartition_tick_ms)).await;
            let active = coordination.active_repartition_transitions();
            // Drop local state for any grow whose marker is gone (completed).
            let active_keys: std::collections::HashSet<(String, Option<String>)> = active
                .iter()
                .map(|(topic, group, _)| (topic.clone(), group.clone()))
                .collect();
            // Forget adoption timers for transitions that are no longer active.
            drain_complete_since.retain(|(topic, group, _), _| {
                active_keys.contains(&(topic.clone(), group.clone()))
            });
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
                    // Fence the finalize (retire + clear) on cluster-wide client
                    // adoption of the new routing: proceed once every node's acked
                    // topology generation has caught up to the transition's adoption
                    // generation (stamped at the cutover), or the adoption timeout
                    // elapses (the publish version-fence is the correctness backstop,
                    // so the timeout is safe). A marker with no adoption generation
                    // predates this fencing and is not gated.
                    let timer_key = (topic.clone(), group.clone(), doc.version);
                    let first_complete = *drain_complete_since
                        .entry(timer_key.clone())
                        .or_insert_with(std::time::Instant::now);
                    let global_adoption =
                        coordination.global_topology_adoption(adoption_liveness_ttl);
                    if !repartition_adoption_satisfied(
                        doc.adoption_generation,
                        global_adoption,
                        first_complete.elapsed(),
                        adoption_timeout,
                    ) {
                        // Drained but not yet adopted: wait for clients (or the
                        // timeout) before retiring partitions or clearing the marker.
                        continue;
                    }
                    drain_complete_since.remove(&timer_key);
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
pub fn declare_coordinator_for_ganglion(parts: &TcpGanglionParts) -> Arc<dyn DeclareCoordinator> {
    let coordination = parts.coordination.clone();
    let stream_coordination = parts.coordination.clone();
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
        declare_stream: Arc::new(
            move |topic, count, durability, retention, replication_factor| {
                let coordination = stream_coordination.clone();
                Box::pin(async move {
                    let config = coordination
                        .declare_stream(&topic, count, durability, retention, replication_factor)
                        .await
                        .map_err(|error| error.to_string())?;
                    for partition in 0..config.partition_count {
                        let stream = StreamIdentity::new(topic.clone(), Partition::new(partition));
                        coordination
                            .register_stream(&stream)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    Ok(config.partition_count)
                })
            },
        ),
    }) as Arc<dyn DeclareCoordinator>
}

/// Run a Fibril broker/admin server from an already loaded config.
///
/// The binary is intentionally a thin wrapper around this function. Keeping the
/// server composition here lets integration tests reuse production wiring
/// without shelling out to `fibril-server`.
/// First-boot setup: serve only the localhost setup page until the operator
/// picks a TLS path (generate, supply, or an explicit skip), persist the
/// choice as the config overlay plus material, and record the
/// completed-setup marker. The broker listener stays down throughout, so no
/// plaintext traffic can precede the choice. With tls already configured
/// explicitly there is nothing to decide, so the marker is written directly.
async fn run_first_boot_setup(config: &ServerConfig) -> Result<(), FibrilServerError> {
    let data_dir = config.server.data_dir.clone();
    std::fs::create_dir_all(&data_dir).map_err(|err| {
        FibrilServerError::TlsSetup(fibril_tls::TlsSetupError::WriteMaterial {
            path: data_dir.clone(),
            source: err,
        })
    })?;
    if config.tls != fibril_config::TlsSection::default()
        && config.auth != fibril_config::AuthSection::default()
    {
        write_setup_marker(&data_dir)?;
        tracing::info!(
            "setup mode: tls and auth are already configured explicitly, marking setup complete"
        );
        return Ok(());
    }

    // Loopback only: the supply path uploads a private key, and no
    // credentials exist yet to protect a wider bind.
    let bind = std::net::SocketAddr::from(([127, 0, 0, 1], config.admin.listener.bind.port()));
    tracing::info!(
        "FIRST-BOOT SETUP: the broker is not serving yet. Open http://{bind}/ to choose \
         how connections are secured (generate TLS material, supply a certificate, or \
         continue without TLS). The choice persists in {} and {} under the data dir; \
         delete the marker and boot with setup mode to run this again",
        fibril_config::CONFIG_OVERLAY_FILE,
        fibril_config::SETUP_MARKER_FILE,
    );

    let sans = tls::san_hosts_from_advertise(&config.broker_advertise_addresses());
    let apply_dir = data_dir.clone();
    let apply: fibril_admin::setup::ApplySetup = Arc::new(move |submission| {
        apply_setup_submission(&apply_dir, &sans, submission).map_err(|err| err.to_string())
    });
    let applied = fibril_admin::setup::run_setup_server(bind, apply)
        .await
        .map_err(FibrilServerError::AdminListener)?;
    tracing::info!("first-boot setup complete: {}", applied.summary);
    Ok(())
}

fn apply_setup_submission(
    data_dir: &std::path::Path,
    sans: &[String],
    submission: fibril_admin::setup::SetupSubmission,
) -> Result<fibril_admin::setup::SetupApplied, FibrilServerError> {
    use fibril_admin::setup::{ClusterSecretChoice, SetupApplied, TlsSetupChoice};

    let mut overlay_tls = fibril_config::TlsSection::default();
    let tls_summary = match submission.tls {
        TlsSetupChoice::AutoSelfSigned => {
            let built =
                tls::build_server_tls(&fibril_config::TlsMode::AutoSelfSigned, data_dir, sans)?
                    .ok_or_else(|| {
                        FibrilServerError::TlsSetup(
                            fibril_tls::TlsSetupError::InvalidUploadedMaterial {
                                detail: "generation produced no material".to_string(),
                            },
                        )
                    })?;
            let TlsMaterialSource::Generated {
                dir,
                ca_fingerprint,
            } = built.source
            else {
                return Err(FibrilServerError::TlsSetup(
                    fibril_tls::TlsSetupError::InvalidUploadedMaterial {
                        detail: "generation reported operator-supplied material".to_string(),
                    },
                ));
            };
            overlay_tls.enabled = true;
            overlay_tls.auto_self_signed = true;
            format!(
                "TLS enabled with generated material at {}. CA SHA-256 fingerprint: \
                 {ca_fingerprint}. Clients trust {}/ca.pem or pin the fingerprint.",
                dir.display(),
                dir.display()
            )
        }
        TlsSetupChoice::Provided { cert_pem, key_pem } => {
            let (cert_path, key_path) =
                fibril_tls::store_provided_material(data_dir, &cert_pem, &key_pem)?;
            overlay_tls.enabled = true;
            overlay_tls.cert_path = Some(cert_path.clone());
            overlay_tls.key_path = Some(key_path);
            format!(
                "TLS enabled with the supplied certificate, stored at {}.",
                cert_path.display()
            )
        }
        TlsSetupChoice::SkipTls => {
            "Continuing without TLS by explicit choice. Enable it later via the tls \
             config section or fibrilctl cert generate."
                .to_string()
        }
    };

    let mut summary = vec![tls_summary];

    let overlay_auth = submission.admin_credentials.map(|(username, password)| {
        summary.push(format!("Created admin user \"{username}\"."));
        fibril_config::AuthSection {
            allow_default_loopback: true,
            seed_users: vec![fibril_config::SeedUser { username, password }],
        }
    });

    if let Some(choice) = submission.cluster_secret {
        let secret = match choice {
            ClusterSecretChoice::Generate => generate_cluster_secret(),
            ClusterSecretChoice::Provided(value) => value,
        };
        write_cluster_secret(data_dir, &secret)?;
        summary.push("Cluster secret written to the data dir.".to_string());
    }

    let overlay = fibril_config::ConfigOverlay {
        tls: Some(overlay_tls),
        auth: overlay_auth,
    };
    write_config_overlay(data_dir, &overlay)?;
    write_setup_marker(data_dir)?;
    Ok(SetupApplied {
        summary: summary.join(" "),
    })
}

fn generate_cluster_secret() -> String {
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_mut(8) {
        let value = fastrand::u64(..).to_le_bytes();
        chunk.copy_from_slice(&value[..chunk.len()]);
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn write_cluster_secret(data_dir: &std::path::Path, secret: &str) -> Result<(), FibrilServerError> {
    let path = data_dir.join(fibril_config::CLUSTER_SECRET_FILE);
    std::fs::write(&path, format!("{secret}\n")).map_err(|source| {
        FibrilServerError::TlsSetup(fibril_tls::TlsSetupError::WriteMaterial {
            path: path.clone(),
            source,
        })
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(
            |source| {
                FibrilServerError::TlsSetup(fibril_tls::TlsSetupError::WriteMaterial {
                    path: path.clone(),
                    source,
                })
            },
        )?;
    }
    Ok(())
}

fn write_config_overlay(
    data_dir: &std::path::Path,
    overlay: &fibril_config::ConfigOverlay,
) -> Result<(), FibrilServerError> {
    overlay
        .write(data_dir)
        .map_err(FibrilServerError::TlsConfig)
}

fn write_setup_marker(data_dir: &std::path::Path) -> Result<(), FibrilServerError> {
    let path = data_dir.join(fibril_config::SETUP_MARKER_FILE);
    let stamp = format!("completed_at_ms = {}\n", fibril_util::unix_millis());
    std::fs::write(&path, stamp).map_err(|source| {
        FibrilServerError::TlsSetup(fibril_tls::TlsSetupError::WriteMaterial { path, source })
    })
}

pub async fn run_server_from_config(config: ServerConfig) -> Result<(), FibrilServerError> {
    let mut config = config;
    // The setup-owned overlay layers below explicit config: it applies only
    // when file/env/CLI left the tls section untouched.
    config
        .apply_setup_overlay()
        .map_err(FibrilServerError::TlsConfig)?;
    if config.setup_pending() {
        run_first_boot_setup(&config).await?;
        config
            .apply_setup_overlay()
            .map_err(FibrilServerError::TlsConfig)?;
    }
    let config = config;

    let metrics = Metrics::new(3 * 60 * 60);
    let keratin_default = KeratinConfig::default();
    let keratin_message_cfg = KeratinConfig {
        fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
        min_fsync_interval_ms: config.storage.keratin.min_fsync_interval_ms,
        batch_linger_ms: config.storage.keratin.batch_linger_ms,
        segment_max_bytes: config.storage.keratin.message_log.segment_max_bytes,
        tail_cache_bytes: config.storage.keratin.tail_cache_bytes,
        segment_preallocate_bytes: config.storage.keratin.segment_preallocate_bytes,
        max_inflight_fsyncs: config.storage.keratin.max_inflight_fsyncs,
        pipeline_commit_records: config.storage.keratin.pipeline_commit_records,
        ..keratin_default
    };
    let keratin_event_cfg = KeratinConfig {
        fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
        min_fsync_interval_ms: config.storage.keratin.min_fsync_interval_ms,
        batch_linger_ms: config.storage.keratin.batch_linger_ms,
        segment_max_bytes: config.storage.keratin.event_log.segment_max_bytes,
        flush_target_bytes: keratin_default.flush_target_bytes / 8,
        max_batch_bytes: keratin_default.max_batch_bytes / 8,
        index_stride_bytes: keratin_default.index_stride_bytes / 8,
        // The event log is not tail-followed by consumers, so no read cache.
        tail_cache_bytes: 0,
        segment_preallocate_bytes: config.storage.keratin.segment_preallocate_bytes,
        max_inflight_fsyncs: config.storage.keratin.max_inflight_fsyncs,
        pipeline_commit_records: config.storage.keratin.pipeline_commit_records,
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

    engine.set_recovery_mismatch_policy(match config.recovery.on_mismatch {
        RecoveryMismatchMode::Quarantine => RecoveryMismatchPolicy::Quarantine,
        RecoveryMismatchMode::Refuse => RecoveryMismatchPolicy::Refuse,
        RecoveryMismatchMode::Ignore => RecoveryMismatchPolicy::Ignore,
    });

    let runtime_seed = runtime_seed_from_config(&config);
    // Node-to-node trust: the cluster secret is operator-provisioned and
    // required in cluster mode, where replication must authenticate across
    // machines. Users are data and cannot bootstrap that trust.
    let cluster_secret = config
        .resolve_cluster_secret()
        .map_err(FibrilServerError::TlsConfig)?;
    if config.coordination.mode == CoordinationMode::Ganglion && cluster_secret.is_none() {
        return Err(FibrilServerError::MissingClusterSecret);
    }
    let cluster_secret: Option<Arc<str>> = cluster_secret.map(Arc::from);

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

    let seed_users: Vec<(String, String)> = config
        .auth
        .seed_users
        .iter()
        .map(|seed| (seed.username.clone(), seed.password.clone()))
        .collect();
    let user_store = Arc::new(
        UserStoreManager::load_from_stroma_engine(&engine, &seed_users)
            .await
            .map_err(FibrilServerError::UserStore)?,
    );

    // TLS material is resolved before the coordinator starts so the raft
    // listener can serve the same certificate. In ganglion mode peers dial
    // this node at its coordination peer address, so that host joins the
    // generated certificate's names alongside the broker advertise hosts.
    let mut tls_sans = tls::san_hosts_from_advertise(&config.broker_advertise_addresses());
    if config.coordination.mode == CoordinationMode::Ganglion {
        let section = &config.coordination.ganglion;
        if let Some(own_addr) = section.peers.get(&section.raft_node_id.to_string()) {
            for host in tls::san_hosts_from_advertise(std::slice::from_ref(own_addr)) {
                if !tls_sans.contains(&host) {
                    tls_sans.push(host);
                }
            }
        }
    }
    let server_tls = tls::build_server_tls_with_client_auth(
        &config.tls.mode().map_err(FibrilServerError::TlsConfig)?,
        &config.server.data_dir,
        &tls_sans,
        &tls::ClientAuthPolicy {
            mode: config.tls.client_auth,
            ca_path: config.tls.client_ca_path.clone(),
        },
    )?;
    match &server_tls {
        Some(tls::ServerTls {
            source:
                TlsMaterialSource::Generated {
                    dir,
                    ca_fingerprint,
                },
            ..
        }) => {
            tracing::info!(
                "TLS enabled with per-deployment generated material at {}. CA SHA-256 \
                 fingerprint: {ca_fingerprint}. Clients trust {}/ca.pem or pin the \
                 fingerprint",
                dir.display(),
                dir.display()
            );
        }
        Some(_) => tracing::info!("TLS enabled with operator-supplied certificate"),
        None => {}
    }

    let raft_tls = if config.coordination.mode == CoordinationMode::Ganglion
        && config.tls.inter_broker_enabled()
    {
        let Some(server_tls) = &server_tls else {
            return Err(FibrilServerError::InterBrokerTlsWithoutMaterial);
        };
        Some(RaftTlsTransport {
            connector: peer_connector_from_config(&config)?,
            acceptor: server_tls.acceptor.clone(),
        })
    } else {
        None
    };

    let ganglion_parts = open_tcp_ganglion_parts(&config, raft_tls).await?;
    if let Some(parts) = &ganglion_parts {
        spawn_ganglion_catalogue_sync(
            parts,
            engine.clone(),
            Duration::from_millis(config.coordination.ganglion.heartbeat_interval_ms),
        );
        spawn_ganglion_runtime_settings_sync(parts, runtime_settings.clone());
        parts.coordination.spawn_user_store_sync(user_store.clone());
    }

    let ownership = queue_ownership_for_ganglion(ganglion_parts.as_ref());
    let stream_ownership = stream_ownership_for_ganglion(ganglion_parts.as_ref());
    let broker = Broker::new_with_ownerships(
        engine.clone(),
        broker_cfg,
        Some(metrics.broker()),
        ownership,
        stream_ownership,
    );

    // Tracks the lowest topology generation each connection has acked, so the
    // repartition controller can fence a cutover on cluster-wide client adoption.
    // Shared between the connection handlers (which record acks) and the heartbeat
    // labels (which report this node's minimum).
    let topology_adoption = Arc::new(TopologyAdoptionTracker::new());

    // The peer connector encrypts follower-to-owner replication dials. Built
    // here so a bad peer CA fails boot with the config error instead of
    // surfacing as per-dial replication failures.
    let peer_tls = if ganglion_parts.is_some() && config.tls.inter_broker_enabled() {
        Some(peer_connector_from_config(&config)?)
    } else {
        None
    };

    // Set by the admin drain endpoint; published as a heartbeat label so the
    // controller evacuates this node's partitions before the process stops.
    let draining = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let _ganglion_broker_tasks = match (ganglion_parts.as_ref(), cluster_secret.as_deref()) {
        (Some(parts), Some(secret)) => Some(spawn_ganglion_broker_tasks(
            parts,
            broker.clone(),
            &config,
            runtime,
            Some(topology_adoption.clone()),
            secret,
            peer_tls,
            draining.clone(),
            metrics.broker(),
        )),
        // The ganglion-requires-secret check above makes this unreachable,
        // stated here so a future reorder cannot silently spawn
        // unauthenticated node connections.
        (Some(_), None) => return Err(FibrilServerError::MissingClusterSecret),
        (None, _) => None,
    };

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

    if user_store.has_users() && !config.tls.enabled {
        tracing::warn!(
            "users are configured but tls.enabled is off: passwords travel in \
             cleartext on non-loopback connections. Enable the tls section or set \
             tls.auto_self_signed = true"
        );
    }
    let auth_handler = StoreAuthHandler::new(
        user_store.clone(),
        config.auth.allow_default_loopback,
        cluster_secret.clone(),
    );
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
    // Cluster routing comes from coordination; standalone serves a local topology
    // so a single node still tells clients its stream partition counts (queue
    // partitioning is cluster-only for now).
    let topology_source: Option<Arc<dyn ClientTopologySource>> = match ganglion_parts.as_ref() {
        Some(parts) => Some(topology_source_for_ganglion(parts)),
        None => Some(Arc::new(LocalTopologySource {
            broker: broker.clone(),
        })),
    };
    let declare_coordinator = ganglion_parts
        .as_ref()
        .map(declare_coordinator_for_ganglion);

    let broker_observability = {
        let broker = broker.clone();
        Arc::new(move || {
            serde_json::to_value(broker.sparse_queue_observability_report()).unwrap_or_default()
        })
    };

    let tls_status = match &server_tls {
        None => {
            "disabled - enable via the tls config section or fibrilctl cert generate".to_string()
        }
        Some(tls) => {
            let source = match &tls.source {
                TlsMaterialSource::Generated { ca_fingerprint, .. } => {
                    format!("generated material, CA SHA-256 {ca_fingerprint}")
                }
                TlsMaterialSource::Provided => "operator certificates".to_string(),
            };
            let admin = if config.tls.admin_tls() {
                "admin HTTPS on"
            } else {
                "admin HTTPS off (reverse proxy opt-out)"
            };
            format!("enabled ({source}), {admin}")
        }
    };
    let admin_tls_config = if config.tls.admin_tls() {
        server_tls.as_ref().map(|tls| tls.server_config.clone())
    } else {
        None
    };
    // ServerTls stays alive behind an Arc so the admin reload endpoint can
    // swap material for the lifetime of the process.
    let server_tls = server_tls.map(Arc::new);
    let tls_acceptor = server_tls.as_ref().map(|tls| tls.acceptor.clone());

    let user_admin: Arc<dyn UserAdmin> = match ganglion_parts.as_ref() {
        Some(parts) => Arc::new(GanglionUserAdmin::new(
            parts.coordination.clone(),
            user_store.clone(),
        )),
        None => Arc::new(LocalUserAdmin::new(user_store.clone())),
    };

    let admin = AdminServer::new(
        metrics.clone(),
        stroma_metrics,
        AdminConfig {
            bind: config.admin.listener.bind.to_string(),
            auth: admin_auth_handler,
            tls: admin_tls_config,
            metrics_per_channel: config.admin.metrics_per_channel,
        },
        Some(StartupConfigSummary {
            data_dir: config.server.data_dir.display().to_string(),
            tls_status,
            broker_bind: config.broker.listener.bind.to_string(),
            admin_bind: config.admin.listener.bind.to_string(),
            admin_auth_enabled: config.admin.auth.enabled,
            keratin_fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
            keratin_min_fsync_interval_ms: config.storage.keratin.min_fsync_interval_ms,
            keratin_segment_preallocate_bytes: config.storage.keratin.segment_preallocate_bytes
                as u64,
            keratin_max_inflight_fsyncs: config.storage.keratin.max_inflight_fsyncs as u64,
            keratin_pipeline_commit_records: config.storage.keratin.pipeline_commit_records,
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

    // Plexus stream observability + declare surface (the hosting broker).
    let admin = admin.with_streams(broker.clone());
    let admin = admin.with_users(user_admin);
    let admin = match &server_tls {
        Some(tls) => {
            let reload_tls = tls.clone();
            let info_tls = tls.clone();
            admin
                .with_tls_reload(Arc::new(move || {
                    reload_tls.reload().map_err(|err| err.to_string())
                }))
                .with_cert_info(Arc::new(move || {
                    let meta = info_tls.leaf_metadata();
                    serde_json::json!({
                        "fingerprint": meta.fingerprint,
                        "not_before_unix": meta.not_before_unix,
                        "not_after_unix": meta.not_after_unix,
                        "subject": meta.subject,
                    })
                }))
        }
        None => admin,
    };

    // Operator drain: announce a planned restart to connected clients and,
    // in coordinated mode, hand partition ownership off before returning.
    let drain_controller = {
        let controller = ConnectionSettingsDrainController::new(connection_settings.clone());
        match ganglion_parts.as_ref() {
            Some(parts) => controller.with_handoff(
                parts.coordination.clone(),
                config.coordination.node_id.clone(),
                draining.clone(),
                runtime_settings.subscribe(),
            ),
            None => controller,
        }
    };
    let admin = admin.with_drain(Arc::new(drain_controller));

    // Operator test publish from the queue detail page, through the real
    // publish path so counters, delivery, and durability all engage.
    let admin = admin.with_test_publisher(Arc::new(AdminTestPublisher {
        broker: broker.clone(),
        engine: engine.clone(),
    }));

    // The parked-drain attention rule reads this node's draining state.
    let admin = admin.with_draining_flag(draining.clone());

    // Per-broker exclusive-cohort view (this node's local cohort membership,
    // with each member's live per-partition coverage for the queue detail page).
    let admin = admin.with_cohorts({
        let broker = broker.clone();
        Arc::new(move || serde_json::to_value(broker.local_cohort_coverage()).unwrap_or_default())
    });

    // The coordination listener handle must outlive the server futures.
    let mut _consensus_server = None;
    let admin = match ganglion_parts {
        None => admin
            .with_coordination(single_node_admin_coordination(&config))
            // The lone ring's load comes straight from local counters (a
            // short recent window, so the diagram tracks bursts).
            .with_node_rates({
                let stats = metrics.broker();
                let node_id = config.coordination.node_id.clone();
                Arc::new(move || {
                    let published = stats.published.ops.sum_last(5) / 5;
                    let delivered = stats.delivered.ops.sum_last(5) / 5;
                    std::iter::once((node_id.clone(), (published, delivered))).collect()
                })
            }),
        Some(parts) => {
            _consensus_server = Some(parts.consensus_server.clone());
            let topology_source = parts.coordination.clone();
            let consensus_server = parts.consensus_server;
            let controller_status = parts.controller_status;
            let liveness_source = parts.coordination.clone();
            let liveness_ttl =
                Duration::from_millis(config.coordination.ganglion.liveness_ttl_ms);
            admin
                .with_cluster_mode()
                .with_liveness(Arc::new(move || {
                    liveness_source
                        .live_nodes(liveness_ttl)
                        .into_keys()
                        .collect()
                }))
                .with_node_raft_ids({
                    let source = parts.coordination.clone();
                    Arc::new(move || source.node_raft_ids())
                })
                .with_node_rates({
                    let source = parts.coordination.clone();
                    Arc::new(move || source.node_rates())
                })
                .with_coordination(parts.coordination.clone())
                .with_consensus_topology(Arc::new(move || {
                    let mut value =
                        serde_json::to_value(topology_source.consensus_node().topology())
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
            tls_acceptor,
            broker.clone(),
            tcp_metrics,
            connection_metrics,
            Some(auth_handler.clone()),
            connection_settings,
            topology_source,
            declare_coordinator,
            Some(topology_adoption),
        )
        .await
        .map_err(FibrilServerError::BrokerListener)
    };
    let admin_server_fut =
        async move { admin.run().await.map_err(FibrilServerError::AdminListener) };

    tokio::try_join!(broker_server_fut, admin_server_fut)?;

    Ok(())
}

/// User management over the local durable store (standalone mode).
pub struct LocalUserAdmin {
    store: Arc<UserStoreManager>,
}

impl LocalUserAdmin {
    pub fn new(store: Arc<UserStoreManager>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl UserAdmin for LocalUserAdmin {
    async fn list_users(&self) -> Result<Vec<AdminUserInfo>, String> {
        Ok(user_infos(&self.store))
    }

    async fn upsert_user(&self, username: &str, password: &str) -> Result<(), String> {
        self.store
            .upsert_user(username, password)
            .await
            .map_err(|err| err.to_string())
    }

    async fn remove_user(&self, username: &str) -> Result<(), String> {
        self.store
            .remove_user(username)
            .await
            .map_err(|err| err.to_string())
    }
}

/// User management through the cluster-authoritative document: edits CAS
/// into coordination and apply locally at once, other nodes adopt via the
/// user store sync task.
pub struct GanglionUserAdmin {
    coordination: Arc<GanglionCoordination>,
    local: Arc<UserStoreManager>,
}

impl GanglionUserAdmin {
    pub fn new(coordination: Arc<GanglionCoordination>, local: Arc<UserStoreManager>) -> Self {
        Self {
            coordination,
            local,
        }
    }

    async fn mutate_cluster<F>(&self, edit: F) -> Result<(), String>
    where
        F: Fn(&mut fibril_broker::auth_store::UserDocument) -> Result<(), String>,
    {
        use fibril_coordination_ganglion::ClusterUsersUpdateOutcome;
        for _ in 0..8 {
            let current = self
                .coordination
                .users_document()
                .map_err(|err| err.to_string())?;
            let (version, mut document) = match current {
                Some(current) => (current.cluster_version, current.document),
                // First cluster write starts from the local (seeded) view so
                // config-seeded users are not dropped by the first edit.
                None => (0, self.local.snapshot().document.as_ref().clone()),
            };
            edit(&mut document)?;
            match self
                .coordination
                .update_users(version, &document)
                .await
                .map_err(|err| err.to_string())?
            {
                ClusterUsersUpdateOutcome::Stored(stored) => {
                    self.local
                        .adopt_document(stored.cluster_version, stored.document)
                        .await
                        .map_err(|err| err.to_string())?;
                    return Ok(());
                }
                ClusterUsersUpdateOutcome::Conflict(_) => continue,
            }
        }
        Err("user update lost the cluster CAS race repeatedly, try again".to_string())
    }
}

#[async_trait]
impl UserAdmin for GanglionUserAdmin {
    async fn list_users(&self) -> Result<Vec<AdminUserInfo>, String> {
        Ok(user_infos(&self.local))
    }

    async fn upsert_user(&self, username: &str, password: &str) -> Result<(), String> {
        let password_hash =
            fibril_broker::auth_store::hash_password(password).map_err(|err| err.to_string())?;
        let username = username.to_string();
        self.mutate_cluster(move |document| {
            let now = fibril_util::unix_millis();
            document
                .users
                .entry(username.clone())
                .and_modify(|record| {
                    record.password_hash = password_hash.clone();
                    record.updated_ms = now;
                })
                .or_insert_with(|| fibril_broker::auth_store::UserRecord {
                    password_hash: password_hash.clone(),
                    created_ms: now,
                    updated_ms: now,
                });
            Ok(())
        })
        .await
    }

    async fn remove_user(&self, username: &str) -> Result<(), String> {
        let username = username.to_string();
        self.mutate_cluster(move |document| {
            if document.users.remove(&username).is_none() {
                return Err(format!("unknown user `{username}`"));
            }
            Ok(())
        })
        .await
    }
}

fn user_infos(store: &UserStoreManager) -> Vec<AdminUserInfo> {
    store
        .list()
        .into_iter()
        .map(|user| AdminUserInfo {
            username: user.username,
            created_ms: user.created_ms,
            updated_ms: user.updated_ms,
        })
        .collect()
}

pub struct GanglionRuntimeSettingsStore {
    coordination: Arc<GanglionCoordination>,
}

impl GanglionRuntimeSettingsStore {
    pub fn new(coordination: Arc<GanglionCoordination>) -> Self {
        Self { coordination }
    }
}

#[async_trait]
impl RuntimeSettingsClusterStore for GanglionRuntimeSettingsStore {
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

/// Standalone topology source: a single node serving its own declared streams so
/// clients can spread publishes across stream partitions without coordination.
/// Queues are not surfaced (their standalone partitioning is not tracked here) and
/// there is one owner (this node), so `owner_endpoint` is `None` and clients use
/// their direct connection.
struct LocalTopologySource {
    broker: Arc<Broker<StromaEngine>>,
}

impl ClientTopologySource for LocalTopologySource {
    fn topology(&self) -> TopologyOk {
        // One entry per partition (owner is this node, so `owner_endpoint` is
        // None and clients use their direct connection), repeating the
        // authoritative count across the topic's partitions.
        let streams = self
            .broker
            .stream_partition_counts()
            .into_iter()
            .flat_map(|(topic, partition_count)| {
                (0..partition_count).map(move |partition| StreamTopologyEntry {
                    topic: topic.clone(),
                    partition: Partition::new(partition),
                    owner_endpoints: Vec::new(),
                    partitioning_version: 0,
                    partition_count,
                })
            })
            .collect();
        TopologyOk {
            generation: 0,
            queues: Vec::new(),
            streams,
        }
    }

    fn owner_endpoint(
        &self,
        _topic: &str,
        _partition: Partition,
        _group: Option<&str>,
    ) -> Option<(String, u64)> {
        None
    }
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
                    owner_endpoints: queue
                        .owner_endpoints
                        .iter()
                        .filter_map(|endpoint| AdvertisedAddress::parse(endpoint))
                        .collect(),
                    partitioning_version: queue.partitioning_version,
                    partition_count: queue.partition_count,
                })
                .collect(),
            streams: topology
                .streams
                .into_iter()
                .map(|stream| StreamTopologyEntry {
                    topic: stream.topic,
                    partition: stream.partition,
                    owner_endpoints: stream
                        .owner_endpoints
                        .iter()
                        .filter_map(|endpoint| AdvertisedAddress::parse(endpoint))
                        .collect(),
                    partitioning_version: stream.partitioning_version,
                    partition_count: stream.partition_count,
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
                    .owner_endpoints
                    .into_iter()
                    .next()
                    .map(|endpoint| (endpoint, queue.partitioning_version))
            })
    }

    fn stream_owner_endpoint(&self, topic: &str, partition: Partition) -> Option<(String, u64)> {
        (self.fetch)()
            .streams
            .into_iter()
            .find(|stream| stream.topic == topic && stream.partition == partition)
            .and_then(|stream| {
                stream
                    .owner_endpoints
                    .into_iter()
                    .next()
                    .map(|endpoint| (endpoint, stream.partitioning_version))
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
    pub declare_stream: Arc<
        dyn Fn(String, u32, u8, StreamRetentionConfig, Option<u32>) -> DeclareFut + Send + Sync,
    >,
}

impl DeclareCoordinator for CoordinationDeclareCoordinator {
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

    fn declare_stream<'a>(
        &'a self,
        topic: &'a str,
        partition_count: u32,
        durability: u8,
        retention: StreamRetention,
        replication_factor: Option<u32>,
    ) -> futures::future::BoxFuture<'a, Result<u32, String>> {
        (self.declare_stream)(
            topic.to_string(),
            partition_count,
            durability,
            StreamRetentionConfig {
                max_age_ms: retention.max_age_ms,
                max_bytes: retention.max_bytes,
                max_records: retention.max_records,
            },
            replication_factor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_rate_tracker_reports_deltas_per_second() {
        let mut tracker = HeartbeatRateTracker::new();
        // First tick has no baseline.
        assert_eq!(tracker.tick(1000, 500), (0, 0));
        // Deltas divide by the (tiny) elapsed time; force a known baseline
        // by rewriting the stored instant one second into the past.
        if let Some((at, published, delivered)) = tracker.prev.as_mut() {
            *at -= Duration::from_secs(1);
            assert_eq!((*published, *delivered), (1000, 500));
        }
        assert_eq!(tracker.tick(4000, 2500), (3000, 2000));
        // A counter going backwards (restart) must not underflow.
        if let Some((at, _, _)) = tracker.prev.as_mut() {
            *at -= Duration::from_secs(1);
        }
        assert_eq!(tracker.tick(0, 0), (0, 0));
    }

    #[test]
    fn repartition_finalize_gate_waits_for_adoption_then_times_out() {
        let timeout = Duration::from_secs(30);
        let target = Some(5);

        // Stamped but the fleet has not caught up, and the wait is young -> hold.
        assert!(!repartition_adoption_satisfied(
            target,
            Some(4),
            Duration::from_secs(1),
            timeout
        ));
        // No adoption signal at all (e.g. no acking clients), young -> hold.
        assert!(!repartition_adoption_satisfied(
            target,
            None,
            Duration::from_secs(1),
            timeout
        ));
        // The fleet caught up to the target -> finalize, regardless of the timer.
        assert!(repartition_adoption_satisfied(
            target,
            Some(5),
            Duration::from_secs(1),
            timeout
        ));
        assert!(repartition_adoption_satisfied(
            target,
            Some(6),
            Duration::from_secs(1),
            timeout
        ));
        // Not adopted, but the adoption wait has elapsed -> finalize via timeout.
        assert!(repartition_adoption_satisfied(
            target,
            None,
            Duration::from_secs(31),
            timeout
        ));
        // An unstamped transition (pre-adoption-fencing marker) is never gated.
        assert!(repartition_adoption_satisfied(
            None,
            None,
            Duration::from_secs(0),
            timeout
        ));
    }

    #[test]
    fn drain_handoff_gate_completes_on_zero_owned_or_timeout() {
        let timeout = Duration::from_secs(30);

        // Still owning partitions, wait young -> keep waiting.
        assert!(!drain_handoff_complete(3, Duration::from_secs(1), timeout));
        // Everything moved -> done, regardless of the timer.
        assert!(drain_handoff_complete(0, Duration::from_secs(0), timeout));
        // The bounded wait is spent -> done (reactive failover backstops).
        assert!(drain_handoff_complete(3, Duration::from_secs(30), timeout));
        assert!(drain_handoff_complete(3, Duration::from_secs(45), timeout));
    }

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
        let coordination_addr = free_loopback_addr();
        let mut config = ServerConfig::default();
        config.server.data_dir = root.join("data");
        config.coordination.mode = CoordinationMode::Ganglion;
        config.coordination.node_id = "broker-a".into();
        config.coordination.ganglion.raft_node_id = 1;
        config.coordination.ganglion.listen = coordination_addr;
        config
            .coordination
            .ganglion
            .peers
            .insert("1".into(), coordination_addr.to_string());
        config.coordination.ganglion.bootstrap = true;
        config.coordination.ganglion.data_dir = root.join("coordination");
        config.coordination.ganglion.heartbeat_interval_ms = 50;
        config.coordination.ganglion.controller_tick_ms = 50;
        config.coordination.ganglion.liveness_ttl_ms = 200;

        let parts = open_tcp_ganglion_parts(&config, None)
            .await
            .expect("start ganglion")
            .expect("ganglion parts");
        parts
            .coordination
            .consensus_node()
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
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }
}
