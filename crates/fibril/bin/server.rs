use std::sync::Arc;

use fibril_admin::{AdminConfig, AdminServer, StartupConfigSummary};
use fibril_broker::{
    broker::{Broker, BrokerConfig},
    coordination::StaticCoordination,
    queue_engine::{KeratinConfig, SnapshotConfig, StromaEngine},
    runtime_settings::{
        ConnectionRuntimeSettings as BrokerConnectionRuntimeSettings, DeliveryRuntimeSettings,
        IdleQueueCleanupRuntimeSettings, RuntimeSettings, RuntimeSettingsLocks,
        RuntimeSettingsManager,
    },
};
use fibril_config::{CoordinationMode, ServerConfig};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::{
    ClientTopologySource, ConnectionRuntimeSettings as ProtocolConnectionRuntimeSettings,
    ConnectionSettings, QueueDeclareCoordinator, run_server,
};
use fibril_protocol::v1::{QueueTopologyEntry, TopologyOk};
use fibril_util::{StaticAuthHandler, init_tracing};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Bridges the coordination provider's client topology into the protocol
/// handler's `ClientTopologySource`, keeping coordination-ganglion free of a
/// protocol dependency. The closure fetches a fresh snapshot each call.
struct CoordinationTopologySource {
    fetch: Arc<dyn Fn() -> fibril_coordination_ganglion::ClientTopology + Send + Sync>,
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
                })
                .collect(),
        }
    }

    fn owner_endpoint(
        &self,
        topic: &str,
        partition: u32,
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

/// Bridges queue-declare partitioning writes to the coordination provider. The
/// boxed-future closure captures the provider, avoiding naming its generic type
/// and keeping coordination-ganglion free of a protocol dependency.
type DeclareFut = futures::future::BoxFuture<'static, Result<u32, String>>;

struct CoordinationDeclareCoordinator {
    declare: Arc<dyn Fn(String, Option<String>, u32) -> DeclareFut + Send + Sync>,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = ServerConfig::load()?;

    let metrics = Metrics::new(3 * 60 * 60); // 3 hours
    let keratin_default = KeratinConfig::default();
    let keratin_message_cfg = KeratinConfig {
        fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
        segment_max_bytes: config.storage.keratin.message_log.segment_max_bytes,
        ..keratin_default
    };
    let keratin_event_cfg = KeratinConfig {
        fsync_interval_ms: config.storage.keratin.fsync_interval_ms,
        segment_max_bytes: config.storage.keratin.event_log.segment_max_bytes,
        flush_target_bytes: keratin_default.flush_target_bytes / 8,
        max_batch_bytes: keratin_default.max_batch_bytes / 8,
        index_stride_bytes: keratin_default.index_stride_bytes / 8,
        ..keratin_default
    };
    let engine = StromaEngine::open(
        &config.server.data_dir,
        fibril_broker::queue_engine::StromaKeratinConfig {
            message_log: keratin_message_cfg,
            event_log: keratin_event_cfg,
        },
        SnapshotConfig::default(),
    )
    .await?;
    let runtime_seed = RuntimeSettings {
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
        replication: fibril_broker::runtime_settings::ReplicationRuntimeSettings {
            confirm_timeout_ms: config.runtime_seed.replication.confirm_timeout_ms,
            caught_up_poll_ms: config.runtime_seed.replication.caught_up_poll_ms,
            retry_poll_ms: config.runtime_seed.replication.retry_poll_ms,
            checkpoint_retry_poll_ms: config.runtime_seed.replication.checkpoint_retry_poll_ms,
            min_in_sync_replicas: config.runtime_seed.replication.min_in_sync_replicas,
            isr_timeout_ms: config.runtime_seed.replication.isr_timeout_ms,
        },
        partitioning: fibril_broker::runtime_settings::PartitioningRuntimeSettings {
            default_partition_count: config.runtime_seed.partitioning.default_partition_count,
        },
    };
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

    // Cluster coordination starts BEFORE the broker so queue ownership can be
    // injected at construction (in ganglion mode brokers serve only
    // assigned queues; standalone keeps owning everything).
    struct GanglionParts {
        coordination: Arc<
            fibril_coordination_ganglion::GanglionCoordination<
                ganglion_openraft::FileRaftLogStore,
                ganglion_openraft::TcpNetworkFactory,
            >,
        >,
        raft_server: Arc<ganglion_openraft::TcpRaftServer>,
        settings_tx:
            tokio::sync::mpsc::UnboundedSender<fibril_broker::runtime_settings::RuntimeSettings>,
        controller_status: Arc<std::sync::RwLock<fibril_coordination_ganglion::ControllerStatus>>,
    }
    let ganglion_parts: Option<GanglionParts> = match config.coordination.mode {
        CoordinationMode::Static => None,
        CoordinationMode::Ganglion => {
            use fibril_coordination_ganglion::GanglionCoordination;
            use ganglion_openraft::openraft::BasicNode;

            let section = &config.coordination.ganglion;
            let data_dir = if section.data_dir.as_os_str().is_empty() {
                config.server.data_dir.join("coordination")
            } else {
                section.data_dir.clone()
            };

            let raft_config = ganglion_openraft::default_raft_config()
                .map_err(|error| anyhow::anyhow!("raft config: {error}"))?;
            let wire_format: ganglion_openraft::WireFormat = section
                .wire_format
                .parse()
                .map_err(|error| anyhow::anyhow!("coordination.ganglion.wire_format: {error}"))?;
            let (node, raft_server) =
                ganglion_openraft::RaftMetadataNode::start_durable_tcp_with_format(
                    section.raft_node_id,
                    raft_config,
                    section.listen,
                    &data_dir,
                    wire_format,
                )
                .await
                .map_err(|error| {
                    anyhow::anyhow!("embedded coordinator failed to start: {error}")
                })?;
            let raft_server = Arc::new(raft_server);

            if section.bootstrap {
                let members: std::collections::BTreeMap<u64, BasicNode> = section
                    .peers
                    .iter()
                    .map(|(id, addr)| {
                        Ok((
                            id.parse::<u64>()
                                .map_err(|_| anyhow::anyhow!("peer id `{id}` is not a u64"))?,
                            BasicNode::new(addr.clone()),
                        ))
                    })
                    .collect::<anyhow::Result<_>>()?;
                // Re-running initialize after first boot is rejected by raft;
                // that is the expected restart path, not an error.
                if let Err(error) = node.initialize(members).await {
                    tracing::info!("coordinator initialize skipped: {error}");
                }
            }

            let coordination = Arc::new(GanglionCoordination::new_with_wire_format(
                config.coordination.node_id.clone(),
                node,
                wire_format,
            ));
            // Catalogue sync: local engine queues become cluster-visible so
            // the controller can assign them (also re-registers pre-existing
            // on-disk queues after restarts).
            let catalogue_engine = engine.clone();
            coordination.spawn_catalogue_sync(
                move || {
                    let engine = catalogue_engine.clone();
                    async move {
                        use fibril_broker::queue_engine::QueueEngine as _;
                        match engine.queue_stats_snapshot().await {
                            Ok(snapshot) => snapshot
                                .queues
                                .keys()
                                .map(|key| {
                                    // Queue stats key by (topic, group) only;
                                    // partition 0 always exists. The full
                                    // partition set is registered by declare.
                                    fibril_broker::coordination::QueueIdentity::new(
                                        key.topic.clone(),
                                        0,
                                        key.group.as_deref(),
                                    )
                                })
                                .collect(),
                            Err(_) => Vec::new(),
                        }
                    }
                },
                std::time::Duration::from_millis(section.heartbeat_interval_ms),
            );

            // Replicated runtime settings: apply cluster documents locally
            // (sync loop) and publish locally-stored updates cluster-wide
            // (publisher task fed by the admin PUT hook).
            coordination.spawn_runtime_settings_sync(runtime_settings.clone());
            let (settings_tx, mut settings_rx) = tokio::sync::mpsc::unbounded_channel();
            let settings_publisher = coordination.clone();
            tokio::spawn(async move {
                while let Some(settings) = settings_rx.recv().await {
                    if let Err(error) = settings_publisher.publish_runtime_settings(&settings).await
                    {
                        tracing::warn!(%error, "cluster runtime-settings publish deferred");
                    }
                }
            });

            // Embedded controller: the raft leader assigns catalogue queues
            // across heartbeat-live brokers; standbys idle.
            let (_controller, controller_status) = coordination.spawn_controller(
                Arc::new(fibril_broker::coordination::DeterministicPartitionPlacement),
                fibril_coordination_ganglion::ControllerConfig {
                    target_followers: section.target_followers,
                    tick: std::time::Duration::from_millis(section.controller_tick_ms),
                    liveness_ttl: std::time::Duration::from_millis(section.liveness_ttl_ms),
                    max_cas_retries: 8,
                },
            );

            Some(GanglionParts {
                coordination,
                raft_server,
                settings_tx,
                controller_status,
            })
        }
    };

    // Ownership switch: assigned-queues-only in cluster mode.
    let ownership: Arc<dyn fibril_broker::broker::QueueOwnership> = match &ganglion_parts {
        Some(parts) => parts.coordination.clone(),
        None => Arc::new(fibril_broker::broker::OwnAllQueues),
    };
    let broker = Broker::new_with_ownership(
        engine.clone(),
        broker_cfg,
        Some(metrics.broker()),
        ownership,
    );

    // Cluster mode: apply assignment transitions and supervise follower
    // replication loops, resolving owners from coordination snapshots.
    if let Some(parts) = &ganglion_parts {
        let resolver = Arc::new(
            fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::new(
                parts.coordination.clone(),
            ),
        );
        broker.spawn_assignment_watcher_with_follower_replication(
            parts.coordination.clone(),
            resolver,
            fibril_broker::broker::FollowerReplicationWorkerConfig {
                follow_runtime_settings: true,
                ..Default::default()
            },
        );

        // Heartbeat with applied-tail labels: registers this broker as live
        // AND advertises its follower replication progress, which the
        // controller uses for progress-aware failover candidate selection.
        let tails_broker = broker.clone();
        parts.coordination.spawn_heartbeat_with_labels(
            fibril_broker::coordination::NodeInfo {
                node_id: config.coordination.node_id.clone(),
                broker_addr: config.broker.listener.bind,
                admin_addr: Some(config.admin.listener.bind),
            },
            std::time::Duration::from_millis(config.coordination.ganglion.heartbeat_interval_ms),
            move || {
                let mut labels = std::collections::BTreeMap::new();
                for worker in tails_broker
                    .sparse_queue_observability_report()
                    .replication_followers
                {
                    if let Some(state) = worker.state {
                        let queue = fibril_broker::coordination::QueueIdentity::new(
                            worker.topic,
                            worker.partition,
                            worker.group.as_deref(),
                        );
                        labels.insert(
                            fibril_coordination_ganglion::applied_tail_label(&queue),
                            format!("{}:{}", state.message_next_offset, state.event_next_offset),
                        );
                    }
                }
                labels
            },
        );
    }
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
    let admin_auth_handler = config.admin.auth.enabled.then(|| {
        StaticAuthHandler::new(
            config.admin.auth.username.clone(),
            config
                .admin
                .auth
                .password
                .clone()
                .expect("validated admin auth password"),
        )
    });

    let stroma_metrics = broker.stroma_metrics();

    // In ganglion mode the protocol handler answers Op::Topology and emits
    // redirects from the committed coordination topology. Standalone has no
    // routing source (clients use their direct connection).
    let topology_source: Option<Arc<dyn ClientTopologySource>> =
        ganglion_parts.as_ref().map(|parts| {
            let coordination = parts.coordination.clone();
            Arc::new(CoordinationTopologySource {
                fetch: Arc::new(move || coordination.client_topology()),
            }) as Arc<dyn ClientTopologySource>
        });

    // In ganglion mode, declare records the queue's partitioning (count +
    // version) in the replicated store via the coordination provider.
    let declare_coordinator: Option<Arc<dyn QueueDeclareCoordinator>> =
        ganglion_parts.as_ref().map(|parts| {
            let coordination = parts.coordination.clone();
            Arc::new(CoordinationDeclareCoordinator {
                declare: Arc::new(move |topic, group, count| {
                    let coordination = coordination.clone();
                    Box::pin(async move {
                        // Record the authoritative partitioning, then register
                        // all N partitions in the catalogue so the controller
                        // assigns each one. The metadata count wins (an existing
                        // queue's count is returned; a conflict errors).
                        let partitioning = coordination
                            .declare_queue_partitioning(&topic, group.as_deref(), count)
                            .await
                            .map_err(|error| error.to_string())?;
                        for partition in 0..partitioning.partition_count {
                            let queue = fibril_broker::coordination::QueueIdentity::new(
                                topic.clone(),
                                partition,
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
        });

    let broker_server_fut = run_server(
        config.broker.listener.bind,
        broker.clone(),
        metrics.tcp(),
        metrics.connections(),
        Some(auth_handler.clone()),
        connection_settings,
        topology_source,
        declare_coordinator,
    );

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
        }),
        Arc::new(engine.clone()),
        Some(broker_observability),
        Some(runtime_settings.clone()),
    );

    // Coordination provider selection (config: [coordination] mode = ...).
    // The raft listener handle must outlive main; hold it here.
    let mut _raft_server = None;
    let admin = match ganglion_parts {
        None => {
            // Single-node view so `fibrilctl topology` reports this broker.
            admin.with_coordination(Arc::new(StaticCoordination::single_node(
                config.coordination.node_id.clone(),
                config.broker.listener.bind,
            )))
        }
        Some(parts) => {
            _raft_server = Some(parts.raft_server.clone());
            let topology_source = parts.coordination.clone();
            let raft_server = parts.raft_server;
            let controller_status = parts.controller_status;
            admin
                .with_coordination(parts.coordination)
                .with_raft_topology(Arc::new(move || {
                    let mut value = serde_json::to_value(topology_source.raft_node().topology())
                        .unwrap_or(serde_json::Value::Null);
                    if let Some(object) = value.as_object_mut() {
                        // Health surfaces (ganglion FAILURE_MODES §4b.1/§4b.5).
                        object.insert(
                            "healthy".into(),
                            serde_json::Value::Bool(topology_source.coordination_healthy()),
                        );
                        object.insert(
                            "listener_serving".into(),
                            serde_json::Value::Bool(raft_server.is_serving()),
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
                .with_settings_publisher(parts.settings_tx)
        }
    };

    metrics.start(
        MetricsConfig {
            log_broker: true,
            log_storage: true,
            log_tcp: true,
        },
        std::time::Duration::from_secs(10),
    );

    let admin_server_dut = admin.run();

    let (broker_res, admin_res) = tokio::join!(broker_server_fut, admin_server_dut);
    broker_res?;
    admin_res?;

    Ok(())
}
