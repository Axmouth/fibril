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
    ConnectionRuntimeSettings as ProtocolConnectionRuntimeSettings, ConnectionSettings, run_server,
};
use fibril_util::{StaticAuthHandler, init_tracing};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    let broker = Broker::new(engine.clone(), broker_cfg, Some(metrics.broker()));
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

    let broker_server_fut = run_server(
        config.broker.listener.bind,
        broker.clone(),
        metrics.tcp(),
        metrics.connections(),
        Some(auth_handler.clone()),
        connection_settings,
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
    let admin = match config.coordination.mode {
        CoordinationMode::Static => {
            // Single-node view so `fibrilctl topology` reports this broker.
            admin.with_coordination(Arc::new(StaticCoordination::single_node(
                config.coordination.node_id.clone(),
                config.broker.listener.bind,
            )))
        }
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
            _raft_server = Some(raft_server.clone());

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
            // Register this broker and keep its heartbeat fresh so it shows up
            // (and stays live) in the cluster's node table.
            coordination.spawn_heartbeat(
                fibril_broker::coordination::NodeInfo {
                    node_id: config.coordination.node_id.clone(),
                    broker_addr: config.broker.listener.bind,
                    admin_addr: Some(config.admin.listener.bind),
                },
                std::time::Duration::from_millis(section.heartbeat_interval_ms),
            );

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

            // Replicated runtime settings: apply cluster documents locally...
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

            let topology_source = coordination.clone();
            admin
                .with_coordination(coordination)
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
                .with_settings_publisher(settings_tx)
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
