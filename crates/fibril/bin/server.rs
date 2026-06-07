use std::sync::Arc;

use fibril_admin::{AdminConfig, AdminServer, StartupConfigSummary};
use fibril_broker::{
    broker::{Broker, BrokerConfig},
    queue_engine::{KeratinConfig, SnapshotConfig, StromaEngine},
    runtime_settings::{
        DeliveryRuntimeSettings, IdleQueueCleanupRuntimeSettings, RuntimeSettings,
        RuntimeSettingsLocks, RuntimeSettingsManager,
    },
};
use fibril_config::ServerConfig;
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::{ConnectionRuntimeSettings, ConnectionSettings, run_server};
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
        .with_publisher_cache_idle_timeout_ms(runtime.idle_queue_cleanup.publisher_idle_timeout_ms);
    {
        let broker = broker.clone();
        let connection_settings = connection_settings.clone();
        let mut runtime_updates = runtime_settings.subscribe();
        tokio::spawn(async move {
            while runtime_updates.changed().await.is_ok() {
                let snapshot = runtime_updates.borrow().clone();
                broker.update_config(BrokerConfig::from_runtime_settings(&snapshot.settings));
                connection_settings.update_runtime(ConnectionRuntimeSettings {
                    publisher_cache_idle_timeout_ms: snapshot
                        .settings
                        .idle_queue_cleanup
                        .publisher_idle_timeout_ms,
                });
            }
        });
    }

    let auth_handler = StaticAuthHandler::new("fibril".to_string(), "fibril".to_string());

    let stroma_metrics = broker.stroma_metrics();

    let broker_server_fut = run_server(
        config.broker.listener.bind,
        broker,
        metrics.tcp(),
        metrics.connections(),
        Some(auth_handler.clone()),
        connection_settings,
    );

    let broker_observability = {
        let broker = broker.clone();
        Arc::new(move || {
            serde_json::to_value(broker.sparse_queue_observability_snapshot()).unwrap_or_default()
        })
    };

    let admin = AdminServer::new(
        metrics.clone(),
        stroma_metrics,
        AdminConfig {
            bind: config.admin.listener.bind.to_string(),
            // auth: Some(auth_handler),
            auth: None,
        },
        Some(StartupConfigSummary {
            data_dir: config.server.data_dir.display().to_string(),
            broker_bind: config.broker.listener.bind.to_string(),
            admin_bind: config.admin.listener.bind.to_string(),
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
        Some(runtime_settings),
    );

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
