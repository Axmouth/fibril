use std::sync::Arc;

use fibril_admin::{AdminConfig, AdminServer};
use fibril_broker::{
    broker::{Broker, BrokerConfig},
    queue_engine::{KeratinConfig, SnapshotConfig, StromaEngine},
};
use fibril_config::ServerConfig;
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::{ConnectionSettings, run_server};
use fibril_util::{StaticAuthHandler, init_tracing};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = ServerConfig::load()?;
    let delivery = &config.runtime_seed.delivery;
    let idle_cleanup = config.idle_queue_cleanup_internal();

    let metrics = Metrics::new(3 * 60 * 60); // 3 hours
    let engine = StromaEngine::open(
        &config.server.data_dir,
        KeratinConfig::default(),
        SnapshotConfig::default(),
    )
    .await?;
    let broker_cfg = BrokerConfig {
        inflight_ttl_ms: delivery.inflight_ttl_ms,
        expiry_poll_min_ms: delivery.expiry_poll_min_ms,
        expiry_batch_max: delivery.expiry_batch_max,
        delivery_poll_max_ms: delivery.delivery_poll_max_ms,
        queue_idle_evict_after_ms: idle_cleanup.queue_idle_evict_after_ms,
        queue_idle_sweep_interval_ms: idle_cleanup.queue_idle_sweep_interval_ms,
    };
    let broker = Broker::new(engine.clone(), broker_cfg, Some(metrics.broker()));

    let auth_handler = StaticAuthHandler::new("fibril".to_string(), "fibril".to_string());

    let stroma_metrics = broker.stroma_metrics();

    let broker_server_fut = run_server(
        config.broker.listener.bind,
        broker,
        metrics.tcp(),
        metrics.connections(),
        Some(auth_handler.clone()),
        ConnectionSettings::new(None)
            .with_publisher_cache_idle_timeout_ms(idle_cleanup.publisher_idle_timeout_ms),
    );

    let admin = AdminServer::new(
        metrics.clone(),
        stroma_metrics,
        AdminConfig {
            bind: config.admin.listener.bind.to_string(),
            // auth: Some(auth_handler),
            auth: None,
        },
        Arc::new(engine.clone()),
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
