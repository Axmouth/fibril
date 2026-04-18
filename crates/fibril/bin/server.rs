use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use fibril_admin::{AdminConfig, AdminServer};
use fibril_broker::{
    broker::{Broker, BrokerConfig},
    queue_engine::{KeratinConfig, SnapshotConfig, StromaEngine},
};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::run_server;
use fibril_util::{StaticAuthHandler, init_tracing};
use mimalloc::MiMalloc;

#[cfg(windows)]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // TODO configurable stuff
    let root = "test_data/server";
    let metrics = Metrics::new(3 * 60 * 60); // 3 hours
    let engine = StromaEngine::open(
        &root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker_cfg = BrokerConfig {
        inflight_ttl_ms: 200000,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 8192,
        delivery_poll_max_ms: 100000, // Make tests timeout if they rely on polling to pass, to indicate the issue
    };
    let broker = Broker::new(engine.clone(), broker_cfg, Some(metrics.broker()));

    let auth_handler = StaticAuthHandler::new("fibril".to_string(), "fibril".to_string());

    let broker_server_fut = run_server(
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([0, 0, 0, 0]), 9876)),
        broker,
        metrics.tcp(),
        metrics.connections(),
        Some(auth_handler.clone()),
    );

    let admin = AdminServer::new(
        metrics.clone(),
        AdminConfig {
            bind: "0.0.0.0:8080".into(),
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
