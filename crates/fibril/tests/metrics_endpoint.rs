//! The /metrics scrape end to end: boot a standalone broker, move messages
//! through it, and assert the node-level Prometheus counters advanced.

use std::time::Duration;

use fibril::run_server_from_config;
use fibril_client::ClientOptions;
use fibril_config::ServerConfig;
use tokio::net::TcpStream;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "fibril-metrics-{tag}-{}-{}",
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

/// First sample value for a family, skipping comment lines and families
/// that merely share the name as a prefix.
fn sample_value(body: &str, name: &str) -> f64 {
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| {
            let (sample_name, value) = line.rsplit_once(' ')?;
            let sample_name = sample_name.split('{').next()?;
            (sample_name == name).then(|| value.parse().ok())?
        })
        .unwrap_or_else(|| panic!("no sample for {name} in:\n{body}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_scrape_reflects_broker_traffic() {
    let root = temp_root("scrape");
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    let broker_addr = config.broker.listener.bind;
    let admin_addr = config.admin.listener.bind;
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });
    for _ in 0..1200 {
        if TcpStream::connect(broker_addr).await.is_ok()
            && TcpStream::connect(admin_addr).await.is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .connect(broker_addr.to_string().as_str())
        .await
        .expect("client connect");
    let mut sub = client
        .subscribe("metrics.jobs")
        .expect("subscribe builder")
        .sub()
        .await
        .expect("subscribe");
    let publisher = client.publisher("metrics.jobs").expect("publisher");
    for _ in 0..5 {
        publisher.publish("payload").await.expect("publish");
    }
    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
            .await
            .expect("delivery within timeout")
            .expect("message");
        msg.complete().await.expect("complete");
    }

    let body = reqwest::get(format!("http://{admin_addr}/metrics"))
        .await
        .expect("scrape request")
        .text()
        .await
        .expect("scrape body");

    assert!(sample_value(&body, "fibril_broker_published_total") >= 5.0);
    assert!(sample_value(&body, "fibril_broker_delivered_total") >= 5.0);
    assert!(sample_value(&body, "fibril_broker_acked_total") >= 5.0);
    assert!(sample_value(&body, "fibril_tcp_connections_open") >= 1.0);
    assert!(sample_value(&body, "fibril_subscriptions_open") >= 1.0);

    client.shutdown().await;
    handle.abort();
}
