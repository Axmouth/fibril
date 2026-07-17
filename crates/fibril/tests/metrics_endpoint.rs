//! The /metrics scrape end to end: boot a standalone broker, move messages
//! through it, and assert the node-level Prometheus counters advanced and
//! the per-channel series honor `admin.metrics_per_channel`.

use std::time::Duration;

use fibril_client::ClientOptions;
use fibril_config::ServerConfig;

mod common;
use common::BootedServer;

async fn boot(tag: &str, configure: impl Fn(&mut ServerConfig)) -> BootedServer {
    common::boot_server("fibril-metrics", tag, true, configure).await
}

/// Publish and settle five messages on `metrics.jobs`, leaving the queue
/// materialized. Returns the still-connected client.
async fn move_traffic(broker_addr: std::net::SocketAddr) -> fibril_client::Client {
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
    client
}

async fn scrape(admin_addr: std::net::SocketAddr) -> String {
    reqwest::get(format!("http://{admin_addr}/metrics"))
        .await
        .expect("scrape request")
        .text()
        .await
        .expect("scrape body")
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
async fn metrics_scrape_reflects_broker_traffic_and_channels() {
    let server = boot("scrape", |_| {}).await;
    let client = move_traffic(server.broker_addr).await;

    // A materialized stream so the stream families have a series too.
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://{}/admin/api/streams", server.admin_addr))
        .json(&serde_json::json!({ "topic": "metrics.events" }))
        .send()
        .await
        .expect("declare stream");
    assert!(resp.status().is_success(), "declare: {}", resp.status());

    let body = scrape(server.admin_addr).await;

    assert!(sample_value(&body, "fibril_broker_published_total") >= 5.0);
    assert!(sample_value(&body, "fibril_broker_delivered_total") >= 5.0);
    assert!(sample_value(&body, "fibril_broker_acked_total") >= 5.0);
    assert!(sample_value(&body, "fibril_tcp_connections_open") >= 1.0);
    assert!(sample_value(&body, "fibril_subscriptions_open") >= 1.0);

    // Per-channel series are on by default and label the materialized
    // channels only.
    assert!(
        body.contains(r#"fibril_queue_ready{topic="metrics.jobs",group="",partition="0"}"#),
        "{body}"
    );
    assert!(
        body.contains(r#"fibril_queue_inflight{topic="metrics.jobs",group="",partition="0"}"#),
        "{body}"
    );
    assert!(
        body.contains(r#"fibril_stream_subscriptions{topic="metrics.events",partition="0"}"#),
        "{body}"
    );
    assert!(
        body.contains(r#"fibril_stream_lag_evictions_total{topic="metrics.events",partition="0"}"#),
        "{body}"
    );
    // A stream never shows up as a queue series.
    assert!(!body.contains(r#"fibril_queue_ready{topic="metrics.events""#));

    client.shutdown().await;
    server.handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_per_channel_off_drops_channel_series_only() {
    let server = boot("flag-off", |config| {
        config.admin.metrics_per_channel = false;
    })
    .await;
    let client = move_traffic(server.broker_addr).await;

    let body = scrape(server.admin_addr).await;

    assert!(sample_value(&body, "fibril_broker_published_total") >= 5.0);
    assert!(!body.contains("fibril_queue_ready"), "{body}");
    assert!(!body.contains("fibril_queue_inflight"), "{body}");
    assert!(!body.contains("fibril_stream_"), "{body}");

    client.shutdown().await;
    server.handle.abort();
}
