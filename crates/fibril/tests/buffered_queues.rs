//! Three real broker processes with `replica_buffered:2` assignment
//! durability. New queues are buffered unless their declare names a mode, and
//! the mode is fixed after the first declare. With background sync off, a
//! buffered queue confirms publishes that its owner has not synced, while a
//! durable queue next to it syncs every write. Killing the buffered queue's
//! owner process loses no confirmed message, since a process crash keeps the
//! page cache and the followers hold written copies.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage, QueueConfig, SubEvent};
use tokio::process::{Child, Command};

#[allow(dead_code)]
mod common;

const SECRET: &str = "buffered-queues-test-secret";

struct Node {
    name: String,
    broker: String,
    admin: String,
    child: Option<Child>,
    log: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

fn spawn_node(root: &Path, id: u64, raft: &[String; 3]) -> Node {
    let broker = common::free_loopback_addr().to_string();
    let admin = common::free_loopback_addr().to_string();
    let toml = format!(
        r#"[server]
data_dir = "{data}"

[broker.listener]
bind = "{broker}"

[admin.listener]
bind = "{admin}"

[coordination]
mode = "ganglion"
node_id = "node-{id}"

[coordination.ganglion]
raft_node_id = {id}
listen = "{listen}"
bootstrap = {bootstrap}
heartbeat_interval_ms = 300
liveness_ttl_ms = 2000
controller_tick_ms = 300
controller_startup_grace_ms = 0
target_followers = 2

[coordination.ganglion.peers]
"1" = "{r1}"
"2" = "{r2}"
"3" = "{r3}"

[coordination.ganglion.assignment_durability]
mode = "replica_buffered"
nodes = 2
"#,
        data = root.join(format!("data-{id}")).display(),
        listen = raft[id as usize - 1],
        bootstrap = id == 1,
        r1 = raft[0],
        r2 = raft[1],
        r3 = raft[2],
    );
    let config = root.join(format!("node-{id}.toml"));
    std::fs::write(&config, toml).unwrap();
    let log = root.join(format!("node-{id}.log"));
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_fibril-server"))
        .env("FIBRIL_CONFIG", &config)
        .env("FIBRIL_CLUSTER_SECRET", SECRET)
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(out.try_clone().unwrap())
        .stderr(out)
        .kill_on_drop(true)
        .spawn()
        .expect("spawn fibril-server");
    Node {
        name: format!("node-{id}"),
        broker,
        admin,
        child: Some(child),
        log,
    }
}

async fn admin_get(admin: &str, path: &str) -> Option<serde_json::Value> {
    reqwest::Client::new()
        .get(format!("http://{admin}{path}"))
        .basic_auth("fibril", Some("fibril"))
        .send()
        .await
        .ok()?
        .json::<serde_json::Value>()
        .await
        .ok()
}

/// The partition 0 assignment of `topic`, as the admin topology shows it.
async fn assignment(admin: &str, topic: &str) -> Option<serde_json::Value> {
    let topology = admin_get(admin, "/admin/api/topology").await?;
    topology["coordination"]["assignments"]
        .as_array()?
        .iter()
        .find(|a| a["topic"] == topic && a["partition"].as_u64() == Some(0))
        .cloned()
}

/// The owner's debug entry for partition 0 of `topic`.
async fn owner_queue_debug(admin: &str, topic: &str) -> Option<serde_json::Value> {
    let debug = admin_get(admin, "/admin/api/queues_debug").await?;
    debug["queues"]
        .as_array()?
        .iter()
        .find(|q| q["topic"] == topic && q["partition"].as_u64() == Some(0))
        .cloned()
}

async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn declare(client: &Client, config: QueueConfig) -> Result<(), FibrilError> {
    let mut last = None;
    for _ in 0..50 {
        match client.declare_queue(config.clone()).await {
            Ok(()) => return Ok(()),
            Err(err @ FibrilError::Failure { code: 409, .. }) => return Err(err),
            Err(err) => {
                eprintln!("declare: {err}");
                last = Some(err);
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    }
    Err(last.unwrap())
}

async fn publish_confirmed(client: &Client, topic: &str, payload: &str) {
    let publisher = client.publisher(topic).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match publisher.publish_confirmed(NewMessage::text(payload)).await {
            Ok(_) => return,
            Err(err) => {
                assert!(Instant::now() < deadline, "publish of {payload} never confirmed: {err}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_queue_confirms_before_fsync_and_survives_an_owner_kill() {
    let _permit = common::BOOT_PERMITS.acquire_many(3).await.unwrap();
    let root = common::temp_root("fibril-buffered-queues", "cluster");
    let raft: [String; 3] = std::array::from_fn(|_| common::free_loopback_addr().to_string());
    let mut nodes: Vec<Node> = (1..=3).map(|id| spawn_node(&root, id, &raft)).collect();

    let result = tokio::time::timeout(Duration::from_secs(240), run_scenario(&mut nodes)).await;
    if !matches!(result, Ok(())) {
        for node in &nodes {
            let log = std::fs::read_to_string(&node.log).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(60).collect();
            eprintln!("---- {} log tail ----", node.name);
            for line in tail.into_iter().rev() {
                eprintln!("{line}");
            }
        }
        panic!("scenario failed or timed out: {result:?}");
    }
    let _ = std::fs::remove_dir_all(&root);
}

async fn run_scenario(nodes: &mut [Node]) {
    let admin = nodes[0].admin.clone();
    wait_until("three registered brokers", Duration::from_secs(60), || async {
        admin_get(&admin, "/admin/api/topology").await.is_some_and(|t| {
            t["coordination"]["nodes"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
        })
    })
    .await;

    // Background sync off, so only the writes themselves could sync.
    let current = admin_get(&admin, "/admin/api/runtime-settings").await.unwrap();
    let mut settings = current["settings"].clone();
    settings["storage"]["background_sync_interval_ms"] = 0.into();
    let response = reqwest::Client::new()
        .put(format!("http://{admin}/admin/api/runtime-settings"))
        .basic_auth("fibril", Some("fibril"))
        .json(&serde_json::json!({
            "expected_version": current["version"],
            "settings": settings,
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    assert!(status.is_success(), "{status} {}", response.text().await.unwrap_or_default());

    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .publish_timeout_ms(30_000)
        .connect(&nodes[0].broker)
        .await
        .unwrap();

    // The cluster default makes a new queue buffered. Naming a mode on a
    // later declare must match the first one.
    declare(&client, QueueConfig::new("orders").unwrap()).await.unwrap();
    declare(&client, QueueConfig::new("audit").unwrap().durable()).await.unwrap();
    declare(&client, QueueConfig::new("orders").unwrap().buffered()).await.unwrap();
    let conflict = declare(&client, QueueConfig::new("orders").unwrap().durable()).await;
    assert!(matches!(conflict, Err(FibrilError::Failure { code: 409, .. })), "{conflict:?}");
    let conflict = declare(&client, QueueConfig::new("audit").unwrap().buffered()).await;
    assert!(matches!(conflict, Err(FibrilError::Failure { code: 409, .. })), "{conflict:?}");

    let mut owners = HashMap::new();
    for (topic, mode) in [("orders", "replica_buffered"), ("audit", "replica_durable")] {
        wait_until("both queues assigned", Duration::from_secs(60), || async {
            assignment(&admin, topic)
                .await
                .is_some_and(|a| a["durability"]["mode"] == mode && a["durability"]["nodes"] == 2)
        })
        .await;
        let owner = assignment(&admin, topic).await.unwrap()["owner"]
            .as_str()
            .unwrap()
            .to_string();
        owners.insert(topic, owner);
    }
    let owner_admin = |topic: &str| {
        let owner = &owners[topic];
        nodes.iter().find(|n| &n.name == owner).unwrap().admin.clone()
    };

    // Confirmed but not synced on the owner of the buffered queue. The
    // runtime setting reaches each broker shortly after it is stored, so
    // publish until the lag shows.
    let mut confirmed = BTreeSet::new();
    let orders_admin = owner_admin("orders");
    let mut lagging = false;
    for round in 0..20 {
        for i in 0..10 {
            let payload = format!("order-{round}-{i}");
            publish_confirmed(&client, "orders", &payload).await;
            confirmed.insert(payload);
        }
        let debug = owner_queue_debug(&orders_admin, "orders").await.unwrap();
        assert_eq!(debug["buffered"], true, "{debug}");
        if debug["log_sync"]["durable_lag_records"].as_u64().unwrap_or(0) > 0 {
            lagging = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(lagging, "a buffered queue confirmed only synced records");

    for i in 0..5 {
        publish_confirmed(&client, "audit", &format!("audit-{i}")).await;
    }
    let debug = owner_queue_debug(&owner_admin("audit"), "audit").await.unwrap();
    assert_eq!(debug["buffered"], false, "{debug}");
    assert_eq!(debug["log_sync"]["durable_lag_records"], 0, "{debug}");

    // A process crash keeps the page cache, and the followers hold written
    // copies, so every confirmed message is still delivered.
    let killed = owners["orders"].clone();
    let victim = nodes.iter_mut().find(|n| n.name == killed).unwrap();
    victim.child.as_mut().unwrap().kill().await.unwrap();
    victim.child = None;
    let survivor = nodes.iter().find(|n| n.name != killed).unwrap();
    let survivor_admin = survivor.admin.clone();
    wait_until("a new owner for orders", Duration::from_secs(60), || async {
        assignment(&survivor_admin, "orders")
            .await
            .is_some_and(|a| a["owner"] != killed.as_str())
    })
    .await;

    let consumer = ClientOptions::new()
        .auth("fibril", "fibril")
        .connect(&survivor.broker)
        .await
        .unwrap();
    let mut sub = consumer.subscribe("orders").unwrap().prefetch(16).sub().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut received = BTreeSet::new();
    while !confirmed.is_subset(&received) {
        assert!(
            Instant::now() < deadline,
            "lost confirmed messages: {:?}",
            confirmed.difference(&received).collect::<Vec<_>>()
        );
        match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
            Ok(SubEvent::Delivery(msg)) => {
                received.insert(String::from_utf8(msg.payload.clone()).unwrap());
                let _ = msg.complete().await;
            }
            Ok(SubEvent::Closed(reason)) => panic!("subscription closed: {reason:?}"),
            Err(_) => eprintln!("no delivery for 10s, still waiting"),
        }
    }
}
