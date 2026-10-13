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
    spawn_node_on(root, id, raft, broker, admin)
}

/// Start node `id` on fixed addresses, also to restart it in place.
fn spawn_node_on(root: &Path, id: u64, raft: &[String; 3], broker: String, admin: String) -> Node {
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

/// Turn background sync off, so only the writes themselves could sync.
async fn disable_background_sync(admin: &str) {
    let current = admin_get(admin, "/admin/api/runtime-settings").await.unwrap();
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
}

async fn wait_for_cluster(admin: &str) {
    wait_until("three registered brokers", Duration::from_secs(60), || async {
        admin_get(admin, "/admin/api/topology").await.is_some_and(|t| {
            t["coordination"]["nodes"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
        })
    })
    .await;
}

/// Run `scenario` and print node log tails when it fails or times out.
async fn run_with_logs<F>(nodes: &mut Vec<Node>, scenario: F)
where
    F: AsyncFnOnce(&mut Vec<Node>),
{
    let result = tokio::time::timeout(Duration::from_secs(240), scenario(nodes)).await;
    if result.is_err() {
        for node in nodes.iter() {
            let log = std::fs::read_to_string(&node.log).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(80).collect();
            eprintln!("---- {} log tail ----", node.name);
            for line in tail.into_iter().rev() {
                eprintln!("{line}");
            }
        }
        panic!("scenario timed out");
    }
}

async fn run_scenario(nodes: &mut [Node]) {
    let admin = nodes[0].admin.clone();
    wait_for_cluster(&admin).await;

    disable_background_sync(&admin).await;

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

/// Receive and ack until every payload in `want` has arrived.
async fn consume_all(client: &Client, topic: &str, want: &BTreeSet<String>) {
    let mut sub = client.subscribe(topic).unwrap().prefetch(16).sub().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut received = BTreeSet::new();
    while !want.is_subset(&received) {
        assert!(
            Instant::now() < deadline,
            "lost confirmed messages: {:?}",
            want.difference(&received).collect::<Vec<_>>()
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

/// The directory of the queue log behind `topic` under a node's data dir.
#[cfg(feature = "io-fault-injection")]
fn queue_log_root(data: &Path, topic: &str) -> PathBuf {
    fn walk(dir: &Path, topic: &str, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if path.join("segments").is_dir()
                && path.components().any(|c| c.as_os_str() == topic)
            {
                found.push(path);
            } else {
                walk(&path, topic, found);
            }
        }
    }
    let mut found = Vec::new();
    walk(data, topic, &mut found);
    assert_eq!(found.len(), 1, "queue logs for {topic}: {found:?}");
    found.pop().unwrap()
}

/// The owner of a buffered queue loses power: its process dies and the
/// records it wrote but never synced are gone. The followers hold written
/// copies of every confirmed message, so none may be lost, whether the owner
/// comes back before the cluster notices or after its queue moved.
#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_power_loss_with_a_quick_restart_loses_no_confirmed_message() {
    owner_power_loss(true).await;
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_power_loss_after_failover_loses_no_confirmed_message() {
    owner_power_loss(false).await;
}

#[cfg(feature = "io-fault-injection")]
async fn owner_power_loss(quick: bool) {
    let _permit = common::BOOT_PERMITS.acquire_many(3).await.unwrap();
    let root = common::temp_root("fibril-buffered-power", if quick { "quick" } else { "late" });
    let raft: [String; 3] = std::array::from_fn(|_| common::free_loopback_addr().to_string());
    let mut nodes: Vec<Node> = (1..=3).map(|id| spawn_node(&root, id, &raft)).collect();
    let scenario_root = root.clone();
    run_with_logs(&mut nodes, async move |nodes: &mut Vec<Node>| {
        let root = scenario_root;
        let admin = nodes[0].admin.clone();
        wait_for_cluster(&admin).await;
        disable_background_sync(&admin).await;
        let client = ClientOptions::new()
            .auth("fibril", "fibril")
            .publish_timeout_ms(30_000)
            .connect(&nodes[0].broker)
            .await
            .unwrap();
        declare(&client, QueueConfig::new("orders").unwrap()).await.unwrap();
        wait_until("orders assigned with two followers", Duration::from_secs(60), || async {
            assignment(&admin, "orders").await.is_some_and(|a| {
                a["followers"].as_array().is_some_and(|f| f.len() == 2)
            })
        })
        .await;
        let owner = assignment(&admin, "orders").await.unwrap()["owner"]
            .as_str()
            .unwrap()
            .to_string();
        let index = nodes.iter().position(|n| n.name == owner).unwrap();
        let owner_admin = nodes[index].admin.clone();

        let mut confirmed = BTreeSet::new();
        let mut durable_end = None;
        for round in 0..20 {
            for i in 0..10 {
                let payload = format!("before-{round}-{i}");
                publish_confirmed(&client, "orders", &payload).await;
                confirmed.insert(payload);
            }
            let debug = owner_queue_debug(&owner_admin, "orders").await.unwrap();
            let sync = &debug["log_sync"];
            if sync["durable_lag_records"].as_u64().unwrap_or(0) > 0 {
                durable_end = sync["durable_next"].as_u64();
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let durable_end = durable_end.expect("the owner never held unsynced confirmed records");
        drop(client);

        // Power loss: the process dies and its unsynced writes are gone.
        let id = index as u64 + 1;
        let (broker, node_admin) = (nodes[index].broker.clone(), nodes[index].admin.clone());
        nodes[index].child.as_mut().unwrap().kill().await.unwrap();
        nodes[index].child = None;
        let log_root = queue_log_root(&root.join(format!("data-{id}")), "orders");
        keratin_log::io_fault_injection::lose_unsynced_on_next_open(&log_root, durable_end)
            .unwrap();
        eprintln!("{owner} lost power, cut to {durable_end} with {} confirmed", confirmed.len());

        let survivor = (index + 1) % 3;
        let survivor_admin = nodes[survivor].admin.clone();
        if !quick {
            wait_until("a new owner for orders", Duration::from_secs(60), || async {
                assignment(&survivor_admin, "orders")
                    .await
                    .is_some_and(|a| a["owner"] != owner.as_str())
            })
            .await;
        }
        nodes[index] = spawn_node_on(&root, id, &raft, broker, node_admin);

        let client = ClientOptions::new()
            .auth("fibril", "fibril")
            .publish_timeout_ms(30_000)
            .connect(&nodes[survivor].broker)
            .await
            .unwrap();
        for i in 0..10 {
            let payload = format!("after-{i}");
            publish_confirmed(&client, "orders", &payload).await;
            confirmed.insert(payload);
        }
        consume_all(&client, "orders", &confirmed).await;
    })
    .await;
    if std::env::var_os("KEEP_TEST_DATA").is_none() {
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// Every node holding a buffered queue loses power at once. Each copy drops
/// what it never synced, so recent confirmed messages may be gone, which the
/// mode allows. The queue must recover from the longest surviving history
/// and keep serving, and every message it still delivers must be one that was
/// published.
#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_cluster_power_loss_recovers_the_longest_surviving_history() {
    let _permit = common::BOOT_PERMITS.acquire_many(3).await.unwrap();
    let root = common::temp_root("fibril-buffered-power", "all");
    let raft: [String; 3] = std::array::from_fn(|_| common::free_loopback_addr().to_string());
    let mut nodes: Vec<Node> = (1..=3).map(|id| spawn_node(&root, id, &raft)).collect();
    let scenario_root = root.clone();
    run_with_logs(&mut nodes, async move |nodes: &mut Vec<Node>| {
        let root = scenario_root;
        let admin = nodes[0].admin.clone();
        wait_for_cluster(&admin).await;
        disable_background_sync(&admin).await;
        let client = ClientOptions::new()
            .auth("fibril", "fibril")
            .publish_timeout_ms(30_000)
            .connect(&nodes[0].broker)
            .await
            .unwrap();
        declare(&client, QueueConfig::new("orders").unwrap()).await.unwrap();
        wait_until("orders assigned with two followers", Duration::from_secs(60), || async {
            assignment(&admin, "orders").await.is_some_and(|a| {
                a["followers"].as_array().is_some_and(|f| f.len() == 2)
            })
        })
        .await;
        let mut published = BTreeSet::new();
        for i in 0..40 {
            let payload = format!("before-{i}");
            publish_confirmed(&client, "orders", &payload).await;
            published.insert(payload);
        }
        drop(client);

        // Read every copy's durable end, then cut every copy back to it.
        let mut cuts = Vec::new();
        for node in nodes.iter() {
            let debug = owner_queue_debug(&node.admin, "orders").await.unwrap();
            cuts.push(debug["log_sync"]["durable_next"].as_u64().unwrap());
        }
        let mut addresses = Vec::new();
        for node in nodes.iter_mut() {
            node.child.as_mut().unwrap().kill().await.unwrap();
            node.child = None;
            addresses.push((node.broker.clone(), node.admin.clone()));
        }
        for (index, cut) in cuts.iter().enumerate() {
            let log_root = queue_log_root(&root.join(format!("data-{}", index + 1)), "orders");
            keratin_log::io_fault_injection::lose_unsynced_on_next_open(&log_root, *cut).unwrap();
        }
        eprintln!("every node lost power, cut to {cuts:?}");
        for (index, (broker, admin)) in addresses.into_iter().enumerate() {
            nodes[index] = spawn_node_on(&root, index as u64 + 1, &raft, broker, admin);
        }
        wait_for_cluster(&nodes[0].admin).await;

        let client = ClientOptions::new()
            .auth("fibril", "fibril")
            .publish_timeout_ms(30_000)
            .connect(&nodes[0].broker)
            .await
            .unwrap();
        let mut after = BTreeSet::new();
        for i in 0..10 {
            let payload = format!("after-{i}");
            publish_confirmed(&client, "orders", &payload).await;
            after.insert(payload);
        }
        // Everything published after recovery arrives, and nothing that was
        // never published does.
        let mut sub = client.subscribe("orders").unwrap().prefetch(16).sub().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut received = BTreeSet::new();
        while !after.is_subset(&received) {
            assert!(Instant::now() < deadline, "missing {:?}", after.difference(&received).collect::<Vec<_>>());
            match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
                Ok(SubEvent::Delivery(msg)) => {
                    let payload = String::from_utf8(msg.payload.clone()).unwrap();
                    assert!(
                        published.contains(&payload) || after.contains(&payload),
                        "delivered a message nobody published: {payload}"
                    );
                    received.insert(payload);
                    let _ = msg.complete().await;
                }
                Ok(SubEvent::Closed(reason)) => panic!("subscription closed: {reason:?}"),
                Err(_) => eprintln!("no delivery for 10s, still waiting"),
            }
        }
        let survived = received.intersection(&published).count();
        eprintln!("{survived} of {} messages from before the power loss survived", published.len());
    })
    .await;
    if std::env::var_os("KEEP_TEST_DATA").is_none() {
        let _ = std::fs::remove_dir_all(&root);
    }
}
