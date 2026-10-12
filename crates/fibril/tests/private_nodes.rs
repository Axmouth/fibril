//! Three real broker processes: one public, two private. The private brokers
//! advertise only an unroutable client address (TEST-NET-1) while their peer
//! addresses work, so a client that ever dialed a private broker would hang
//! and fail the test. The client knows only the public broker, publishes with
//! confirms, consumes and acks through it, and keeps going when the private
//! owner is killed mid-test and its partitions move to the other private
//! broker (and, when placement allows, to the public broker itself).
//!
//! A second scenario covers more of the client surface through the public
//! broker: an exclusive consumer group and a restart of the public broker
//! itself, after which the owner redelivers what the old session held. A
//! third, ignored for now, grows a queue owned by a private broker while the
//! client publishes and consumes. Streams through a gateway are covered against a real owner
//! in crates/protocol/tests/forwarding_tests.rs.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fibril_client::{
    Client, ClientOptions, FibrilError, NewMessage, QueueConfig, SubEvent, Subscription,
};

use tokio::process::{Child, Command};

#[allow(dead_code)]
mod common;

const SECRET: &str = "private-nodes-test-secret";
const UNROUTABLE: &str = "192.0.2.10:9876";

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

fn node_config(
    root: &Path,
    id: u64,
    private: bool,
    broker: &str,
    admin: &str,
    raft: &[String; 3],
) -> PathBuf {
    let visibility = if private {
        format!("visibility = \"private\"\npeer_advertise = \"{broker}\"\n")
    } else {
        String::new()
    };
    let advertise = if private { UNROUTABLE } else { broker };
    let toml = format!(
        r#"[server]
data_dir = "{data}"

[broker]
{visibility}
[broker.listener]
bind = "{broker}"
advertise = ["{advertise}"]

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
target_followers = 1

[coordination.ganglion.peers]
"1" = "{r1}"
"2" = "{r2}"
"3" = "{r3}"

[coordination.ganglion.assignment_durability]
mode = "majority_durable"
"#,
        data = root.join(format!("data-{id}")).display(),
        listen = raft[id as usize - 1],
        bootstrap = id == 1,
        r1 = raft[0],
        r2 = raft[1],
        r3 = raft[2],
    );
    let path = root.join(format!("node-{id}.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

fn spawn_node(root: &Path, id: u64, private: bool, raft: &[String; 3]) -> Node {
    let broker = common::free_loopback_addr().to_string();
    let admin = common::free_loopback_addr().to_string();
    spawn_node_on(root, id, private, raft, broker, admin)
}

/// Start node `id` on fixed addresses, also to restart it in place.
fn spawn_node_on(
    root: &Path,
    id: u64,
    private: bool,
    raft: &[String; 3],
    broker: String,
    admin: String,
) -> Node {
    let config = node_config(root, id, private, &broker, &admin, raft);
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

async fn admin_topology(admin: &str) -> Option<serde_json::Value> {
    let response = reqwest::get(format!("http://{admin}/admin/api/topology"))
        .await
        .ok()?;
    response.json::<serde_json::Value>().await.ok()
}

/// Owner and followers of each assigned queue partition 0, by topic.
async fn assignments(admin: &str) -> HashMap<String, (String, Vec<String>)> {
    let Some(topology) = admin_topology(admin).await else {
        return HashMap::new();
    };
    topology["coordination"]["assignments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|a| a["partition"].as_u64() == Some(0))
        .map(|a| {
            let followers = a["followers"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|f| f.as_str().map(str::to_string))
                .collect();
            (
                a["topic"].as_str().unwrap_or_default().to_string(),
                (
                    a["owner"].as_str().unwrap_or_default().to_string(),
                    followers,
                ),
            )
        })
        .collect()
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

/// Publish with confirms through the public broker until it confirms. A
/// `504` means the outcome is unknown, so the test publishes the payload
/// again and tolerates the duplicate, which is what an application would do.
async fn publish_until_confirmed(client: &Client, topic: &str, payload: &str) {
    publish_message_until_confirmed(client, topic, NewMessage::text(payload), payload).await;
}

async fn publish_message_until_confirmed(
    client: &Client,
    topic: &str,
    message: NewMessage,
    payload: &str,
) {
    let publisher = client.publisher(topic).unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match publisher.publish_confirmed(message.clone()).await {
            Ok(_) => return,
            Err(err) => {
                assert!(
                    Instant::now() < deadline,
                    "publish of {payload} never confirmed: {err}"
                );
                let unknown = matches!(
                    err,
                    FibrilError::Failure {
                        code: fibril_protocol::v1::ERR_GATEWAY_TIMEOUT,
                        ..
                    }
                );
                eprintln!("publish {payload} failed (unknown outcome: {unknown}): {err}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_owner_is_served_through_the_public_broker_across_a_failover() {
    let _permit = common::BOOT_PERMITS.acquire_many(3).await.unwrap();
    let root = common::temp_root("fibril-private-nodes", "cluster");
    let raft: [String; 3] = std::array::from_fn(|_| common::free_loopback_addr().to_string());
    let mut nodes = vec![
        spawn_node(&root, 1, false, &raft),
        spawn_node(&root, 2, true, &raft),
        spawn_node(&root, 3, true, &raft),
    ];
    let public = nodes[0].broker.clone();
    let public_admin = nodes[0].admin.clone();

    let result = tokio::time::timeout(
        Duration::from_secs(240),
        run_scenario(&mut nodes, &public, &public_admin),
    )
    .await;
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

async fn run_scenario(nodes: &mut [Node], public: &str, public_admin: &str) {
    // All three brokers registered and the raft group up.
    wait_until(
        "three registered brokers",
        Duration::from_secs(60),
        || async {
            admin_topology(public_admin).await.is_some_and(|t| {
                t["coordination"]["nodes"]
                    .as_array()
                    .is_some_and(|nodes| nodes.len() == 3)
            })
        },
    )
    .await;

    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .publish_timeout_ms(30_000)
        .connect(public)
        .await
        .expect("connect to the public broker");

    let topics: Vec<String> = (0..12).map(|i| format!("private.jobs.{i}")).collect();
    for topic in &topics {
        let mut declared = false;
        for _ in 0..50 {
            match client.declare_queue(QueueConfig::new(topic).unwrap()).await {
                Ok(()) => {
                    declared = true;
                    break;
                }
                Err(err) => {
                    eprintln!("declare {topic}: {err}");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        }
        assert!(declared, "could not declare {topic}");
    }

    // Pick a private owner with a queue whose only follower is the other
    // private broker, so the failover moves it to a private node the client
    // still cannot reach. A queue of the same owner followed by the public
    // broker is used too when there is one: its failover lands on the
    // gateway itself.
    let deadline = Instant::now() + Duration::from_secs(60);
    let (owner, chosen) = loop {
        let assigned = assignments(public_admin).await;
        let pick = ["node-2", "node-3"].iter().find_map(|owner| {
            let other = if *owner == "node-2" {
                "node-3"
            } else {
                "node-2"
            };
            let followed_by = |follower: &str| {
                topics.iter().find(|topic| {
                    assigned
                        .get(*topic)
                        .is_some_and(|(o, f)| o == owner && f.len() == 1 && f[0] == follower)
                })
            };
            let to_private = followed_by(other)?;
            let mut chosen = vec![to_private.clone()];
            if let Some(to_public) = followed_by("node-1") {
                chosen.push(to_public.clone());
            }
            Some((owner.to_string(), chosen))
        });
        if let Some(pick) = pick {
            break pick;
        }
        assert!(
            Instant::now() < deadline,
            "no queue with a private owner and a private follower: {assigned:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    eprintln!("using {chosen:?}, owned by private {owner}");

    // Topology sends the client to the public broker for every queue.
    let topology = client.fetch_topology().await.expect("topology");
    for entry in &topology.queues {
        for endpoint in &entry.owner_endpoints {
            assert_eq!(
                endpoint.target(),
                public,
                "topology named a private broker for {}",
                entry.topic
            );
        }
    }

    let mut subs = Vec::new();
    for topic in &chosen {
        let sub = client
            .subscribe(topic)
            .unwrap()
            .prefetch(4)
            .sub()
            .await
            .expect("subscribe through the public broker");
        subs.push(sub);
    }

    for (topic, sub) in chosen.iter().zip(subs.iter_mut()) {
        let mut confirmed = BTreeSet::new();
        for i in 0..10 {
            let payload = format!("{topic}/before-{i}");
            publish_until_confirmed(&client, topic, &payload).await;
            confirmed.insert(payload);
        }
        consume_until(sub, &confirmed, Duration::from_secs(30)).await;
    }
    eprintln!("confirmed, consumed and acked through the public broker");

    // The public broker forwarded, the owner served a forwarded session.
    let public_log = std::fs::read_to_string(&nodes[0].log).unwrap_or_default();
    assert!(
        public_log.contains("upstream session opened"),
        "no upstream session on the gateway"
    );
    let owner_log = nodes
        .iter()
        .find(|node| node.name == owner)
        .map(|node| std::fs::read_to_string(&node.log).unwrap_or_default())
        .unwrap_or_default();
    assert!(
        owner_log.contains("forwarded session opened"),
        "the owner served no forwarded session"
    );

    // Kill the private owner. The controller moves each partition to its
    // follower and the client keeps working through the public broker.
    let victim = nodes
        .iter_mut()
        .find(|node| node.name == owner)
        .expect("owner process");
    victim
        .child
        .take()
        .unwrap()
        .kill()
        .await
        .expect("kill owner");
    eprintln!("killed {owner}");

    for topic in &chosen {
        wait_until(
            "the partition to move off the dead owner",
            Duration::from_secs(90),
            || {
                let topic = topic.clone();
                let owner = owner.clone();
                async move {
                    assignments(public_admin)
                        .await
                        .get(&topic)
                        .is_some_and(|(now, _)| !now.is_empty() && *now != owner)
                }
            },
        )
        .await;
        let new_owner = assignments(public_admin).await[topic].0.clone();
        eprintln!("{topic} moved to {new_owner}");
    }

    for (topic, sub) in chosen.iter().zip(subs.iter_mut()) {
        let mut after = BTreeSet::new();
        for i in 0..10 {
            let payload = format!("{topic}/after-{i}");
            publish_until_confirmed(&client, topic, &payload).await;
            after.insert(payload);
        }
        // Every confirmed publish is delivered. Duplicates are allowed after
        // a failover or a retried unknown outcome.
        consume_until(sub, &after, Duration::from_secs(90)).await;
        eprintln!("{topic}: all post-failover messages delivered");
    }

    let topology = client.fetch_topology().await.expect("topology");
    for entry in &topology.queues {
        for endpoint in &entry.owner_endpoints {
            assert_eq!(endpoint.target(), public);
        }
    }
    client.shutdown().await;
}

/// Receive and ack until every payload in `want` has arrived.
async fn consume_until(sub: &mut Subscription, want: &BTreeSet<String>, budget: Duration) {
    let deadline = Instant::now() + budget;
    let mut received = BTreeSet::new();
    while !want.is_subset(&received) {
        assert!(
            Instant::now() < deadline,
            "missing confirmed messages: {:?}",
            want.difference(&received).collect::<Vec<_>>()
        );
        match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
            Ok(SubEvent::Delivery(msg)) => {
                received.insert(String::from_utf8(msg.payload.clone()).unwrap());
                let _ = msg.complete().await;
            }
            Ok(SubEvent::Closed(reason)) => panic!("subscription closed for good: {reason:?}"),
            Err(_) => eprintln!("no delivery for 10s, still waiting"),
        }
    }
}

const PRIVATE_NODES: [&str; 2] = ["node-1", "node-2"];

/// Wait until one of `topics` has a private owner, and return it.
async fn topic_with_private_owner<F, Fut>(what: &str, topics: &[String], owners: F) -> String
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = HashMap<String, String>>,
{
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let owners = owners().await;
        if let Some(topic) = topics.iter().find(|topic| {
            owners
                .get(*topic)
                .is_some_and(|owner| PRIVATE_NODES.contains(&owner.as_str()))
        }) {
            return topic.clone();
        }
        assert!(
            Instant::now() < deadline,
            "no {what} with a private owner: {owners:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn declare_queues(client: &Client, topics: &[String]) {
    for topic in topics {
        let mut declared = false;
        for _ in 0..50 {
            match client.declare_queue(QueueConfig::new(topic).unwrap()).await {
                Ok(()) => {
                    declared = true;
                    break;
                }
                Err(err) => {
                    eprintln!("declare {topic}: {err}");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        }
        assert!(declared, "could not declare {topic}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclusive_group_and_a_public_restart_through_the_gateway() {
    run_surface_test("surface", false).await;
}

/// A live grow of a privately owned queue while the client publishes and
/// consumes through the public broker. Ignored: after the grow, deliveries
/// of messages that land on the source partition can stop for good. That
/// also happens when the source partition is owned by the public broker
/// itself, so with no forwarding involved, and needs its own fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "post-grow deliveries on the source partition can stall, with or without forwarding"]
async fn repartition_while_forwarding() {
    run_surface_test("grow", true).await;
}

async fn run_surface_test(tag: &str, with_repartition: bool) {
    let _permit = common::BOOT_PERMITS.acquire_many(3).await.unwrap();
    let root = common::temp_root("fibril-private-nodes", tag);
    let raft: [String; 3] = std::array::from_fn(|_| common::free_loopback_addr().to_string());
    // The public broker is node 3, so restarting it never restarts the
    // node that bootstraps the raft group.
    let mut nodes = vec![
        spawn_node(&root, 1, true, &raft),
        spawn_node(&root, 2, true, &raft),
        spawn_node(&root, 3, false, &raft),
    ];

    let result = tokio::time::timeout(
        Duration::from_secs(420),
        run_surface_scenario(&root, &raft, &mut nodes, with_repartition),
    )
    .await;
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

async fn run_surface_scenario(
    root: &Path,
    raft: &[String; 3],
    nodes: &mut Vec<Node>,
    with_repartition: bool,
) {
    let public = nodes[2].broker.clone();
    let public_admin = nodes[2].admin.clone();
    wait_until(
        "three registered brokers",
        Duration::from_secs(60),
        || async {
            admin_topology(&public_admin).await.is_some_and(|t| {
                t["coordination"]["nodes"]
                    .as_array()
                    .is_some_and(|nodes| nodes.len() == 3)
            })
        },
    )
    .await;
    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .publish_timeout_ms(30_000)
        .connect(&public)
        .await
        .expect("connect to the public broker");

    // ---- Exclusive consumer group --------------------------------------
    let cohorts: Vec<String> = (0..8).map(|i| format!("private.cohort.{i}")).collect();
    declare_queues(&client, &cohorts).await;
    let cohort = topic_with_private_owner("queue", &cohorts, || async {
        assignments(&public_admin)
            .await
            .into_iter()
            .map(|(topic, (owner, _))| (topic, owner))
            .collect()
    })
    .await;
    let mut group = client
        .subscribe(&cohort)
        .unwrap()
        .exclusive()
        .prefetch(4)
        .sub()
        .await
        .expect("exclusive group subscribe through the public broker");
    let mut want = BTreeSet::new();
    for i in 0..5 {
        let payload = format!("{cohort}/{i}");
        publish_until_confirmed(&client, &cohort, &payload).await;
        want.insert(payload);
    }
    consume_until(&mut group, &want, Duration::from_secs(60)).await;
    eprintln!("exclusive group consumes through the public broker");

    // ---- Repartition while forwarding ----------------------------------
    let grows: Vec<String> = (0..8).map(|i| format!("private.grow.{i}")).collect();
    declare_queues(&client, &grows).await;
    let grow = topic_with_private_owner("queue", &grows, || async {
        assignments(&public_admin)
            .await
            .into_iter()
            .map(|(topic, (owner, _))| (topic, owner))
            .collect()
    })
    .await;
    if with_repartition {
        // A consumer settles deliveries as they come while the test publishes,
        // as an application would.
        let sub = client
            .subscribe(&grow)
            .unwrap()
            .prefetch(8)
            .sub()
            .await
            .expect("subscribe the queue to grow");
        let received = std::sync::Arc::new(std::sync::Mutex::new(BTreeSet::new()));
        let consumer = tokio::spawn({
            let received = received.clone();
            let mut sub = sub;
            async move {
                loop {
                    match sub.recv().await {
                        SubEvent::Delivery(msg) => {
                            received
                                .lock()
                                .unwrap()
                                .insert(String::from_utf8(msg.payload.clone()).unwrap());
                            let _ = msg.complete().await;
                        }
                        SubEvent::Closed(reason) => {
                            panic!("subscription closed for good: {reason:?}")
                        }
                    }
                }
            }
        });
        let mut want = BTreeSet::new();
        for i in 0..30 {
            if i == 10 {
                let response = reqwest::Client::new()
                    .post(format!("http://{public_admin}/admin/api/repartition"))
                    .basic_auth("fibril", Some("fibril"))
                    .json(&serde_json::json!({ "topic": grow, "partition_count": 3 }))
                    .send()
                    .await
                    .expect("repartition request");
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                assert!(status.is_success(), "repartition failed: {status} {body}");
                eprintln!("grew {grow} to 3 partitions while forwarding");
                // The client learns the new partitioning from a topology push or
                // a fetch before it publishes again.
                wait_until(
                    "the client to see three partitions",
                    Duration::from_secs(60),
                    || async {
                        let _ = client.fetch_topology().await;
                        client
                            .catalogue()
                            .queues
                            .iter()
                            .any(|queue| queue.topic == grow && queue.partition_count == 3)
                    },
                )
                .await;
            }
            let payload = format!("{grow}/{i}");
            publish_until_confirmed(&client, &grow, &payload).await;
            want.insert(payload);
        }
        // Every confirmed publish is delivered across the grow, the new
        // partitions included.
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let missing: Vec<String> = {
                let received = received.lock().unwrap();
                want.difference(&received).cloned().collect()
            };
            if missing.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "confirmed publishes never delivered across the repartition: {missing:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        consumer.abort();
        eprintln!("all messages delivered across the repartition");
    }

    // ---- Public broker restart -----------------------------------------
    let others: Vec<String> = grows
        .iter()
        .filter(|topic| **topic != grow)
        .cloned()
        .collect();
    let restart = topic_with_private_owner("queue", &others, || async {
        assignments(&public_admin)
            .await
            .into_iter()
            .map(|(topic, (owner, _))| (topic, owner))
            .collect()
    })
    .await;
    let mut sub = client
        .subscribe(&restart)
        .unwrap()
        .prefetch(8)
        .sub()
        .await
        .expect("subscribe before the restart");
    publish_until_confirmed(&client, &restart, "held").await;
    let held = loop {
        match tokio::time::timeout(Duration::from_secs(30), sub.recv()).await {
            Ok(SubEvent::Delivery(msg)) if msg.payload == b"held" => break msg,
            Ok(SubEvent::Delivery(msg)) => {
                let _ = msg.complete().await;
            }
            Ok(SubEvent::Closed(reason)) => panic!("subscription closed: {reason:?}"),
            Err(_) => panic!("the held message never arrived"),
        }
    };
    let public_node = nodes.pop().expect("public node");
    let (broker, admin) = (public_node.broker.clone(), public_node.admin.clone());
    let mut public_node = public_node;
    public_node
        .child
        .take()
        .unwrap()
        .kill()
        .await
        .expect("kill the public broker");
    drop(public_node);
    eprintln!("killed the public broker");
    tokio::time::sleep(Duration::from_secs(1)).await;
    nodes.push(spawn_node_on(root, 3, false, raft, broker, admin));
    // The held delivery cannot settle into a session that is gone.
    drop(held);

    // The client reconnects through the same address, its supervisor
    // resubscribes, and the owner redelivers what the old session held.
    let mut want = BTreeSet::from(["held".to_string()]);
    for i in 0..5 {
        let payload = format!("{restart}/after-restart-{i}");
        publish_until_confirmed(&client, &restart, &payload).await;
        want.insert(payload);
    }
    consume_until(&mut sub, &want, Duration::from_secs(120)).await;
    eprintln!("the held message redelivered and new ones arrived after the restart");

    let topology = client.fetch_topology().await.expect("topology");
    for entry in &topology.queues {
        for endpoint in &entry.owner_endpoints {
            assert_eq!(endpoint.target(), public);
        }
    }
    client.shutdown().await;
}
