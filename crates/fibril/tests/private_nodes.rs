//! Three real broker processes: one public, two private. The private brokers
//! advertise only an unroutable client address (TEST-NET-1) while their peer
//! addresses work, so a client that ever dialed a private broker would hang
//! and fail the test. The client knows only the public broker, publishes with
//! confirms, consumes and acks through it, and keeps going when the private
//! owner is killed mid-test and its partitions move to the other private
//! broker (and, when placement allows, to the public broker itself).

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
    let config = node_config(root, id, private, &broker, &admin, raft);
    let log = root.join(format!("node-{id}.log"));
    let out = std::fs::File::create(&log).unwrap();
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
    let publisher = client.publisher(topic).unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match publisher.publish_confirmed(NewMessage::text(payload)).await {
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
