//! Coordination playground: brokers + embedded raft controller in one process.
//!
//! Three `GanglionCoordination` providers share a raft group. The raft leader
//! acts as controller: it assigns queues across the live brokers with fencing
//! epochs; every provider observes committed assignments through its watch.
//!
//! Usage:
//!   cargo run -p fibril-coordination-ganglion --example coordination_demo -- \
//!       [--script "status; assign orders 2; kill broker-1; assign orders 2; status; quit"]
//!
//! Commands:
//!   status                 per-broker view: live set, assignments, epochs
//!   assign <topic> <parts> controller assigns a queue with that many partitions
//!   kill <broker-id>       drop a broker from the live set (next assign reassigns)
//!   revive <broker-id>     return a broker to the live set
//!   quit

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use fibril_broker::coordination::{
    Coordination, DeterministicPartitionPlacement, NodeInfo, QueueIdentity,
    ReplicationDurabilityPolicy,
};
use fibril_broker::Partition;
use fibril_coordination_ganglion::GanglionCoordination;
use ganglion_openraft::{
    default_raft_config, openraft::BasicNode, InProcessRouter, RaftMetadataNode,
};

struct Playground {
    providers: Vec<GanglionCoordination>,
    live: HashMap<String, NodeInfo>,
    queues: Vec<QueueIdentity>,
}

impl Playground {
    async fn controller(&self) -> Option<&GanglionCoordination> {
        for _ in 0..100 {
            for provider in &self.providers {
                if provider.is_leader().await {
                    return Some(provider);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    async fn status(&self) {
        let live: Vec<&str> = self.live.keys().map(String::as_str).collect();
        println!("live brokers: {live:?}");
        for provider in &self.providers {
            let snapshot = provider.snapshot();
            let role = if provider.is_leader().await {
                "controller"
            } else {
                "standby"
            };
            println!(
                "{} [{role}]: generation={} assignments={}",
                provider.node_id(),
                snapshot.generation,
                snapshot.assignments.len()
            );
            let mut assignments: Vec<_> = snapshot.assignments.values().collect();
            assignments.sort_by_key(|assignment| {
                (assignment.queue.topic.clone(), assignment.queue.partition)
            });
            for assignment in assignments {
                println!(
                    "    {}/{} owner={} followers={:?} epoch={}",
                    assignment.queue.topic,
                    assignment.queue.partition,
                    assignment.owner,
                    assignment.followers,
                    assignment.epoch,
                );
            }
        }
    }

    async fn assign(&mut self, topic: &str, partitions: u32) {
        for partition in 0..partitions {
            let queue = QueueIdentity::new(topic, Partition::new(partition), None);
            if !self.queues.contains(&queue) {
                self.queues.push(queue);
            }
        }
        let Some(controller) = self.controller().await else {
            println!("assign failed: no controller elected");
            return;
        };
        match controller
            .control_iteration(
                &DeterministicPartitionPlacement,
                &self.queues,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
                &self.live,
                8,
            )
            .await
        {
            Ok(Some(snapshot)) => println!(
                "controller {} committed generation {}",
                controller.node_id(),
                snapshot.generation
            ),
            Ok(None) => println!("assign raced a leadership change; retry"),
            Err(error) => println!("assign failed: {error}"),
        }
    }

    fn kill(&mut self, broker: &str) {
        match self.live.remove(broker) {
            Some(_) => println!("{broker} removed from live set (run `assign` to reassign)"),
            None => println!("{broker} is not live"),
        }
    }

    fn revive(&mut self, broker: &str, info: NodeInfo) {
        self.live.insert(broker.to_string(), info);
        println!("{broker} back in the live set");
    }
}

fn broker_info(index: usize) -> NodeInfo {
    NodeInfo {
        node_id: format!("broker-{}", index + 1),
        broker_addr: format!("127.0.0.1:{}", 9000 + index as u16)
            .parse()
            .expect("addr"),
        admin_addr: None,
    }
}

async fn run_command(playground: &mut Playground, line: &str) -> bool {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        [] => {}
        ["status"] => playground.status().await,
        ["assign", topic, partitions] => {
            let partitions: u32 = partitions.parse().unwrap_or(1);
            let topic = topic.to_string();
            playground.assign(&topic, partitions).await;
        }
        ["kill", broker] => playground.kill(broker),
        ["revive", broker] => {
            let index = broker
                .rsplit('-')
                .next()
                .and_then(|raw| raw.parse::<usize>().ok())
                .unwrap_or(1)
                .saturating_sub(1);
            playground.revive(broker, broker_info(index));
        }
        ["quit"] | ["exit"] => return false,
        _ => println!("commands: status | assign <topic> <partitions> | kill <broker> | revive <broker> | quit"),
    }
    true
}

fn main() {
    let script = {
        let mut args = std::env::args().skip(1);
        match (args.next().as_deref(), args.next()) {
            (Some("--script"), Some(script)) => Some(script),
            (None, _) => None,
            _ => {
                eprintln!("usage: coordination_demo [--script \"cmd; cmd\"]");
                std::process::exit(2);
            }
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async {
        let router = InProcessRouter::new();
        let config = default_raft_config().expect("raft config");

        let mut raft_nodes = Vec::new();
        for id in 1..=3u64 {
            raft_nodes.push(
                RaftMetadataNode::start(id, config.clone(), &router)
                    .await
                    .expect("raft node"),
            );
        }
        let members: BTreeMap<u64, BasicNode> = (1..=3u64)
            .map(|id| (id, BasicNode::new(format!("broker-{id}"))))
            .collect();
        raft_nodes[0].initialize(members).await.expect("initialize");
        raft_nodes[0]
            .wait_for_any_leader(Duration::from_secs(10))
            .await
            .expect("election");

        let providers: Vec<GanglionCoordination> = raft_nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| GanglionCoordination::new(format!("broker-{}", index + 1), node))
            .collect();
        let live: HashMap<String, NodeInfo> = (0..3)
            .map(|index| (format!("broker-{}", index + 1), broker_info(index)))
            .collect();

        let mut playground = Playground {
            providers,
            live,
            queues: Vec::new(),
        };
        println!("3 brokers up; raft leader acts as controller");
        playground.status().await;

        match script {
            Some(script) => {
                for command in script.split(';') {
                    let command = command.trim();
                    println!("> {command}");
                    if !run_command(&mut playground, command).await {
                        break;
                    }
                }
            }
            None => {
                use std::io::{BufRead as _, Write as _};
                let stdin = std::io::stdin();
                print!("> ");
                std::io::stdout().flush().ok();
                for line in stdin.lock().lines() {
                    let line = line.expect("stdin read");
                    if !run_command(&mut playground, line.trim()).await {
                        break;
                    }
                    print!("> ");
                    std::io::stdout().flush().ok();
                }
            }
        }

        for provider in &playground.providers {
            let _ = provider.raft_node().shutdown().await;
        }
        println!("playground stopped");
    });
}
