use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use fibril_storage::{Group, LogId, Topic};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueIdentity {
    pub topic: Topic,
    pub partition: LogId,
    pub group: Option<Group>,
}

impl QueueIdentity {
    pub fn new(topic: impl Into<Topic>, partition: LogId, group: Option<&str>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            group: group.map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,
    pub broker_addr: SocketAddr,
    pub admin_addr: Option<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub queue: QueueIdentity,
    pub owner: String,
    pub followers: Vec<String>,
    pub epoch: u64,
}

impl PartitionAssignment {
    pub fn new(
        queue: QueueIdentity,
        owner: impl Into<String>,
        followers: Vec<String>,
        epoch: u64,
    ) -> Self {
        Self {
            queue,
            owner: owner.into(),
            followers,
            epoch,
        }
    }

    pub fn is_owned_by(&self, node_id: &str) -> bool {
        self.owner == node_id
    }

    pub fn is_followed_by(&self, node_id: &str) -> bool {
        self.followers.iter().any(|follower| follower == node_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoordinationSnapshot {
    pub nodes: HashMap<String, NodeInfo>,
    pub assignments: HashMap<QueueIdentity, PartitionAssignment>,
    pub generation: u64,
}

impl CoordinationSnapshot {
    pub fn assignment_for(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Option<&PartitionAssignment> {
        self.assignments
            .get(&QueueIdentity::new(topic, partition, group))
    }

    pub fn owned_by(&self, node_id: &str) -> Vec<PartitionAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.is_owned_by(node_id))
            .cloned()
            .collect()
    }

    pub fn followed_by(&self, node_id: &str) -> Vec<PartitionAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.is_followed_by(node_id))
            .cloned()
            .collect()
    }
}

pub type CoordinationStream = watch::Receiver<CoordinationSnapshot>;

pub trait Coordination: std::fmt::Debug + Send + Sync {
    fn node_id(&self) -> &str;
    fn snapshot(&self) -> CoordinationSnapshot;
    fn watch(&self) -> CoordinationStream;

    fn assignment_for(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Option<PartitionAssignment> {
        self.snapshot()
            .assignment_for(topic, partition, group)
            .cloned()
    }

    fn owns_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool {
        self.assignment_for(topic, partition, group)
            .is_some_and(|assignment| assignment.is_owned_by(self.node_id()))
    }

    fn follows_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool {
        self.assignment_for(topic, partition, group)
            .is_some_and(|assignment| assignment.is_followed_by(self.node_id()))
    }

    fn owner_for(&self, topic: &str, partition: LogId, group: Option<&str>) -> Option<NodeInfo> {
        let snapshot = self.snapshot();
        let owner = snapshot
            .assignment_for(topic, partition, group)?
            .owner
            .clone();
        snapshot.nodes.get(&owner).cloned()
    }

    fn follower_assignments(&self) -> Vec<PartitionAssignment> {
        self.snapshot().followed_by(self.node_id())
    }

    fn owned_assignments(&self) -> Vec<PartitionAssignment> {
        self.snapshot().owned_by(self.node_id())
    }
}

#[derive(Debug, Clone)]
pub struct StaticCoordination {
    node_id: String,
    snapshot: Arc<RwLock<CoordinationSnapshot>>,
    tx: watch::Sender<CoordinationSnapshot>,
}

impl StaticCoordination {
    pub fn new(node_id: impl Into<String>, snapshot: CoordinationSnapshot) -> Self {
        let (tx, _rx) = watch::channel(snapshot.clone());
        Self {
            node_id: node_id.into(),
            snapshot: Arc::new(RwLock::new(snapshot)),
            tx,
        }
    }

    pub fn single_node(node_id: impl Into<String>, broker_addr: SocketAddr) -> Self {
        let node_id = node_id.into();
        let mut nodes = HashMap::new();
        nodes.insert(
            node_id.clone(),
            NodeInfo {
                node_id: node_id.clone(),
                broker_addr,
                admin_addr: None,
            },
        );
        Self::new(
            node_id,
            CoordinationSnapshot {
                nodes,
                assignments: HashMap::new(),
                generation: 0,
            },
        )
    }

    pub fn update_snapshot(&self, snapshot: CoordinationSnapshot) {
        *self
            .snapshot
            .write()
            .expect("coordination snapshot poisoned") = snapshot.clone();
        let _ = self.tx.send(snapshot);
    }
}

impl Coordination for StaticCoordination {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.snapshot
            .read()
            .expect("coordination snapshot poisoned")
            .clone()
    }

    fn watch(&self) -> CoordinationStream {
        self.tx.subscribe()
    }
}

/// Single-node default used before cluster coordination is configured.
///
/// This intentionally reports ownership for every queue, matching the current
/// standalone broker behavior.
#[derive(Debug, Clone)]
pub struct NoopCoordination {
    inner: StaticCoordination,
}

impl Default for NoopCoordination {
    fn default() -> Self {
        Self {
            inner: StaticCoordination::single_node(
                "local",
                "127.0.0.1:0".parse().expect("valid loopback socket"),
            ),
        }
    }
}

impl Coordination for NoopCoordination {
    fn node_id(&self) -> &str {
        self.inner.node_id()
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.inner.snapshot()
    }

    fn watch(&self) -> CoordinationStream {
        self.inner.watch()
    }

    fn owns_queue(&self, _topic: &str, _partition: LogId, _group: Option<&str>) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> CoordinationSnapshot {
        let mut nodes = HashMap::new();
        nodes.insert(
            "node-a".to_string(),
            NodeInfo {
                node_id: "node-a".to_string(),
                broker_addr: "127.0.0.1:1001".parse().unwrap(),
                admin_addr: Some("127.0.0.1:2001".parse().unwrap()),
            },
        );
        nodes.insert(
            "node-b".to_string(),
            NodeInfo {
                node_id: "node-b".to_string(),
                broker_addr: "127.0.0.1:1002".parse().unwrap(),
                admin_addr: None,
            },
        );

        let queue = QueueIdentity::new("emails", 0, Some("workers"));
        let mut assignments = HashMap::new();
        assignments.insert(
            queue.clone(),
            PartitionAssignment::new(queue, "node-a", vec!["node-b".to_string()], 7),
        );

        CoordinationSnapshot {
            nodes,
            assignments,
            generation: 42,
        }
    }

    #[test]
    fn static_coordination_reports_owner_and_followers() {
        let coordination = StaticCoordination::new("node-a", snapshot());

        assert!(coordination.owns_queue("emails", 0, Some("workers")));
        assert!(!coordination.follows_queue("emails", 0, Some("workers")));

        let owner = coordination
            .owner_for("emails", 0, Some("workers"))
            .expect("owner node");
        assert_eq!(owner.node_id, "node-a");
        assert_eq!(owner.broker_addr, "127.0.0.1:1001".parse().unwrap());

        let owned = coordination.owned_assignments();
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].epoch, 7);
    }

    #[test]
    fn static_coordination_reports_local_follow_assignments() {
        let coordination = StaticCoordination::new("node-b", snapshot());

        assert!(!coordination.owns_queue("emails", 0, Some("workers")));
        assert!(coordination.follows_queue("emails", 0, Some("workers")));

        let followed = coordination.follower_assignments();
        assert_eq!(followed.len(), 1);
        assert_eq!(followed[0].queue.topic.to_string(), "emails");
        assert_eq!(followed[0].epoch, 7);
    }

    #[tokio::test]
    async fn static_coordination_watch_observes_snapshot_update() {
        let coordination = StaticCoordination::new("node-a", snapshot());
        let mut watch = coordination.watch();

        let mut next = snapshot();
        next.generation = 43;
        coordination.update_snapshot(next);

        watch.changed().await.unwrap();
        assert_eq!(watch.borrow().generation, 43);
    }

    #[test]
    fn noop_coordination_preserves_standalone_owner_behavior() {
        let coordination = NoopCoordination::default();

        assert!(coordination.owns_queue("anything", 99, Some("group")));
        assert!(
            coordination
                .owner_for("anything", 99, Some("group"))
                .is_none()
        );
    }
}
