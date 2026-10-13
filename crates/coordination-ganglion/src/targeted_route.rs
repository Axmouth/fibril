//! Queue routing reads retain the committed-state lock and snapshot coherence.

use super::*;

impl GanglionCoordination {
    /// Resolve one queue route without cloning the cluster or constructing topology.
    pub fn client_queue_owner_endpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<(String, u64)> {
        let route = self.client_queue_route(topic, partition, group)?;
        let version = route.partitioning_version;
        route
            .client_endpoints
            .into_iter()
            .next()
            .map(|endpoint| (endpoint, version))
    }

    /// Routing view of one queue partition, with the same gateway rule as
    /// [`GanglionCoordination::client_topology`].
    pub fn client_queue_route(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<ClientRoute> {
        let resource = to_ganglion_resource(&QueueIdentity::new(topic, partition, group));
        let key = queue_partitioning_key(topic, group);
        // Keep this callback limited to lookups and copying route inputs. Parsing,
        // formatting, I/O, and calls back into coordination belong outside the lock.
        let (owner, endpoint, advertised, private, gateway_inputs, partitioning) =
            self.node.read_committed(|state| {
                let assignment = state.assignments.get(&resource)?;
                // Full topology keys endpoints by NodeInfo.node_id, with the last
                // BTreeMap value winning duplicates. Preserve that behavior even
                // for installed snapshots with noncanonical node map keys.
                let node = state
                    .nodes
                    .values()
                    .rev()
                    .find(|node| node.node_id == assignment.owner)?;
                let private = gateway::node_is_private(node);
                // Gateway inputs are only needed for a private owner.
                let gateway_inputs: Vec<ganglion_core::NodeInfo> = if private {
                    state
                        .nodes
                        .values()
                        .filter(|node| !gateway::node_is_private(node))
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };
                Some((
                    assignment.owner.clone(),
                    node.endpoint.clone(),
                    node.labels.get(ADVERTISE_LABEL).cloned(),
                    private,
                    gateway_inputs,
                    state.attributes.get(&key).cloned(),
                ))
            })?;
        // Parsing is outside the state-machine lock; all inputs came from the
        // same state, so owner and partition version remain coherent.
        let partitioning_version = partitioning
            .as_deref()
            .and_then(|raw| serde_json::from_str::<QueuePartitioning>(raw).ok())
            .map(|part| part.partitioning_version)
            .unwrap_or(DEFAULT_PARTITIONING_VERSION);
        let (owner_endpoints, client_endpoints) = if private {
            let candidates = gateway::gateway_candidates(
                gateway_inputs.iter(),
                unix_millis_now(),
                self.gateway_liveness_ttl_ms(),
                node_advertise_endpoints,
            );
            let chosen =
                gateway::rendezvous_gateway(&candidates, topic, group, partition.id())
                    .map(|gateway| gateway.endpoints.clone())
                    .unwrap_or_default();
            (Vec::new(), chosen)
        } else {
            let own = advertise_endpoints(&endpoint, advertised.as_deref());
            (own.clone(), own)
        };
        Some(ClientRoute {
            owner_node_id: owner,
            owner_endpoints,
            client_endpoints,
            partitioning_version,
        })
    }

    /// Routing view of one stream partition. Streams are rare on this path,
    /// so it reads the full topology.
    pub fn client_stream_route(&self, topic: &str, partition: Partition) -> Option<ClientRoute> {
        let topology = self.client_topology();
        let stream = topology
            .streams
            .into_iter()
            .find(|stream| {
                stream.topic == topic
                    && stream.partition == partition
                    && !stream.owner_node_id.is_empty()
            })?;
        let owner_endpoints = if stream.via_gateway {
            Vec::new()
        } else {
            stream.owner_endpoints.clone()
        };
        Some(ClientRoute {
            owner_node_id: stream.owner_node_id,
            owner_endpoints,
            client_endpoints: stream.owner_endpoints,
            partitioning_version: stream.partitioning_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_openraft::{default_raft_config, InProcessRouter};
    use std::collections::BTreeMap;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn targeted_route_keeps_owner_and_version_coherent_during_commits() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let router = InProcessRouter::new();
        let node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        node.initialize(BTreeMap::from([(
            1,
            ganglion_openraft::openraft::BasicNode::new("one"),
        )]))
        .await
        .unwrap();
        node.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let provider = Arc::new(GanglionCoordination::new("owner-a", node));
        let resource = to_ganglion_resource(&QueueIdentity::new("orders", Partition::new(0), None));
        let snapshot = |version: u64| {
            let owner = format!("owner-{}", version % 2);
            let mut state = ganglion_core::CoordinationSnapshot {
                generation: version,
                ..Default::default()
            };
            state.nodes.insert(
                owner.clone(),
                ganglion_core::NodeInfo::new(
                    &owner,
                    format!("owner-{}-v{version}:9000", version % 2),
                    None,
                ),
            );
            state.assignments.insert(
                resource.clone(),
                ganglion_core::PartitionAssignment::new(resource.clone(), owner, vec![], version),
            );
            state.attributes.insert(
                queue_partitioning_key("orders", None),
                format!("{{\"partition_count\":1,\"partitioning_version\":{version}}}"),
            );
            state
        };
        provider
            .consensus_node()
            .write_snapshot(snapshot(1))
            .await
            .unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let provider = provider.clone();
            let done = done.clone();
            readers.push(tokio::spawn(async move {
                let mut reads = 0;
                while !done.load(Ordering::Acquire) {
                    let (endpoint, version) = provider
                        .client_queue_owner_endpoint("orders", Partition::new(0), None)
                        .unwrap();
                    assert_eq!(endpoint, format!("owner-{}-v{version}:9000", version % 2));
                    reads += 1;
                    tokio::task::yield_now().await;
                }
                reads
            }));
        }
        for version in 2..=65 {
            provider
                .consensus_node()
                .write_snapshot(snapshot(version))
                .await
                .unwrap();
        }
        done.store(true, Ordering::Release);
        let mut reads = 0;
        for reader in readers {
            reads += reader.await.unwrap();
        }
        assert!(reads > 0);
        assert_eq!(
            provider.client_queue_owner_endpoint("orders", Partition::new(0), None),
            Some(("owner-1-v65:9000".to_owned(), 65))
        );
        provider.consensus_node().shutdown().await.unwrap();
    }

    fn assert_matches_full(provider: &GanglionCoordination) {
        let full = provider.client_topology();
        for topic in ["orders", "missing"] {
            for partition in [0, 1] {
                for group in [None, Some(""), Some("workers"), Some("absent")] {
                    let expected = full
                        .queues
                        .iter()
                        .find(|queue| {
                            queue.topic == topic
                                && queue.partition.id() == partition
                                && queue.group.as_deref() == group
                        })
                        .and_then(|queue| {
                            queue
                                .owner_endpoints
                                .first()
                                .map(|endpoint| (endpoint.clone(), queue.partitioning_version))
                        });
                    assert_eq!(
                        provider.client_queue_owner_endpoint(
                            topic,
                            Partition::new(partition),
                            group
                        ),
                        expected,
                        "topic={topic} partition={partition} group={group:?}"
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn targeted_route_matches_topology_after_committed_changes() {
        let router = InProcessRouter::new();
        let node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        node.initialize(BTreeMap::from([(
            1,
            ganglion_openraft::openraft::BasicNode::new("one"),
        )]))
        .await
        .unwrap();
        node.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let provider = GanglionCoordination::new("owner-a", node);
        let mut state = ganglion_core::CoordinationSnapshot::default();
        state.nodes.insert(
            "owner-a".into(),
            ganglion_core::NodeInfo::new("owner-a", "a:9000", None),
        );
        state.nodes.insert(
            "owner-b".into(),
            ganglion_core::NodeInfo::new("owner-b", "b:9000", None),
        );
        for group in [None, Some(""), Some("workers")] {
            let resource =
                to_ganglion_resource(&QueueIdentity::new("orders", Partition::new(0), group));
            state.assignments.insert(
                resource.clone(),
                ganglion_core::PartitionAssignment::new(
                    resource,
                    "owner-a",
                    vec!["owner-b".into()],
                    1,
                ),
            );
        }
        for step in 1..=9 {
            state.generation = step;
            match step {
                2 => {
                    state.nodes.get_mut("owner-a").unwrap().labels.insert(
                        ADVERTISE_LABEL.into(),
                        encode_advertise(&["public-a:8000".into(), "backup:8000".into()]),
                    );
                }
                3 => {
                    state.attributes.insert(
                        queue_partitioning_key("orders", Some("workers")),
                        r#"{"partition_count":2,"partitioning_version":19}"#.into(),
                    );
                }
                4 => {
                    for assignment in state.assignments.values_mut() {
                        assignment.owner = "owner-b".into();
                        assignment.epoch += 1;
                    }
                }
                5 => {
                    state
                        .nodes
                        .get_mut("owner-b")
                        .unwrap()
                        .labels
                        .insert(ADVERTISE_LABEL.into(), "invalid".into());
                }
                6 => {
                    state.nodes.get_mut("owner-b").unwrap().endpoint.clear();
                }
                7 => {
                    state.nodes.insert(
                        "z-noncanonical-key".into(),
                        ganglion_core::NodeInfo::new("owner-b", "replacement:9000", None),
                    );
                }
                8 => {
                    state.attributes.insert(
                        queue_partitioning_key("orders", Some("workers")),
                        "invalid".into(),
                    );
                }
                9 => {
                    state.nodes.clear();
                }
                _ => {}
            }
            provider
                .consensus_node()
                .write_snapshot(state.clone())
                .await
                .unwrap();
            assert_matches_full(&provider);
            if step == 3 {
                assert_eq!(
                    provider.client_queue_owner_endpoint(
                        "orders",
                        Partition::new(0),
                        Some("workers")
                    ),
                    Some(("public-a:8000".into(), 19))
                );
            }
            if step == 4 {
                assert_eq!(
                    provider.client_queue_owner_endpoint(
                        "orders",
                        Partition::new(0),
                        Some("workers")
                    ),
                    Some(("b:9000".into(), 19))
                );
            }
        }
        provider.consensus_node().shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn private_owner_routes_through_a_live_public_gateway() {
        let router = InProcessRouter::new();
        let node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        node.initialize(BTreeMap::from([(
            1,
            ganglion_openraft::openraft::BasicNode::new("one"),
        )]))
        .await
        .unwrap();
        node.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let provider = GanglionCoordination::new("public-a", node);
        let mut state = ganglion_core::CoordinationSnapshot {
            generation: 1,
            ..Default::default()
        };
        let mut private = ganglion_core::NodeInfo::new("private-p", "10.0.0.9:9876", None);
        private
            .labels
            .insert(VISIBILITY_LABEL.into(), PRIVATE_VISIBILITY.into());
        state.nodes.insert("private-p".into(), private);
        let mut public = ganglion_core::NodeInfo::new("public-a", "10.0.0.1:9876", None);
        public.labels.insert(
            ADVERTISE_LABEL.into(),
            encode_advertise(&["lb.example:9876".into()]),
        );
        public
            .labels
            .insert(HEARTBEAT_LABEL.into(), unix_millis_now().to_string());
        state.nodes.insert("public-a".into(), public);
        let resource = to_ganglion_resource(&QueueIdentity::new("orders", Partition::new(0), None));
        state.assignments.insert(
            resource.clone(),
            ganglion_core::PartitionAssignment::new(resource, "private-p", vec![], 1),
        );
        provider
            .consensus_node()
            .write_snapshot(state.clone())
            .await
            .unwrap();

        let route = provider
            .client_queue_route("orders", Partition::new(0), None)
            .unwrap();
        assert_eq!(route.owner_node_id, "private-p");
        assert!(route.owner_endpoints.is_empty());
        assert_eq!(route.client_endpoints, vec!["lb.example:9876".to_string()]);
        let full = provider.client_topology();
        assert!(full.queues[0].via_gateway);
        assert_eq!(full.queues[0].owner_endpoints, route.client_endpoints);
        // The private node's own peer address never reaches clients.
        assert!(!format!("{full:?}").contains("10.0.0.9"));
        assert_eq!(
            provider.node_peer_address("private-p").as_deref(),
            Some("10.0.0.9:9876")
        );
        assert_matches_full(&provider);
        assert!(!provider.private_nodes_without_gateway());

        // A dead public broker is no gateway: the route stays owned but has no
        // client endpoints, which clients treat as an unknown owner.
        state.generation = 2;
        state.nodes.get_mut("public-a").unwrap().labels.insert(
            HEARTBEAT_LABEL.into(),
            (unix_millis_now() - 600_000).to_string(),
        );
        provider
            .consensus_node()
            .write_snapshot(state.clone())
            .await
            .unwrap();
        let route = provider
            .client_queue_route("orders", Partition::new(0), None)
            .unwrap();
        assert!(route.client_endpoints.is_empty());
        assert_matches_full(&provider);
        assert!(provider.private_nodes_without_gateway());
        provider.consensus_node().shutdown().await.unwrap();
    }
}
