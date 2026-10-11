//! Gateway choice for partitions owned by private brokers.
//!
//! A private broker publishes no client endpoints. Clients are sent to one
//! live, non-draining public broker instead, chosen by rendezvous hashing of
//! the partition over the public node ids. Every broker computes the same
//! answer from the same committed state, the load spreads over the public
//! brokers, and a public broker joining or leaving moves only its share.

use super::{DRAINING_LABEL, HEARTBEAT_LABEL, PRIVATE_VISIBILITY, VISIBILITY_LABEL};

/// A public broker that can serve as a gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayCandidate {
    pub node_id: String,
    /// The broker's client endpoints in priority order. Never empty.
    pub endpoints: Vec<String>,
}

/// Whether a node carries the private visibility label.
pub fn node_is_private(node: &ganglion_core::NodeInfo) -> bool {
    node.labels.get(VISIBILITY_LABEL).map(String::as_str) == Some(PRIVATE_VISIBILITY)
}

/// Whether a node's last heartbeat is within `ttl_ms` of `now_ms`. A node with
/// no heartbeat label counts as live, matching `GanglionCoordination::live_nodes`.
pub fn node_is_live(node: &ganglion_core::NodeInfo, now_ms: u64, ttl_ms: u64) -> bool {
    node.labels
        .get(HEARTBEAT_LABEL)
        .and_then(|raw| raw.parse::<u64>().ok())
        .is_none_or(|beat| now_ms.saturating_sub(beat) <= ttl_ms)
}

/// Live, non-draining public nodes with a client list, sorted by node id.
/// `endpoints` maps a node to its client list (see `node_advertise_endpoints`).
pub fn gateway_candidates<'a>(
    nodes: impl IntoIterator<Item = &'a ganglion_core::NodeInfo>,
    now_ms: u64,
    ttl_ms: u64,
    endpoints: impl Fn(&ganglion_core::NodeInfo) -> Vec<String>,
) -> Vec<GatewayCandidate> {
    let mut out: Vec<GatewayCandidate> = nodes
        .into_iter()
        .filter(|node| {
            !node_is_private(node)
                && !node.labels.contains_key(DRAINING_LABEL)
                && node_is_live(node, now_ms, ttl_ms)
        })
        .filter_map(|node| {
            let endpoints = endpoints(node);
            (!endpoints.is_empty()).then(|| GatewayCandidate {
                node_id: node.node_id.clone(),
                endpoints,
            })
        })
        .collect();
    out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    out.dedup_by(|a, b| a.node_id == b.node_id);
    out
}

/// The gateway for one partition: the candidate with the highest rendezvous
/// score. `None` when there is no candidate.
pub fn rendezvous_gateway<'a>(
    candidates: &'a [GatewayCandidate],
    topic: &str,
    group: Option<&str>,
    partition: u32,
) -> Option<&'a GatewayCandidate> {
    candidates
        .iter()
        .max_by_key(|candidate| {
            (
                rendezvous_score(topic, group, partition, &candidate.node_id),
                // Ties are vanishingly rare; break them by id so the answer
                // never depends on input order.
                std::cmp::Reverse(candidate.node_id.as_str()),
            )
        })
}

/// A stable 64-bit score. Hand-rolled FNV-1a with a splitmix64 finalizer so
/// the value never changes across Rust versions or processes, which a
/// mixed-version cluster needs to agree on the gateway.
fn rendezvous_score(topic: &str, group: Option<&str>, partition: u32, node_id: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
    };
    // Length prefixes keep ("ab", "c") and ("a", "bc") apart.
    for part in [Some(topic), group, Some(node_id)] {
        match part {
            Some(value) => {
                feed(&[1]);
                feed(&(value.len() as u64).to_be_bytes());
                feed(value.as_bytes());
            }
            None => feed(&[0]),
        }
    }
    feed(&partition.to_be_bytes());
    // splitmix64 finalizer spreads FNV's weak low bits.
    let mut z = hash.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, endpoint: &str) -> ganglion_core::NodeInfo {
        ganglion_core::NodeInfo::new(id, endpoint, None)
    }

    fn candidate(id: &str) -> GatewayCandidate {
        GatewayCandidate {
            node_id: id.into(),
            endpoints: vec![format!("{id}:9876")],
        }
    }

    fn own_endpoint(node: &ganglion_core::NodeInfo) -> Vec<String> {
        if node.endpoint.is_empty() {
            Vec::new()
        } else {
            vec![node.endpoint.clone()]
        }
    }

    #[test]
    fn score_is_pinned_so_every_version_agrees() {
        // A changed hash would split a mixed-version cluster's gateway choice.
        assert_eq!(
            rendezvous_score("orders", None, 0, "broker-1"),
            rendezvous_score("orders", None, 0, "broker-1")
        );
        assert_ne!(
            rendezvous_score("ab", Some("c"), 0, "n"),
            rendezvous_score("a", Some("bc"), 0, "n")
        );
        assert_ne!(
            rendezvous_score("orders", None, 0, "n"),
            rendezvous_score("orders", Some(""), 0, "n")
        );
        assert_eq!(
            rendezvous_score("orders", Some("workers"), 3, "broker-2"),
            0xeaab_a035_ffd4_70e1,
            "pinned value changed"
        );
    }

    #[test]
    fn choice_ignores_candidate_order() {
        let forward = vec![candidate("a"), candidate("b"), candidate("c")];
        let mut reversed = forward.clone();
        reversed.reverse();
        for partition in 0..64 {
            assert_eq!(
                rendezvous_gateway(&forward, "orders", None, partition).map(|c| &c.node_id),
                rendezvous_gateway(&reversed, "orders", None, partition).map(|c| &c.node_id),
            );
        }
        assert!(rendezvous_gateway(&[], "orders", None, 0).is_none());
    }

    #[test]
    fn choice_spreads_and_moves_only_the_leaving_share() {
        let all = vec![candidate("a"), candidate("b"), candidate("c")];
        let without_b: Vec<_> = all.iter().filter(|c| c.node_id != "b").cloned().collect();
        let mut counts = std::collections::HashMap::new();
        for partition in 0..300 {
            let before = rendezvous_gateway(&all, "orders", None, partition)
                .unwrap()
                .node_id
                .clone();
            *counts.entry(before.clone()).or_insert(0) += 1;
            let after = rendezvous_gateway(&without_b, "orders", None, partition)
                .unwrap()
                .node_id
                .clone();
            if before != "b" {
                assert_eq!(before, after, "partition {partition} moved without cause");
            }
        }
        for id in ["a", "b", "c"] {
            assert!(counts[id] > 50, "{id} got {} of 300", counts[id]);
        }
    }

    #[test]
    fn candidates_skip_private_draining_dead_and_unreachable_nodes() {
        let now = 1_000_000;
        let mut public = node("pub", "pub:9876");
        public
            .labels
            .insert(HEARTBEAT_LABEL.into(), (now - 100).to_string());
        let mut private = node("priv", "priv:9876");
        private
            .labels
            .insert(VISIBILITY_LABEL.into(), PRIVATE_VISIBILITY.into());
        let mut draining = node("drain", "drain:9876");
        draining.labels.insert(DRAINING_LABEL.into(), "1".into());
        let mut dead = node("dead", "dead:9876");
        dead.labels
            .insert(HEARTBEAT_LABEL.into(), (now - 10_000).to_string());
        let no_endpoint = node("blank", "");
        let no_heartbeat = node("fresh", "fresh:9876");
        let nodes = [public, private, draining, dead, no_endpoint, no_heartbeat];
        let candidates = gateway_candidates(nodes.iter(), now, 5_000, own_endpoint);
        let ids: Vec<_> = candidates.iter().map(|c| c.node_id.as_str()).collect();
        assert_eq!(ids, vec!["fresh", "pub"]);
    }

    #[test]
    fn shared_address_gateways_resolve_to_that_address() {
        let shared = |id: &str| GatewayCandidate {
            node_id: id.into(),
            endpoints: vec!["lb.example:9876".into()],
        };
        let candidates = vec![shared("a"), shared("b")];
        for partition in 0..16 {
            assert_eq!(
                rendezvous_gateway(&candidates, "orders", None, partition)
                    .unwrap()
                    .endpoints,
                vec!["lb.example:9876".to_string()]
            );
        }
    }
}
