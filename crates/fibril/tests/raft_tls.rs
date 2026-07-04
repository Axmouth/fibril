//! Coordination consensus over TLS: two raft nodes with certificates from
//! one shared deployment CA (node B's leaf minted from the copied CA pair)
//! elect a leader through the TLS dialer and acceptor seams.

use std::path::PathBuf;
use std::time::Duration;

use fibril::raft_tls::{TlsRaftAcceptor, TlsRaftDialer};
use fibril::tls::{GENERATED_TLS_DIR, TlsMode, build_peer_connector, build_server_tls};
use ganglion::openraft::BasicNode;
use ganglion::{RaftMetadataNode, WireFormat, default_raft_config};

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "fibril-raft-tls-{tag}-{}-{}",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_cluster_forms_over_tls() {
    // Node A generates the deployment CA, node B mints its leaf from the
    // copied CA pair (the shared-CA cluster lane).
    let root_a = temp_root("node-a");
    let tls_a = build_server_tls(&TlsMode::AutoSelfSigned, &root_a, &[])
        .expect("material a")
        .expect("enabled");
    let root_b = temp_root("node-b");
    let gen_b = root_b.join(GENERATED_TLS_DIR);
    std::fs::create_dir_all(&gen_b).expect("b tls dir");
    let gen_a = root_a.join(GENERATED_TLS_DIR);
    std::fs::copy(gen_a.join("ca.pem"), gen_b.join("ca.pem")).expect("copy ca.pem");
    std::fs::copy(gen_a.join("ca.key"), gen_b.join("ca.key")).expect("copy ca.key");
    let tls_b = build_server_tls(&TlsMode::AutoSelfSigned, &root_b, &[])
        .expect("material b")
        .expect("enabled");

    let addr_a = free_loopback_addr();
    let addr_b = free_loopback_addr();

    let (node_a, _server_a) = RaftMetadataNode::start_durable_tcp_with_transport(
        1,
        default_raft_config().expect("raft config"),
        addr_a,
        root_a.join("raft"),
        WireFormat::default(),
        TlsRaftDialer::new(build_peer_connector(None, &root_a).expect("connector a")),
        TlsRaftAcceptor::new(tls_a.acceptor.clone()),
    )
    .await
    .expect("start node a");
    let (node_b, _server_b) = RaftMetadataNode::start_durable_tcp_with_transport(
        2,
        default_raft_config().expect("raft config"),
        addr_b,
        root_b.join("raft"),
        WireFormat::default(),
        TlsRaftDialer::new(build_peer_connector(None, &root_b).expect("connector b")),
        TlsRaftAcceptor::new(tls_b.acceptor.clone()),
    )
    .await
    .expect("start node b");

    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, BasicNode::new(addr_a.to_string()));
    members.insert(2u64, BasicNode::new(addr_b.to_string()));
    node_a.initialize(members).await.expect("initialize");

    // Both nodes agreeing on a leader requires vote and append RPCs to flow
    // through the TLS transport in both directions.
    let leader_a = node_a
        .wait_for_any_leader(Duration::from_secs(15))
        .await
        .expect("leader seen from a");
    let leader_b = node_b
        .wait_for_any_leader(Duration::from_secs(15))
        .await
        .expect("leader seen from b");
    assert_eq!(leader_a, leader_b);
}
