//! Data dir layout at server startup: fresh dirs are initialized, foreign or
//! older layouts are refused before anything is written, first-boot setup
//! writes into node/, and a killed server restarts on the same data dir.

mod common;

use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use fibril::{FibrilServerError, run_server_from_config};
use fibril_config::node_layout::{self, NodeLayoutError};
use fibril_config::{NodeLayout, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;

fn temp_root(tag: &str) -> PathBuf {
    common::temp_root("fibril-node-layout", tag)
}

fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

fn config_for(data_dir: &Path) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.server.data_dir = data_dir.to_path_buf();
    config.broker.listener.bind = common::free_loopback_addr();
    config.admin.listener.bind = common::free_loopback_addr();
    config
}

async fn startup_error(config: ServerConfig) -> FibrilServerError {
    tokio::time::timeout(Duration::from_secs(10), run_server_from_config(config))
        .await
        .expect("startup must fail fast")
        .expect_err("startup must be refused")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_layout_data_dir_refuses_startup_without_writing() {
    let root = temp_root("old");
    let data_dir = root.join("data");
    for dir in ["events", "messages", "global", "tls", "coordination"] {
        std::fs::create_dir_all(data_dir.join(dir)).expect("legacy dir");
    }
    std::fs::write(data_dir.join("setup_complete"), "").expect("legacy marker");
    let before = entries(&data_dir);

    let error = startup_error(config_for(&data_dir)).await;
    assert!(
        matches!(
            error,
            FibrilServerError::NodeLayout(NodeLayoutError::OldLayout { .. })
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("older Fibril data layout"),
        "{error}"
    );
    assert_eq!(
        entries(&data_dir),
        before,
        "the old data dir is left untouched"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_top_level_entry_refuses_startup() {
    let root = temp_root("unknown");
    let data_dir = root.join("data");
    NodeLayout::open_or_init(&data_dir, "local").expect("init");
    std::fs::write(data_dir.join("stray.bin"), "x").expect("stray file");

    let error = startup_error(config_for(&data_dir)).await;
    assert!(
        matches!(
            error,
            FibrilServerError::NodeLayout(NodeLayoutError::UnknownEntries { .. })
        ),
        "{error}"
    );
    assert!(error.to_string().contains("stray.bin"), "{error}");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_node_data_dir_refuses_startup() {
    let root = temp_root("foreign");
    let data_dir = root.join("data");
    NodeLayout::open_or_init(&data_dir, "broker-a").expect("init as broker-a");

    let mut config = config_for(&data_dir);
    config.coordination.node_id = "broker-b".into();
    let error = startup_error(config).await;
    assert!(
        matches!(
            &error,
            FibrilServerError::NodeLayout(NodeLayoutError::ForeignNode { found, expected, .. })
                if found == "broker-a" && expected == "broker-b"
        ),
        "{error}"
    );
    // Storage was never opened for the wrong node.
    assert!(entries(&data_dir.join("storage")).is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_and_found_only_data_dir_boots_and_keeps_it() {
    let booted = common::boot_server("fibril-node-layout", "lost-found", false, |config| {
        std::fs::create_dir_all(config.server.data_dir.join("lost+found")).expect("lost+found");
    })
    .await;
    assert_eq!(
        entries(&booted.data_dir),
        ["cache", "fibril-node", "lost+found", "node", "storage"]
    );
    booted.handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_tls_generation_is_redone_on_restart() {
    // A previous boot crashed while generating material: the layout exists,
    // node/ holds a half-written staging dir and there is no tls/ yet.
    let booted = common::boot_server("fibril-node-layout", "tls-crash", false, |config| {
        config.tls.enabled = true;
        config.tls.auto_self_signed = true;
        config.tls.admin_enabled = Some(false);
        let layout =
            NodeLayout::open_or_init(&config.server.data_dir, &config.coordination.node_id)
                .expect("init");
        let staging = layout.node().join(".tls.tmp");
        std::fs::create_dir_all(&staging).expect("staging dir");
        std::fs::write(staging.join("ca.pem"), "half written").expect("partial file");
    })
    .await;

    let layout = NodeLayout::new(&booted.data_dir);
    assert_eq!(entries(&layout.node()), ["tls"]);
    assert_eq!(
        entries(&layout.generated_tls()),
        ["ca.key", "ca.pem", "server.key", "server.pem"]
    );
    tls_handshake(booted.broker_addr, &layout.generated_tls().join("ca.pem")).await;
    booted.handle.abort();
}

fn tls_connector(ca_pem: &Path) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    let file = std::fs::File::open(ca_pem).expect("open ca.pem");
    for cert in rustls_pemfile::certs(&mut BufReader::new(file)) {
        roots.add(cert.expect("parse ca cert")).expect("add root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

async fn tls_handshake(addr: std::net::SocketAddr, ca_pem: &Path) {
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    tls_connector(ca_pem)
        .connect(name, tcp)
        .await
        .expect("tls handshake against the generated CA");
}

/// A `fibril-server` process. Killed on drop so a failed assertion does not
/// leak it.
struct ServerProcess {
    child: Child,
    log: PathBuf,
    broker_addr: std::net::SocketAddr,
    admin_addr: std::net::SocketAddr,
}

impl ServerProcess {
    fn spawn(data_dir: &Path, log: PathBuf) -> Self {
        let broker_addr = common::free_loopback_addr();
        let admin_addr = common::free_loopback_addr();
        let out = std::fs::File::create(&log).expect("log file");
        let err = out.try_clone().expect("log file clone");
        let child = Command::new(env!("CARGO_BIN_EXE_fibril-server"))
            .env_clear()
            .env("FIBRIL_DATA_DIR", data_dir)
            .env("FIBRIL_BROKER_BIND", broker_addr.to_string())
            .env("FIBRIL_ADMIN_BIND", admin_addr.to_string())
            .env("FIBRIL_SETUP_MODE", "true")
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .spawn()
            .expect("spawn fibril-server");
        Self {
            child,
            log,
            broker_addr,
            admin_addr,
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Wait for `addr` to accept connections while the process is alive.
    async fn wait_for(&mut self, addr: std::net::SocketAddr) {
        for _ in 0..1200 {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                panic!("fibril-server exited with {status}:\n{}", self.log_text());
            }
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("{addr} never came up:\n{}", self.log_text());
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn http_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("http connect");
    stream.write_all(request.as_bytes()).await.expect("write");
    stream.flush().await.expect("flush");
    let mut body = Vec::new();
    let _ = stream.read_to_end(&mut body).await;
    String::from_utf8_lossy(&body).to_string()
}

/// Setup mode on a fresh data dir writes every node file under node/. After
/// a hard kill, a second process on the same data dir skips setup and serves
/// TLS from the same CA.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_flow_writes_node_files_and_a_killed_server_restarts() {
    let root = temp_root("setup-restart");
    let data_dir = root.join("data");
    let layout = NodeLayout::new(&data_dir);

    let mut first = ServerProcess::spawn(&data_dir, root.join("first.log"));
    first.wait_for(first.admin_addr).await;
    let body = "mode=auto&secret_mode=generate";
    let response = http_request(
        first.admin_addr,
        &format!(
            "POST /setup HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    assert!(response.contains("setup complete"), "{response}");
    first.wait_for(first.broker_addr).await;

    assert_eq!(
        entries(&data_dir),
        ["cache", "fibril-node", "node", "storage"]
    );
    assert_eq!(
        entries(&layout.node()),
        [
            "cluster.secret",
            "config-overlay.toml",
            "setup_complete",
            "tls"
        ]
    );
    assert_eq!(
        entries(&layout.generated_tls()),
        ["ca.key", "ca.pem", "server.key", "server.pem"]
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [
            layout.cluster_secret(),
            layout.config_overlay(),
            layout.generated_tls().join("ca.key"),
            layout.generated_tls().join("server.key"),
        ] {
            let mode = std::fs::metadata(&path).expect("meta").permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "{}", path.display());
        }
    }
    let ca_pem = layout.generated_tls().join("ca.pem");
    let ca_before = fibril::tls::ca_fingerprint(&ca_pem).expect("fingerprint");
    tls_handshake(first.broker_addr, &ca_pem).await;
    first.kill();

    // Same data dir, still in setup mode: the marker in node/ skips setup,
    // so the broker listener comes up without a second form post.
    let mut second = ServerProcess::spawn(&data_dir, root.join("second.log"));
    second.wait_for(second.broker_addr).await;
    tls_handshake(second.broker_addr, &ca_pem).await;
    assert_eq!(
        fibril::tls::ca_fingerprint(&ca_pem).expect("fingerprint"),
        ca_before
    );
    assert_eq!(
        entries(&data_dir),
        ["cache", "fibril-node", "node", "storage"]
    );
    second.kill();

    // The layout marker records the default node id.
    NodeLayout::open_or_init(&data_dir, "local").expect("reopen as the same node");
    assert!(data_dir.join(node_layout::NODE_MARKER_FILE).is_file());
    let _ = std::fs::remove_dir_all(root);
}
