//! Broker listener TLS integration: a TLS HELLO round trip over generated
//! material, and named mismatches in both directions instead of hangs or
//! opaque failures.

use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use fibril_protocol::v1::frame::Frame;
use fibril_protocol::v1::wire::{decode_error_message, decode_hello_ok, encode_hello};
use fibril_protocol::v1::{ERR_TLS_REQUIRED, Hello, Op, PROTOCOL_V1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "fibril-tls-listener-{tag}-{}-{}",
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

struct BootedServer {
    broker_addr: std::net::SocketAddr,
    admin_addr: std::net::SocketAddr,
    data_dir: PathBuf,
    handle: tokio::task::JoinHandle<()>,
}

async fn boot_server(tag: &str, tls_auto: bool) -> BootedServer {
    boot_server_with(tag, |config| {
        config.tls.enabled = tls_auto;
        config.tls.auto_self_signed = tls_auto;
    })
    .await
}

async fn boot_server_with(tag: &str, configure: impl FnOnce(&mut ServerConfig)) -> BootedServer {
    let root = temp_root(tag);
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    configure(&mut config);
    let broker_addr = config.broker.listener.bind;
    let admin_addr = config.admin.listener.bind;
    let data_dir = config.server.data_dir.clone();
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });
    for _ in 0..1200 {
        if TcpStream::connect(broker_addr).await.is_ok() {
            return BootedServer {
                broker_addr,
                admin_addr,
                data_dir,
                handle,
            };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("broker listener did not come up");
}

fn hello_frame(request_id: u64) -> Vec<u8> {
    let hello = Hello {
        client_name: "tls-listener-test".into(),
        client_version: "0".into(),
        protocol_version: PROTOCOL_V1,
        resume: None,
    };
    frame_bytes(&encode_hello(request_id, &hello).expect("encode hello"))
}

fn frame_bytes(frame: &Frame) -> Vec<u8> {
    let mut out = Vec::with_capacity(20 + frame.payload.len());
    out.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&frame.version.to_be_bytes());
    out.extend_from_slice(&frame.opcode.to_be_bytes());
    out.extend_from_slice(&frame.flags.to_be_bytes());
    out.extend_from_slice(&frame.request_id.to_be_bytes());
    out.extend_from_slice(&frame.payload);
    out
}

async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Frame {
    let mut header = [0u8; 20];
    stream.read_exact(&mut header).await.expect("frame header");
    let payload_len = u32::from_be_bytes(header[0..4].try_into().expect("len")) as usize;
    let mut payload = vec![0u8; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .expect("frame payload");
    Frame {
        version: u16::from_be_bytes(header[4..6].try_into().expect("version")),
        opcode: u16::from_be_bytes(header[6..8].try_into().expect("opcode")),
        flags: u32::from_be_bytes(header[8..12].try_into().expect("flags")),
        request_id: u64::from_be_bytes(header[12..20].try_into().expect("request id")),
        payload: payload.into(),
    }
}

fn tls_connector(ca_pem: Option<&Path>) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca_pem) = ca_pem {
        let file = std::fs::File::open(ca_pem).expect("open ca.pem");
        for cert in rustls_pemfile::certs(&mut BufReader::new(file)) {
            roots.add(cert.expect("parse ca cert")).expect("add root");
        }
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

async fn write_all<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) {
    stream.write_all(bytes).await.expect("write");
    stream.flush().await.expect("flush");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_serves_hello_over_generated_material() {
    let server = boot_server("hello", true).await;
    let (addr, data_dir) = (server.broker_addr, server.data_dir.clone());
    let server = server.handle;
    let ca_pem = data_dir.join("tls").join("ca.pem");
    assert!(ca_pem.exists(), "generated CA missing at {ca_pem:?}");

    let connector = tls_connector(Some(&ca_pem));
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    let mut stream = connector.connect(name, tcp).await.expect("tls handshake");

    write_all(&mut stream, &hello_frame(7)).await;
    let frame = read_frame(&mut stream).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    assert_eq!(frame.request_id, 7);
    let ok = decode_hello_ok(&frame).expect("hello ok body");
    assert_eq!(ok.protocol_version, PROTOCOL_V1);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_client_on_tls_listener_gets_a_definitive_error() {
    let booted = boot_server("plain-on-tls", true).await;
    let (addr, server) = (booted.broker_addr, booted.handle);

    let mut stream = TcpStream::connect(addr).await.expect("tcp connect");
    write_all(&mut stream, &hello_frame(9)).await;

    let frame = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
        .await
        .expect("error frame must arrive instead of a silent close");
    assert_eq!(frame.opcode, Op::Error as u16);
    // The reply rides the HELLO's request id so pending-request clients
    // surface it as the HELLO failure.
    assert_eq!(frame.request_id, 9);
    let error = decode_error_message(&frame).expect("error body");
    assert_eq!(error.code, ERR_TLS_REQUIRED);
    assert!(error.message.contains("TLS"), "{}", error.message);

    let mut rest = [0u8; 1];
    let n = stream.read(&mut rest).await.unwrap_or(0);
    assert_eq!(n, 0, "connection must close after the error frame");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_client_on_plaintext_listener_fails_fast_not_hangs() {
    let booted = boot_server("tls-on-plain", false).await;
    let (addr, server) = (booted.broker_addr, booted.handle);

    let connector = tls_connector(None);
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    let result = tokio::time::timeout(Duration::from_secs(5), connector.connect(name, tcp)).await;
    match result {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("handshake against a plaintext listener must not succeed"),
        Err(_) => panic!("handshake must fail fast instead of hanging"),
    }

    server.abort();
}

/// Minimal HTTPS GET over an established TLS stream, returning the status line.
async fn https_get_status_line<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
) -> String {
    write_all(
        stream,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).await.expect("http response bytes");
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_dashboard_serves_https_from_the_same_material() {
    let booted = boot_server("admin-https", true).await;
    wait_for_port(booted.admin_addr).await;
    let ca_pem = booted.data_dir.join("tls").join("ca.pem");

    let connector = tls_connector(Some(&ca_pem));
    let tcp = TcpStream::connect(booted.admin_addr)
        .await
        .expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    let mut stream = connector
        .connect(name, tcp)
        .await
        .expect("admin tls handshake");
    let status = https_get_status_line(&mut stream).await;
    assert!(status.starts_with("HTTP/1.1"), "{status}");

    booted.handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_scrape_works_over_admin_https() {
    let booted = boot_server("metrics-https", true).await;
    wait_for_port(booted.admin_addr).await;
    let ca_pem = booted.data_dir.join("tls").join("ca.pem");

    let connector = tls_connector(Some(&ca_pem));
    let tcp = TcpStream::connect(booted.admin_addr)
        .await
        .expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    let mut stream = connector
        .connect(name, tcp)
        .await
        .expect("admin tls handshake");
    write_all(
        &mut stream,
        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    let mut response = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => response.extend_from_slice(&buf[..n]),
        }
    }
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.contains("fibril_broker_published_total"),
        "{response}"
    );

    booted.handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_enabled_false_keeps_the_dashboard_on_plain_http() {
    let booted = boot_server_with("admin-optout", |config| {
        config.tls.enabled = true;
        config.tls.auto_self_signed = true;
        config.tls.admin_enabled = Some(false);
    })
    .await;
    wait_for_port(booted.admin_addr).await;

    let mut stream = TcpStream::connect(booted.admin_addr)
        .await
        .expect("tcp connect");
    let status = https_get_status_line(&mut stream).await;
    assert!(status.starts_with("HTTP/1.1"), "{status}");

    booted.handle.abort();
}

/// One plaintext HTTP request, reading until the peer closes.
async fn http_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("http connect");
    write_all(&mut stream, request.as_bytes()).await;
    let mut body = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&buf[..n]),
        }
    }
    String::from_utf8_lossy(&body).to_string()
}

async fn wait_for_port(addr: std::net::SocketAddr) {
    for _ in 0..1200 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("port {addr} did not come up");
}

fn setup_mode_config(root: &std::path::Path) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    config.setup.mode = true;
    config
}

async fn post_setup_choice(admin_addr: std::net::SocketAddr, body: &str) -> String {
    wait_for_port(admin_addr).await;
    let page = http_request(
        admin_addr,
        "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(page.contains("first-boot setup"), "{page}");
    http_request(
        admin_addr,
        &format!(
            "POST /setup HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_boot_setup_auto_flow_then_normal_reboot() {
    let root = temp_root("setup-auto");
    let config = setup_mode_config(&root);
    let broker_addr = config.broker.listener.bind;
    let admin_addr = config.admin.listener.bind;
    let data_dir = config.server.data_dir.clone();
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });

    // While setup is pending the broker listener must be down.
    wait_for_port(admin_addr).await;
    assert!(
        TcpStream::connect(broker_addr).await.is_err(),
        "broker must not serve before setup completes"
    );

    let response = post_setup_choice(admin_addr, "mode=auto").await;
    assert!(response.contains("setup complete"), "{response}");

    // The choice persists, and the broker comes up serving TLS.
    wait_for_port(broker_addr).await;
    assert!(data_dir.join("setup_complete").exists());
    assert!(data_dir.join("config-overlay.toml").exists());
    let ca_pem = data_dir.join("tls").join("ca.pem");
    let connector = tls_connector(Some(&ca_pem));
    let tcp = TcpStream::connect(broker_addr).await.expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    let mut stream = connector.connect(name, tcp).await.expect("tls handshake");
    write_all(&mut stream, &hello_frame(3)).await;
    let frame = read_frame(&mut stream).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    handle.abort();

    // A reboot with the marker present skips setup and adopts the overlay.
    // Asserted at the config layer because the first boot's storage threads
    // still hold the data dir within this test process.
    let mut rebooted = setup_mode_config(&root);
    rebooted.server.data_dir = data_dir.clone();
    assert!(
        !rebooted.setup_pending(),
        "marker must skip setup on reboot"
    );
    assert!(rebooted.apply_setup_overlay().expect("overlay applies"));
    assert_eq!(
        rebooted.tls.mode().expect("overlay tls mode"),
        fibril_config::TlsMode::AutoSelfSigned
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_boot_setup_skip_records_the_choice_and_stays_plaintext() {
    let root = temp_root("setup-skip");
    let config = setup_mode_config(&root);
    let broker_addr = config.broker.listener.bind;
    let admin_addr = config.admin.listener.bind;
    let data_dir = config.server.data_dir.clone();
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });

    let response = post_setup_choice(admin_addr, "mode=skip").await;
    assert!(response.contains("setup complete"), "{response}");

    wait_for_port(broker_addr).await;
    assert!(data_dir.join("setup_complete").exists());
    let mut stream = TcpStream::connect(broker_addr).await.expect("tcp connect");
    write_all(&mut stream, &hello_frame(4)).await;
    let frame = read_frame(&mut stream).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_boot_setup_creates_admin_user_and_cluster_secret() {
    let root = temp_root("setup-user-secret");
    let config = setup_mode_config(&root);
    let broker_addr = config.broker.listener.bind;
    let admin_addr = config.admin.listener.bind;
    let data_dir = config.server.data_dir.clone();
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });

    let body = "mode=auto&admin_username=ops&admin_password=setup-pass\
                &secret_mode=generate";
    let response = post_setup_choice(admin_addr, body).await;
    assert!(response.contains("setup complete"), "{response}");

    // The cluster secret landed in the data dir.
    wait_for_port(broker_addr).await;
    assert!(data_dir.join("cluster.secret").exists());

    // The seeded admin user works remotely (would be rejected if it were the
    // loopback-only default), proving the overlay auth section applied.
    use fibril_client::ClientOptions;
    let ca_pem = data_dir.join("tls").join("ca.pem");
    let client = ClientOptions::new()
        .auth("ops", "setup-pass")
        .tls_ca_path(&ca_pem)
        .tls_server_name("localhost")
        .connect(broker_addr.to_string().as_str())
        .await
        .expect("setup-created user authenticates over the setup-enabled TLS");
    client.shutdown().await;

    handle.abort();
}
