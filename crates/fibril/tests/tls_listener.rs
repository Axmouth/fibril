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

async fn boot_server(
    tag: &str,
    tls_auto: bool,
) -> (std::net::SocketAddr, PathBuf, tokio::task::JoinHandle<()>) {
    let root = temp_root(tag);
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    config.tls.enabled = tls_auto;
    config.tls.auto_self_signed = tls_auto;
    let addr = config.broker.listener.bind;
    let data_dir = config.server.data_dir.clone();
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });
    for _ in 0..400 {
        if TcpStream::connect(addr).await.is_ok() {
            return (addr, data_dir, handle);
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
    let (addr, data_dir, server) = boot_server("hello", true).await;
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
    let (addr, _data_dir, server) = boot_server("plain-on-tls", true).await;

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
    let (addr, _data_dir, server) = boot_server("tls-on-plain", false).await;

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
