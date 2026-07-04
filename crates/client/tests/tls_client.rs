//! Rust client TLS integration against a real TLS broker: both trust paths
//! (CA file and fingerprint pin) and the typed mismatch taxonomy.

use std::path::PathBuf;
use std::time::Duration;

use fibril::run_server_from_config;
use fibril_client::{ClientOptions, FibrilError, NewMessage, QueueConfig};
use fibril_config::ServerConfig;
use tokio::net::TcpStream;

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "fibril-tls-client-{tag}-{}-{}",
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

async fn boot_server(tag: &str, tls_auto: bool) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    boot_server_with(tag, |config| {
        config.tls.enabled = tls_auto;
        config.tls.auto_self_signed = tls_auto;
    })
    .await
}

async fn boot_server_with(
    tag: &str,
    configure: impl FnOnce(&mut ServerConfig),
) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    let root = temp_root(tag);
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    configure(&mut config);
    let addr = config.broker.listener.bind;
    let data_dir = config.server.data_dir.clone();
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });
    for _ in 0..1200 {
        if TcpStream::connect(addr).await.is_ok() {
            return (addr.to_string(), data_dir, handle);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("broker listener did not come up");
}

/// A certificate whose identity names a seeded user connects and works with
/// no password at all: the certificate is the credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_certificate_connects_without_password() {
    let (addr, data_dir, server) = boot_server_with("mtls-cert", |config| {
        config.tls.enabled = true;
        config.tls.auto_self_signed = true;
        config.tls.client_auth = fibril_config::ClientAuthMode::Request;
        config.auth.seed_users = vec![fibril_config::SeedUser {
            username: "svc-a".into(),
            password: "never-used".into(),
        }];
    })
    .await;
    let tls_dir = data_dir.join("tls");
    let (cert_pem, key_pem) =
        fibril::tls::issue_client_certificate(&tls_dir, "svc-a").expect("issue cert");
    let cert_path = tls_dir.join("svc-a.pem");
    let key_path = tls_dir.join("svc-a.key");
    std::fs::write(&cert_path, cert_pem).expect("write cert");
    std::fs::write(&key_path, key_pem).expect("write key");

    let client = ClientOptions::new()
        .tls_ca_path(tls_dir.join("ca.pem"))
        .tls_server_name("localhost")
        .tls_client_cert(&cert_path, &key_path)
        .connect(addr.as_str())
        .await
        .expect("certificate-only connect");
    client
        .declare_queue(QueueConfig::new("mtls-smoke").expect("queue config"))
        .await
        .expect("declare with certificate identity");
    let offset = client
        .publisher("mtls-smoke")
        .expect("publisher")
        .publish_confirmed(NewMessage::raw(b"cert identity".to_vec()))
        .await
        .expect("confirmed publish with certificate identity");
    assert_eq!(offset, 0);

    client.shutdown().await;
    server.abort();
}

/// `require` mode without a client certificate surfaces the typed error
/// with the fix, not a generic disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn certless_client_gets_the_required_certificate_error() {
    let (addr, data_dir, server) = boot_server_with("mtls-certless", |config| {
        config.tls.enabled = true;
        config.tls.auto_self_signed = true;
        config.tls.client_auth = fibril_config::ClientAuthMode::Require;
    })
    .await;
    let ca_pem = data_dir.join("tls").join("ca.pem");

    let err = ClientOptions::new()
        .auth("fibril", "fibril")
        .tls_ca_path(&ca_pem)
        .tls_server_name("localhost")
        .connect(addr.as_str())
        .await
        .expect_err("certless connect must be refused");
    assert!(
        matches!(err, FibrilError::TlsClientCertificateRequired { .. }),
        "{err:?}"
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_client_publishes_over_ca_path_trust() {
    let (addr, data_dir, server) = boot_server("ca-path", true).await;
    let ca_pem = data_dir.join("tls").join("ca.pem");

    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .tls_ca_path(&ca_pem)
        .tls_server_name("localhost")
        .connect(addr.as_str())
        .await
        .expect("TLS connect with CA trust");
    client
        .declare_queue(QueueConfig::new("tls-smoke").expect("queue config"))
        .await
        .expect("declare over TLS");
    let offset = client
        .publisher("tls-smoke")
        .expect("publisher")
        .publish_confirmed(NewMessage::raw(b"over tls".to_vec()))
        .await
        .expect("confirmed publish over TLS");
    assert_eq!(offset, 0);

    client.shutdown().await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_client_connects_with_fingerprint_pin() {
    let (addr, data_dir, server) = boot_server("pin", true).await;
    let ca_pem = data_dir.join("tls").join("ca.pem");
    let fingerprint = fibril::tls::ca_fingerprint(&ca_pem).expect("fingerprint");

    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .tls_ca_fingerprint(fingerprint)
        .connect(addr.as_str())
        .await
        .expect("TLS connect with fingerprint pin");
    client.shutdown().await;

    // A wrong pin must fail as a trust error, not a transport mismatch.
    let wrong = "00".repeat(32);
    let err = ClientOptions::new()
        .tls_ca_fingerprint(wrong)
        .connect(addr.as_str())
        .await
        .expect_err("wrong pin must fail");
    assert!(
        matches!(err, FibrilError::TlsCertificateUntrusted { .. }),
        "{err:?}"
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_client_gets_the_definitive_tls_required_error() {
    let (addr, _data_dir, server) = boot_server("plain-client", true).await;

    let err = ClientOptions::new()
        .connect(addr.as_str())
        .await
        .expect_err("plaintext connect to a TLS broker must fail");
    assert!(matches!(err, FibrilError::TlsRequiredByBroker), "{err:?}");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_client_against_plaintext_broker_names_the_probable_cause() {
    let (addr, _data_dir, server) = boot_server("tls-client-plain-broker", false).await;

    let err = ClientOptions::new()
        .tls_ca_fingerprint("11".repeat(32))
        .connect(addr.as_str())
        .await
        .expect_err("TLS connect to a plaintext broker must fail");
    assert!(
        matches!(err, FibrilError::TlsNotSupportedByBroker { .. }),
        "{err:?}"
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untrusted_generated_cert_fails_as_trust_error() {
    let (addr, _data_dir, server) = boot_server("untrusted", true).await;

    // System roots cannot know the per-deployment CA, so this must surface
    // as a certificate trust failure with its own guidance.
    let err = ClientOptions::new()
        .tls()
        .tls_server_name("localhost")
        .connect(addr.as_str())
        .await
        .expect_err("untrusted generated CA must fail verification");
    assert!(
        matches!(err, FibrilError::TlsCertificateUntrusted { .. }),
        "{err:?}"
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plaintext_publish_same_harness() {
    let (addr, _data_dir, server) = boot_server("plain-control", false).await;
    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .connect(addr.as_str())
        .await
        .expect("plaintext connect");
    client
        .declare_queue(QueueConfig::new("plain-smoke").expect("queue config"))
        .await
        .expect("declare plaintext");
    let offset = client
        .publisher("plain-smoke")
        .expect("publisher")
        .publish_confirmed(NewMessage::raw(b"plain".to_vec()))
        .await
        .expect("confirmed publish plaintext");
    assert_eq!(offset, 0);
    client.shutdown().await;
    server.abort();
}
