//! Broker authentication against the user store, through a real client.
//! The loopback-only rejection of default credentials is unit-tested in the
//! broker (integration sockets are always loopback here).

use std::time::Duration;

use fibril::run_server_from_config;
use fibril_client::{ClientOptions, FibrilError};
use fibril_config::{SeedUser, ServerConfig};
use tokio::net::TcpStream;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "fibril-auth-client-{tag}-{}-{}",
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

async fn boot_server(tag: &str, seeds: Vec<SeedUser>) -> (String, tokio::task::JoinHandle<()>) {
    let root = temp_root(tag);
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    config.auth.seed_users = seeds;
    let addr = config.broker.listener.bind;
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });
    for _ in 0..1200 {
        if TcpStream::connect(addr).await.is_ok() {
            return (addr.to_string(), handle);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("broker listener did not come up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_user_authenticates_and_wrong_password_is_denied() {
    let (addr, server) = boot_server(
        "seeded",
        vec![SeedUser {
            username: "ops".into(),
            password: "strong-password".into(),
        }],
    )
    .await;

    let client = ClientOptions::new()
        .auth("ops", "strong-password")
        .connect(addr.as_str())
        .await
        .expect("seeded user connects");
    client.shutdown().await;

    let err = ClientOptions::new()
        .auth("ops", "wrong")
        .connect(addr.as_str())
        .await
        .expect_err("wrong password must fail");
    assert!(
        matches!(&err, FibrilError::Failure { code: 401, .. }),
        "{err:?}"
    );

    // Default credentials keep working from loopback alongside real users.
    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .connect(addr.as_str())
        .await
        .expect("default credentials from loopback");
    client.shutdown().await;

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_fibril_user_shadows_the_default_pair() {
    let (addr, server) = boot_server(
        "shadow",
        vec![SeedUser {
            username: "fibril".into(),
            password: "rotated".into(),
        }],
    )
    .await;

    let err = ClientOptions::new()
        .auth("fibril", "fibril")
        .connect(addr.as_str())
        .await
        .expect_err("built-in pair must stop working once a real fibril user exists");
    assert!(
        matches!(&err, FibrilError::Failure { code: 401, .. }),
        "{err:?}"
    );

    let client = ClientOptions::new()
        .auth("fibril", "rotated")
        .connect(addr.as_str())
        .await
        .expect("rotated password works");
    client.shutdown().await;

    server.abort();
}
