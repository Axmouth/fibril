//! User management through the admin API, end to end against a booted
//! standalone broker: create a user, authenticate with it via a client,
//! rotate the password, then remove the user.

use std::time::Duration;

use fibril::run_server_from_config;
use fibril_client::{ClientOptions, FibrilError};
use fibril_config::ServerConfig;
use tokio::net::TcpStream;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "fibril-user-admin-{tag}-{}-{}",
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

struct Booted {
    broker: String,
    admin: String,
    handle: tokio::task::JoinHandle<()>,
}

async fn boot() -> Booted {
    let root = temp_root("api");
    let mut config = ServerConfig::default();
    config.server.data_dir = root.join("data");
    config.broker.listener.bind = free_loopback_addr();
    config.admin.listener.bind = free_loopback_addr();
    let broker = config.broker.listener.bind;
    let admin = config.admin.listener.bind;
    let handle = tokio::spawn(async move {
        let _ = run_server_from_config(config).await;
    });
    for _ in 0..1200 {
        if TcpStream::connect(broker).await.is_ok() && TcpStream::connect(admin).await.is_ok() {
            return Booted {
                broker: broker.to_string(),
                admin: admin.to_string(),
                handle,
            };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("broker did not come up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_api_user_lifecycle_reaches_the_auth_path() {
    let server = boot().await;
    let http = reqwest::Client::new();
    let base = format!("http://{}/admin/api/users", server.admin);

    // Create a user.
    let resp = http
        .post(&base)
        .json(&serde_json::json!({ "username": "ops", "password": "first-pass" }))
        .send()
        .await
        .expect("create request");
    assert!(resp.status().is_success(), "create: {}", resp.status());

    // It appears in the listing without a hash field.
    let listed: serde_json::Value = http
        .get(&base)
        .send()
        .await
        .expect("list request")
        .json()
        .await
        .expect("list json");
    let text = listed.to_string();
    assert!(text.contains("ops"), "{text}");
    assert!(
        !text.contains("hash") && !text.to_lowercase().contains("argon2"),
        "{text}"
    );

    // Authenticate with it through a client.
    let client = ClientOptions::new()
        .auth("ops", "first-pass")
        .connect(server.broker.as_str())
        .await
        .expect("new user authenticates");
    client.shutdown().await;

    // Rotate the password.
    let resp = http
        .post(&base)
        .json(&serde_json::json!({ "username": "ops", "password": "second-pass" }))
        .send()
        .await
        .expect("rotate request");
    assert!(resp.status().is_success());
    let err = ClientOptions::new()
        .auth("ops", "first-pass")
        .connect(server.broker.as_str())
        .await
        .expect_err("old password must stop working");
    assert!(
        matches!(err, FibrilError::Failure { code: 401, .. }),
        "{err:?}"
    );
    let client = ClientOptions::new()
        .auth("ops", "second-pass")
        .connect(server.broker.as_str())
        .await
        .expect("rotated password works");
    client.shutdown().await;

    // Remove the user.
    let resp = http
        .delete(format!("{base}/ops"))
        .send()
        .await
        .expect("delete request");
    assert!(resp.status().is_success());
    let err = ClientOptions::new()
        .auth("ops", "second-pass")
        .connect(server.broker.as_str())
        .await
        .expect_err("removed user must fail");
    assert!(
        matches!(err, FibrilError::Failure { code: 401, .. }),
        "{err:?}"
    );

    server.handle.abort();
}
