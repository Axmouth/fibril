//! User management through the admin API, end to end against a booted
//! standalone broker: create a user, authenticate with it via a client,
//! rotate the password, then remove the user.

use fibril_client::{ClientOptions, FibrilError};

mod common;

struct Booted {
    broker: String,
    admin: String,
    handle: tokio::task::JoinHandle<()>,
    _permit: tokio::sync::SemaphorePermit<'static>,
}

async fn boot() -> Booted {
    let booted = common::boot_server("fibril-user-admin", "api", true, |_| {}).await;
    Booted {
        broker: booted.broker_addr.to_string(),
        admin: booted.admin_addr.to_string(),
        handle: booted.handle,
        _permit: booted.boot_permit,
    }
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
