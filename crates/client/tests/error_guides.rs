//! Client-local error guides stay in parity with clients/error_guides.json.
//!
//! A client-local error is one the client raises itself, without a broker error
//! frame (connection refused, heartbeat timeout). The shared JSON is the single
//! source of truth for the wording all three clients must carry, the same way
//! clients/wire_vectors.json pins the wire encoding. Broker-side guides ride
//! error frames and are covered by broker tests instead.

use std::net::TcpListener;

use fibril_client::ClientOptions;

const GUIDES: &str = include_str!("../../../clients/error_guides.json");

fn must_contain(case: &str) -> Vec<String> {
    let guides: serde_json::Value = serde_json::from_str(GUIDES).expect("error_guides.json parses");
    guides[case]["must_contain"]
        .as_array()
        .unwrap_or_else(|| panic!("case {case} has a must_contain array"))
        .iter()
        .map(|value| value.as_str().expect("keyword is a string").to_lowercase())
        .collect()
}

#[tokio::test]
async fn connection_refused_carries_the_shared_guide() {
    // Bind then drop a listener to get an address with nothing listening, so the
    // connect is refused rather than hanging or reaching a real broker.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let address = listener.local_addr().expect("local addr").to_string();
    drop(listener);

    let error = ClientOptions::new()
        .disable_auto_reconnect()
        .connect(address)
        .await
        .expect_err("connecting to a closed port must fail");
    let message = error.to_string().to_lowercase();

    for keyword in must_contain("connection_refused") {
        assert!(
            message.contains(&keyword),
            "connection_refused guide is missing {keyword:?}: {message}"
        );
    }
}
