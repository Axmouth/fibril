//! Publish one message and receive it back, verifying the payload round-trips.
//!
//! Run a broker on 127.0.0.1:9876 (or set FIBRIL_ADDR), then:
//!   cargo run -p fibril-client --example roundtrip

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage};

#[tokio::main]
async fn main() -> Result<(), FibrilError> {
    let addr = std::env::var("FIBRIL_ADDR").unwrap_or_else(|_| "127.0.0.1:9876".to_string());
    // The built-in fibril/fibril credentials are accepted from loopback.
    let client =
        Client::connect(addr.as_str(), ClientOptions::new().auth("fibril", "fibril")).await?;

    let topic = "roundtrip";
    let mut sub = client.subscribe(topic)?.prefetch(1).sub_auto_ack().await?;

    let offset = client
        .publisher(topic)?
        .publish_confirmed(NewMessage::text("hello"))
        .await?;
    println!("confirmed publish at offset {offset}");

    let msg = sub.recv().await.expect("the round-trip message");
    assert_eq!(msg.text()?, "hello", "payload round-trips");
    println!("received {}", msg.text()?);
    Ok(())
}
