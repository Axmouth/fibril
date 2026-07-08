//! A confirmed publish returns an increasing offset, and a delayed publish is
//! withheld before its deadline and arrives after it.
//!
//! Run a broker on 127.0.0.1:9876 (or set FIBRIL_ADDR), then:
//!   cargo run -p fibril-client --example confirmed_delayed

use std::time::Duration;

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage};

#[tokio::main]
async fn main() -> Result<(), FibrilError> {
    let addr = std::env::var("FIBRIL_ADDR").unwrap_or_else(|_| "127.0.0.1:9876".to_string());
    let client =
        Client::connect(addr.as_str(), ClientOptions::new().auth("fibril", "fibril")).await?;

    let topic = "orders";
    let mut sub = client.subscribe(topic)?.prefetch(8).sub_auto_ack().await?;
    let publisher = client.publisher(topic)?;

    let first = publisher
        .publish_confirmed(NewMessage::text("now"))
        .await?;
    let second = publisher
        .publish_confirmed(NewMessage::text("also now"))
        .await?;
    assert!(
        second > first,
        "offsets increase across confirmed publishes"
    );
    println!("confirmed offsets {first} then {second}");

    // Withheld until the delay elapses (1 second), then delivered normally.
    publisher
        .publish_delayed_confirmed(NewMessage::text("later"), Duration::from_secs(1))
        .await?;
    println!("published one message delayed by 1s");

    for _ in 0..3 {
        let msg = sub.recv().await.expect("a delivery");
        println!("received {}", msg.text()?);
    }
    Ok(())
}
