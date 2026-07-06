//! Plexus (fan-out stream): every subscriber sees every record. Declare a stream,
//! subscribe, publish a burst, and read it all back.
//!
//! Run a broker on 127.0.0.1:9876 (or set FIBRIL_ADDR), then:
//!   cargo run -p fibril-client --example stream

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage, StreamConfig};

#[tokio::main]
async fn main() -> Result<(), FibrilError> {
    let addr = std::env::var("FIBRIL_ADDR").unwrap_or_else(|_| "127.0.0.1:9876".to_string());
    let client =
        Client::connect(addr.as_str(), ClientOptions::new().auth("fibril", "fibril")).await?;

    let topic = "events";
    client
        .declare_plexus(StreamConfig::new(topic)?.partitions(1))
        .await?;

    // Read only records published from now on (from_latest); auto-ack advances the
    // cursor past each record as it is delivered.
    let mut sub = client
        .stream(topic)?
        .from_latest()
        .prefetch(64)
        .sub_auto_ack()
        .await?;
    let publisher = client.publisher(topic)?;

    for seq in 0..5 {
        publisher
            .publish_confirmed(NewMessage::content(format!("record-{seq}")))
            .await?;
    }

    for _ in 0..5 {
        let record = sub.recv().await.expect("a stream record");
        println!("stream record {}", record.content()?);
    }
    Ok(())
}
