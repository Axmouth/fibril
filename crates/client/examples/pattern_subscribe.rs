//! Pattern subscribe: fan in across every work queue whose topic matches a glob,
//! picking up newly declared matching queues automatically.
//!
//! Run a broker on 127.0.0.1:9876 (or set FIBRIL_ADDR), then:
//!   cargo run -p fibril-client --example pattern_subscribe

use fibril_client::{Client, ClientOptions, FibrilError};

#[tokio::main]
async fn main() -> Result<(), FibrilError> {
    let addr = std::env::var("FIBRIL_ADDR").unwrap_or_else(|_| "127.0.0.1:9876".to_string());
    let client = Client::connect(addr.as_str(), ClientOptions::new()).await?;

    // Opt in to the routing surface, then fan in across every "events.*" queue.
    // Queues declared after this call attach on their own.
    let mut sub = client.routing().subscribe_pattern("events.*").sub().await?;

    println!("listening for events.* (new matching queues attach automatically)");
    while let fibril_client::SubEvent::Delivery((source, msg)) = sub.recv().await {
        let message = msg.complete().await?;
        println!("{}: {}", source.topic, message.text().unwrap_or("<binary>"));
    }
    println!("subscription closed: {:?}", sub.close_reason());
    Ok(())
}
