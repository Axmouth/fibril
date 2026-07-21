//! A manually-acked delivery that is nacked with requeue is redelivered, and after
//! it is completed no further copy arrives.
//!
//! Run a broker on 127.0.0.1:9876 (or set FIBRIL_ADDR), then:
//!   cargo run -p fibril-client --example manual_ack_retry

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage};

#[tokio::main]
async fn main() -> Result<(), FibrilError> {
    let addr = std::env::var("FIBRIL_ADDR").unwrap_or_else(|_| "127.0.0.1:9876".to_string());
    let client =
        Client::connect(addr.as_str(), ClientOptions::new().auth("fibril", "fibril")).await?;

    // Distinct from other examples so a shared broker cannot cross-feed this one.
    let topic = "tasks";
    let mut sub = client.subscribe(topic)?.prefetch(4).sub().await?; // manual ack
    client
        .publisher(topic)?
        .publish_confirmed(NewMessage::text("work"))
        .await?;

    let first = sub.recv().await.delivery().expect("first delivery");
    assert_eq!(first.payload, b"work", "first payload");
    println!("first delivery: requeuing for another attempt");
    first.retry().await?; // requeue for redelivery

    let second = sub.recv().await.delivery().expect("redelivery after retry");
    assert_eq!(second.payload, b"work", "redelivered payload");
    println!("redelivery: completing");
    second.complete().await?;
    Ok(())
}
