# fibril-client

Async Rust client for the [Fibril](https://fibril.sh) message broker: durable
queues with explicit settlement, Plexus streams, partition routing across a
cluster, reconnects and confirmed publishing.

Fibril is pre-alpha. The client API can still change between releases.

## Example

```rust,no_run
use fibril_client::{Client, ClientOptions, FibrilError, NewMessage, SubEvent};

#[tokio::main]
async fn main() -> Result<(), FibrilError> {
    let client = Client::connect("127.0.0.1:9876", ClientOptions::new().auth("fibril", "fibril")).await?;

    client
        .publisher("tasks")?
        .publish_confirmed(NewMessage::text("work"))
        .await?;

    let mut sub = client.subscribe("tasks")?.prefetch(16).sub().await?;
    while let SubEvent::Delivery(msg) = sub.recv().await {
        println!("got {}", String::from_utf8_lossy(&msg.payload));
        msg.complete().await?; // or msg.retry() to requeue, msg.fail() to settle as failed
    }
    Ok(())
}
```

Messages are `#[must_use]`, so a delivery that is never settled is a compiler
warning. More examples are in the repository's `crates/client/examples`.

## Links

- [Docs](https://fibril.sh/) and the [client guide](https://fibril.sh/clients/)
- [Feature parity across the five clients](https://github.com/Axmouth/fibril/blob/main/clients/FEATURE_MATRIX.md)

## License

MIT. See [AI_POLICY.md](https://github.com/Axmouth/fibril/blob/main/AI_POLICY.md)
for the text and data mining reservation that comes with this package.
