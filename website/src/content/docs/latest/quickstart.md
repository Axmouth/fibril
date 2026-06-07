---
title: Quickstart
description: Run the current Fibril broker from source.
---

The current distribution is a source checkout. Published binaries and production configuration are not ready yet.

## Prerequisites

- A current Rust toolchain
- Git

## Run the broker

```sh
git clone https://github.com/Axmouth/fibril.git
cd fibril
cargo run --release --bin fibril-server
```

The development server currently:

- listens for broker TCP traffic on `0.0.0.0:9876`
- serves the early admin interface on `0.0.0.0:8081`
- stores durable state under `server_data/`
- uses development authentication defaults in the server binary

Do not expose this development server directly to the public internet.

## Rust client shape

The Rust client lives in `crates/client`. It supports publishers, delayed publish, and manual-ack subscriptions:

```rust
let client = ClientOptions::new()
    .auth("fibril", "fibril")
    .connect("127.0.0.1:9876")
    .await?;

let publisher = client.publisher("email.send")?;
publisher.publish("hello").await?;
publisher.publish_delayed("hello later", 30u64).await?;

let mut sub = client
    .subscribe("email.send")?
    .prefetch(32)
    .sub_manual_ack()
    .await?;

while let Some(msg) = sub.recv().await {
    process(msg.content()?).await?;
    msg.complete().await?;
}
```

## TypeScript client shape

The TypeScript client lives in `clients/typescript`. It mirrors the current Rust client shape where practical:

```ts
import { ClientOptions } from "@fibril/client";

const client = await new ClientOptions()
  .withAuth("fibril", "fibril")
  .connect("127.0.0.1:9876");

const publisher = client.publisher("email.send");
await publisher.publish({ body: "hello" });
await publisher.publishDelayed({ body: "hello later" }, 30_000);

const sub = await client
  .subscribe("email.send")
  .prefetch(32)
  .subManualAck();

for await (const msg of sub) {
  const body = msg.deserialize<{ body: string }>();
  await process(body);
  await msg.complete();
}
```

The API is evolving. Treat examples as a guide to the current source tree rather than a stable package contract.

For more publishing, subscription, delayed publish, and message encoding examples, see [client usage](/latest/clients/).

For startup config, runtime settings, and idle queue cleanup options, see [configuration](/latest/configuration/).
