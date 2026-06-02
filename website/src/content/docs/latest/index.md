---
title: Fibril documentation
description: Start here for the current Fibril documentation.
---

Fibril is a lightweight Rust message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination.

It is currently pre-alpha. The useful baseline is working, but APIs, persistence formats, protocol details, and operational behavior can still change.

## Where to start

- Follow the [quickstart](/latest/quickstart/) to run the broker from source.
- Read the [core model](/latest/concepts/core-model/) for the queue lifecycle.
- Read [retries and delays](/latest/reliability/retries-delays/) and [dead lettering](/latest/reliability/dead-lettering/) for the parts that are still being wired through the public API.
- Check [project status](/latest/status/) before depending on a feature.

## Design intent

Message handling should read like the intent of the system:

```rust
while let Some(msg) = sub.recv().await {
    process(msg.content()?).await?;
    msg.complete().await?;
}
```

Messages can also be retried or failed explicitly:

```rust
msg.retry().await?;
msg.fail().await?;
```

Delayed retries have a client API, but the end-to-end path is still under development. See [project status](/latest/status/).

## Current documentation

These docs track the active pre-1.0 codebase at `/latest/`. Milestone snapshots can be added when behavior is stable enough to preserve.
