---
title: Fibril documentation
description: Start here for the current Fibril documentation.
---

Fibril is a lightweight message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination. The broker is implemented in Rust, but the user-facing model is about durable messaging rather than a Rust-only ecosystem.

It is currently pre-alpha. The useful baseline is working, but APIs, persistence formats, protocol details, and operational behavior can still change.

## Where to start

- Follow the [quickstart](/latest/quickstart/) to run the broker from source.
- Use the [client guide](/latest/clients/) for Rust and TypeScript publishing and subscription examples.
- Read the [core model](/latest/concepts/core-model/) for the queue lifecycle.
- Read [retries and delays](/latest/reliability/retries-delays/) and [dead lettering](/latest/reliability/dead-lettering/) for the parts that are still being wired through the public API.
- Read [inactivity and eviction](/latest/concepts/inactivity-eviction/) for lazy loading and idle resource cleanup behavior.
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

The protocol and storage model intentionally lean toward small common-case records. Always-present message metadata is stored as positional metadata, not repeated header-map keys, and uncommon operations use distinct frames or events instead of adding optional fields to hot paths. For example, delayed publish uses a delayed-publish frame rather than adding a delay field to every publish frame.

This is not a rule against richer commands. Infrequent configuration commands can carry complete settings when that makes atomic updates clearer, such as declaring a queue with all of its settings in one command.

## Current documentation

These docs track the active pre-1.0 codebase at `/latest/`. Milestone snapshots can be added when behavior is stable enough to preserve.
