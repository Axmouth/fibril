---
title: Fibril documentation
description: Start here for the current Fibril documentation.
---

Fibril is a lightweight message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination. The broker is implemented in Rust, but the user-facing model is about durable messaging rather than a Rust-only ecosystem.

It is currently pre-alpha. The useful baseline is working, but APIs, persistence formats, protocol details, and operational behavior can still change.

## Where to start

- Follow the [quickstart](/latest/quickstart/) to run the broker from source.
- Use the [client guide](/latest/clients/) for Rust and TypeScript publishing and subscription examples.
- Use the [admin dashboard guide](/latest/admin-dashboard/) for queues, settings, message inspection, and DLQ replay.
- Read the [core model](/latest/concepts/core-model/) for the queue lifecycle.
- Read [retries and delays](/latest/reliability/retries-delays/) and [dead lettering](/latest/reliability/dead-lettering/) for reliability features and their current limits.
- Read [many idle queues](/latest/concepts/many-idle-queues/) if your workload defines many queues but only uses a few at once.
- Check [project status](/latest/status/) before depending on a feature.
- Check [implemented surface](/latest/implemented-surface/) when you need the detailed answer for whether a path is wired and under what conditions.
- Check the [roadmap](/latest/roadmap/) for recently landed work and near-term pending items.
- Use the [optimization log](/latest/development/optimization-log/) for benchmark-first performance investigations.

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
msg.retry_after(30).await?;
msg.fail().await?;
```

Delayed retry is wired through the broker, protocol, Rust client, and TypeScript client. See [project status](/latest/status/).

The protocol and storage model intentionally lean toward small common-case records. Always-present message metadata is stored as positional metadata, not repeated header-map keys, and uncommon operations use distinct frames or events instead of adding optional fields to hot paths. For example, delayed publish uses a delayed-publish frame rather than adding a delay field to every publish frame.

This is not a rule against richer commands. Infrequent configuration commands can carry complete settings when that makes atomic updates clearer, such as declaring a queue with all of its settings in one command.

## Current documentation

These docs track the active pre-1.0 codebase at `/latest/`. Milestone snapshots can be added when behavior is stable enough to preserve.
