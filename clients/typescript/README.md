# Fibril TypeScript Client

TypeScript client for the Fibril broker. Mirrors the API of the Rust client
where idiomatic; diverges where TS conventions are clearer (async iterables
for subscriptions, `bigint` for u64 fields).

## Install

```bash
npm install @fibril/client
```

Requires Node.js >= 20.

## Quick start

```ts
import { ClientOptions } from "@fibril/client";

const client = await new ClientOptions()
  .withAuth("user", "pass")
  .connect("localhost:9876");

// Publish (with broker confirmation)
const pub = client.publisher("orders");
const offset = await pub.publish({ id: 42, item: "widget" });

// Subscribe with manual ack
const sub = await client
  .subscribe("orders")
  .group("workers")
  .prefetch(100)
  .subManualAck();

for await (const msg of sub) {
  const order = msg.deserialize<{ id: number; item: string }>();
  try {
    await processOrder(order);
    await msg.complete();
  } catch {
    await msg.retry();
  }
}

await client.shutdown();
```

## Subscription modes

- `subManualAck()` — each `InflightMessage` must be settled with
  `complete()`, `fail()`, or `retry()`. Calling more than once throws.
- `subAutoAck()` — receives `Message` directly with no settle action.
  Acks are tracked client-side; the server still expects manual settles
  matching the Rust client's behavior.

## Error handling

All errors are subclasses of `FibrilError`:

- `DisconnectionError` — connection failed or was lost
- `BrokenPipeError` — internal engine has shut down
- `ServerError` — server returned an error code (with `.code`)
- `SerializationError` / `DeserializationError` — msgpack issues
- `EofError` — connection ended during handshake
- `UnexpectedError` — protocol violation or unknown state

When the engine dies, in-flight `publish()` calls reject with the
appropriate error and subscription iterators throw — they do not end
silently.

## Wire format

64-bit values (offsets, sub ids, delivery tag epochs, timestamps) are
exposed as `bigint`. Application-visible timestamps (`message.published`,
`message.publishReceived`) are converted to `number` (ms since epoch)
since the safe integer range covers millennia.

## Limitations

- `reconnect_restore` (resubscribing after reconnect) is not implemented.
  Use `reconnect()` and re-create publishers/subscriptions.
- No write timeouts; relies on heartbeat (3× interval) to detect dead
  half-open connections.
