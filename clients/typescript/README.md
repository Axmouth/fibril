# Fibril TypeScript Client

TypeScript client for the Fibril broker. Mirrors the API of the Rust client
where idiomatic; diverges where TS conventions are clearer (async iterables
for subscriptions, `bigint` for u64 fields).

## Install

```bash
npm install @fibril/client
```

Requires Node.js >= 20.

msgpack is optional. The client works with raw bytes, text, and JSON without it;
install `@msgpack/msgpack` only if you publish or consume msgpack payloads (the
default for plain values passed to `publish`):

```bash
npm install @msgpack/msgpack
```

## Quick start

```ts
import { ClientOptions, NewMessage } from "@fibril/client";

const client = await new ClientOptions()
  .withAuth("user", "pass")
  .connect("localhost:9876");

// Publish; use the confirmed variant when you need the broker offset.
const pub = client.publisher("orders");
const offset = await pub.publishConfirmed({ id: 42, item: "widget" });
const confirmation = await pub.publishWithConfirmation({ id: 43, item: "bolt" });
const pipelinedOffset = await confirmation.confirmed();
await pub.publishDelayed(NewMessage.json({ id: 43 }), 30_000);

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

`group("default")` is treated the same as leaving the group unset.

## Subscription modes

- `subManualAck()` — each `InflightMessage` must be settled with
  `complete()`, `fail()`, or `retry()`. Calling more than once throws.
- `subAutoAck()` — receives `Message` directly with no settle action.
  The subscribe request is marked auto-ack and the client exposes delivered
  messages without a settlement handle.

## Examples

Runnable examples live in `examples/`. The `*.example.ts` files double as light
end-to-end tests: each connects to a real broker, exercises one feature, and
self-validates, exiting non-zero on failure.

```sh
# Start a broker, run every example, tear it down (exits non-zero if any fail):
examples/run-all.sh

# Or run one against an already-running broker:
FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/roundtrip.example.ts
```

Continuous examples (e.g. `stream.example.ts`) run forever by default so you can
watch packets flow, and take `--check` (or `FIBRIL_CHECK=1`) for a bounded,
self-validating burst that exits. `run-all.sh` passes `--check`. `hello.ts` and
`demo.ts` are illustrative walkthroughs rather than tests.

## Error handling

All errors are subclasses of `FibrilError`:

- `DisconnectionError` — connection failed or was lost
- `BrokenPipeError` — internal engine has shut down
- `ServerError` — server returned an error code (with `.code`)
- `SerializationError` / `DeserializationError` — msgpack issues
- `EofError` — connection ended during handshake
- `UnexpectedError` — protocol violation or unknown state

When the engine dies, in-flight `publishConfirmed()` calls reject with the
appropriate error and subscription iterators throw — they do not end
silently.

## Wire format

64-bit values (offsets, sub ids, delivery tag epochs, timestamps) are
exposed as `bigint`. Application-visible timestamps (`message.published`,
`message.publishReceived`) are converted to `number` (ms since epoch)
since the safe integer range covers millennia.

Fibril reserves `fibril.*` and `stroma.*` headers for system metadata.
Application messages should use another prefix for custom headers.

Content type is exposed through `NewMessage.contentType(...)` and
`message.contentType()`. It is sent as compact message metadata rather than as
an entry in the custom header map.

Use `NewMessage.json(...)`, `NewMessage.msgpack(...)`, `NewMessage.raw(...)`,
or `NewMessage.content(...)` when you want to choose the payload encoding
explicitly. Plain values passed to publish methods use msgpack.

## Limitations

- Automatic resubscribe after a rejected or mismatched resume is not implemented.
  When reconciliation returns `keep`, active subscriptions continue on the
  existing stream. Otherwise recreate the subscription at the application level.
- No write timeouts; relies on heartbeat (3× interval) to detect dead
  half-open connections.
- Delayed retry is exposed through `retryAfter(...)`.
- Global DLQ target configuration is available through the admin UI/API and
  `fibrilctl admin global-dlq`. Queue retry/DLQ
  policy can be declared with `QueueConfig`.
