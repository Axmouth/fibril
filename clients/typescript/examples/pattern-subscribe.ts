/**
 * Pattern subscribe: fan in across every work queue whose topic matches a glob,
 * picking up newly declared matching queues automatically.
 *
 * Run:
 *   npx tsx examples/pattern-subscribe.ts
 *   # or specify a broker:
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/pattern-subscribe.ts
 */

import { ClientOptions } from "@fibril/client";

const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";

const client = await new ClientOptions({ clientName: "pattern-subscribe-example" }).connect(addr);

// Opt in to the routing surface and fan in across every "events.*" queue.
// Queues declared after this call attach on their own.
const sub = await client.routing().subscribePattern("events.*").sub();

console.log("listening for events.* (new matching queues attach automatically)");
for await (const { source, message } of sub) {
  const settled = await message.complete();
  console.log(`${source.topic}: ${settled.content()}`);
}
