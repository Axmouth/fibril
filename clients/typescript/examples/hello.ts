/**
 * Minimal example: connect, publish one message, receive it.
 *
 * Run:
 *   npx tsx examples/hello.ts
 *   # or specify a broker:
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/hello.ts
 */

import { ClientOptions } from "@fibril/client";

const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";
const user = process.env.FIBRIL_USER ?? "fibril";
const pass = process.env.FIBRIL_PASS ?? "fibril";

const client = await new ClientOptions({ clientName: "hello-example" })
  .withAuth(user, pass)
  .connect(addr);

// Subscribe first.
const sub = await client.subscribe("hello").prefetch(1).subManualAck();

// Publish in the background.
const pub = client.publisher("hello");
const offset = await pub.publish({ greeting: "world", at: Date.now() });
console.log(`published at offset ${offset}`);

// Receive one message.
const msg = await sub.recv();
if (msg) {
  console.log("received:", msg.deserialize<{ greeting: string; at: number }>());
  await msg.complete();
}

await client.shutdown();
