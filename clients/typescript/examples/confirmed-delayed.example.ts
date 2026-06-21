/**
 * Example + light test: confirmed publish returns an offset, and a delayed
 * publish is not delivered before its deadline but arrives after it.
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/confirmed-delayed.example.ts
 */
import { NewMessage } from "@fibril/client";
import { check, connect, runExample, uniqueTopic } from "./checks.js";

await runExample("confirmed-delayed", async () => {
  const client = await connect("example-confirmed-delayed");
  try {
    const topic = uniqueTopic("delayed");
    const sub = await client.subscribe(topic).prefetch(8).subAutoAck();
    const publisher = client.publisher(topic);

    const o1 = await publisher.publishConfirmed(NewMessage.content("now"));
    const o2 = await publisher.publishConfirmed(NewMessage.content("also now"));
    check(o2 > o1, "offsets increase across confirmed publishes");

    // Publish with a 1s delay, then assert nothing arrives early.
    const delayMs = 1_000;
    await publisher.publishDelayedConfirmed(NewMessage.content("later"), delayMs);

    const first = await sub.recv();
    const second = await sub.recv();
    check(first !== null && second !== null, "the two immediate messages arrive");

    // The delayed one should not be ready yet.
    const early = await Promise.race([
      sub.recv(),
      new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 300)),
    ]);
    check(early === "timeout", "the delayed message is withheld before its deadline");

    const late = await sub.recv();
    check(late !== null, "the delayed message arrives after its deadline");

    sub.close();
  } finally {
    await client.shutdown();
  }
});
