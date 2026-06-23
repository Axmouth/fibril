/**
 * Example + light test: publish one JSON message and receive it back, verifying
 * the payload round-trips. Run via run-all.sh, or directly:
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/roundtrip.example.ts
 */
import { NewMessage } from "@fibril/client";
import { assertEq, check, connect, runExample, uniqueTopic } from "./checks.js";

await runExample("roundtrip", async () => {
  const client = await connect("example-roundtrip");
  try {
    const topic = uniqueTopic("roundtrip");
    const sub = await client.subscribe(topic).prefetch(1).sub();

    const offset = await client
      .publisher(topic)
      .publishConfirmed(NewMessage.json({ hello: "world", n: 42 }));
    check(offset >= 0n, "confirmed publish returns a broker offset");

    const msg = await sub.recv();
    check(msg !== null, "a message was delivered");
    const body = msg!.deserialize<{ hello: string; n: number }>();
    assertEq(body.hello, "world", "payload.hello");
    assertEq(body.n, 42, "payload.n");
    await msg!.complete();

    sub.close();
  } finally {
    await client.shutdown();
  }
});
