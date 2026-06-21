/**
 * Example + light test: manual ack with a retry. The first delivery is nacked
 * with requeue, so the broker redelivers it, and the second time it is acked.
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/manual-ack-retry.example.ts
 */
import { NewMessage } from "@fibril/client";
import { assertEq, check, connect, runExample, uniqueTopic } from "./checks.js";

await runExample("manual-ack-retry", async () => {
  const client = await connect("example-manual-ack-retry");
  try {
    const topic = uniqueTopic("retry");
    const sub = await client.subscribe(topic).prefetch(1).subManualAck();

    await client.publisher(topic).publishConfirmed(NewMessage.json({ id: 1 }));

    const first = await sub.recv();
    check(first !== null, "first delivery arrives");
    assertEq(first!.deserialize<{ id: number }>().id, 1, "first payload id");
    // Reject with requeue so the broker redelivers it.
    await first!.retry();

    const second = await sub.recv();
    check(second !== null, "the message is redelivered after a requeue nack");
    assertEq(second!.deserialize<{ id: number }>().id, 1, "redelivered payload id");
    check(second!.offset === first!.offset, "redelivery keeps the same offset");
    await second!.complete();

    sub.close();
  } finally {
    await client.shutdown();
  }
});
