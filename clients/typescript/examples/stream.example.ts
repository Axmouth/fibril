/**
 * Example + light test: a continuous producer/consumer stream.
 *
 * Default: runs forever, printing each packet as it flows, so you can watch a
 * live stream (Ctrl-C to stop).
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/stream.example.ts
 *
 * Check mode (--check or FIBRIL_CHECK=1): publishes a fixed burst, asserts it
 * all comes back, and exits. This is what run-all.sh and CI use.
 *   npx tsx examples/stream.example.ts --check
 */
import { NewMessage } from "@fibril/client";
import { check, checkMode, connect, runExample, timeout, uniqueTopic } from "./checks.js";

const BURST = 50;

await runExample("stream", async () => {
  const client = await connect("example-stream");
  try {
    const topic = uniqueTopic("stream");
    const sub = await client.subscribe(topic).prefetch(64).subAutoAck();
    const publisher = client.publisher(topic);

    if (checkMode()) {
      // Bounded, self-validating burst.
      for (let seq = 0; seq < BURST; seq += 1) {
        await publisher.publish(NewMessage.json({ seq }));
      }
      let received = 0;
      while (received < BURST) {
        const msg = await Promise.race([sub.recv(), timeout(5_000, null)]);
        if (msg === null) break; // closed or timed out
        received += 1;
      }
      check(received >= BURST, `consumed the full burst (got ${received}/${BURST})`);
      sub.close();
      return;
    }

    // Continuous: produce steadily and print each delivery until interrupted.
    let running = true;
    process.on("SIGINT", () => {
      running = false;
    });

    let sent = 0;
    const producer = (async () => {
      while (running) {
        await publisher.publish(NewMessage.json({ seq: sent }));
        sent += 1;
        await timeout(200, null);
      }
    })();

    const consumer = (async () => {
      while (running) {
        const msg = await Promise.race([sub.recv(), timeout(500, null)]);
        if (msg === null) continue;
        const { seq } = msg.deserialize<{ seq: number }>();
        console.log(`[stream] received seq ${seq}`);
      }
    })();

    await Promise.all([producer, consumer]);
    sub.close();
  } finally {
    await client.shutdown();
  }
});
