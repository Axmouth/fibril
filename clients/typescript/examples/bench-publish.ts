/**
 * Single-client saturating publish benchmark (unconfirmed).
 *
 * Run a broker on 127.0.0.1:9876, then:
 *   npx tsx examples/bench-publish.ts
 * Env: FIBRIL_ADDR, SIZE (payload bytes), DURATION_MS, WARMUP_MS.
 */
import { ClientOptions, NewMessage } from "../src/index.js";

const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";
const user = process.env.FIBRIL_USER ?? "fibril";
const pass = process.env.FIBRIL_PASS ?? "fibril";
const size = Number(process.env.SIZE ?? 1024);
const durationMs = Number(process.env.DURATION_MS ?? 8000);
const warmupMs = Number(process.env.WARMUP_MS ?? 2000);

const client = await new ClientOptions({ clientName: "bench" })
  .withAuth(user, pass)
  .connect(addr);
const pub = client.publisher("benchtopic");
const payload = new Uint8Array(size);

// Publish in inner bursts so Date.now() is not read every message.
async function loopUntil(deadline: number): Promise<number> {
  let count = 0;
  while (Date.now() < deadline) {
    for (let i = 0; i < 512; i++) {
      await pub.publish(NewMessage.raw(payload));
      count++;
    }
  }
  return count;
}

await loopUntil(Date.now() + warmupMs);
const start = Date.now();
const count = await loopUntil(start + durationMs);
const elapsed = (Date.now() - start) / 1000;
console.log(`rate=${Math.round(count / elapsed)} msgs/s count=${count} elapsed=${elapsed.toFixed(1)}s`);

await client.shutdown();
