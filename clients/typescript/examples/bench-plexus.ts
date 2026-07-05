/**
 * Plexus (fan-out stream) throughput benchmark.
 *
 * A saturating publisher feeds a stream while CONSUMERS independent stream
 * subscribers each receive every record (fan-out), so deliver throughput is
 * aggregated across all of them. Mirrors bench-pubsub, over a stream instead of a
 * work queue.
 *
 * Run a broker on 127.0.0.1:9876, then:
 *   npx tsx examples/bench-plexus.ts
 *
 * MODE selects what this process runs:
 *   both (default) - publisher and consumers share one event loop (one core)
 *   pub            - publisher only
 *   sub            - consumers only
 *
 * Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE, DURATION_MS, WARMUP_MS,
 * TOPIC, PARTITIONS, PREFETCH, MODE (both|pub|sub), CONSUMERS,
 * DURABILITY (ephemeral|speculative|durable).
 */
import { ClientOptions, NewMessage, StreamConfig } from "../src/index.js";

const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";
const user = process.env.FIBRIL_USER ?? "fibril";
const pass = process.env.FIBRIL_PASS ?? "fibril";
const size = Number(process.env.SIZE ?? 1024);
const durationMs = Number(process.env.DURATION_MS ?? 8000);
const warmupMs = Number(process.env.WARMUP_MS ?? 2000);
const topic = process.env.TOPIC ?? "benchplexus";
const partitions = Number(process.env.PARTITIONS ?? 1);
const prefetch = Number(process.env.PREFETCH ?? 4096);
const mode = process.env.MODE ?? "both";
const consumers = Number(process.env.CONSUMERS ?? 1);
const durability = process.env.DURABILITY ?? "durable";
const doPub = mode === "both" || mode === "pub";
const doSub = mode === "both" || mode === "sub";

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const opts = new ClientOptions({ clientName: "bench" }).withAuth(user, pass);

// Declare the stream once up front on a throwaway connection.
const admin = await opts.connect(addr);
let cfg = new StreamConfig(topic).partitions(partitions);
cfg = durability === "ephemeral" ? cfg.ephemeral() : durability === "speculative" ? cfg.speculative() : cfg.durable();
await admin.declarePlexus(cfg);
await admin.shutdown();

const producer = doPub ? await opts.connect(addr) : null;
const consumerClients = doSub ? await Promise.all(Array.from({ length: consumers }, () => opts.connect(addr))) : [];

let published = 0;
let delivered = 0;
let running = true;

async function publishLoop(): Promise<void> {
  const pub = producer!.publisher(topic);
  const payload = new Uint8Array(size);
  while (running) {
    for (let i = 0; i < 256; i++) {
      await pub.publish(NewMessage.raw(payload));
      published++;
    }
    await new Promise((r) => setImmediate(r));
  }
}

async function consumeLoop(client: (typeof consumerClients)[number]): Promise<void> {
  const sub = await client.stream(topic).fromLatest().prefetch(prefetch).subAutoAck();
  for await (const _msg of sub) {
    delivered++;
    if (!running) break;
  }
}

const tasks: Promise<void>[] = [];
for (const client of consumerClients) tasks.push(consumeLoop(client).catch(() => {}));
if (doPub) tasks.push(publishLoop().catch(() => {}));

await sleep(warmupMs);
const p0 = published;
const d0 = delivered;
const t0 = Date.now();
await sleep(durationMs);
const elapsed = (Date.now() - t0) / 1000;
const p1 = published;
const d1 = delivered;
running = false;

const parts = [`mode=${mode}`, `durability=${durability}`];
if (doPub) parts.push(`publish=${Math.round((p1 - p0) / elapsed)}`);
if (doSub) {
  const per = Math.round((d1 - d0) / elapsed / Math.max(consumers, 1));
  parts.push(`deliver=${Math.round((d1 - d0) / elapsed)} (fanout=${consumers}, per_consumer=${per})`);
}
parts.push(`msgs/s  size=${size}  elapsed=${elapsed.toFixed(1)}s`);
console.log(parts.join("  "));

if (producer) await producer.shutdown();
for (const client of consumerClients) await client.shutdown();
await Promise.allSettled(tasks);
