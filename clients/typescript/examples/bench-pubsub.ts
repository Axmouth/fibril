/**
 * Combined publish + deliver throughput benchmark.
 *
 * A saturating publisher and a consumer run at the same time over separate
 * connections to one broker, so this measures publish and deliver throughput
 * together (and whether deliver keeps up with publish, i.e. the backlog).
 *
 * Run a broker on 127.0.0.1:9876, then:
 *   npx tsx examples/bench-pubsub.ts
 *
 * MODE selects what this process runs:
 *   both (default) - publisher and consumer share one event loop (one core)
 *   pub            - publisher only (run a separate MODE=sub process to consume)
 *   sub            - consumer only (run a separate MODE=pub process to feed it)
 *
 * Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE, DURATION_MS, WARMUP_MS,
 * TOPIC, GROUP, PREFETCH, ACK (auto|manual), MODE (both|pub|sub).
 */
import { ClientOptions, NewMessage } from "../src/index.js";

const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";
const user = process.env.FIBRIL_USER ?? "fibril";
const pass = process.env.FIBRIL_PASS ?? "fibril";
const size = Number(process.env.SIZE ?? 1024);
const durationMs = Number(process.env.DURATION_MS ?? 8000);
const warmupMs = Number(process.env.WARMUP_MS ?? 2000);
const topic = process.env.TOPIC ?? "benchpubsub";
const group = process.env.GROUP ?? "workers";
const prefetch = Number(process.env.PREFETCH ?? 4096);
const ack = process.env.ACK ?? "auto";
const mode = process.env.MODE ?? "both";
const doPub = mode === "both" || mode === "pub";
const doSub = mode === "both" || mode === "sub";

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const opts = new ClientOptions({ clientName: "bench" }).withAuth(user, pass);
const producer = doPub ? await opts.connect(addr) : null;
const consumer = doSub ? await opts.connect(addr) : null;

let published = 0;
let delivered = 0;
let running = true;

async function publishLoop(): Promise<void> {
  const pub = producer!.publisherGrouped(topic, group);
  const payload = new Uint8Array(size);
  while (running) {
    for (let i = 0; i < 256; i++) {
      await pub.publish(NewMessage.raw(payload));
      published++;
    }
    // Yield to the event loop each batch so the consumer and timers get to run.
    await new Promise((r) => setImmediate(r));
  }
}

async function consumeLoop(): Promise<void> {
  const builder = consumer!.subscribe(topic).group(group).prefetch(prefetch);
  if (ack === "manual") {
    const sub = await builder.sub();
    for await (const msg of sub) {
      await msg.complete();
      delivered++;
      if (!running) break;
    }
  } else {
    const sub = await builder.subAutoAck();
    for await (const _msg of sub) {
      delivered++;
      if (!running) break;
    }
  }
}

const tasks: Promise<void>[] = [];
if (doSub) tasks.push(consumeLoop().catch(() => {}));
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

const parts = [`mode=${mode}`];
if (doPub) parts.push(`publish=${Math.round((p1 - p0) / elapsed)}`);
if (doSub) parts.push(`deliver=${Math.round((d1 - d0) / elapsed)}`);
parts.push(`msgs/s  ack=${ack} size=${size}`);
if (doPub) parts.push(`published=${p1}`);
if (doSub) parts.push(`delivered=${d1}`);
if (doPub && doSub) parts.push(`backlog=${p1 - d1}`);
parts.push(`elapsed=${elapsed.toFixed(1)}s`);
console.log(parts.join("  "));

if (producer) await producer.shutdown();
if (consumer) await consumer.shutdown();
await Promise.allSettled(tasks);
