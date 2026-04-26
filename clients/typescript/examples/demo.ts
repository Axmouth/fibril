/**
 * Demo: a producer publishing to two topics, with two consumers showing
 * both manual-ack and auto-ack modes. Demonstrates:
 *
 *   - Confirmed publish with offset
 *   - Manual-ack subscription with retry-on-failure
 *   - Auto-ack subscription with simple iteration
 *   - Concurrent producer + consumer tasks
 *   - Graceful shutdown on Ctrl-C
 *
 * Run:
 *   npx tsx examples/demo.ts
 *   FIBRIL_ADDR=127.0.0.1:9876 npx tsx examples/demo.ts
 *
 * With auth:
 *   FIBRIL_USER=alice FIBRIL_PASS=secret npx tsx examples/demo.ts
 */

import {
  ClientOptions,
  FibrilError,
  type InflightMessage,
  type Subscription,
  type AutoAckedSubscription,
} from "@fibril/client";

const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";
const user = process.env.FIBRIL_USER ?? "fibril";
const pass = process.env.FIBRIL_PASS ?? "fibril";

interface Order {
  id: number;
  item: string;
  qty: number;
}

interface Notice {
  text: string;
  ts: number;
}

let opts = new ClientOptions({
  clientName: "fibril-demo",
  clientVersion: "0.1.0",
});
if (user && pass) opts = opts.withAuth(user, pass);

const client = await opts.connect(addr);
console.log(`connected to ${addr}`);

// ---- consumers ----
//
// We subscribe BEFORE starting the producer so the broker has the
// subscriptions registered by the time messages arrive. Otherwise
// (depending on broker semantics) we might miss the first batch.

const orders = await client.subscribe("orders").group("workers").prefetch(50).subManualAck();

const notices = await client.subscribe("notices").group("workers").prefetch(20).subAutoAck();

// ---- shutdown wiring ----

const shutdownController = new AbortController();
const shutdown = (reason: string): void => {
  if (shutdownController.signal.aborted) return;
  console.log(`\nshutting down: ${reason}`);
  shutdownController.abort();
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ---- producer ----

async function produce(): Promise<void> {
  const orderPub = client.publisherGrouped("orders", "workers");
  const noticePub = client.publisherGrouped("notices", "workers");

  let i = 0;
  while (!shutdownController.signal.aborted) {
    i += 1;
    const order: Order = {
      id: i,
      item: ["widget", "gadget", "gizmo"][i % 3]!,
      qty: 1 + (i % 5),
    };
    try {
      const offset = await orderPub.publish(order);
      console.log(`[producer] order #${order.id} -> offset ${offset}`);
    } catch (err) {
      if (err instanceof FibrilError) {
        console.error(`[producer] publish failed: ${err.message}`);
        return; // engine is gone; bail
      }
      throw err;
    }

    // Notices are fire-and-forget.
    if (i % 3 === 0) {
      const notice: Notice = {
        text: `processed batch ${i / 3}`,
        ts: Date.now(),
      };
      await noticePub.publishUnconfirmed(notice).catch(() => {});
    }

    await sleep(500, shutdownController.signal);
  }
}

// ---- order consumer (manual ack + retry on flaky processing) ----

async function consumeOrders(sub: Subscription): Promise<void> {
  try {
    for await (const msg of sub) {
      if (shutdownController.signal.aborted) {
        sub.close();
        break;
      }
      await processOrder(msg);
    }
  } catch (err) {
    if (err instanceof FibrilError) {
      console.error(`[orders] iteration ended with: ${err.message}`);
    } else {
      throw err;
    }
  }
  console.log("[orders] consumer exited");
}

async function processOrder(msg: InflightMessage): Promise<void> {
  let order: Order;
  try {
    order = msg.deserialize<Order>();
  } catch (err) {
    console.error(`[orders] bad payload (offset=${msg.offset}); rejecting`);
    await msg.fail();
    return;
  }

  // Simulate flaky processing: 1 in 7 fails the first time.
  if (order.id % 7 === 0 && !retried.has(order.id)) {
    retried.add(order.id);
    console.log(`[orders] order #${order.id} failed; requeuing`);
    await msg.retry();
    return;
  }

  console.log(`[orders] processed order #${order.id} (${order.qty}× ${order.item})`);
  await msg.complete();
}
const retried = new Set<number>();

// ---- notice consumer (auto-ack, just print) ----

async function consumeNotices(sub: AutoAckedSubscription): Promise<void> {
  try {
    for await (const msg of sub) {
      if (shutdownController.signal.aborted) {
        sub.close();
        break;
      }
      const notice = msg.deserialize<Notice>();
      const lagMs = Date.now() - notice.ts;
      console.log(`[notices] ${notice.text} (lag ${lagMs}ms)`);
    }
  } catch (err) {
    if (err instanceof FibrilError) {
      console.error(`[notices] iteration ended with: ${err.message}`);
    } else {
      throw err;
    }
  }
  console.log("[notices] consumer exited");
}

// ---- helpers ----

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve();
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

// ---- run ----

await Promise.all([produce(), consumeOrders(orders), consumeNotices(notices)]);

await client.shutdown();
console.log("done");
