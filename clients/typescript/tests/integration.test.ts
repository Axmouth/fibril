import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server, type Socket } from "node:net";
import {
  buildFrame,
  decodeFrameBody,
  encodeFrame,
  tryDecodeFrame,
  type Frame,
} from "../src/codec.js";
import {
  COMPLIANCE_STRING,
  Op,
  PROTOCOL_V1,
  type AssignmentChangedMsg,
  type DeclareQueueMsg,
  type Hello,
  type NackMsg,
  type PublishDelayedMsg,
  type PublishMsg,
  type ReconcileClientMsg,
  type SubscribeMsg,
  type TopologyOkMsg,
} from "../src/protocol.js";
import { Client, ClientOptions, QueueConfig, type Catalogue } from "../src/client.js";
import { BrokenPipeError, DisconnectionError, RedirectError } from "../src/errors.js";
import { NewMessage } from "../src/message.js";
import { fnv1a } from "../src/internal/topology.js";

// The wire format carries identity fields as raw 16-byte UUIDs, so the fake
// broker uses byte arrays rather than the hyphenated string form.
const uuid = (fill: number): Uint8Array => new Uint8Array(16).fill(fill);
const OWNER_ID = uuid(0x10);
const CLIENT_ID = uuid(0x00);
const RESUME_TOKEN = uuid(0x20);

function helloOk(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    protocol_version: PROTOCOL_V1,
    owner_id: OWNER_ID,
    client_id: CLIENT_ID,
    resume_token: RESUME_TOKEN,
    resume_outcome: "new",
    server_name: "fake",
    compliance: COMPLIANCE_STRING,
    ...overrides,
  };
}

/**
 * A minimal in-process fake broker that handles HELLO, SUBSCRIBE, PUBLISH,
 * and lets us push deliveries.
 */
class FakeBroker {
  #server: Server;
  #clients: Set<Socket> = new Set();
  port = 0;
  /** Captured frames received from any client. */
  received: Frame[] = [];
  onFrame: ((f: Frame, s: Socket) => void) | null = null;

  async start(): Promise<void> {
    this.#server = createServer((socket) => {
      this.#clients.add(socket);
      let buf = new Uint8Array(0);
      socket.on("data", (chunk) => {
        const merged = new Uint8Array(buf.byteLength + chunk.byteLength);
        merged.set(buf, 0);
        merged.set(chunk, buf.byteLength);
        buf = merged;
        while (true) {
          const r = tryDecodeFrame(buf);
          if (!r) break;
          buf = buf.subarray(r.consumed);
          this.received.push(r.frame);
          this.onFrame?.(r.frame, socket);
        }
      });
      socket.on("close", () => this.#clients.delete(socket));
    });
    await new Promise<void>((resolve) => {
      this.#server.listen(0, "127.0.0.1", () => {
        const addr = this.#server.address();
        if (addr && typeof addr !== "string") this.port = addr.port;
        resolve();
      });
    });
  }

  /** Number of currently open client connections to this broker. */
  get openConnections(): number {
    return this.#clients.size;
  }

  send(socket: Socket, frame: Frame): void {
    socket.write(encodeFrame(frame));
  }

  async stop(): Promise<void> {
    for (const s of this.#clients) s.destroy();
    await new Promise<void>((resolve) => this.#server.close(() => resolve()));
  }
}

test("client connects, handshakes, publishes confirmed", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        const h = decodeFrameBody<Hello>(f);
        assert.equal(h.protocol_version, PROTOCOL_V1);
        assert.equal(h.resume, null);
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Publish) {
        const p = decodeFrameBody<PublishMsg>(f);
        assert.equal(p.group, null);
        assert.deepEqual(p.content_type, { kind: "msg_pack" });
        assert.equal(p.headers["content-type"], undefined);
        if (p.require_confirm) {
          broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 7n }));
        }
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const pub = client.publisherGrouped("t1", " default ");
    const offset = await pub.publishConfirmed({ hello: "world" });
    assert.equal(offset, 7n);
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("client reconnect offers previous resume identity", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    const hellos: Hello[] = [];
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        const h = decodeFrameBody<Hello>(f);
        hellos.push(h);
        broker.send(
          s,
          buildFrame(
            Op.HelloOk,
            f.requestId,
            helloOk(
              h.resume
                ? {
                    client_id: h.resume.client_id,
                    owner_id: h.resume.owner_id,
                    resume_token: h.resume.resume_token,
                    resume_outcome: "resumed",
                  }
                : {},
            ),
          ),
        );
      } else if (f.opcode === Op.ReconcileClient) {
        broker.send(s, buildFrame(Op.ReconcileResult, f.requestId, { subscriptions: [] }));
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions({ superviseSubscriptions: false }).withReconnectReconcilePolicy("restore_client_subscriptions"),
    );
    const outcome = await client.reconnect();

    assert.equal(hellos.length, 2);
    assert.equal(outcome.resumeOutcome, "resumed");
    assert.equal(hellos[0]!.resume, null);
    assert.deepEqual(hellos[1]!.resume, {
      owner_id: OWNER_ID,
      client_id: CLIENT_ID,
      resume_token: RESUME_TOKEN,
    });

    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("existing publisher uses reconnected engine", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    const hellos: Hello[] = [];
    const publishes: PublishMsg[] = [];
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        const h = decodeFrameBody<Hello>(f);
        hellos.push(h);
        broker.send(
          s,
          buildFrame(
            Op.HelloOk,
            f.requestId,
            helloOk(
              h.resume
                ? {
                    client_id: h.resume.client_id,
                    owner_id: h.resume.owner_id,
                    resume_token: h.resume.resume_token,
                    resume_outcome: "resumed",
                  }
                : {},
            ),
          ),
        );
      } else if (f.opcode === Op.ReconcileClient) {
        broker.send(s, buildFrame(Op.ReconcileResult, f.requestId, { subscriptions: [] }));
      } else if (f.opcode === Op.Publish) {
        const p = decodeFrameBody<PublishMsg>(f);
        publishes.push(p);
        broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 99n }));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const pub = client.publisher("jobs");
    const outcome = await client.reconnect();
    const offset = await pub.publishConfirmed("after reconnect");

    assert.equal(outcome.resumeOutcome, "resumed");
    assert.equal(offset, 99n);
    assert.equal(hellos.length, 2);
    assert.equal(publishes.length, 1);
    assert.equal(publishes[0]!.topic, "jobs");

    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("client sends active subscriptions during reconnect reconciliation", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let reconcile: ReconcileClientMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        const h = decodeFrameBody<Hello>(f);
        broker.send(
          s,
          buildFrame(
            Op.HelloOk,
            f.requestId,
            helloOk(
              h.resume
                ? {
                    client_id: h.resume.client_id,
                    owner_id: h.resume.owner_id,
                    resume_token: h.resume.resume_token,
                    resume_outcome: "resumed",
                  }
                : {},
            ),
          ),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 55n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            prefetch: sub.prefetch,
          }),
        );
      } else if (f.opcode === Op.ReconcileClient) {
        reconcile = decodeFrameBody<ReconcileClientMsg>(f);
        const clientSub = {
          sub_id: 55n,
          topic: "jobs",
          group: "workers",
          partition: 0,
          auto_ack: false,
          prefetch: 1,
        };
        const restored = { ...clientSub, sub_id: 66n };
        broker.send(
          s,
          buildFrame(Op.ReconcileResult, f.requestId, {
            subscriptions: [
              {
                client: clientSub,
                server: restored,
                action: "keep",
                reason: "server_id_changed",
              },
            ],
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 99n, {
            sub_id: 66n,
            topic: "jobs",
            group: "workers",
            partition: 0,
            offset: 9n,
            delivery_tag: { epoch: 123n },
            published: 1n,
            publish_received: 2n,
            content_type: null,
            headers: {},
            payload: new Uint8Array([1, 2, 3]),
          }),
        );
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions({ superviseSubscriptions: false }).withReconnectReconcilePolicy("restore_client_subscriptions"),
    );
    const sub = await client.subscribe("jobs").group("workers").sub();
    const outcome = await client.reconnect();

    assert.equal(outcome.resumeOutcome, "resumed");
    assert.deepEqual(reconcile, {
      policy: "restore_client_subscriptions",
      subscriptions: [
        {
          sub_id: 55n,
          topic: "jobs",
          group: "workers",
          partition: 0,
          auto_ack: false,
          prefetch: 1,
        },
      ],
    });

    const delivered = await sub.recv();
    assert.ok(delivered);
    assert.deepEqual([...delivered.payload], [1, 2, 3]);
    assert.deepEqual(delivered.deliveryTag, { epoch: 123n });

    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("default auto reconnect attempts before a new operation", async () => {
  const broker = new FakeBroker();
  await broker.start();
  let stopped = false;
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const pub = client.publisher("jobs");
    await broker.stop();
    stopped = true;
    await new Promise((r) => setTimeout(r, 20));

    await assert.rejects(
      () => pub.publish("after close"),
      (err) => err instanceof DisconnectionError,
    );
  } finally {
    if (!stopped) await broker.stop();
  }
});

test("disabled auto reconnect returns broken pipe for closed engine", async () => {
  const broker = new FakeBroker();
  await broker.start();
  let stopped = false;
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions().disableAutoReconnect(),
    );
    const pub = client.publisher("jobs");
    await broker.stop();
    stopped = true;
    await new Promise((r) => setTimeout(r, 20));

    await assert.rejects(
      () => pub.publish("after close"),
      (err) => err instanceof BrokenPipeError,
    );
  } finally {
    if (!stopped) await broker.stop();
  }
});

test("client applies a pushed topology update and acks it", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    const ownerEndpoint = "127.0.0.1:7123";
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
        // Push a topology with one queue partition owned by ownerEndpoint.
        const topology: TopologyOkMsg = {
          generation: 7n,
          queues: [
            {
              topic: "jobs",
              partition: 0,
              group: null,
              owner_endpoint: ownerEndpoint,
              partitioning_version: 1n,
              partition_count: 1,
            },
          ],
          streams: [],
        };
        broker.send(s, buildFrame(Op.TopologyUpdate, 0n, topology));
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions(),
    );

    // The cache should reflect the pushed owner once the reader loop applies it.
    let owner = client._topology().lookup("jobs", 0, null);
    for (let i = 0; i < 100 && !owner; i += 1) {
      await new Promise((r) => setTimeout(r, 10));
      owner = client._topology().lookup("jobs", 0, null);
    }
    assert.equal(owner?.endpoint, ownerEndpoint);
    assert.equal(client._topology().generation, 7n);

    // The client must ack the generation it now reflects.
    let ack: Frame | undefined;
    for (let i = 0; i < 100 && !ack; i += 1) {
      ack = broker.received.find((r) => r.opcode === Op.TopologyUpdateAck);
      if (!ack) await new Promise((r) => setTimeout(r, 10));
    }
    assert.ok(ack, "client should ack the pushed topology update");
    assert.equal(decodeFrameBody<{ generation: bigint }>(ack).generation, 7n);

    // The push also refreshes the catalogue snapshot.
    const catalogue = client.catalogue();
    assert.equal(catalogue.generation, 7n);
    assert.equal(catalogue.queues.length, 1);
    assert.equal(catalogue.queues[0]?.topic, "jobs");
    assert.equal(catalogue.streams.length, 0);

    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("catalogue change feed reports declared queues and streams", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        const topology: TopologyOkMsg = {
          generation: 5n,
          queues: [
            {
              topic: "jobs",
              partition: 0,
              group: "workers",
              owner_endpoint: null,
              partitioning_version: 1n,
              partition_count: 3,
            },
          ],
          streams: [
            {
              topic: "events",
              partition: 0,
              owner_endpoint: null,
              partitioning_version: 1n,
              partition_count: 2,
            },
          ],
        };
        broker.send(s, buildFrame(Op.TopologyOk, f.requestId, topology));
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions(),
    );

    const changed = new Promise<Catalogue>((resolve) => {
      client.onCatalogueChange(resolve);
    });
    await client.fetchTopology();
    const catalogue = await changed;

    assert.equal(catalogue.generation, 5n);
    assert.equal(catalogue.queues.length, 1);
    assert.equal(catalogue.queues[0]?.topic, "jobs");
    assert.equal(catalogue.queues[0]?.group, "workers");
    assert.equal(catalogue.queues[0]?.partitionCount, 3);
    assert.equal(catalogue.streams.length, 1);
    assert.equal(catalogue.streams[0]?.topic, "events");
    assert.equal(catalogue.streams[0]?.partitionCount, 2);
    assert.deepEqual(client.catalogue(), catalogue);

    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("shutdown client does not auto reconnect", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const pub = client.publisher("jobs");
    await client.shutdown();

    await assert.rejects(
      () => pub.publish("after shutdown"),
      (err) => err instanceof BrokenPipeError,
    );
  } finally {
    await broker.stop();
  }
});

test("publish confirmation handle can be awaited later", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Publish) {
        const p = decodeFrameBody<PublishMsg>(f);
        assert.equal(p.require_confirm, true);
        setTimeout(() => {
          broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 8n }));
        }, 50);
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const pub = client.publisher("t1");
    const confirmation = await pub.publishWithConfirmation({ hello: "world" });
    let resolved = false;
    const confirmed = confirmation.confirmed().then((offset) => {
      resolved = true;
      return offset;
    });

    await new Promise((r) => setTimeout(r, 10));
    assert.equal(resolved, false);
    assert.equal(await confirmed, 8n);
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("client declares queue policy", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let declare: DeclareQueueMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.DeclareQueue) {
        declare = decodeFrameBody<DeclareQueueMsg>(f);
        broker.send(
          s,
          buildFrame(Op.DeclareQueueOk, f.requestId, { status: "stored" }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    await client.declareQueue(
      new QueueConfig("jobs")
        .group("workers")
        .customDeadLetterQueue("_dlq.jobs")
        .maxRetries(3),
    );

    assert.deepEqual(declare, {
      topic: "jobs",
      group: "workers",
      dlq_policy: { kind: "custom", topic: "_dlq.jobs", group: null },
      dlq_max_retries: 3,
      partition_count: null,
      default_message_ttl_ms: null,
    });
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("queue config carries a default message ttl", () => {
  assert.deepEqual(new QueueConfig("ephemeral").defaultMessageTtl(30_000).toWire(), {
    topic: "ephemeral",
    group: null,
    dlq_policy: null,
    dlq_max_retries: null,
    default_message_ttl_ms: 30_000n,
  });
});

test("default and blank groups normalize to ungrouped declarations and subscriptions", async () => {
  assert.deepEqual(new QueueConfig("jobs").group(" default ").toWire(), {
    topic: "jobs",
    group: null,
    dlq_policy: null,
    dlq_max_retries: null,
    default_message_ttl_ms: null,
  });
  assert.deepEqual(new QueueConfig("jobs").group("   ").toWire(), {
    topic: "jobs",
    group: null,
    dlq_policy: null,
    dlq_max_retries: null,
    default_message_ttl_ms: null,
  });

  const broker = new FakeBroker();
  await broker.start();
  try {
    let subscribe: SubscribeMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Subscribe) {
        subscribe = decodeFrameBody<SubscribeMsg>(f);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 1n,
            topic: "jobs",
            group: null,
            partition: 0,
            prefetch: 1,
          }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const sub = await client.subscribe("jobs").group(" default ").sub();

    assert.deepEqual(subscribe, {
      topic: "jobs",
      group: null,
      prefetch: 1,
      auto_ack: false,
      partition: 0,
      consumer_group: null,
      consumer_target: null,
      member_id: null,
    });
    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("client subscribes and receives a delivery", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let subId = 0n;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        subId = 100n;
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: subId,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            prefetch: sub.prefetch,
          }),
        );
        // Push a delivery.
        broker.send(
          s,
          buildFrame(Op.Deliver, 0n, {
            sub_id: subId,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            offset: 1n,
            delivery_tag: { epoch: 42n },
            published: 1000n,
            publish_received: 1001n,
            content_type: { kind: "msg_pack" },
            headers: {},
            payload: new Uint8Array([0xa5, 0x68, 0x65, 0x6c, 0x6c, 0x6f]), // msgpack "hello"
          }),
        );
      } else if (f.opcode === Op.Ack) {
        // ack received; nothing more to do.
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const sub = await client.subscribe("t1").sub();
    const iter = sub[Symbol.asyncIterator]();
    const result = await iter.next();
    assert.equal(result.done, false);
    if (!result.done) {
      const msg = result.value;
      assert.equal(msg.deliveryTag.epoch, 42n);
      assert.equal(msg.offset, 1n);
      assert.equal(msg.deserialize<string>(), "hello");
      await msg.complete();
      // Verify ack was sent.
      await new Promise((r) => setTimeout(r, 20));
      assert.ok(
        broker.received.some((f) => f.opcode === Op.Ack),
        "expected an Ack frame to be received by broker",
      );
    }
    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("delivery accepts array-encoded byte payloads from Rust", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 201n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            prefetch: sub.prefetch,
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 0n, {
            sub_id: 201n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            offset: 3n,
            delivery_tag: { epoch: 100n },
            published: 1000n,
            publish_received: 1001n,
            content_type: { kind: "msg_pack" },
            headers: {},
            payload: [0x81, 0xa2, 0x6f, 0x6b, 0xc3],
          }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const sub = await client.subscribe("array-payload").subAutoAck();
    const msg = await sub.recv();
    assert.ok(msg);
    assert.deepEqual([...msg.raw()], [0x81, 0xa2, 0x6f, 0x6b, 0xc3]);
    assert.deepEqual(msg.deserialize<{ ok: boolean }>(), { ok: true });
    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("subscription ends when the engine disconnects with supervision off", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Subscribe) {
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 1n,
            topic: "t",
            group: null,
            partition: 0,
            prefetch: 10,
          }),
        );
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions({ superviseSubscriptions: false }).disableAutoReconnect(),
    );
    const sub = await client.subscribe("t").sub();

    // Stop the broker; with supervision off the stream ends rather than riding
    // through. The iteration must terminate (not hang).
    setTimeout(() => broker.stop(), 10);
    for await (const _msg of sub) {
      // no-op
    }
    await client.shutdown();
  } finally {
    // broker already stopped
  }
});

test("subscription rides through an owner drop by re-subscribing", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let subscribes = 0;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        broker.send(s, buildFrame(Op.TopologyOk, f.requestId, { generation: 0n, queues: [] }));
      } else if (f.opcode === Op.Subscribe) {
        subscribes += 1;
        const subId = BigInt(subscribes);
        const payload = new Uint8Array([subscribes]); // 1 before the drop, 2 after
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: subId,
            topic: "t",
            group: null,
            partition: 0,
            prefetch: 10,
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 0n, {
            sub_id: subId,
            topic: "t",
            group: null,
            partition: 0,
            offset: subId,
            delivery_tag: { epoch: subId },
            published: 1n,
            publish_received: 2n,
            content_type: null,
            headers: {},
            payload,
          }),
        );
        // Drop the owner after the first delivery: the supervisor must reconnect
        // and re-subscribe to keep the stream alive.
        if (subscribes === 1) setTimeout(() => s.destroy(), 20);
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const sub = await client.subscribe("t").subAutoAck();

    const first = await sub.recv();
    assert.ok(first);
    assert.deepEqual([...first.payload], [1]);

    // Second message arrives only after a transparent re-subscribe.
    const second = await sub.recv();
    assert.ok(second);
    assert.deepEqual([...second.payload], [2]);
    assert.ok(subscribes >= 2);

    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("subscription picks up a partition added by a live grow", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    const addr = `127.0.0.1:${broker.port}`;
    let grown = false;
    const subscribedPartitions = new Set<number>();

    const topologyFor = (count: number): TopologyOkMsg => ({
      generation: 1n,
      queues: Array.from({ length: count }, (_, p) => ({
        topic: "t",
        partition: p,
        group: null,
        owner_endpoint: addr,
        partitioning_version: 1n,
        partition_count: count,
      })),
    });

    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        // First refresh (connect warm) reports 1 partition; later refreshes
        // report 2, simulating a live grow.
        broker.send(s, buildFrame(Op.TopologyOk, f.requestId, topologyFor(grown ? 2 : 1)));
        grown = true;
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        subscribedPartitions.add(sub.partition);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: BigInt(subscribedPartitions.size),
            topic: "t",
            group: null,
            partition: sub.partition,
            prefetch: 10,
          }),
        );
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions({
        subscriptionSuperviseIntervalMs: 25,
        topologyRefreshCooldownMs: 1,
      }),
    );
    const sub = await client.subscribe("t").subAutoAck();

    // Initially one partition; the growth poll should pick up partition 1.
    const deadline = Date.now() + 2000;
    while (subscribedPartitions.size < 2 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 15));
    }
    assert.ok(subscribedPartitions.has(0), "subscribed partition 0");
    assert.ok(subscribedPartitions.has(1), "picked up grown partition 1");

    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("publish without confirm does not block on reply", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      }
      // Note: no PublishOk reply.
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const pub = client.publisher("t1");
    await pub.publish({ x: 1 });
    // Wait briefly to ensure broker received it.
    await new Promise((r) => setTimeout(r, 20));
    assert.ok(broker.received.some((f) => f.opcode === Op.Publish));
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("client publishes delayed frame with headers and deadline", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let delayed: PublishDelayedMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.PublishDelayed) {
        delayed = decodeFrameBody<PublishDelayedMsg>(f);
        broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 11n }));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const deadline = new Date(Date.now() + 10_000);
    const offset = await client
      .publisher("t-delay")
      .publishDelayedConfirmed(NewMessage.json({ hello: "later" }), deadline);

    assert.equal(offset, 11n);
    assert.ok(delayed);
    assert.equal(delayed.topic, "t-delay");
    assert.equal(delayed.require_confirm, true);
    assert.deepEqual(delayed.content_type, { kind: "json" });
    assert.equal(delayed.headers["content-type"], undefined);
    assert.equal(delayed.not_before, BigInt(deadline.getTime()));
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("expiring publisher stamps ttl_ms on published frames", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let published: PublishMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Publish) {
        published = decodeFrameBody<PublishMsg>(f);
        broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 7n }));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const offset = await client
      .publisher("t-ttl")
      .expiring(30_000)
      .publishConfirmed(NewMessage.json({ hello: "ephemeral" }));

    assert.equal(offset, 7n);
    assert.ok(published);
    assert.equal(published.topic, "t-ttl");
    assert.equal(published.ttl_ms, 30_000n);
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("client receives assignment-change events (server push)", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let session: Socket | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        session = s;
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const got = new Promise<AssignmentChangedMsg>((resolve) => {
      client.onAssignmentChange(resolve);
    });

    assert.ok(session);
    broker.send(
      session,
      buildFrame(Op.AssignmentChanged, 0n, {
        topic: "orders",
        group: "workers",
        consumer_group: "g1",
        generation: 3n,
        assigned: [0, 1],
        added: [1],
        revoked: [],
      } satisfies AssignmentChangedMsg),
    );

    const event = await got;
    assert.equal(event.topic, "orders");
    assert.equal(event.consumer_group, "g1");
    assert.equal(event.generation, 3n);
    assert.deepEqual(event.assigned, [0, 1]);
    assert.deepEqual(event.added, [1]);
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("delivery deserializes json by content-type", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 200n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            prefetch: sub.prefetch,
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 0n, {
            sub_id: 200n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            offset: 2n,
            delivery_tag: { epoch: 99n },
            published: 1000n,
            publish_received: 1001n,
            content_type: { kind: "json" },
            headers: {},
            payload: new TextEncoder().encode(JSON.stringify({ ok: true })),
          }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const sub = await client.subscribe("json").subAutoAck();
    const msg = await sub.recv();
    assert.ok(msg);
    assert.deepEqual(msg.deserialize<{ ok: boolean }>(), { ok: true });
    assert.equal(msg.contentType(), "application/json");
    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("manual message retryAfter sends delayed nack", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let nack: NackMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(
          s,
          buildFrame(Op.HelloOk, f.requestId, helloOk()),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 201n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            prefetch: sub.prefetch,
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 77n, {
            sub_id: 201n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            offset: 3n,
            delivery_tag: { epoch: 101n },
            published: 1000n,
            publish_received: 1001n,
            content_type: null,
            headers: {},
            payload: new TextEncoder().encode("retry me"),
          }),
        );
      } else if (f.opcode === Op.Nack) {
        nack = decodeFrameBody<NackMsg>(f);
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const sub = await client.subscribe("manual-delay").sub();
    const msg = await sub.recv();
    assert.ok(msg);
    const deadline = new Date(Date.now() + 10_000);
    await msg.retryAfter(deadline);
    // retryAfter resolves once the command is accepted locally. The fake
    // broker callback can observe the socket frame on the next tick.
    for (let i = 0; i < 20 && nack === null; i += 1) {
      await new Promise((r) => setTimeout(r, 5));
    }

    assert.ok(nack);
    assert.equal(nack.topic, "manual-delay");
    assert.equal(nack.requeue, true);
    assert.equal(nack.not_before, BigInt(deadline.getTime()));
    assert.deepEqual(nack.tags, [{ epoch: 101n }]);
    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("fetchTopology returns the broker topology and warms the routing cache", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        broker.send(
          s,
          buildFrame(Op.TopologyOk, f.requestId, {
            generation: 9n,
            queues: [
              {
                topic: "orders",
                partition: 0,
                group: "workers",
                owner_endpoint: "127.0.0.1:9001",
                partitioning_version: 2n,
                partition_count: 3,
              },
            ],
          }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const topology = await client.fetchTopology();
    assert.equal(topology.generation, 9n);
    assert.equal(topology.queues.length, 1);

    const cache = client._topology();
    assert.deepEqual(cache.partitioning("orders", "workers"), { count: 3, version: 2n });
    assert.deepEqual(cache.lookup("orders", 0, "workers"), {
      endpoint: "127.0.0.1:9001",
      partitioningVersion: 2n,
    });
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("a confirmed publish follows an owner redirect to the new broker", async () => {
  const owner = new FakeBroker();
  await owner.start();
  owner.onFrame = (f, s) => {
    if (f.opcode === Op.Hello) {
      owner.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
    } else if (f.opcode === Op.Publish) {
      owner.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 42n }));
    }
  };

  const bootstrap = new FakeBroker();
  await bootstrap.start();
  bootstrap.onFrame = (f, s) => {
    if (f.opcode === Op.Hello) {
      bootstrap.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
    } else if (f.opcode === Op.Publish) {
      bootstrap.send(
        s,
        buildFrame(Op.Redirect, f.requestId, {
          topic: "orders",
          partition: 0,
          group: null,
          owner_endpoint: `127.0.0.1:${owner.port}`,
          partitioning_version: 1n,
        }),
      );
    }
  };

  try {
    const client = await Client.connect(`127.0.0.1:${bootstrap.port}`, new ClientOptions());
    const offset = await client.publisher("orders").publishConfirmed({ hello: "world" });
    assert.equal(offset, 42n);
    // The redirect point-updated routing to the owner connection.
    assert.equal(
      client._topology().lookup("orders", 0, null)?.endpoint,
      `127.0.0.1:${owner.port}`,
    );
    assert.ok(owner.received.some((f) => f.opcode === Op.Publish));
    await client.shutdown();
  } finally {
    await bootstrap.stop();
    await owner.stop();
  }
});

test("a confirmed publish gives up after maxRedirects with RedirectError", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    // The broker always redirects to itself, so the client loops until the
    // redirect budget is exhausted.
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Publish) {
        broker.send(
          s,
          buildFrame(Op.Redirect, f.requestId, {
            topic: "orders",
            partition: 0,
            group: null,
            owner_endpoint: `127.0.0.1:${broker.port}`,
            partitioning_version: 1n,
          }),
        );
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions().withMaxRedirects(2),
    );
    await assert.rejects(
      () => client.publisher("orders").publishConfirmed({ hello: "world" }),
      (err) => err instanceof RedirectError,
    );
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("partition key routes the publish to the hashed partition on the wire", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let published: PublishMsg | null = null;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        broker.send(
          s,
          buildFrame(Op.TopologyOk, f.requestId, {
            generation: 1n,
            queues: [0, 1, 2, 3].map((partition) => ({
              topic: "orders",
              partition,
              group: null,
              owner_endpoint: `127.0.0.1:${broker.port}`,
              partitioning_version: 5n,
              partition_count: 4,
            })),
          }),
        );
      } else if (f.opcode === Op.Publish) {
        published = decodeFrameBody<PublishMsg>(f);
        broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 0n }));
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    await client.fetchTopology();
    await client.publisher("orders").publishConfirmed(
      NewMessage.json({ id: 1 }).partitionKey("entity-1"),
    );

    assert.ok(published);
    const expected = Number(fnv1a(new TextEncoder().encode("entity-1")) % 4n);
    assert.equal(published.partition, expected);
    assert.equal(published.partitioning_version, 5n);
    assert.deepEqual(published.partition_key, new TextEncoder().encode("entity-1"));
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("owner restart: reconnect into a fresh session still reconciles subscriptions", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let reconciled = false;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        // Always a fresh session ("new"), even when the client offers a resume
        // identity: models an owner that bounced and lost its session state.
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: 55n,
            topic: sub.topic,
            group: sub.group,
            partition: 0,
            prefetch: sub.prefetch,
          }),
        );
      } else if (f.opcode === Op.ReconcileClient) {
        reconciled = true;
        const clientSub = {
          sub_id: 55n,
          topic: "jobs",
          group: null,
          partition: 0,
          auto_ack: false,
          prefetch: 1,
        };
        broker.send(
          s,
          buildFrame(Op.ReconcileResult, f.requestId, {
            subscriptions: [
              { client: clientSub, server: { ...clientSub, sub_id: 66n }, action: "keep", reason: "restored" },
            ],
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 1n, {
            sub_id: 66n,
            topic: "jobs",
            group: null,
            partition: 0,
            offset: 1n,
            delivery_tag: { epoch: 7n },
            published: 1n,
            publish_received: 2n,
            content_type: null,
            headers: {},
            payload: new Uint8Array([9]),
          }),
        );
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions({ superviseSubscriptions: false }).withReconnectReconcilePolicy("restore_client_subscriptions"),
    );
    const sub = await client.subscribe("jobs").sub();
    const outcome = await client.reconnect();

    // The session is fresh, yet reconcile still fired and the stream survived.
    assert.equal(outcome.resumeOutcome, "new");
    assert.ok(reconciled);
    const delivered = await sub.recv();
    assert.ok(delivered);
    assert.deepEqual([...delivered.payload], [9]);

    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("a confirmed publish retries across a transient owner failure", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    let publishAttempts = 0;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        broker.send(s, buildFrame(Op.TopologyOk, f.requestId, { generation: 0n, queues: [] }));
      } else if (f.opcode === Op.Publish) {
        publishAttempts += 1;
        if (publishAttempts === 1) {
          s.destroy(); // owner drops mid-flight: a transient transport failure
        } else {
          broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 5n }));
        }
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions().withPublishTimeout(5_000),
    );
    const offset = await client.publisher("orders").publishConfirmed({ x: 1 });
    assert.equal(offset, 5n);
    assert.ok(publishAttempts >= 2, `expected a retry, saw ${publishAttempts} publish attempts`);
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("a confirmed publish with retry disabled fails fast on a transient failure", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Publish) {
        s.destroy();
      }
    };

    const client = await Client.connect(
      `127.0.0.1:${broker.port}`,
      new ClientOptions().withPublishTimeout(0).disableAutoReconnect(),
    );
    await assert.rejects(() => client.publisher("orders").publishConfirmed({ x: 1 }));
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("a full topology refresh prunes pooled connections to dropped owners", async () => {
  const owner = new FakeBroker();
  await owner.start();
  owner.onFrame = (f, s) => {
    if (f.opcode === Op.Hello) {
      owner.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
    } else if (f.opcode === Op.Publish) {
      owner.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 1n }));
    }
  };

  const bootstrap = new FakeBroker();
  await bootstrap.start();
  let topoRequests = 0;
  bootstrap.onFrame = (f, s) => {
    if (f.opcode === Op.Hello) {
      bootstrap.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
    } else if (f.opcode === Op.Topology) {
      topoRequests += 1;
      const queues =
        topoRequests === 1
          ? [
              {
                topic: "orders",
                partition: 0,
                group: null,
                owner_endpoint: `127.0.0.1:${owner.port}`,
                partitioning_version: 1n,
                partition_count: 1,
              },
            ]
          : []; // after failover the owner owns nothing
      bootstrap.send(
        s,
        buildFrame(Op.TopologyOk, f.requestId, { generation: BigInt(topoRequests), queues }),
      );
    }
  };

  try {
    const client = await Client.connect(`127.0.0.1:${bootstrap.port}`, new ClientOptions());
    await client.fetchTopology();
    await client.publisher("orders").publishConfirmed({ x: 1 });
    assert.equal(owner.openConnections, 1, "owner connection pooled after a routed publish");

    // A second full refresh shows the owner owning nothing: its pooled
    // connection must be pruned and closed.
    await client.fetchTopology();
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(owner.openConnections, 0, "stale owner connection pruned");

    await client.shutdown();
  } finally {
    await bootstrap.stop();
    await owner.stop();
  }
});

test("subscribe fans in over all partitions the topology knows", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        broker.send(
          s,
          buildFrame(Op.TopologyOk, f.requestId, {
            generation: 1n,
            queues: [0, 1, 2].map((partition) => ({
              topic: "orders",
              partition,
              group: null,
              owner_endpoint: `127.0.0.1:${broker.port}`,
              partitioning_version: 1n,
              partition_count: 3,
            })),
          }),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        const partition = sub.partition ?? 0;
        const subId = BigInt(partition + 1);
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: subId,
            topic: "orders",
            group: null,
            partition,
            prefetch: sub.prefetch,
          }),
        );
        broker.send(
          s,
          buildFrame(Op.Deliver, 0n, {
            sub_id: subId,
            topic: "orders",
            group: null,
            partition,
            offset: 0n,
            delivery_tag: { epoch: subId },
            published: 1n,
            publish_received: 2n,
            content_type: null,
            headers: {},
            payload: new Uint8Array([partition]),
          }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    await client.fetchTopology(); // warm the cache so the fan-in covers 3 partitions
    const sub = await client.subscribe("orders").subAutoAck();

    const seen = new Set<number>();
    for (let i = 0; i < 3; i += 1) {
      const msg = await sub.recv();
      assert.ok(msg);
      seen.add(msg.payload[0]!);
    }
    assert.deepEqual([...seen].sort(), [0, 1, 2]);

    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("exclusive subscribe mints a cohort member id and carries it across partitions", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    const minted = new Uint8Array(16).fill(0x7);
    const subscribeReqs: SubscribeMsg[] = [];
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Topology) {
        broker.send(
          s,
          buildFrame(Op.TopologyOk, f.requestId, {
            generation: 1n,
            queues: [0, 1].map((partition) => ({
              topic: "orders",
              partition,
              group: null,
              owner_endpoint: `127.0.0.1:${broker.port}`,
              partitioning_version: 1n,
              partition_count: 2,
            })),
          }),
        );
      } else if (f.opcode === Op.Subscribe) {
        const sub = decodeFrameBody<SubscribeMsg>(f);
        subscribeReqs.push(sub);
        const partition = sub.partition ?? 0;
        // The broker mints a member id when the client offers none.
        const memberId = sub.member_id ?? minted;
        broker.send(
          s,
          buildFrame(Op.SubscribeOk, f.requestId, {
            sub_id: BigInt(partition + 1),
            topic: "orders",
            group: null,
            partition,
            prefetch: sub.prefetch,
            consumer_group: sub.consumer_group,
            consumer_target: sub.consumer_target,
            member_id: memberId,
          }),
        );
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    await client.fetchTopology();
    const sub = await client.subscribe("orders").consumerGroup("workers").subAutoAck();

    assert.equal(subscribeReqs.length, 2);
    assert.equal(subscribeReqs[0]!.consumer_group, "workers");
    assert.equal(subscribeReqs[0]!.member_id, null); // first offers none
    assert.deepEqual(subscribeReqs[1]!.member_id, minted); // later carry the minted id

    sub.close();
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("reliable publisher stamps producer ids and retries until confirmed", async () => {
  const broker = new FakeBroker();
  await broker.start();
  try {
    const publishes: PublishMsg[] = [];
    let attempts = 0;
    broker.onFrame = (f, s) => {
      if (f.opcode === Op.Hello) {
        broker.send(s, buildFrame(Op.HelloOk, f.requestId, helloOk()));
      } else if (f.opcode === Op.Publish) {
        publishes.push(decodeFrameBody<PublishMsg>(f));
        attempts += 1;
        if (attempts === 1) {
          // Retryable but not transport-transient, so the inner confirmed publish
          // surfaces it and the reliable loop re-publishes.
          broker.send(s, buildFrame(Op.Error, f.requestId, { code: 409, message: "owner moved" }));
        } else {
          broker.send(s, buildFrame(Op.PublishOk, f.requestId, { offset: 7n }));
        }
      }
    };

    const client = await Client.connect(`127.0.0.1:${broker.port}`, new ClientOptions());
    const reliable = client.publisher("jobs").reliable();
    const pid = reliable.producerId;
    const offset = await reliable.publish("hello");

    assert.equal(offset, 7n);
    assert.equal(publishes.length, 2);
    assert.equal(publishes[0]!.headers["fibril.client.producer_id"], pid);
    assert.equal(publishes[0]!.headers["fibril.client.producer_seq"], "0");
    // A retry re-sends the same sequence.
    assert.equal(publishes[1]!.headers["fibril.client.producer_seq"], "0");

    await client.shutdown();
  } finally {
    await broker.stop();
  }
});
