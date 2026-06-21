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
  type DeclareQueueMsg,
  type Hello,
  type NackMsg,
  type PublishDelayedMsg,
  type PublishMsg,
  type ReconcileClientMsg,
  type SubscribeMsg,
} from "../src/protocol.js";
import { Client, ClientOptions, QueueConfig } from "../src/client.js";
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
    const sub = await client.subscribe("jobs").group("workers").subManualAck();
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
    });
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("default and blank groups normalize to ungrouped declarations and subscriptions", async () => {
  assert.deepEqual(new QueueConfig("jobs").group(" default ").toWire(), {
    topic: "jobs",
    group: null,
    dlq_policy: null,
    dlq_max_retries: null,
  });
  assert.deepEqual(new QueueConfig("jobs").group("   ").toWire(), {
    topic: "jobs",
    group: null,
    dlq_policy: null,
    dlq_max_retries: null,
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
    const sub = await client.subscribe("jobs").group(" default ").subManualAck();

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
    const sub = await client.subscribe("t1").subManualAck();
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
    const sub = await client.subscribe("t").subManualAck();

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
    const sub = await client.subscribe("manual-delay").subManualAck();
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
    const sub = await client.subscribe("jobs").subManualAck();
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
