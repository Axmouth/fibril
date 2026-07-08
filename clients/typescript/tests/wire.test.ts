import { test } from "node:test";
import { adv } from "./helpers.js";
import assert from "node:assert/strict";

import { WireError } from "../src/errors.js";

import {
  Reader,
  Writer,
  encodeHelloBody,
  decodeHelloBody,
  encodeHelloOkBody,
  decodeHelloOkBody,
  encodeAuthBody,
  decodeAuthBody,
  encodeErrorBody,
  decodeErrorBody,
  type ResumeIdentity,
} from "../src/wire.js";

const uuid = (fill: number): Uint8Array => new Uint8Array(16).fill(fill);

test("primitives round-trip", () => {
  const w = new Writer();
  w.u8(0xab);
  w.u16(0x1234);
  w.u32(0xdeadbeef);
  w.u64(0x0102030405060708n);
  w.str("héllo 🌍");
  w.bool(true);
  w.bool(false);
  w.uuid(uuid(7));
  w.optionalStr(null);
  w.optionalStr("present");
  w.optionalUuid(uuid(9));
  w.optionalUuid(null);

  const r = new Reader(w.finish());
  assert.equal(r.u8(), 0xab);
  assert.equal(r.u16(), 0x1234);
  assert.equal(r.u32(), 0xdeadbeef);
  assert.equal(r.u64(), 0x0102030405060708n);
  assert.equal(r.str(), "héllo 🌍");
  assert.equal(r.bool(), true);
  assert.equal(r.bool(), false);
  assert.deepEqual(r.uuid(), uuid(7));
  assert.equal(r.optionalStr(), null);
  assert.equal(r.optionalStr(), "present");
  assert.deepEqual(r.optionalUuid(), uuid(9));
  assert.equal(r.optionalUuid(), null);
  r.finish();
});

test("big-endian byte layout matches the wire spec", () => {
  const w = new Writer();
  w.u32(0x01020304);
  assert.deepEqual(w.finish(), new Uint8Array([0x01, 0x02, 0x03, 0x04]));

  const s = new Writer();
  s.str("AB"); // u32 len = 2, then 'A','B'
  assert.deepEqual(s.finish(), new Uint8Array([0, 0, 0, 2, 0x41, 0x42]));
});

test("Reader.finish rejects trailing bytes", () => {
  const w = new Writer();
  w.u8(1);
  w.u8(2);
  const r = new Reader(w.finish());
  assert.equal(r.u8(), 1);
  assert.throws(() => r.finish(), /trailing/);
});

test("hello body round-trips with and without resume", () => {
  const noResume = { clientName: "fibril-ts", clientVersion: "0.1.0", protocolVersion: 1 };
  assert.deepEqual(decodeHelloBody(encodeHelloBody(noResume)), {
    ...noResume,
    resume: null,
  });

  const resume: ResumeIdentity = { ownerId: uuid(1), clientId: uuid(2), resumeToken: uuid(3) };
  const withResume = { clientName: "c", clientVersion: "v", protocolVersion: 1, resume };
  assert.deepEqual(decodeHelloBody(encodeHelloBody(withResume)), withResume);
});

test("hello_ok body round-trips across resume outcomes", () => {
  for (const outcome of ["new", "resumed", "resume_not_found", "resume_rejected"] as const) {
    const hello = {
      protocolVersion: 1,
      ownerId: uuid(4),
      clientId: uuid(5),
      resumeToken: uuid(6),
      resumeOutcome: outcome,
      serverName: "fibril",
      compliance: "ok",
    };
    assert.deepEqual(decodeHelloOkBody(encodeHelloOkBody(hello)), hello);
  }
});

test("auth and error bodies round-trip", () => {
  const auth = { username: "fibril", password: "secret" };
  assert.deepEqual(decodeAuthBody(encodeAuthBody(auth)), auth);

  const err = { code: 409, message: "not the owner" };
  assert.deepEqual(decodeErrorBody(encodeErrorBody(err)), err);
});

test("magic mismatch is rejected", () => {
  const body = encodeAuthBody({ username: "a", password: "b" });
  assert.throws(() => decodeHelloBody(body), /magic/);
});

import {
  encodePublishBody,
  decodePublishBody,
  encodePublishDelayedBody,
  decodePublishDelayedBody,
  encodePublishOkBody,
  decodePublishOkBody,
  encodeDeliverBody,
  decodeDeliverBody,
  encodeAckBody,
  decodeAckBody,
  encodeNackBody,
  decodeNackBody,
  encodeDeclareQueueBody,
  decodeDeclareQueueBody,
  encodeDeclareQueueOkBody,
  decodeDeclareQueueOkBody,
  encodeSubscribeBody,
  decodeSubscribeBody,
  encodeSubscribeOkBody,
  decodeSubscribeOkBody,
} from "../src/wire.js";

test("publish body round-trips (with and without optionals)", () => {
  const full = {
    topic: "orders",
    partition: 3,
    group: "g",
    requireConfirm: true,
    contentType: "json" as const,
    headers: { a: "1", b: "two" },
    payload: new Uint8Array([1, 2, 3, 4]),
    published: 123n,
    partitionKey: new Uint8Array([9, 9]),
    partitioningVersion: 7n,
    ttlMs: 30000n,
  };
  assert.deepEqual(decodePublishBody(encodePublishBody(full)), full);

  const minimal = {
    topic: "t",
    partition: 0,
    group: null,
    requireConfirm: false,
    contentType: null,
    headers: {},
    payload: new Uint8Array(),
    published: 0n,
    partitionKey: null,
    partitioningVersion: 0n,
    ttlMs: null,
  };
  assert.deepEqual(decodePublishBody(encodePublishBody(minimal)), minimal);
});

test("publish_delayed body round-trips with custom content type", () => {
  const p = {
    topic: "t",
    partition: 1,
    group: null,
    requireConfirm: true,
    notBefore: 999n,
    contentType: { custom: "application/x-thing" },
    headers: { k: "v" },
    payload: new Uint8Array([5]),
    published: 1n,
    partitionKey: null,
    partitioningVersion: 2n,
  };
  assert.deepEqual(decodePublishDelayedBody(encodePublishDelayedBody(p)), p);
});

test("publish_ok body round-trips", () => {
  assert.deepEqual(decodePublishOkBody(encodePublishOkBody({ offset: 42n })), { offset: 42n });
});

test("deliver body round-trips", () => {
  const d = {
    subId: 77n,
    topic: "jobs",
    group: null,
    partition: 0,
    offset: 9n,
    deliveryTag: { epoch: 123n },
    published: 1n,
    publishReceived: 2n,
    contentType: "msgpack" as const,
    headers: {},
    payload: new Uint8Array([0xde, 0xad]),
  };
  assert.deepEqual(decodeDeliverBody(encodeDeliverBody(d)), d);
});

test("ack and nack bodies round-trip", () => {
  const ack = { topic: "t", group: "g", partition: 2, tags: [{ epoch: 1n }, { epoch: 2n }] };
  assert.deepEqual(decodeAckBody(encodeAckBody(ack)), ack);

  const nackRequeue = {
    topic: "t",
    group: null,
    partition: 0,
    tags: [{ epoch: 5n }],
    requeue: true,
    notBefore: 1000n,
  };
  assert.deepEqual(decodeNackBody(encodeNackBody(nackRequeue)), nackRequeue);

  const nackPlain = {
    topic: "t",
    group: null,
    partition: 0,
    tags: [],
    requeue: false,
    notBefore: null,
  };
  assert.deepEqual(decodeNackBody(encodeNackBody(nackPlain)), nackPlain);
});

test("declare_queue body round-trips across dlq policies", () => {
  const discard = {
    topic: "t",
    group: null,
    dlqPolicy: "discard" as const,
    dlqMaxRetries: 3,
    partitionCount: 4,
    defaultMessageTtlMs: 30000n,
  };
  assert.deepEqual(decodeDeclareQueueBody(encodeDeclareQueueBody(discard)), discard);

  const custom = {
    topic: "t",
    group: "g",
    dlqPolicy: { custom: { topic: "_dlq.t", group: null } },
    dlqMaxRetries: null,
    partitionCount: null,
    defaultMessageTtlMs: null,
  };
  assert.deepEqual(decodeDeclareQueueBody(encodeDeclareQueueBody(custom)), custom);

  const none = {
    topic: "t",
    group: null,
    dlqPolicy: null,
    dlqMaxRetries: null,
    partitionCount: null,
    defaultMessageTtlMs: null,
  };
  assert.deepEqual(decodeDeclareQueueBody(encodeDeclareQueueBody(none)), none);

  assert.deepEqual(
    decodeDeclareQueueOkBody(encodeDeclareQueueOkBody({ status: "created", partitionCount: 4 })),
    { status: "created", partitionCount: 4 },
  );
});

test("subscribe and subscribe_ok bodies round-trip", () => {
  const member = new Uint8Array(16).fill(3);
  const sub = {
    topic: "jobs",
    partition: 0,
    group: null,
    prefetch: 32,
    autoAck: false,
    consumerGroup: "workers",
    consumerTarget: 2,
    memberId: member,
  };
  assert.deepEqual(decodeSubscribeBody(encodeSubscribeBody(sub)), sub);

  const ok = {
    subId: 88n,
    topic: "jobs",
    partition: 0,
    group: null,
    prefetch: 32,
    consumerGroup: null,
    consumerTarget: null,
    memberId: null,
  };
  assert.deepEqual(decodeSubscribeOkBody(encodeSubscribeOkBody(ok)), ok);
});

import {
  encodeReconcileClientBody,
  decodeReconcileClientBody,
  encodeReconcileServerBody,
  decodeReconcileServerBody,
  encodeReconcileResultBody,
  decodeReconcileResultBody,
  type ReconcileSubscription,
} from "../src/wire.js";

const reconcileSub = (subId: bigint): ReconcileSubscription => ({
  subId,
  topic: "jobs",
  partition: 0,
  group: null,
  autoAck: false,
  prefetch: 32,
  consumerGroup: null,
  consumerTarget: null,
  memberId: null,
});

test("reconcile client/server bodies round-trip", () => {
  const rc = {
    policy: "restore" as const,
    subscriptions: [reconcileSub(1n), reconcileSub(2n)],
  };
  assert.deepEqual(decodeReconcileClientBody(encodeReconcileClientBody(rc)), rc);

  const rs = { subscriptions: [reconcileSub(7n)] };
  assert.deepEqual(decodeReconcileServerBody(encodeReconcileServerBody(rs)), rs);
});

test("reconcile result body round-trips across actions", () => {
  const rr = {
    subscriptions: [
      {
        client: reconcileSub(1n),
        server: reconcileSub(2n),
        action: "keep" as const,
        reason: "matched",
      },
      {
        client: reconcileSub(3n),
        server: null,
        action: "close_client_side" as const,
        reason: "server_missing",
      },
    ],
  };
  assert.deepEqual(decodeReconcileResultBody(encodeReconcileResultBody(rr)), rr);
});

import {
  encodeTopologyRequestBody,
  decodeTopologyRequestBody,
  encodeTopologyOkBody,
  decodeTopologyOkBody,
  encodeRedirectBody,
  decodeRedirectBody,
} from "../src/wire.js";

test("topology request body round-trips with and without filters", () => {
  const full = { topic: "orders", group: "workers" };
  assert.deepEqual(decodeTopologyRequestBody(encodeTopologyRequestBody(full)), full);

  const empty = { topic: null, group: null };
  assert.deepEqual(decodeTopologyRequestBody(encodeTopologyRequestBody(empty)), empty);
});

test("topology ok body round-trips across entries", () => {
  const topology = {
    generation: 12n,
    queues: [
      {
        topic: "orders",
        partition: 0,
        group: "workers",
        ownerEndpoints: [adv("127.0.0.1:9001")],
        partitioningVersion: 3n,
        partitionCount: 4,
      },
      {
        topic: "orders",
        partition: 1,
        group: "workers",
        ownerEndpoints: [],
        partitioningVersion: 3n,
        partitionCount: 4,
      },
    ],
    streams: [
      {
        topic: "events",
        partition: 2,
        ownerEndpoints: [adv("10.0.0.9:7100")],
        partitioningVersion: 4n,
        partitionCount: 3,
      },
    ],
  };
  assert.deepEqual(decodeTopologyOkBody(encodeTopologyOkBody(topology)), topology);

  const emptyTopology = { generation: 0n, queues: [], streams: [] };
  assert.deepEqual(decodeTopologyOkBody(encodeTopologyOkBody(emptyTopology)), emptyTopology);
});

test("redirect body round-trips", () => {
  const redirect = {
    topic: "orders",
    partition: 2,
    group: null,
    ownerEndpoints: [adv("127.0.0.1:9002")],
    partitioningVersion: 5n,
  };
  assert.deepEqual(decodeRedirectBody(encodeRedirectBody(redirect)), redirect);
});

test("decode failures throw typed WireError with a kind", () => {
  // Bad magic on a publish body.
  const good = encodePublishBody({
    topic: "t",
    partition: 0,
    group: null,
    requireConfirm: false,
    contentType: null,
    headers: {},
    payload: new Uint8Array(),
    published: 0n,
    partitionKey: null,
    partitioningVersion: 0n,
    ttlMs: null,
  });
  const badMagic = good.slice();
  badMagic[0] ^= 0xff;
  assert.throws(
    () => decodePublishBody(badMagic),
    (err: unknown) => err instanceof WireError && err.kind === "invalid_magic",
  );

  // Truncated body (header only) -> unexpected_eof.
  assert.throws(
    () => decodePublishBody(good.slice(0, 4)),
    (err: unknown) => err instanceof WireError && err.kind === "unexpected_eof",
  );

  // Trailing bytes after a valid body.
  const trailing = new Uint8Array(good.length + 1);
  trailing.set(good);
  assert.throws(
    () => decodePublishBody(trailing),
    (err: unknown) => err instanceof WireError && err.kind === "trailing_bytes",
  );
});

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import {
  encodeDeclarePlexusBody,
  decodeDeclarePlexusBody,
  encodeDeclarePlexusOkBody,
  decodeDeclarePlexusOkBody,
  encodeSubscribeStreamBody,
  decodeSubscribeStreamBody,
  encodeTopologyUpdateBody,
  decodeTopologyUpdateBody,
  encodeTopologyUpdateAckBody,
  decodeTopologyUpdateAckBody,
  encodeGoingAwayBody,
  decodeGoingAwayBody,
  type DeclarePlexus,
  type SubscribeStream,
  type TopologyOk,
} from "../src/wire.js";

import * as wireModule from "../src/wire.js";

const toHex = (b: Uint8Array): string =>
  Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");

// The shared cross-client fixture lives at clients/wire_vectors.json (two levels
// up from this tests directory).
const VECTORS: Record<string, string> = JSON.parse(
  readFileSync(
    join(dirname(fileURLToPath(import.meta.url)), "..", "..", "wire_vectors.json"),
    "utf-8",
  ),
);

const fromHex = (hex: string): Uint8Array =>
  new Uint8Array((hex.match(/../g) ?? []).map((b) => parseInt(b, 16)));

// Maps every shared vector name to the body op whose codec produces it, so the
// conformance test below is data-driven over the whole fixture. A new vector with
// no entry here fails the guard, forcing a matching TS codec case.
const CODEC_OP: Record<string, string> = {
  ack: "Ack",
  assignment: "AssignmentChanged",
  auth: "Auth",
  declare: "DeclareQueue",
  declare_min: "DeclareQueue",
  declare_ok: "DeclareQueueOk",
  declare_plexus: "DeclarePlexus",
  declare_plexus_min: "DeclarePlexus",
  declare_plexus_ok: "DeclarePlexusOk",
  deliver: "Deliver",
  error: "Error",
  going_away: "GoingAway",
  hello: "Hello",
  hello_no_resume: "Hello",
  hello_ok: "HelloOk",
  nack: "Nack",
  nack_no_nb: "Nack",
  publish: "Publish",
  publish_custom_ct: "Publish",
  publish_delayed: "PublishDelayed",
  publish_no_ttl: "Publish",
  publish_ok: "PublishOk",
  reconcile_client: "ReconcileClient",
  redirect: "Redirect",
  subscribe: "Subscribe",
  subscribe_min: "Subscribe",
  subscribe_ok: "SubscribeOk",
  subscribe_stream: "SubscribeStream",
  subscribe_stream_min: "SubscribeStream",
  topology_ok: "TopologyOk",
  topology_req: "TopologyRequest",
  topology_update: "TopologyUpdate",
  topology_update_ack: "TopologyUpdateAck",
};

const decoders = wireModule as unknown as Record<string, (b: Uint8Array) => unknown>;
const encoders = wireModule as unknown as Record<string, (v: unknown) => Uint8Array>;

// The canonical decoded value each shared vector encodes from. These are the
// authoritative cross-client inputs (the same values the reference and the other
// clients pin), so the test below checks both directions against them.
const CANONICAL: Record<string, unknown> = {
  ack: { topic: "t", group: null, partition: 0, tags: [{ epoch: 1n }, { epoch: 2n }] },
  assignment: {
    topic: "t",
    group: null,
    consumerGroup: "cg",
    generation: 6n,
    assigned: [0, 1, 2],
    added: [2],
    revoked: [],
  },
  auth: { username: "u", password: "p" },
  declare: {
    topic: "t",
    group: "g",
    dlqPolicy: { custom: { topic: "dlq", group: null } },
    dlqMaxRetries: 3,
    partitionCount: 4,
    defaultMessageTtlMs: 30000n,
  },
  declare_min: {
    topic: "t",
    group: null,
    dlqPolicy: null,
    dlqMaxRetries: null,
    partitionCount: null,
    defaultMessageTtlMs: null,
  },
  declare_ok: { status: "created", partitionCount: 4 },
  declare_plexus: {
    topic: "t",
    partitionCount: 4,
    durability: "speculative",
    retention: { maxAgeMs: 60000n, maxBytes: null, retainRecords: 1000000n },
    replicationFactor: 2,
  },
  declare_plexus_min: {
    topic: "t",
    partitionCount: null,
    durability: "durable",
    retention: { maxAgeMs: null, maxBytes: null, retainRecords: null },
    replicationFactor: null,
  },
  declare_plexus_ok: { status: "created", partitionCount: 4 },
  deliver: {
    subId: 11n,
    topic: "t",
    group: "g",
    partition: 2,
    offset: 100n,
    deliveryTag: { epoch: 5n },
    published: 7n,
    publishReceived: 8n,
    contentType: "msgpack",
    headers: { h: "1" },
    payload: new Uint8Array([3, 2, 1]),
  },
  error: { code: 409, message: "not owner" },
  going_away: { graceMs: 30000n, message: "broker restarting for upgrade" },
  hello: {
    clientName: "py-client",
    clientVersion: "0.1.0",
    protocolVersion: 1,
    resume: {
      ownerId: new Uint8Array(16).fill(1),
      clientId: new Uint8Array(16).fill(2),
      resumeToken: new Uint8Array(16).fill(3),
    },
  },
  hello_no_resume: { clientName: "c", clientVersion: "v", protocolVersion: 1, resume: null },
  hello_ok: {
    protocolVersion: 1,
    ownerId: new Uint8Array(16).fill(9),
    clientId: new Uint8Array(16).fill(8),
    resumeToken: new Uint8Array(16).fill(7),
    resumeOutcome: "resumed",
    serverName: "srv",
    compliance: "v=1;x",
  },
  nack: {
    topic: "t",
    group: "g",
    partition: 1,
    tags: [{ epoch: 9n }],
    requeue: true,
    notBefore: 5000n,
  },
  nack_no_nb: { topic: "t", group: null, partition: 0, tags: [], requeue: false, notBefore: null },
  publish: {
    topic: "orders",
    group: "g",
    partition: 3,
    requireConfirm: true,
    contentType: "json",
    headers: { "x-a": "1" },
    published: 1234567890n,
    partitionKey: new Uint8Array([9, 9]),
    partitioningVersion: 5n,
    payload: new Uint8Array([1, 2, 3, 4]),
    ttlMs: 60000n,
  },
  publish_custom_ct: {
    topic: "t",
    group: null,
    partition: 0,
    requireConfirm: false,
    contentType: { custom: "application/x-thing" },
    headers: {},
    published: 1n,
    partitionKey: null,
    partitioningVersion: 0n,
    payload: new Uint8Array([7]),
    ttlMs: null,
  },
  publish_delayed: {
    topic: "t",
    group: null,
    partition: 1,
    requireConfirm: true,
    notBefore: 999n,
    contentType: "text",
    headers: { k: "v" },
    published: 42n,
    partitionKey: null,
    partitioningVersion: 2n,
    payload: new Uint8Array([5, 6]),
  },
  publish_no_ttl: {
    topic: "t",
    group: null,
    partition: 0,
    requireConfirm: false,
    contentType: null,
    headers: {},
    published: 0n,
    partitionKey: null,
    partitioningVersion: 0n,
    payload: new Uint8Array([]),
    ttlMs: null,
  },
  publish_ok: { offset: 777n },
  reconcile_client: {
    policy: "restore",
    subscriptions: [
      {
        subId: 1n,
        topic: "t",
        partition: 0,
        group: null,
        autoAck: false,
        prefetch: 8,
        consumerGroup: null,
        consumerTarget: null,
        memberId: null,
      },
    ],
  },
  redirect: {
    topic: "t",
    partition: 1,
    group: "g",
    ownerEndpoints: [{ host: "h", port: 1, tags: [] }],
    partitioningVersion: 3n,
  },
  subscribe: {
    topic: "t",
    partition: 1,
    group: "g",
    prefetch: 32,
    autoAck: false,
    consumerGroup: "cg",
    consumerTarget: 2,
    memberId: new Uint8Array(16).fill(4),
  },
  subscribe_min: {
    topic: "t",
    partition: 0,
    group: null,
    prefetch: 0,
    autoAck: true,
    consumerGroup: null,
    consumerTarget: null,
    memberId: null,
  },
  subscribe_ok: {
    subId: 5n,
    topic: "t",
    partition: 1,
    group: "g",
    prefetch: 16,
    consumerGroup: "cg",
    consumerTarget: null,
    memberId: new Uint8Array(16).fill(4),
  },
  subscribe_stream: {
    topic: "t",
    partition: 1,
    durableName: "c1",
    start: { kind: "bytime", value: 1234n },
    filter: [
      ["region", "eu-*"],
      ["kind", "order"],
    ],
    prefetch: 16,
    autoAck: false,
  },
  subscribe_stream_min: {
    topic: "t",
    partition: 0,
    durableName: null,
    start: { kind: "latest" },
    filter: [],
    prefetch: 0,
    autoAck: true,
  },
  topology_ok: {
    generation: 12n,
    queues: [
      {
        topic: "t",
        partition: 0,
        group: null,
        ownerEndpoints: [{ host: "127.0.0.1", port: 7000, tags: [] }],
        partitioningVersion: 1n,
        partitionCount: 2,
      },
      {
        topic: "t",
        partition: 1,
        group: null,
        ownerEndpoints: [],
        partitioningVersion: 1n,
        partitionCount: 2,
      },
    ],
    streams: [
      {
        topic: "s",
        partition: 2,
        ownerEndpoints: [{ host: "10.0.0.9", port: 7100, tags: [] }],
        partitioningVersion: 4n,
        partitionCount: 3,
      },
    ],
  },
  topology_req: { topic: "t", group: null },
  topology_update: {
    generation: 12n,
    queues: [
      {
        topic: "t",
        partition: 0,
        group: null,
        ownerEndpoints: [{ host: "127.0.0.1", port: 7000, tags: [] }],
        partitioningVersion: 1n,
        partitionCount: 2,
      },
    ],
    streams: [
      {
        topic: "s",
        partition: 2,
        ownerEndpoints: [{ host: "10.0.0.9", port: 7100, tags: [] }],
        partitioningVersion: 4n,
        partitionCount: 3,
      },
    ],
  },
  topology_update_ack: { generation: 12n },
};

// Byte-exact conformance for the WHOLE fixture, both directions: every shared
// vector encodes from its canonical value to the exact bytes, and decodes back to
// that value. Data-driven over the fixture, so a TS codec that diverges from the
// cross-client spec on any op is caught (not just the handful checked inline
// above), and a new vector with no CODEC_OP / CANONICAL entry fails the guard.
test("every shared vector encodes from and decodes to its canonical value", () => {
  for (const [name, hex] of Object.entries(VECTORS)) {
    const op = CODEC_OP[name];
    assert.ok(op, `shared vector "${name}" has no TS codec mapped in CODEC_OP`);
    assert.ok(name in CANONICAL, `shared vector "${name}" has no canonical value in CANONICAL`);
    const input = CANONICAL[name];
    assert.equal(
      toHex(encoders[`encode${op}Body`](input)),
      hex,
      `${name} does not encode to the shared vector bytes`,
    );
    assert.deepEqual(
      decoders[`decode${op}Body`](fromHex(hex)),
      input,
      `${name} does not decode to its canonical value`,
    );
  }
});

test("plexus stream bodies match shared vectors and round-trip", () => {
  const declare: DeclarePlexus = {
    topic: "t",
    partitionCount: 4,
    durability: "speculative",
    retention: { maxAgeMs: 60000n, maxBytes: null, retainRecords: 1000000n },
    replicationFactor: 2,
  };
  assert.equal(toHex(encodeDeclarePlexusBody(declare)), VECTORS.declare_plexus);
  assert.deepEqual(decodeDeclarePlexusBody(encodeDeclarePlexusBody(declare)), declare);

  const declareMin: DeclarePlexus = {
    topic: "t",
    partitionCount: null,
    durability: "durable",
    retention: { maxAgeMs: null, maxBytes: null, retainRecords: null },
    replicationFactor: null,
  };
  assert.equal(toHex(encodeDeclarePlexusBody(declareMin)), VECTORS.declare_plexus_min);
  assert.deepEqual(decodeDeclarePlexusBody(encodeDeclarePlexusBody(declareMin)), declareMin);

  const ok = { status: "created", partitionCount: 4 };
  assert.equal(toHex(encodeDeclarePlexusOkBody(ok)), VECTORS.declare_plexus_ok);
  assert.deepEqual(decodeDeclarePlexusOkBody(encodeDeclarePlexusOkBody(ok)), ok);

  const sub: SubscribeStream = {
    topic: "t",
    partition: 1,
    durableName: "c1",
    start: { kind: "bytime", value: 1234n },
    filter: [
      ["region", "eu-*"],
      ["kind", "order"],
    ],
    prefetch: 16,
    autoAck: false,
  };
  assert.equal(toHex(encodeSubscribeStreamBody(sub)), VECTORS.subscribe_stream);
  assert.deepEqual(decodeSubscribeStreamBody(encodeSubscribeStreamBody(sub)), sub);

  const subMin: SubscribeStream = {
    topic: "t",
    partition: 0,
    durableName: null,
    start: { kind: "latest" },
    filter: [],
    prefetch: 0,
    autoAck: true,
  };
  assert.equal(toHex(encodeSubscribeStreamBody(subMin)), VECTORS.subscribe_stream_min);
  assert.deepEqual(decodeSubscribeStreamBody(encodeSubscribeStreamBody(subMin)), subMin);
});

test("topology update push + ack bodies match shared vectors and round-trip", () => {
  const topology: TopologyOk = {
    generation: 12n,
    queues: [
      {
        topic: "t",
        partition: 0,
        group: null,
        ownerEndpoints: [adv("127.0.0.1:7000")],
        partitioningVersion: 1n,
        partitionCount: 2,
      },
    ],
    streams: [
      {
        topic: "s",
        partition: 2,
        ownerEndpoints: [adv("10.0.0.9:7100")],
        partitioningVersion: 4n,
        partitionCount: 3,
      },
    ],
  };
  assert.equal(toHex(encodeTopologyUpdateBody(topology)), VECTORS.topology_update);
  assert.deepEqual(decodeTopologyUpdateBody(encodeTopologyUpdateBody(topology)), topology);

  const ack = { generation: 12n };
  assert.equal(toHex(encodeTopologyUpdateAckBody(ack)), VECTORS.topology_update_ack);
  assert.deepEqual(decodeTopologyUpdateAckBody(encodeTopologyUpdateAckBody(ack)), ack);
});

test("going away body matches shared vector and round-trips", () => {
  const notice = { graceMs: 30000n, message: "broker restarting for upgrade" };
  assert.equal(toHex(encodeGoingAwayBody(notice)), VECTORS.going_away);
  assert.deepEqual(decodeGoingAwayBody(encodeGoingAwayBody(notice)), notice);
});
