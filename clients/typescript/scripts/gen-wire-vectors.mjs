// Regenerates the shared cross-client wire vectors at clients/wire_vectors.json
// from the TypeScript wire codec. The Python client and the Rust protocol crate
// pin their encoders to the same file, so all three agree on the exact bytes.
//
// Build the TS client first (`npx tsc`), then run `node scripts/gen-wire-vectors.mjs`.
// Keep this in sync with crates/protocol/tests/wire_vectors.rs and the Python
// test cases when adding or changing an op.

import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as w from "../dist/wire.js";

const here = dirname(fileURLToPath(import.meta.url));
const outPath = join(here, "..", "..", "wire_vectors.json");

const hex = (u) => Buffer.from(u).toString("hex");
const uuid = (b) => new Uint8Array(16).fill(b);

const out = {};

out.hello = hex(
  w.encodeHelloBody({
    clientName: "py-client",
    clientVersion: "0.1.0",
    protocolVersion: 1,
    resume: { ownerId: uuid(1), clientId: uuid(2), resumeToken: uuid(3) },
  }),
);
out.hello_no_resume = hex(
  w.encodeHelloBody({ clientName: "c", clientVersion: "v", protocolVersion: 1, resume: null }),
);
out.hello_ok = hex(
  w.encodeHelloOkBody({
    protocolVersion: 1,
    ownerId: uuid(9),
    clientId: uuid(8),
    resumeToken: uuid(7),
    resumeOutcome: "resumed",
    serverName: "srv",
    compliance: "v=1;x",
  }),
);
out.auth = hex(w.encodeAuthBody({ username: "u", password: "p" }));
out.error = hex(w.encodeErrorBody({ code: 409, message: "not owner" }));
out.publish = hex(
  w.encodePublishBody({
    topic: "orders",
    partition: 3,
    group: "g",
    requireConfirm: true,
    contentType: "json",
    headers: { "x-a": "1" },
    payload: new Uint8Array([1, 2, 3, 4]),
    published: 1234567890n,
    partitionKey: new Uint8Array([9, 9]),
    partitioningVersion: 5n,
    ttlMs: 60000n,
  }),
);
out.publish_no_ttl = hex(
  w.encodePublishBody({
    topic: "t",
    partition: 0,
    group: null,
    requireConfirm: false,
    contentType: null,
    headers: {},
    payload: new Uint8Array(0),
    published: 0n,
    partitionKey: null,
    partitioningVersion: 0n,
    ttlMs: null,
  }),
);
out.publish_custom_ct = hex(
  w.encodePublishBody({
    topic: "t",
    partition: 0,
    group: null,
    requireConfirm: false,
    contentType: { custom: "application/x-thing" },
    headers: {},
    payload: new Uint8Array([7]),
    published: 1n,
    partitionKey: null,
    partitioningVersion: 0n,
    ttlMs: null,
  }),
);
out.publish_delayed = hex(
  w.encodePublishDelayedBody({
    topic: "t",
    partition: 1,
    group: null,
    requireConfirm: true,
    notBefore: 999n,
    contentType: "text",
    headers: { k: "v" },
    payload: new Uint8Array([5, 6]),
    published: 42n,
    partitionKey: null,
    partitioningVersion: 2n,
  }),
);
out.publish_ok = hex(w.encodePublishOkBody({ offset: 777n }));
out.deliver = hex(
  w.encodeDeliverBody({
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
  }),
);
out.ack = hex(
  w.encodeAckBody({ topic: "t", group: null, partition: 0, tags: [{ epoch: 1n }, { epoch: 2n }] }),
);
out.nack = hex(
  w.encodeNackBody({
    topic: "t",
    group: "g",
    partition: 1,
    tags: [{ epoch: 9n }],
    requeue: true,
    notBefore: 5000n,
  }),
);
out.nack_no_nb = hex(
  w.encodeNackBody({ topic: "t", group: null, partition: 0, tags: [], requeue: false, notBefore: null }),
);
out.declare = hex(
  w.encodeDeclareQueueBody({
    topic: "t",
    group: "g",
    dlqPolicy: { custom: { topic: "dlq", group: null } },
    dlqMaxRetries: 3,
    partitionCount: 4,
    defaultMessageTtlMs: 30000n,
  }),
);
out.declare_min = hex(
  w.encodeDeclareQueueBody({
    topic: "t",
    group: null,
    dlqPolicy: null,
    dlqMaxRetries: null,
    partitionCount: null,
    defaultMessageTtlMs: null,
  }),
);
out.declare_ok = hex(w.encodeDeclareQueueOkBody({ status: "created", partitionCount: 4 }));
out.assignment = hex(
  w.encodeAssignmentChangedBody({
    topic: "t",
    group: null,
    consumerGroup: "cg",
    generation: 6n,
    assigned: [0, 1, 2],
    added: [2],
    revoked: [],
  }),
);
out.subscribe = hex(
  w.encodeSubscribeBody({
    topic: "t",
    partition: 1,
    group: "g",
    prefetch: 32,
    autoAck: false,
    consumerGroup: "cg",
    consumerTarget: 2,
    memberId: uuid(4),
  }),
);
out.subscribe_min = hex(
  w.encodeSubscribeBody({
    topic: "t",
    partition: 0,
    group: null,
    prefetch: 0,
    autoAck: true,
    consumerGroup: null,
    consumerTarget: null,
    memberId: null,
  }),
);
out.subscribe_ok = hex(
  w.encodeSubscribeOkBody({
    subId: 5n,
    topic: "t",
    partition: 1,
    group: "g",
    prefetch: 16,
    consumerGroup: "cg",
    consumerTarget: null,
    memberId: uuid(4),
  }),
);
out.topology_req = hex(w.encodeTopologyRequestBody({ topic: "t", group: null }));
out.topology_ok = hex(
  w.encodeTopologyOkBody({
    generation: 12n,
    queues: [
      {
        topic: "t",
        partition: 0,
        group: null,
        ownerEndpoint: "127.0.0.1:7000",
        partitioningVersion: 1n,
        partitionCount: 2,
      },
      {
        topic: "t",
        partition: 1,
        group: null,
        ownerEndpoint: null,
        partitioningVersion: 1n,
        partitionCount: 2,
      },
    ],
    streams: [
      {
        topic: "s",
        partition: 2,
        ownerEndpoint: "10.0.0.9:7100",
        partitioningVersion: 4n,
        partitionCount: 3,
      },
    ],
  }),
);
out.redirect = hex(
  w.encodeRedirectBody({
    topic: "t",
    partition: 1,
    group: "g",
    ownerEndpoint: "h:1",
    partitioningVersion: 3n,
  }),
);
out.reconcile_client = hex(
  w.encodeReconcileClientBody({
    policy: "restore_client_subscriptions",
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
  }),
);

writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
console.log(`wrote ${Object.keys(out).length} vectors to ${outPath}`);
