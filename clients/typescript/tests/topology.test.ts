import { test } from "node:test";
import { adv } from "./helpers.js";
import assert from "node:assert/strict";

import {
  TopologyCache,
  fnv1a,
  routePartition,
  type RoundRobin,
} from "../src/internal/topology.js";
import type { TopologyOkMsg } from "../src/protocol.js";

const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

test("fnv1a matches the canonical 64-bit FNV-1a vectors", () => {
  // Empty input is the offset basis; "a" is a published reference vector. These
  // pin both constants so the client agrees with the broker on key routing.
  assert.equal(fnv1a(new Uint8Array()), 0xcbf29ce484222325n);
  assert.equal(fnv1a(enc("a")), 0xaf63dc4c8601ec8cn);
});

test("fnv1a is deterministic and key-sensitive", () => {
  assert.equal(fnv1a(enc("entity-123")), fnv1a(enc("entity-123")));
  assert.notEqual(fnv1a(enc("a")), fnv1a(enc("b")));
});

function topology(): TopologyOkMsg {
  return {
    generation: 7n,
    queues: [
      {
        topic: "orders",
        partition: 0,
        group: "workers",
        owner_endpoints: [adv("127.0.0.1:9001")],
        partitioning_version: 3n,
        partition_count: 2,
      },
      {
        topic: "orders",
        partition: 1,
        group: "workers",
        owner_endpoints: [], // owner mid-failover: count known, owner unresolved
        partitioning_version: 3n,
        partition_count: 2,
      },
      {
        topic: "events",
        partition: 0,
        group: null,
        owner_endpoints: [adv("127.0.0.1:9002")],
        partitioning_version: 1n,
        partition_count: 1,
      },
    ],
  };
}

test("replace populates counts and resolved owners", () => {
  const cache = new TopologyCache();
  cache.replace(topology());

  assert.equal(cache.generation, 7n);
  assert.ok(cache.isPopulated());
  assert.ok(cache.knowsTopic("orders", "workers"));
  assert.ok(cache.knowsTopic("events", null));
  assert.equal(cache.knowsTopic("missing", null), false);

  assert.deepEqual(cache.partitioning("orders", "workers"), { count: 2, version: 3n });
  assert.deepEqual(cache.lookup("orders", 0, "workers"), {
    endpoint: "127.0.0.1:9001",
    partitioningVersion: 3n,
  });

  // Count is known even though the owner is unresolved.
  assert.equal(cache.lookup("orders", 1, "workers"), undefined);
  assert.ok(cache.knowsTopic("orders", "workers"));

  assert.deepEqual(cache.endpoints(), new Set(["127.0.0.1:9001", "127.0.0.1:9002"]));
});

test("replace resolves stream owners under the null group", () => {
  // A stream entry feeds both the partitioning count and the per-partition
  // owner (keyed by null group), so a stream publish routes to its owner.
  const cache = new TopologyCache();
  cache.replace({
    generation: 4n,
    queues: [],
    streams: [
      {
        topic: "logs",
        partition: 0,
        owner_endpoints: [adv("127.0.0.1:9100")],
        partitioning_version: 2n,
        partition_count: 2,
      },
      {
        topic: "logs",
        partition: 1,
        owner_endpoints: [], // owner unresolved mid-failover
        partitioning_version: 2n,
        partition_count: 2,
      },
    ],
  });

  assert.deepEqual(cache.partitioning("logs", null), { count: 2, version: 2n });
  assert.deepEqual(cache.lookup("logs", 0, null), {
    endpoint: "127.0.0.1:9100",
    partitioningVersion: 2n,
  });
  assert.equal(cache.lookup("logs", 1, null), undefined);
});

test("null group is distinct from a same-named string group", () => {
  const cache = new TopologyCache();
  cache.replace({
    generation: 1n,
    queues: [
      {
        topic: "t",
        partition: 0,
        group: null,
        owner_endpoints: [adv("127.0.0.1:1")],
        partitioning_version: 0n,
        partition_count: 1,
      },
      {
        topic: "t",
        partition: 0,
        group: "g",
        owner_endpoints: [adv("127.0.0.1:2")],
        partitioning_version: 0n,
        partition_count: 1,
      },
    ],
  });
  assert.equal(cache.lookup("t", 0, null)?.endpoint, "127.0.0.1:1");
  assert.equal(cache.lookup("t", 0, "g")?.endpoint, "127.0.0.1:2");
});

test("applyRedirect point-updates an owner and invalidate drops it", () => {
  const cache = new TopologyCache();
  cache.applyRedirect({
    topic: "orders",
    partition: 1,
    group: "workers",
    owner_endpoints: [adv("127.0.0.1:9009")],
    partitioning_version: 4n,
  });
  assert.deepEqual(cache.lookup("orders", 1, "workers"), {
    endpoint: "127.0.0.1:9009",
    partitioningVersion: 4n,
  });

  cache.invalidate("orders", 1, "workers");
  assert.equal(cache.lookup("orders", 1, "workers"), undefined);
});

test("routePartition: unknown queue routes to partition 0 under version 0", () => {
  const cache = new TopologyCache();
  const rr: RoundRobin = { next: 0 };
  assert.deepEqual(routePartition(cache, "cold", null, null, rr), {
    partition: 0,
    partitioningVersion: 0n,
  });
});

test("routePartition: single-partition queue always routes to 0", () => {
  const cache = new TopologyCache();
  cache.replace(topology());
  const rr: RoundRobin = { next: 0 };
  const route = routePartition(cache, "events", null, enc("any-key"), rr);
  assert.deepEqual(route, { partition: 0, partitioningVersion: 1n });
});

test("routePartition: key routing is deterministic and in range", () => {
  const cache = new TopologyCache();
  cache.replace(topology());
  const rr: RoundRobin = { next: 0 };

  const a = routePartition(cache, "orders", "workers", enc("entity-1"), rr);
  const b = routePartition(cache, "orders", "workers", enc("entity-1"), rr);
  assert.equal(a.partition, b.partition);
  assert.equal(a.partitioningVersion, 3n);
  assert.ok(a.partition >= 0 && a.partition < 2);

  const expected = Number(fnv1a(enc("entity-1")) % 2n);
  assert.equal(a.partition, expected);
});

test("routePartition: keyless publishes round-robin across partitions", () => {
  const cache = new TopologyCache();
  cache.replace(topology());
  const rr: RoundRobin = { next: 0 };

  const seen = [
    routePartition(cache, "orders", "workers", null, rr).partition,
    routePartition(cache, "orders", "workers", null, rr).partition,
    routePartition(cache, "orders", "workers", null, rr).partition,
  ];
  assert.deepEqual(seen, [0, 1, 0]);
  assert.equal(rr.next, 3);
});
