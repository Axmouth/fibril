// Client-side routing state: a cache of queue ownership learned from
// Op.Topology / Op.Redirect, plus the pure partition-selection logic. Mirrors
// the Rust client (crates/client/src/lib.rs TopologyCache + route_partition).
// This module is connection-free on purpose so the routing decisions can be
// tested in isolation; the connection pool that acts on them lives elsewhere.

import type { RedirectMsg, TopologyOkMsg } from "../protocol.js";

const MASK64 = 0xffffffffffffffffn;
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;

/**
 * Stable 64-bit FNV-1a hash for partition selection. Must stay byte-for-byte
 * identical to the broker and every other client so a given key always lands on
 * the same partition (per-key ordering).
 */
export function fnv1a(bytes: Uint8Array): bigint {
  let hash = FNV_OFFSET;
  for (const byte of bytes) {
    hash = (hash ^ BigInt(byte)) & MASK64;
    hash = (hash * FNV_PRIME) & MASK64;
  }
  return hash;
}

/** Owner of one queue partition for routing. */
export interface OwnerEntry {
  endpoint: string;
  partitioningVersion: bigint;
}

/** Authoritative partitioning of a logical queue (topic, group). */
export interface PartitioningEntry {
  count: number;
  version: bigint;
}

/**
 * Outcome of routing one publish: the chosen partition and the partitioning
 * version it was decided under. The version is stamped on the wire so the owner
 * can fence a stale routing view.
 */
export interface Route {
  partition: number;
  partitioningVersion: bigint;
}

// Composite Map keys. JSON encoding keeps a null group distinct from an empty
// string group, which a naive string join would not.
const queueKey = (topic: string, partition: number, group: string | null): string =>
  JSON.stringify([topic, partition, group ?? null]);
const countKey = (topic: string, group: string | null): string =>
  JSON.stringify([topic, group ?? null]);

/**
 * Cache of queue ownership and partitioning. Empty (standalone brokers, or a
 * cold client) means "no routing info" and callers fall back to the bootstrap
 * connection.
 */
export class TopologyCache {
  generation = 0n;
  lastRefreshMs = 0;
  #byQueue = new Map<string, OwnerEntry>();
  #counts = new Map<string, PartitioningEntry>();

  lookup(topic: string, partition: number, group: string | null): OwnerEntry | undefined {
    return this.#byQueue.get(queueKey(topic, partition, group));
  }

  /** Authoritative partitioning for (topic, group), if known. */
  partitioning(topic: string, group: string | null): PartitioningEntry | undefined {
    return this.#counts.get(countKey(topic, group));
  }

  /**
   * Whether this (topic, group) is declared in the cluster. Counts stay
   * populated while an owner is mid-failover, so this remains true through a
   * transition and only goes false when the topic is genuinely absent.
   */
  knowsTopic(topic: string, group: string | null): boolean {
    return this.partitioning(topic, group) !== undefined;
  }

  /** Whether the cache holds a real cluster view (at least one declared queue). */
  isPopulated(): boolean {
    return this.#counts.size > 0;
  }

  /** Endpoints that currently own at least one partition. */
  endpoints(): Set<string> {
    const out = new Set<string>();
    for (const entry of this.#byQueue.values()) out.add(entry.endpoint);
    return out;
  }

  /** Replace the whole cache from a full topology snapshot. */
  replace(topology: TopologyOkMsg): void {
    this.generation = topology.generation;
    this.#byQueue.clear();
    this.#counts.clear();
    for (const queue of topology.queues) {
      this.#counts.set(countKey(queue.topic, queue.group), {
        count: Math.max(queue.partition_count, 1),
        version: queue.partitioning_version,
      });
      // No owner endpoints means the owner node is not in the registry yet (e.g.
      // mid-failover). Keep the count but leave ownership unresolved. The wire
      // carries a priority-ordered list; use the first for now (connecting by
      // name resolves service names). Trying the list in order is a later brick.
      const queueOwner = queue.owner_endpoints[0];
      if (queueOwner === undefined) continue;
      this.#byQueue.set(queueKey(queue.topic, queue.partition, queue.group), {
        endpoint: `${queueOwner.host}:${queueOwner.port}`,
        partitioningVersion: queue.partitioning_version,
      });
    }
    // Streams have no group (keyed under group null). They carry per-partition
    // owners just like queues, so a publisher both spreads across partitions and
    // routes each to its owner. A topic is either a stream or a queue, so the
    // group-null cache entries never collide.
    for (const stream of topology.streams ?? []) {
      this.#counts.set(countKey(stream.topic, null), {
        count: Math.max(stream.partition_count, 1),
        version: stream.partitioning_version,
      });
      const streamOwner = stream.owner_endpoints[0];
      if (streamOwner === undefined) continue;
      this.#byQueue.set(queueKey(stream.topic, stream.partition, null), {
        endpoint: `${streamOwner.host}:${streamOwner.port}`,
        partitioningVersion: stream.partitioning_version,
      });
    }
  }

  /** Point-update one partition's owner from a redirect. */
  applyRedirect(redirect: RedirectMsg): void {
    const owner = redirect.owner_endpoints[0];
    if (owner === undefined) return;
    this.#byQueue.set(queueKey(redirect.topic, redirect.partition, redirect.group), {
      endpoint: `${owner.host}:${owner.port}`,
      partitioningVersion: redirect.partitioning_version,
    });
  }

  /** Drop one partition's owner, e.g. after it stops being reachable. */
  invalidate(topic: string, partition: number, group: string | null): void {
    this.#byQueue.delete(queueKey(topic, partition, group));
  }
}

/** Mutable round-robin cursor for keyless publishes. */
export interface RoundRobin {
  next: number;
}

/**
 * Select the partition for a publish to (topic, group):
 * a key routes by hash(key) % N, otherwise round-robin over N. N is the
 * authoritative count from the cache; an unknown N (cold cache or standalone)
 * routes to partition 0 under version 0, which matches a single-partition queue.
 */
export function routePartition(
  cache: TopologyCache,
  topic: string,
  group: string | null,
  key: Uint8Array | null,
  roundRobin: RoundRobin,
): Route {
  const partitioning = cache.partitioning(topic, group);
  const version = partitioning?.version ?? 0n;
  const count = Math.max(partitioning?.count ?? 1, 1);
  if (count === 1) {
    return { partition: 0, partitioningVersion: version };
  }
  let index: number;
  if (key !== null) {
    index = Number(fnv1a(key) % BigInt(count));
  } else {
    const n = roundRobin.next;
    roundRobin.next = n + 1;
    index = n % count;
  }
  return { partition: index, partitioningVersion: version };
}
