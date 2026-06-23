"""Client-side routing: a cache of queue ownership plus partition selection.

Mirrors the Rust client (``crates/client`` TopologyCache + route_partition). The
cache is connection-free so routing decisions are testable in isolation. The
connection pool that acts on them lives in ``client.py``. Routing is cache-only
and reactive: a cold or standalone cache routes to partition 0, and a misroute is
corrected by a broker redirect rather than a pre-flight lookup.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..wire import Redirect, TopologyOk, fnv1a


@dataclass
class OwnerEntry:
    endpoint: str
    partitioning_version: int


@dataclass
class PartitioningEntry:
    count: int
    version: int


@dataclass
class Route:
    """The chosen partition and the partitioning version it was decided under."""

    partition: int
    partitioning_version: int


@dataclass
class RoundRobin:
    """Mutable round-robin cursor for keyless publishes."""

    next: int = 0


_QueueKey = tuple[str, int, Optional[str]]
_CountKey = tuple[str, Optional[str]]


class TopologyCache:
    """Cache of queue ownership and partitioning, warmed by topology and redirects."""

    def __init__(self) -> None:
        self.generation = 0
        self.last_refresh_ms = 0.0
        self._by_queue: dict[_QueueKey, OwnerEntry] = {}
        self._counts: dict[_CountKey, PartitioningEntry] = {}

    def lookup(self, topic: str, partition: int, group: Optional[str]) -> Optional[OwnerEntry]:
        return self._by_queue.get((topic, partition, group))

    def partitioning(self, topic: str, group: Optional[str]) -> Optional[PartitioningEntry]:
        return self._counts.get((topic, group))

    def knows_topic(self, topic: str, group: Optional[str]) -> bool:
        """Whether this (topic, group) is declared in the cluster.

        Counts stay populated while an owner is mid-failover, so this remains true
        through a transition and only goes false when the topic is truly absent.
        """
        return self.partitioning(topic, group) is not None

    def is_populated(self) -> bool:
        """Whether the cache holds a real cluster view (at least one declared queue)."""
        return len(self._counts) > 0

    def endpoints(self) -> set[str]:
        """Endpoints that currently own at least one partition."""
        return {entry.endpoint for entry in self._by_queue.values()}

    def replace(self, topology: TopologyOk) -> None:
        """Replace the whole cache from a full topology snapshot."""
        self.generation = topology.generation
        self._by_queue.clear()
        self._counts.clear()
        for queue in topology.queues:
            self._counts[(queue.topic, queue.group)] = PartitioningEntry(
                count=max(queue.partition_count, 1),
                version=queue.partitioning_version,
            )
            # No owner endpoint means the owner node is not in the registry yet
            # (e.g. mid-failover). Keep the count but leave ownership unresolved.
            if queue.owner_endpoint is None:
                continue
            self._by_queue[(queue.topic, queue.partition, queue.group)] = OwnerEntry(
                endpoint=queue.owner_endpoint,
                partitioning_version=queue.partitioning_version,
            )
        # Streams have no group and no per-partition owner entries; they only feed
        # the partitioning cache so a publisher spreads across their partitions.
        for stream in topology.streams:
            self._counts[(stream.topic, None)] = PartitioningEntry(
                count=max(stream.partition_count, 1),
                version=stream.partitioning_version,
            )

    def apply_redirect(self, redirect: Redirect) -> None:
        """Point-update one partition's owner from a redirect."""
        self._by_queue[(redirect.topic, redirect.partition, redirect.group)] = OwnerEntry(
            endpoint=redirect.owner_endpoint,
            partitioning_version=redirect.partitioning_version,
        )

    def invalidate(self, topic: str, partition: int, group: Optional[str]) -> None:
        """Drop one partition's owner, e.g. after it stops being reachable."""
        self._by_queue.pop((topic, partition, group), None)


def route_partition(
    cache: TopologyCache,
    topic: str,
    group: Optional[str],
    key: Optional[bytes],
    round_robin: RoundRobin,
) -> Route:
    """Select the partition for a publish to (topic, group).

    A key routes by ``hash(key) % N``, otherwise round-robin over N. N is the
    authoritative count from the cache. An unknown N (cold cache or standalone)
    routes to partition 0 under version 0, matching a single-partition queue.
    """
    partitioning = cache.partitioning(topic, group)
    version = partitioning.version if partitioning is not None else 0
    count = max(partitioning.count if partitioning is not None else 1, 1)
    if count == 1:
        return Route(partition=0, partitioning_version=version)
    if key is not None:
        index = fnv1a(key) % count
    else:
        index = round_robin.next % count
        round_robin.next += 1
    return Route(partition=index, partitioning_version=version)
