"""Routing-cache tests mirroring the Rust/TS clients.

Covers stream ownership resolution: a stream entry must feed both the
partitioning count and the per-partition owner (keyed by group ``None``), so a
stream publish routes to its owner the same way a queue publish does.
"""

from fibril import wire
from fibril.internal.topology import OwnerEntry, TopologyCache


def test_replace_resolves_stream_owners_under_none_group() -> None:
    cache = TopologyCache()
    cache.replace(
        wire.TopologyOk(
            generation=4,
            queues=[],
            streams=[
                wire.StreamTopologyEntry("logs", 0, [wire.AdvertisedAddress("127.0.0.1", 9100)], 2, 2),
                # owner unresolved mid-failover: count known, owner absent
                wire.StreamTopologyEntry("logs", 1, [], 2, 2),
            ],
        )
    )

    partitioning = cache.partitioning("logs", None)
    assert partitioning is not None
    assert partitioning.count == 2
    assert cache.lookup("logs", 0, None) == OwnerEntry("127.0.0.1:9100", 2)
    assert cache.lookup("logs", 1, None) is None


def test_stream_owner_distinct_from_same_named_queue_is_not_possible_but_keys_isolate() -> None:
    # A topic is either a stream or a queue, but the cache keys still isolate the
    # group-None stream entry from any string-group queue entry on another topic.
    cache = TopologyCache()
    cache.replace(
        wire.TopologyOk(
            generation=1,
            queues=[wire.QueueTopologyEntry("orders", 0, "g", [wire.AdvertisedAddress("127.0.0.1", 1)], 0, 1)],
            streams=[wire.StreamTopologyEntry("logs", 0, [wire.AdvertisedAddress("127.0.0.1", 2)], 0, 1)],
        )
    )
    assert cache.lookup("orders", 0, "g") is not None
    assert cache.lookup("logs", 0, None) == OwnerEntry("127.0.0.1:2", 0)
