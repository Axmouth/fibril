"""Client, publisher, and subscription tests over the in-process fake broker."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from fibril import wire
from fibril.client import Client, ClientOptions, QueueConfig
from fibril.message import HEADER_PRODUCER_ID, HEADER_PRODUCER_SEQ, NewMessage

from fake_broker import FakeBroker


@pytest_asyncio.fixture
async def broker():
    b = FakeBroker()
    await b.start()
    try:
        yield b
    finally:
        await b.stop()


async def _connect(broker: FakeBroker, *, supervise: bool = False) -> Client:
    return await Client.connect(
        (broker.host, broker.port),
        ClientOptions(supervise_subscriptions=supervise),
    )


def _topology_two_partitions(broker: FakeBroker) -> wire.TopologyOk:
    owners = [wire.AdvertisedAddress(broker.host, broker.port)]
    return wire.TopologyOk(
        generation=1,
        queues=[
            wire.QueueTopologyEntry("jobs", 0, None, owners, 1, 2),
            wire.QueueTopologyEntry("jobs", 1, None, owners, 1, 2),
        ],
    )


async def test_publish_confirmed(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        offset = await client.publisher("jobs").publish_confirmed({"id": 1})
        assert offset == 1
        assert broker.publishes[0].content_type == "msgpack"
    finally:
        await client.shutdown()


async def test_publish_unconfirmed(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        await client.publisher("jobs").publish_bytes(b"x")
        await asyncio.sleep(0.05)
        assert broker.publishes[0].payload == b"x"
        assert broker.publishes[0].require_confirm is False
    finally:
        await client.shutdown()


async def test_expiring_sets_ttl_in_ms_from_seconds(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        await client.publisher("jobs").expiring(60).publish_confirmed({"id": 1})
        assert broker.publishes[0].ttl_ms == 60_000
    finally:
        await client.shutdown()


async def test_pipelined_confirmation(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        conf = await client.publisher("jobs").publish_with_confirmation({"id": 1})
        assert await conf.confirmed() == 1
    finally:
        await client.shutdown()


async def test_reliable_publisher_stamps_producer_headers(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        rp = client.publisher("jobs").reliable()
        await rp.publish({"id": 1})
        headers = broker.publishes[0].headers
        assert headers[HEADER_PRODUCER_ID] == rp.producer_id
        assert headers[HEADER_PRODUCER_SEQ] == "0"
    finally:
        await client.shutdown()


async def test_declare_queue_via_config(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        await client.declare_queue(
            QueueConfig("jobs").discard_dead_letters().default_message_ttl(30)
        )
        assert broker.declares[0].dlq_policy == "discard"
        assert broker.declares[0].default_message_ttl_ms == 30_000
    finally:
        await client.shutdown()


async def test_auto_ack_subscription_delivers(broker: FakeBroker) -> None:
    broker.deliver_on_subscribe = [b"hello"]
    client = await _connect(broker)
    try:
        sub = await client.subscribe("jobs").sub_auto_ack()
        msg = await asyncio.wait_for(sub.recv(), timeout=1)
        assert msg is not None and msg.text() == "hello"
        assert broker.subscribes[0].auto_ack is True
        sub.close()
    finally:
        await client.shutdown()


async def test_manual_ack_subscription_completes(broker: FakeBroker) -> None:
    broker.deliver_on_subscribe = [b"task"]
    client = await _connect(broker)
    try:
        sub = await client.subscribe("jobs").sub()
        msg = await asyncio.wait_for(sub.recv(), timeout=1)
        assert msg is not None and msg.text() == "task"
        await msg.complete()
        await asyncio.sleep(0.05)
        assert len(broker.acks) == 1
        with pytest.raises(Exception):
            await msg.complete()  # double-settle rejected
        sub.close()
    finally:
        await client.shutdown()


async def test_fan_in_over_two_partitions(broker: FakeBroker) -> None:
    broker.topology = _topology_two_partitions(broker)
    broker.deliver_on_subscribe = [b"m"]
    client = await _connect(broker)
    try:
        await client.fetch_topology("jobs")
        sub = await client.subscribe("jobs").sub_auto_ack()
        # One delivery per partition, fanned into the merged stream.
        first = await asyncio.wait_for(sub.recv(), timeout=1)
        second = await asyncio.wait_for(sub.recv(), timeout=1)
        assert first is not None and second is not None
        assert len(broker.subscribes) == 2
        assert {s.partition for s in broker.subscribes} == {0, 1}
        sub.close()
    finally:
        await client.shutdown()


# Mirrors the Rust client's stream_subscribe_sends_durable_filtered_request:
# the stream builder carries durable name, start, filters, prefetch, ack mode.
async def test_stream_subscribe_sends_durable_filtered_request(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        sub = await (
            client.stream("events")
            .durable("analytics")
            .from_earliest()
            .filter("region", "eu-*")
            .filter("kind", "order")
            .prefetch(8)
            .sub()
        )
        assert len(broker.stream_subscribes) == 1
        req = broker.stream_subscribes[0]
        assert req.topic == "events"
        assert req.partition == 0
        assert req.durable_name == "analytics"
        assert req.start.kind == "earliest"
        assert req.filter == [("region", "eu-*"), ("kind", "order")]
        assert req.prefetch == 8
        assert req.auto_ack is False
        sub.close()
    finally:
        await client.shutdown()


# Mirrors the Rust client's stream_subscribe_fans_in_over_explicit_partitions.
async def test_stream_subscription_fans_in_over_explicit_partitions(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        sub = await client.stream("events").partitions(3).sub_auto_ack()
        assert {s.partition for s in broker.stream_subscribes} == {0, 1, 2}
        assert all(s.auto_ack for s in broker.stream_subscribes)
        assert all(s.start.kind == "latest" for s in broker.stream_subscribes)
        sub.close()
    finally:
        await client.shutdown()


async def test_keyless_publish_round_robins_partitions(broker: FakeBroker) -> None:
    broker.topology = _topology_two_partitions(broker)
    client = await _connect(broker)
    try:
        await client.fetch_topology("jobs")
        pub = client.publisher("jobs")
        await pub.publish_confirmed({"n": 1})
        await pub.publish_confirmed({"n": 2})
        assert [p.partition for p in broker.publishes] == [0, 1]
    finally:
        await client.shutdown()


async def test_partition_key_routes_consistently(broker: FakeBroker) -> None:
    broker.topology = _topology_two_partitions(broker)
    client = await _connect(broker)
    try:
        await client.fetch_topology("jobs")
        pub = client.publisher("jobs")
        expected = wire.fnv1a(b"k") % 2
        await pub.publish_confirmed(NewMessage.raw(b"a").partition_key("k"))
        await pub.publish_confirmed(NewMessage.raw(b"b").partition_key("k"))
        assert [p.partition for p in broker.publishes] == [expected, expected]
    finally:
        await client.shutdown()


async def test_applies_pushed_topology_update_and_acks(broker: FakeBroker) -> None:
    owner_endpoint = "127.0.0.1:7123"
    broker.push_topology_on_hello = wire.TopologyOk(
        generation=7,
        queues=[
            wire.QueueTopologyEntry(
                "jobs", 0, None, [wire.AdvertisedAddress("127.0.0.1", 7123)], 1, 1
            )
        ],
    )
    client = await _connect(broker)
    try:
        # The reader loop applies the push asynchronously after connect returns.
        for _ in range(100):
            if client._topology.lookup("jobs", 0, None) is not None:
                break
            await asyncio.sleep(0.01)
        owner = client._topology.lookup("jobs", 0, None)
        assert owner is not None
        assert owner.endpoint == owner_endpoint
        assert client._topology.generation == 7

        # The client must ack the generation it now reflects.
        for _ in range(100):
            if broker.topology_acks:
                break
            await asyncio.sleep(0.01)
        assert broker.topology_acks
        assert broker.topology_acks[-1].generation == 7
    finally:
        await client.shutdown()


async def test_catalogue_reflects_pushed_topology(broker: FakeBroker) -> None:
    broker.push_topology_on_hello = wire.TopologyOk(
        generation=7,
        queues=[wire.QueueTopologyEntry("jobs", 0, "workers", [], 1, 3)],
        streams=[wire.StreamTopologyEntry("events", 0, [], 1, 2)],
    )
    client = await _connect(broker)
    try:
        for _ in range(100):
            if client.catalogue().generation == 7:
                break
            await asyncio.sleep(0.01)
        cat = client.catalogue()
        assert cat.generation == 7
        assert [(q.topic, q.group, q.partition_count) for q in cat.queues] == [
            ("jobs", "workers", 3)
        ]
        assert [(s.topic, s.partition_count) for s in cat.streams] == [("events", 2)]
    finally:
        await client.shutdown()


async def test_on_catalogue_change_fires(broker: FakeBroker) -> None:
    broker.topology = wire.TopologyOk(
        generation=5,
        queues=[wire.QueueTopologyEntry("jobs", 0, None, [], 1, 1)],
    )
    client = await _connect(broker)
    try:
        seen: list = []
        client.on_catalogue_change(seen.append)
        await client.fetch_topology()
        assert len(seen) == 1
        assert seen[0].generation == 5
        assert seen[0].queues[0].topic == "jobs"
        assert client.catalogue() == seen[0]
    finally:
        await client.shutdown()
