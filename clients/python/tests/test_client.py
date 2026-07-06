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


async def test_publish_with_confirmation(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        conf = await client.publisher("jobs").publish_with_confirmation({"id": 1})
        assert await conf.confirmed() == 1
    finally:
        await client.shutdown()


async def test_publish_delayed_with_confirmation(broker: FakeBroker) -> None:
    client = await _connect(broker)
    try:
        conf = await client.publisher("jobs").publish_delayed_with_confirmation({"id": 1}, 30)
        assert await conf.confirmed() == 1
        # It goes out on the delayed frame carrying a not_before deadline, not the
        # immediate publish path.
        assert len(broker.delayed_publishes) == 1
        assert broker.delayed_publishes[0].not_before > 0
        assert broker.publishes == []
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


async def test_fan_in_picks_up_grown_partition(broker: FakeBroker) -> None:
    owners = [wire.AdvertisedAddress(broker.host, broker.port)]
    # Start with a single partition; the fan-in subscribes it.
    broker.topology = wire.TopologyOk(
        generation=1, queues=[wire.QueueTopologyEntry("jobs", 0, None, owners, 1, 1)]
    )
    client = await Client.connect(
        (broker.host, broker.port),
        ClientOptions(
            supervise_subscriptions=True,
            subscription_supervise_interval_ms=20,
            topology_refresh_cooldown_ms=0,
        ),
    )
    try:
        await client.fetch_topology("jobs")
        sub = await client.subscribe("jobs").sub_auto_ack()
        for _ in range(100):
            if {s.partition for s in broker.subscribes} == {0}:
                break
            await asyncio.sleep(0.01)
        assert {s.partition for s in broker.subscribes} == {0}

        # Grow the topic to two partitions. The fan-in's growth loop refreshes
        # topology and must attach the new partition without the caller re-subscribing.
        broker.topology = wire.TopologyOk(
            generation=2,
            queues=[
                wire.QueueTopologyEntry("jobs", 0, None, owners, 1, 2),
                wire.QueueTopologyEntry("jobs", 1, None, owners, 1, 2),
            ],
        )
        for _ in range(200):
            if {s.partition for s in broker.subscribes} == {0, 1}:
                break
            await asyncio.sleep(0.01)
        assert {s.partition for s in broker.subscribes} == {0, 1}
        sub.close()
    finally:
        await client.shutdown()


def _topology_three_queues(broker: FakeBroker) -> wire.TopologyOk:
    owners = [wire.AdvertisedAddress(broker.host, broker.port)]
    return wire.TopologyOk(
        generation=1,
        queues=[
            wire.QueueTopologyEntry("events.click", 0, None, owners, 1, 1),
            wire.QueueTopologyEntry("events.view", 0, None, owners, 1, 1),
            wire.QueueTopologyEntry("orders.new", 0, None, owners, 1, 1),
        ],
    )


async def test_pattern_subscription_fans_in_matching_queues(broker: FakeBroker) -> None:
    broker.topology = _topology_three_queues(broker)
    broker.deliver_on_subscribe = [b"x"]
    client = await _connect(broker)
    try:
        await client.fetch_topology()
        sub = await client.routing().subscribe_pattern("events.*").sub()
        # Both events.* queues deliver; orders.new must not.
        sources = set()
        for _ in range(2):
            item = await asyncio.wait_for(sub.recv(), timeout=1)
            assert item is not None
            await item.message.complete()
            sources.add(item.source.topic)
        assert sources == {"events.click", "events.view"}
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


async def test_ignores_stale_topology_push_but_acks_current(broker: FakeBroker) -> None:
    from fibril.codec import build_frame
    from fibril.frames import encode_body
    from fibril.protocol import Op

    # A newer generation the client adopts.
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
        for _ in range(100):
            if client._topology.generation == 7:
                break
            await asyncio.sleep(0.01)
        assert client._topology.generation == 7

        # A stale push (older generation) naming a different owner must be ignored, so
        # a late duplicate from a bounced broker cannot rewind routing. The client
        # still acks the generation it currently reflects so the broker can fence.
        stale = wire.TopologyOk(
            generation=5,
            queues=[
                wire.QueueTopologyEntry(
                    "jobs", 0, None, [wire.AdvertisedAddress("127.0.0.1", 7999)], 1, 1
                )
            ],
        )
        await broker.push(build_frame(Op.TOPOLOGY_UPDATE, 0, encode_body(Op.TOPOLOGY_UPDATE, stale)))

        for _ in range(100):
            if len(broker.topology_acks) >= 2:
                break
            await asyncio.sleep(0.01)
        # Routing still reflects generation 7 and its owner (the stale push was ignored).
        assert client._topology.generation == 7
        owner = client._topology.lookup("jobs", 0, None)
        assert owner is not None
        assert owner.endpoint == "127.0.0.1:7123"
        # The ack carried the current generation (7), not the stale 5.
        assert broker.topology_acks[-1].generation == 7
    finally:
        await client.shutdown()


async def test_publish_gives_up_after_max_redirects(broker: FakeBroker) -> None:
    from fibril.errors import RedirectError

    # The broker redirects every publish back to itself, so the client keeps
    # following the redirect. It must give up after max_redirects and surface the
    # redirect rather than chase it forever.
    broker.redirect_publish = wire.Redirect(
        "jobs", 0, None, [wire.AdvertisedAddress(broker.host, broker.port)], 1
    )
    client = await Client.connect((broker.host, broker.port), ClientOptions(max_redirects=2))
    try:
        with pytest.raises(RedirectError):
            await client.publisher("jobs").publish_confirmed({"id": 1})
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


@pytest.mark.asyncio
async def test_on_going_away_fires(broker: FakeBroker) -> None:
    from fibril.codec import build_frame
    from fibril.frames import encode_body
    from fibril.protocol import Op

    client = await _connect(broker)
    try:
        seen: list = []
        client.on_going_away(seen.append)
        notice = wire.GoingAway(grace_ms=30000, message="broker restarting for upgrade")
        await broker.push(build_frame(Op.GOING_AWAY, 0, encode_body(Op.GOING_AWAY, notice)))
        # Give the reader loop a tick to dispatch the pushed frame.
        for _ in range(50):
            if seen:
                break
            await asyncio.sleep(0.01)
        assert len(seen) == 1
        assert seen[0].grace_ms == 30000
        assert seen[0].message == "broker restarting for upgrade"
    finally:
        await client.shutdown()


def test_write_coalescing_knobs_thread_to_engine() -> None:
    opts = ClientOptions().with_write_coalescing(max_bytes=4096, max_frames=8, window_ms=2.0)
    assert opts.write_coalesce_bytes == 4096
    assert opts.write_coalesce_count == 8
    assert opts.write_coalesce_window_ms == 2.0
    eng = opts.engine_options()
    assert eng.write_coalesce_bytes == 4096
    assert eng.write_coalesce_count == 8
    assert eng.write_coalesce_window_s == 0.002  # ms converted to seconds
    # Only the passed limits change; the rest keep their defaults.
    only_frames = ClientOptions().with_write_coalescing(max_frames=1)
    assert only_frames.write_coalesce_count == 1
    assert only_frames.write_coalesce_bytes == ClientOptions().write_coalesce_bytes
    for bad in (dict(max_bytes=0), dict(max_frames=0), dict(window_ms=-1)):
        with pytest.raises(ValueError):
            ClientOptions().with_write_coalescing(**bad)  # type: ignore[arg-type]


def test_publish_timeout_defaults_on_and_is_configurable() -> None:
    assert ClientOptions().publish_timeout_ms == 30_000
    # Zero disables the bound; any positive value overrides the default.
    assert ClientOptions(publish_timeout_ms=0).publish_timeout_ms == 0
    assert ClientOptions(publish_timeout_ms=5_000).publish_timeout_ms == 5_000


def test_user_headers_cannot_set_reserved_namespaces() -> None:
    from fibril.errors import FibrilError

    # A normal user header is kept.
    assert NewMessage.raw(b"x").header("trace-id", "abc").headers["trace-id"] == "abc"

    # The fibril.* and stroma.* namespaces are reserved for broker-stamped headers,
    # so a user header in them is rejected rather than allowed to spoof one.
    for reserved in ("fibril.client.producer_id", "fibril.retries", "stroma.dlq.source_topic"):
        with pytest.raises(FibrilError):
            NewMessage.raw(b"x").header(reserved, "evil")


async def test_nonretryable_close_surfaces_without_reconnect(broker: FakeBroker) -> None:
    from fibril.codec import build_frame
    from fibril.errors import ServerError
    from fibril.frames import encode_body
    from fibril.protocol import Op

    client = await _connect(broker)
    try:
        # Establish the session, then push a connection-level error (no correlated
        # request) with a non-retryable code.
        await client.fetch_topology()
        connections_before = len(broker._writers)
        err = wire.ErrorMsg(code=403, message="forbidden")
        await broker.push(build_frame(Op.ERROR, 0, encode_body(Op.ERROR, err)))
        # Let the reader loop dispatch the error and close the engine.
        for _ in range(50):
            if client._engine.is_closed():
                break
            await asyncio.sleep(0.01)
        # The next op surfaces the broker error instead of storming reconnects.
        with pytest.raises(ServerError) as exc:
            await client.fetch_topology()
        assert exc.value.code == 403
        # No reconnect was attempted: the broker saw no new connection.
        assert len(broker._writers) == connections_before
    finally:
        await client.shutdown()
