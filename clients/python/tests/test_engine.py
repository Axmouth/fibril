"""Engine tests over the in-process fake broker (real wire bytes)."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from fibril import wire
from fibril.codec import Frame, build_frame
from fibril.engine import (
    Delivered,
    Engine,
    EngineOptions,
    Inflight,
    SettleContext,
    SubscriptionRegistry,
    _Registered,
)
from fibril.errors import (
    DisconnectionError,
    RedirectError,
    ServerError,
    StaleDeliveryError,
    is_transient_error,
    retry_advice,
)
from fibril.subscription import InflightMessage
from fibril.frames import encode_body
from fibril.protocol import Op

from fake_broker import FakeBroker


def _connection_error(code: int, message: str) -> Frame:
    """A connection-level ERROR frame (request id 0, so no waiter matches)."""
    return build_frame(Op.ERROR, 0, encode_body(Op.ERROR, wire.ErrorMsg(code=code, message=message)))


@pytest_asyncio.fixture
async def broker():
    b = FakeBroker()
    await b.start()
    try:
        yield b
    finally:
        await b.stop()


async def _connect(
    broker: FakeBroker,
    *,
    registry: SubscriptionRegistry | None = None,
    **opts: object,
) -> Engine:
    reader, writer = await asyncio.open_connection(broker.host, broker.port)
    return await Engine.start(
        reader, writer, EngineOptions(**opts), SettleContext(), registry=registry  # type: ignore[arg-type]
    )


def _sub(topic: str = "jobs", *, auto_ack: bool, prefetch: int = 10) -> wire.Subscribe:
    return wire.Subscribe(
        topic=topic, partition=0, group=None, prefetch=prefetch, auto_ack=auto_ack
    )


async def test_handshake_sets_resume_identity(broker: FakeBroker) -> None:
    eng = await _connect(broker)
    try:
        assert eng.resume_identity.owner_id == b"\x01" * 16
        assert eng.resume_outcome == "new"
    finally:
        eng.shutdown()


async def test_compliance_mismatch_rejected(broker: FakeBroker) -> None:
    broker.compliance = "not-the-marker"
    with pytest.raises(DisconnectionError):
        await _connect(broker)


async def test_auth_failure_raises(broker: FakeBroker) -> None:
    broker.auth_ok = False
    with pytest.raises(ServerError):
        await _connect(broker, auth=wire.Auth("u", "bad"))


async def test_confirmed_publish_returns_offset(broker: FakeBroker) -> None:
    eng = await _connect(broker)
    try:
        msg = wire.Publish(
            topic="jobs",
            partition=0,
            group=None,
            require_confirm=False,
            content_type="text",
            headers={},
            payload=b"x",
            published=0,
        )
        offset = await eng.publish(msg, confirm=True)
        assert offset == 1
        assert broker.publishes[0].require_confirm is True
        assert broker.publishes[0].payload == b"x"
    finally:
        eng.shutdown()


async def test_declare_queue(broker: FakeBroker) -> None:
    eng = await _connect(broker)
    try:
        await eng.declare_queue(
            wire.DeclareQueue("jobs", None, "discard", 5, partition_count=2)
        )
        assert broker.declares[0].topic == "jobs"
        assert broker.declares[0].partition_count == 2
    finally:
        eng.shutdown()


async def test_manual_subscribe_deliver_and_ack(broker: FakeBroker) -> None:
    broker.deliver_on_subscribe = [b"hello"]
    eng = await _connect(broker)
    try:
        result = await eng.subscribe(_sub(auto_ack=False), supervised=False)
        item = await asyncio.wait_for(result.queue.recv(), timeout=1)
        assert isinstance(item, Inflight)
        assert item.payload == b"hello"
        await eng.ack(
            item.topic, item.group, item.partition, item.delivery_tag, item.deliver_request_id
        )
        await asyncio.sleep(0.05)
        assert len(broker.acks) == 1
        assert broker.acks[0].tags[0].epoch == item.delivery_tag.epoch
    finally:
        eng.shutdown()


# A delivery held across a NON-resumed reconnect settles to a typed
# StaleDeliveryError and sends no frame. The message redelivers per at-least-once.
async def test_held_delivery_goes_stale_across_a_non_resumed_reconnect(
    broker: FakeBroker,
) -> None:
    broker.deliver_on_subscribe = [b"task"]
    settle = SettleContext()
    reader1, writer1 = await asyncio.open_connection(broker.host, broker.port)
    eng1 = await Engine.start(reader1, writer1, EngineOptions(), settle)
    try:
        result = await eng1.subscribe(_sub(auto_ack=False), supervised=False)
        item = await asyncio.wait_for(result.queue.recv(), timeout=1)
        assert isinstance(item, Inflight)
        held = InflightMessage(eng1, item)

        # A non-resumed reconnect: a fresh engine binds the SAME settle context and
        # claims a new incarnation, so the held delivery (incarnation 0) goes stale.
        reader2, writer2 = await asyncio.open_connection(broker.host, broker.port)
        eng2 = await Engine.start(reader2, writer2, EngineOptions(), settle)
        try:
            with pytest.raises(StaleDeliveryError):
                await held.complete()
            await asyncio.sleep(0.05)
            assert broker.acks == [], "a stale settle must send no ack"
        finally:
            eng2.shutdown()
    finally:
        eng1.shutdown()


# A delivery held across a RESUMED reconnect settles to the CURRENT engine.
# Settlement is keyed by (topic, group, partition, tag), not the client sub id.
async def test_held_delivery_settles_to_current_engine_across_a_resumed_reconnect(
    broker: FakeBroker,
) -> None:
    broker.deliver_on_subscribe = [b"task"]
    settle = SettleContext()
    reader1, writer1 = await asyncio.open_connection(broker.host, broker.port)
    eng1 = await Engine.start(reader1, writer1, EngineOptions(), settle)
    result = await eng1.subscribe(_sub(auto_ack=False), supervised=False)
    item = await asyncio.wait_for(result.queue.recv(), timeout=1)
    assert isinstance(item, Inflight)
    held = InflightMessage(eng1, item)

    # A resumed reconnect keeps the incarnation, so the held delivery still
    # settles - through the new engine.
    broker.resume_outcome = "resumed"
    reader2, writer2 = await asyncio.open_connection(broker.host, broker.port)
    eng2 = await Engine.start(reader2, writer2, EngineOptions(), settle)
    try:
        # Drop the ORIGIN engine: settling must route to the current one anyway.
        eng1.shutdown()
        await held.complete()
        await asyncio.sleep(0.05)
        assert len(broker.acks) == 1
        assert broker.acks[0].tags[0].epoch == item.delivery_tag.epoch
        assert broker.acks[0].topic == item.topic
    finally:
        eng2.shutdown()


# A stale delivery is do-not-retry and not a transient transport failure - the
# message redelivers on its own.
def test_stale_delivery_is_do_not_retry() -> None:
    err = StaleDeliveryError()
    assert retry_advice(err) == "do_not_retry"
    assert is_transient_error(err) is False


async def test_auto_subscribe_delivers_settled(broker: FakeBroker) -> None:
    broker.deliver_on_subscribe = [b"auto"]
    eng = await _connect(broker)
    try:
        result = await eng.subscribe(_sub(auto_ack=True), supervised=False)
        item = await asyncio.wait_for(result.queue.recv(), timeout=1)
        assert isinstance(item, Delivered) and not isinstance(item, Inflight)
        assert item.payload == b"auto"
        # Server-side settle: the broker received auto_ack=true, no client ack.
        assert broker.subscribes[0].auto_ack is True
        await asyncio.sleep(0.05)
        assert broker.acks == []
    finally:
        eng.shutdown()


async def test_topology_fetch(broker: FakeBroker) -> None:
    broker.topology = wire.TopologyOk(
        generation=5,
        queues=[wire.QueueTopologyEntry("jobs", 0, None, [wire.AdvertisedAddress("127.0.0.1", 7000)], 1, 1)],
    )
    eng = await _connect(broker)
    try:
        topo = await eng.fetch_topology("jobs")
        assert topo.generation == 5
        assert topo.queues[0].owner_endpoints == [wire.AdvertisedAddress("127.0.0.1", 7000)]
    finally:
        eng.shutdown()


async def test_redirect_surfaces_typed_error(broker: FakeBroker) -> None:
    broker.redirect_publish = wire.Redirect("jobs", 0, None, [wire.AdvertisedAddress("127.0.0.1", 7001)], 2)
    eng = await _connect(broker)
    try:
        msg = wire.Publish("jobs", 0, None, True, "text", {}, b"x", 0)
        with pytest.raises(RedirectError) as exc:
            await eng.publish(msg, confirm=True)
        assert exc.value.redirect.owner_endpoints == [wire.AdvertisedAddress("127.0.0.1", 7001)]
    finally:
        eng.shutdown()


async def test_server_error_surfaces(broker: FakeBroker) -> None:
    broker.error_publish = (404, "no such topic")
    eng = await _connect(broker)
    try:
        msg = wire.Publish("jobs", 0, None, True, "text", {}, b"x", 0)
        with pytest.raises(ServerError) as exc:
            await eng.publish(msg, confirm=True)
        assert exc.value.code == 404
    finally:
        eng.shutdown()


async def test_disconnect_closes_subscription(broker: FakeBroker) -> None:
    eng = await _connect(broker)
    result = await eng.subscribe(_sub(auto_ack=False), supervised=False)
    await broker.stop()  # sever the connection
    item = await asyncio.wait_for(result.queue.recv(), timeout=1)
    assert item is None  # closed stream ends cleanly
    assert eng.is_closed()


async def test_reconnect_reconciles_registered_subs(broker: FakeBroker) -> None:
    from fibril.internal.bounded_queue import BoundedQueue

    registry: SubscriptionRegistry = {
        55: _Registered(
            reconcile=wire.ReconcileSubscription(
                sub_id=55, topic="jobs", partition=0, group=None, auto_ack=False, prefetch=1
            ),
            queue=BoundedQueue(1),
            auto_ack=False,
        )
    }
    eng = await _connect(broker, registry=registry)
    try:
        assert len(broker.reconciles) == 1
        assert broker.reconciles[0].subscriptions[0].sub_id == 55
    finally:
        eng.shutdown()


async def test_buffered_unconfirmed_publishes_all_deliver_in_order(broker: FakeBroker) -> None:
    # A burst of fire-and-forget publishes is coalesced into fewer socket writes.
    # Every frame must still reach the broker, in order, none dropped or merged.
    eng = await _connect(broker)
    try:
        for i in range(200):
            msg = wire.Publish(
                topic="jobs",
                partition=0,
                group=None,
                require_confirm=False,
                content_type="text",
                headers={},
                payload=bytes([i]),
                published=0,
            )
            await eng.publish(msg, confirm=False)
        # The burst stays buffered until the loop next yields; then it flushes.
        for _ in range(50):
            if len(broker.publishes) >= 200:
                break
            await asyncio.sleep(0.01)
        assert len(broker.publishes) == 200
        assert [p.payload for p in broker.publishes] == [bytes([i]) for i in range(200)]
    finally:
        eng.shutdown()


async def test_nonretryable_connection_error_preserves_code(broker: FakeBroker) -> None:
    # A connection-level error frame (no correlated request) with a non-retryable
    # code closes the engine preserving the broker code, so the reconnect path can
    # surface it instead of storming back into the same rejection.
    eng = await _connect(broker)
    await broker.push(_connection_error(403, "forbidden"))
    await eng.wait_closed()
    reason = eng.close_reason()
    assert isinstance(reason, ServerError)
    assert reason.code == 403
    assert retry_advice(reason) == "do_not_retry"


async def test_retryable_connection_error_stays_transient(broker: FakeBroker) -> None:
    # A retryable code (5xx) still closes as a transient disconnect, so the
    # existing reconnect/failover path is unchanged.
    eng = await _connect(broker)
    await broker.push(_connection_error(503, "unavailable"))
    await eng.wait_closed()
    reason = eng.close_reason()
    assert isinstance(reason, DisconnectionError)
    assert retry_advice(reason) == "retry"
