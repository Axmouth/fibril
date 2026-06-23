"""Blocking facade tests: synchronous calls bridged to the async core.

The broker runs on its own background loop thread so the synchronous client
talks to it over a real socket, exactly as a real deployment would.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Iterator

import pytest

from fibril.blocking import BlockingClient
from fibril.client import ClientOptions

from fake_broker import FakeBroker


@pytest.fixture
def broker() -> Iterator[FakeBroker]:
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    b = FakeBroker()
    asyncio.run_coroutine_threadsafe(b.start(), loop).result()
    try:
        yield b
    finally:
        asyncio.run_coroutine_threadsafe(b.stop(), loop).result()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)


def _opts() -> ClientOptions:
    return ClientOptions(supervise_subscriptions=False)


def test_blocking_publish_confirmed(broker: FakeBroker) -> None:
    with BlockingClient.connect((broker.host, broker.port), _opts()) as client:
        offset = client.publisher("jobs").publish_confirmed({"id": 1})
        assert offset == 1


def test_blocking_pipelined_confirmation(broker: FakeBroker) -> None:
    with BlockingClient.connect((broker.host, broker.port), _opts()) as client:
        conf = client.publisher("jobs").publish_with_confirmation({"id": 1})
        assert conf.confirmed() == 1


def test_blocking_auto_ack_iteration(broker: FakeBroker) -> None:
    broker.deliver_on_subscribe = [b"hello"]
    with BlockingClient.connect((broker.host, broker.port), _opts()) as client:
        sub = client.subscribe("jobs").sub_auto_ack()
        for msg in sub:
            assert msg.text() == "hello"
            break
        sub.close()


def test_blocking_manual_ack_complete(broker: FakeBroker) -> None:
    broker.deliver_on_subscribe = [b"task"]
    with BlockingClient.connect((broker.host, broker.port), _opts()) as client:
        sub = client.subscribe("jobs").sub()
        msg = sub.recv()
        assert msg is not None and msg.text() == "task"
        msg.complete()
        sub.close()
    assert len(broker.acks) == 1


def test_blocking_expiring_seconds(broker: FakeBroker) -> None:
    with BlockingClient.connect((broker.host, broker.port), _opts()) as client:
        client.publisher("jobs").expiring(45).publish_confirmed({"id": 1})
    assert broker.publishes[0].ttl_ms == 45_000
