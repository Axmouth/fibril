"""Blocking facade over the async client.

A thin synchronous wrapper, not a second implementation: it runs the async
client on a dedicated background event-loop thread and bridges each call with
``asyncio.run_coroutine_threadsafe(...).result()``. Subscriptions become plain
iterators that pull from the async subscription the same way. All the hard parts
(routing, redirects, continuity, reconnect) stay in the async core.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable, Coroutine, Optional, TypeVar

from . import wire
from .client import (
    Address,
    Client,
    ClientOptions,
    QueueConfig,
    ReconnectOutcome,
    StreamConfig,
)
from .message import Publishable
from .publisher import Delay, Publisher, PublishConfirmation, ReliablePublisher
from .subscription import (
    AutoAckedSubscription,
    InflightMessage,
    Message,
    StreamSubscriptionBuilder,
    Subscription,
    SubscriptionBuilder,
)

_T = TypeVar("_T")
AssignmentHandler = Callable[[wire.AssignmentChanged], None]


def _start_loop() -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="fibril-loop", daemon=True)
    thread.start()
    return loop, thread


class BlockingClient:
    """Synchronous client. Use :meth:`connect` or as a context manager."""

    def __init__(
        self, loop: asyncio.AbstractEventLoop, thread: threading.Thread, client: Client
    ) -> None:
        self._loop = loop
        self._thread = thread
        self._client = client

    @classmethod
    def connect(cls, address: Address, opts: Optional[ClientOptions] = None) -> "BlockingClient":
        loop, thread = _start_loop()
        try:
            client = asyncio.run_coroutine_threadsafe(
                Client.connect(address, opts), loop
            ).result()
        except BaseException:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            raise
        return cls(loop, thread, client)

    def _run(self, coro: Coroutine[Any, Any, _T]) -> _T:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def publisher(self, topic: str) -> "BlockingPublisher":
        return BlockingPublisher(self, self._client.publisher(topic))

    def publisher_grouped(self, topic: str, group: str) -> "BlockingPublisher":
        return BlockingPublisher(self, self._client.publisher_grouped(topic, group))

    def subscribe(self, topic: str) -> "BlockingSubscriptionBuilder":
        return BlockingSubscriptionBuilder(self, self._client.subscribe(topic))

    def stream(self, topic: str) -> "BlockingStreamSubscriptionBuilder":
        return BlockingStreamSubscriptionBuilder(self, self._client.stream(topic))

    def declare_queue(self, config: QueueConfig) -> None:
        self._run(self._client.declare_queue(config))

    def declare_plexus(self, config: StreamConfig) -> None:
        self._run(self._client.declare_plexus(config))

    def fetch_topology(
        self, topic: Optional[str] = None, group: Optional[str] = None
    ) -> wire.TopologyOk:
        return self._run(self._client.fetch_topology(topic, group))

    def reconnect(self) -> ReconnectOutcome:
        return self._run(self._client.reconnect())

    def on_assignment_change(self, handler: AssignmentHandler) -> Callable[[], None]:
        return self._client.on_assignment_change(handler)

    def shutdown(self) -> None:
        try:
            self._run(self._client.shutdown())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2)

    def __enter__(self) -> "BlockingClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()


class BlockingPublisher:
    """Synchronous publisher."""

    def __init__(self, owner: BlockingClient, publisher: Publisher) -> None:
        self._owner = owner
        self._publisher = publisher

    @property
    def topic(self) -> str:
        return self._publisher.topic

    @property
    def group(self) -> Optional[str]:
        return self._publisher.group

    def publish(self, payload: Publishable) -> None:
        self._owner._run(self._publisher.publish(payload))

    def publish_confirmed(self, payload: Publishable) -> int:
        return self._owner._run(self._publisher.publish_confirmed(payload))

    def publish_with_confirmation(self, payload: Publishable) -> "BlockingPublishConfirmation":
        conf = self._owner._run(self._publisher.publish_with_confirmation(payload))
        return BlockingPublishConfirmation(self._owner, conf)

    def publish_bytes(self, payload: bytes) -> None:
        self._owner._run(self._publisher.publish_bytes(payload))

    def publish_bytes_confirmed(self, payload: bytes) -> int:
        return self._owner._run(self._publisher.publish_bytes_confirmed(payload))

    def publish_delayed(self, payload: Publishable, delay: Delay) -> None:
        self._owner._run(self._publisher.publish_delayed(payload, delay))

    def publish_delayed_confirmed(self, payload: Publishable, delay: Delay) -> int:
        return self._owner._run(self._publisher.publish_delayed_confirmed(payload, delay))

    def expiring(self, ttl: Delay) -> "BlockingPublisher":
        return BlockingPublisher(self._owner, self._publisher.expiring(ttl))

    def reliable(self) -> "BlockingReliablePublisher":
        return BlockingReliablePublisher(self._owner, self._publisher.reliable())


class BlockingReliablePublisher:
    """Synchronous reliable publisher."""

    def __init__(self, owner: BlockingClient, publisher: ReliablePublisher) -> None:
        self._owner = owner
        self._publisher = publisher

    @property
    def producer_id(self) -> str:
        return self._publisher.producer_id

    def max_attempts(self, n: int) -> "BlockingReliablePublisher":
        self._publisher.max_attempts(n)
        return self

    def publish(self, payload: Publishable) -> int:
        return self._owner._run(self._publisher.publish(payload))


class BlockingPublishConfirmation:
    """Synchronous handle for a pipelined confirmed publish."""

    def __init__(self, owner: BlockingClient, conf: PublishConfirmation) -> None:
        self._owner = owner
        self._conf = conf

    def confirmed(self) -> int:
        return self._owner._run(self._conf.confirmed())


class BlockingSubscriptionBuilder:
    """Synchronous subscription builder."""

    def __init__(self, owner: BlockingClient, builder: SubscriptionBuilder) -> None:
        self._owner = owner
        self._builder = builder

    def group(self, group: str) -> "BlockingSubscriptionBuilder":
        self._builder.group(group)
        return self

    def consumer_group(self, consumer_group: str) -> "BlockingSubscriptionBuilder":
        self._builder.consumer_group(consumer_group)
        return self

    def consumer_target(self, target: int) -> "BlockingSubscriptionBuilder":
        self._builder.consumer_target(target)
        return self

    def prefetch(self, prefetch: int) -> "BlockingSubscriptionBuilder":
        self._builder.prefetch(prefetch)
        return self

    def sub_manual_ack(self) -> "BlockingSubscription":
        sub = self._owner._run(self._builder.sub_manual_ack())
        return BlockingSubscription(self._owner, sub)

    def sub_auto_ack(self) -> "BlockingAutoAckedSubscription":
        sub = self._owner._run(self._builder.sub_auto_ack())
        return BlockingAutoAckedSubscription(self._owner, sub)


class BlockingStreamSubscriptionBuilder:
    """Synchronous Plexus (fan-out stream) subscription builder."""

    def __init__(self, owner: BlockingClient, builder: StreamSubscriptionBuilder) -> None:
        self._owner = owner
        self._builder = builder

    def partitions(self, count: int) -> "BlockingStreamSubscriptionBuilder":
        self._builder.partitions(count)
        return self

    def durable(self, name: str) -> "BlockingStreamSubscriptionBuilder":
        self._builder.durable(name)
        return self

    def from_latest(self) -> "BlockingStreamSubscriptionBuilder":
        self._builder.from_latest()
        return self

    def from_earliest(self) -> "BlockingStreamSubscriptionBuilder":
        self._builder.from_earliest()
        return self

    def from_offset(self, offset: int) -> "BlockingStreamSubscriptionBuilder":
        self._builder.from_offset(offset)
        return self

    def from_last(self, count: int) -> "BlockingStreamSubscriptionBuilder":
        self._builder.from_last(count)
        return self

    def from_time(self, time_ms: int) -> "BlockingStreamSubscriptionBuilder":
        self._builder.from_time(time_ms)
        return self

    def filter(self, header: str, pattern: str) -> "BlockingStreamSubscriptionBuilder":
        self._builder.filter(header, pattern)
        return self

    def prefetch(self, prefetch: int) -> "BlockingStreamSubscriptionBuilder":
        self._builder.prefetch(prefetch)
        return self

    def sub_manual_ack(self) -> "BlockingSubscription":
        sub = self._owner._run(self._builder.sub_manual_ack())
        return BlockingSubscription(self._owner, sub)

    def sub_auto_ack(self) -> "BlockingAutoAckedSubscription":
        sub = self._owner._run(self._builder.sub_auto_ack())
        return BlockingAutoAckedSubscription(self._owner, sub)


async def _call(fn: Callable[[], None]) -> None:
    fn()


class BlockingSubscription:
    """Synchronous manual-ack subscription. Iterate with ``for``."""

    def __init__(self, owner: BlockingClient, sub: Subscription) -> None:
        self._owner = owner
        self._sub = sub

    def recv(self) -> Optional["BlockingInflightMessage"]:
        msg = self._owner._run(self._sub.recv())
        if msg is None:
            return None
        return BlockingInflightMessage(self._owner, msg)

    def close(self) -> None:
        self._owner._run(_call(self._sub.close))

    def __iter__(self) -> "BlockingSubscription":
        return self

    def __next__(self) -> "BlockingInflightMessage":
        msg = self.recv()
        if msg is None:
            raise StopIteration
        return msg


class BlockingAutoAckedSubscription:
    """Synchronous auto-ack subscription. Iterate with ``for``."""

    def __init__(self, owner: BlockingClient, sub: AutoAckedSubscription) -> None:
        self._owner = owner
        self._sub = sub

    def recv(self) -> Optional[Message]:
        # Message methods are pure and thread-safe, so the async value is returned
        # directly with no wrapper.
        return self._owner._run(self._sub.recv())

    def close(self) -> None:
        self._owner._run(_call(self._sub.close))

    def __iter__(self) -> "BlockingAutoAckedSubscription":
        return self

    def __next__(self) -> Message:
        msg = self.recv()
        if msg is None:
            raise StopIteration
        return msg


class BlockingInflightMessage:
    """Synchronous manual-ack message: settle with complete/fail/retry."""

    def __init__(self, owner: BlockingClient, msg: InflightMessage) -> None:
        self._owner = owner
        self._msg = msg

    # Data accessors delegate to the underlying message (all pure).
    @property
    def payload(self) -> bytes:
        return self._msg.payload

    @property
    def headers(self) -> dict[str, str]:
        return self._msg.headers

    @property
    def offset(self) -> int:
        return self._msg.offset

    @property
    def delivery_tag(self) -> wire.DeliveryTag:
        return self._msg.delivery_tag

    def content_type(self) -> Optional[str]:
        return self._msg.content_type()

    def deserialize(self) -> object:
        return self._msg.deserialize()

    def json(self) -> object:
        return self._msg.json()

    def msgpack(self) -> object:
        return self._msg.msgpack()

    def raw(self) -> bytes:
        return self._msg.raw()

    def text(self) -> str:
        return self._msg.text()

    def complete(self) -> Message:
        return self._owner._run(self._msg.complete())

    def fail(self) -> Message:
        return self._owner._run(self._msg.fail())

    def retry(self) -> Message:
        return self._owner._run(self._msg.retry())

    def retry_after(self, delay: Delay) -> Message:
        return self._owner._run(self._msg.retry_after(delay))
