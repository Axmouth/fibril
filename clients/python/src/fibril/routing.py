"""Routing / discovery surface, opt in via :meth:`Client.routing`.

Built entirely on the public client API (the subscribe builders, the catalogue
feed, and the connection it already holds), so it adds no coupling to the
connection core and composes with the delivery-guarantee wrappers: a pattern
subscription yields the same message types a single-channel subscription does,
and reliable publishing is still reached through the same client.

Mirrors the Rust and TypeScript clients' ``RoutingClient``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from . import wire
from .internal.bounded_queue import BoundedQueue
from .subscription import AutoAckedSubscription, InflightMessage, Message, Subscription

if TYPE_CHECKING:
    from .client import Catalogue, Client
    from .publisher import Publisher
    from .subscription import StreamSubscriptionBuilder, SubscriptionBuilder

# Per-channel buffering for the merged pattern stream, multiplied by prefetch.
_CHANNEL_FANIN_BUFFER = 32

M = TypeVar("M")


@dataclass(frozen=True)
class PatternSource:
    """The channel a pattern-delivered message came from. A pattern fans in across
    many channels, so each message is paired with its source for routing back to
    per-topic handling."""

    topic: str
    #: The queue's group namespace, or ``None`` for the ungrouped default and for
    #: streams (which have no group).
    group: Optional[str]


@dataclass
class PatternMessage(Generic[M]):
    """A pattern delivery: the message plus the channel it came from."""

    source: PatternSource
    message: M


class RoutingClient:
    """Owned routing/discovery view over a :class:`Client`.

    Discovery is a first-class but opt-in capability, kept off the default client
    surface: obtain one with :meth:`Client.routing`. It shares the client's
    connection and re-exposes the normal operations (publish, subscribe, stream),
    so routing composes with them rather than replacing them.
    """

    def __init__(self, client: "Client") -> None:
        self._client = client

    @property
    def client(self) -> "Client":
        """The underlying client, for any operation not surfaced here directly."""
        return self._client

    def publisher(self, topic: str) -> "Publisher":
        return self._client.publisher(topic)

    def subscribe(self, topic: str) -> "SubscriptionBuilder":
        return self._client.subscribe(topic)

    def stream(self, topic: str) -> "StreamSubscriptionBuilder":
        return self._client.stream(topic)

    def catalogue(self) -> "Catalogue":
        return self._client.catalogue()

    def on_catalogue_change(self, handler: "Callable[[Catalogue], None]") -> Callable[[], None]:
        return self._client.on_catalogue_change(handler)

    def subscribe_pattern(self, pattern: str) -> "PatternSubscribeBuilder":
        """Begin a pattern subscription over the work queues whose topic matches
        ``pattern``.

        ``pattern`` is a ``*``-wildcard glob (each ``*`` matches any run of
        characters, including empty), the same grammar as the per-subscription
        header filter, with no regex. ``"*"`` matches every topic. The
        subscription fans in across every currently-matching queue and keeps
        attaching queues that start matching later, so newly declared channels are
        picked up without a reconnect."""
        return PatternSubscribeBuilder(self._client, _TopicGlob(pattern))

    def subscribe_stream_pattern(self, pattern: str) -> "StreamPatternSubscribeBuilder":
        """Begin a pattern subscription over the Plexus streams whose topic matches
        ``pattern``. Same matching and auto-pickup behaviour as
        :meth:`subscribe_pattern`, over streams instead of work queues."""
        return StreamPatternSubscribeBuilder(self._client, _TopicGlob(pattern))


class PatternSubscribeBuilder:
    """Builder for a work-queue pattern subscription."""

    def __init__(self, client: "Client", glob: "_TopicGlob") -> None:
        self._client = client
        self._glob = glob
        self._prefetch = 1
        self._consumer_group: Optional[str] = None

    def prefetch(self, prefetch: int) -> "PatternSubscribeBuilder":
        """Per-channel prefetch, applied to every attached queue."""
        self._prefetch = prefetch
        return self

    def consumer_group(self, consumer_group: str) -> "PatternSubscribeBuilder":
        """Consume every matched queue as part of the named exclusive cohort."""
        self._consumer_group = consumer_group
        return self

    async def sub(self) -> "PatternSubscription[InflightMessage]":
        """Start with manual acknowledgement. Each delivered message must be settled."""
        prefetch = self._prefetch
        consumer_group = self._consumer_group
        out: BoundedQueue[PatternMessage[InflightMessage]] = _merged_queue(prefetch)

        async def attach(client: "Client", source: PatternSource, dst: "BoundedQueue[Any]") -> _ActiveChannel:
            return await _attach_queue(client, source, prefetch, consumer_group, dst, auto_ack=False)

        fan: _PatternFanIn[InflightMessage] = _PatternFanIn(self._client, self._glob, "queue", out, attach)
        await fan.start()
        return PatternSubscription(fan, out)

    async def sub_auto_ack(self) -> "PatternSubscription[Message]":
        """Start with client-side automatic acknowledgement, yielding settled messages."""
        prefetch = self._prefetch
        consumer_group = self._consumer_group
        out: BoundedQueue[PatternMessage[Message]] = _merged_queue(prefetch)

        async def attach(client: "Client", source: PatternSource, dst: "BoundedQueue[Any]") -> _ActiveChannel:
            return await _attach_queue(client, source, prefetch, consumer_group, dst, auto_ack=True)

        fan: _PatternFanIn[Message] = _PatternFanIn(self._client, self._glob, "queue", out, attach)
        await fan.start()
        return PatternSubscription(fan, out)


class StreamPatternSubscribeBuilder:
    """Builder for a stream pattern subscription."""

    def __init__(self, client: "Client", glob: "_TopicGlob") -> None:
        self._client = client
        self._glob = glob
        self._prefetch = 16
        self._start: wire.StreamStart = wire.StreamStart(kind="latest")
        self._filter: List[Tuple[str, str]] = []
        self._durable_name: Optional[str] = None

    def prefetch(self, prefetch: int) -> "StreamPatternSubscribeBuilder":
        """Per-channel prefetch, applied to every attached stream."""
        self._prefetch = prefetch
        return self

    def from_latest(self) -> "StreamPatternSubscribeBuilder":
        """Begin each attached stream at the live tail (the default)."""
        self._start = wire.StreamStart(kind="latest")
        return self

    def from_earliest(self) -> "StreamPatternSubscribeBuilder":
        """Begin each attached stream at the oldest retained record."""
        self._start = wire.StreamStart(kind="earliest")
        return self

    def from_last(self, count: int) -> "StreamPatternSubscribeBuilder":
        """Begin ``count`` records back from each stream's tail."""
        self._start = wire.StreamStart(kind="nback", value=count)
        return self

    def from_time(self, time_ms: int) -> "StreamPatternSubscribeBuilder":
        """Begin at the first record at or after this wall-clock time (ms)."""
        self._start = wire.StreamStart(kind="bytime", value=time_ms)
        return self

    def filter(self, header: str, pattern: str) -> "StreamPatternSubscribeBuilder":
        """Add a header-match clause applied to every attached stream. Repeatable, AND-ed."""
        self._filter.append((header, pattern))
        return self

    def durable(self, name: str) -> "StreamPatternSubscribeBuilder":
        """Use a durable broker-side cursor of this name on every attached stream.
        each stream tracks its own cursor under the name."""
        self._durable_name = name
        return self

    async def sub(self) -> "PatternSubscription[InflightMessage]":
        """Start with manual acknowledgement. Completing a message advances its
        stream's durable cursor."""
        config = self._config()
        out: BoundedQueue[PatternMessage[InflightMessage]] = _merged_queue(config.prefetch)

        async def attach(client: "Client", source: PatternSource, dst: "BoundedQueue[Any]") -> _ActiveChannel:
            return await _attach_stream(client, source, config, dst, auto_ack=False)

        fan: _PatternFanIn[InflightMessage] = _PatternFanIn(self._client, self._glob, "stream", out, attach)
        await fan.start()
        return PatternSubscription(fan, out)

    async def sub_auto_ack(self) -> "PatternSubscription[Message]":
        """Start with client-side automatic acknowledgement, yielding settled messages."""
        config = self._config()
        out: BoundedQueue[PatternMessage[Message]] = _merged_queue(config.prefetch)

        async def attach(client: "Client", source: PatternSource, dst: "BoundedQueue[Any]") -> _ActiveChannel:
            return await _attach_stream(client, source, config, dst, auto_ack=True)

        fan: _PatternFanIn[Message] = _PatternFanIn(self._client, self._glob, "stream", out, attach)
        await fan.start()
        return PatternSubscription(fan, out)

    def _config(self) -> "_StreamAttachConfig":
        return _StreamAttachConfig(
            prefetch=self._prefetch,
            start=self._start,
            filter=list(self._filter),
            durable_name=self._durable_name,
        )


class PatternSubscription(Generic[M]):
    """A live fan-in over every channel matching a glob, with auto-pickup of
    channels that start matching later. Each item is a :class:`PatternMessage`
    carrying its source. Iterate with ``async for``; call :meth:`close` to stop
    every attached channel and the catalogue watcher."""

    def __init__(self, fan: "_PatternFanIn[M]", out: "BoundedQueue[PatternMessage[M]]") -> None:
        self._fan = fan
        self._out = out

    async def recv(self) -> Optional[PatternMessage[M]]:
        """Receive the next message and the channel it came from, or ``None`` when closed."""
        return await self._out.recv()

    def close(self) -> None:
        """Stop the subscription: every attached channel and the catalogue watcher."""
        self._fan.close()

    def __aiter__(self) -> "PatternSubscription[M]":
        return self

    async def __anext__(self) -> PatternMessage[M]:
        item = await self._out.recv()
        if item is None:
            raise StopAsyncIteration
        return item


# ---- internal orchestration -------------------------------------------------


@dataclass
class _StreamAttachConfig:
    prefetch: int
    start: wire.StreamStart
    filter: List[Tuple[str, str]]
    durable_name: Optional[str]


class _ActiveChannel:
    """A subscribed channel forwarding into the merged stream, plus how to stop it."""

    def __init__(self, sub: Union[Subscription, AutoAckedSubscription], task: "asyncio.Task[None]") -> None:
        self._sub = sub
        self._task = task

    def close(self) -> None:
        self._task.cancel()
        self._sub.close()


_AttachFn = Callable[["Client", PatternSource, "BoundedQueue[Any]"], Awaitable[_ActiveChannel]]


def _merged_queue(prefetch: int) -> "BoundedQueue[Any]":
    return BoundedQueue(max(prefetch, 1) * _CHANNEL_FANIN_BUFFER)


class _PatternFanIn(Generic[M]):
    """Drives a pattern subscription: attach the matching channels now and
    reconcile the attached set against the live catalogue on every change
    (attaching new matches, closing vanished ones). Stops on :meth:`close`."""

    def __init__(
        self,
        client: "Client",
        glob: "_TopicGlob",
        kind: str,
        out: "BoundedQueue[Any]",
        attach: _AttachFn,
    ) -> None:
        self._client = client
        self._glob = glob
        self._kind = kind
        self._out = out
        self._attach = attach
        self._active: Dict[Tuple[str, Optional[str]], _ActiveChannel] = {}
        self._unsubscribe: Optional[Callable[[], None]] = None
        self._closed = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        await self._reconcile()
        if self._closed:
            return
        # The catalogue feed fires on topology pushes, so new channels are picked
        # up without polling.
        self._unsubscribe = self._client.on_catalogue_change(self._on_change)

    def _on_change(self, _catalogue: "Catalogue") -> None:
        # The listener is synchronous, so schedule the async reconcile on the loop.
        if not self._closed:
            asyncio.create_task(self._reconcile())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._unsubscribe is not None:
            self._unsubscribe()
        for channel in self._active.values():
            channel.close()
        self._active.clear()
        self._out.close()

    async def _reconcile(self) -> None:
        async with self._lock:
            if self._closed:
                return
            matching = _matching_keys(self._client.catalogue(), self._glob, self._kind)
            live = {(source.topic, source.group) for source in matching}

            # Close channels that vanished so a later re-declare re-attaches.
            for key in list(self._active.keys()):
                if key not in live:
                    self._active.pop(key).close()

            for source in matching:
                key = (source.topic, source.group)
                if key in self._active:
                    continue
                try:
                    channel = await self._attach(self._client, source, self._out)
                except Exception:
                    # Best effort: retried on the next catalogue change.
                    continue
                if self._closed:
                    channel.close()
                    return
                self._active[key] = channel


def _matching_keys(catalogue: "Catalogue", glob: "_TopicGlob", kind: str) -> List[PatternSource]:
    if kind == "queue":
        return [
            PatternSource(queue.topic, queue.group)
            for queue in catalogue.queues
            if glob.matches(queue.topic)
        ]
    return [
        PatternSource(stream.topic, None)
        for stream in catalogue.streams
        if glob.matches(stream.topic)
    ]


async def _attach_queue(
    client: "Client",
    source: PatternSource,
    prefetch: int,
    consumer_group: Optional[str],
    out: "BoundedQueue[Any]",
    auto_ack: bool,
) -> _ActiveChannel:
    builder = client.subscribe(source.topic)
    if source.group is not None:
        builder = builder.group(source.group)
    builder = builder.prefetch(prefetch)
    if consumer_group is not None:
        builder = builder.consumer_group(consumer_group)
    sub = await (builder.sub_auto_ack() if auto_ack else builder.sub())
    return _spawn_pump(sub, source, out)


async def _attach_stream(
    client: "Client",
    source: PatternSource,
    config: _StreamAttachConfig,
    out: "BoundedQueue[Any]",
    auto_ack: bool,
) -> _ActiveChannel:
    builder = client.stream(source.topic).prefetch(config.prefetch)
    start = config.start
    if start.kind == "latest":
        builder = builder.from_latest()
    elif start.kind == "earliest":
        builder = builder.from_earliest()
    elif start.kind == "nback":
        builder = builder.from_last(start.value)
    elif start.kind == "bytime":
        builder = builder.from_time(start.value)
    for header, pattern in config.filter:
        builder = builder.filter(header, pattern)
    if config.durable_name is not None:
        builder = builder.durable(config.durable_name)
    sub = await (builder.sub_auto_ack() if auto_ack else builder.sub())
    return _spawn_pump(sub, source, out)


def _spawn_pump(
    sub: Union[Subscription, AutoAckedSubscription], source: PatternSource, out: "BoundedQueue[Any]"
) -> _ActiveChannel:
    task = asyncio.create_task(_pump(sub, source, out))
    return _ActiveChannel(sub, task)


async def _pump(
    sub: Union[Subscription, AutoAckedSubscription], source: PatternSource, out: "BoundedQueue[Any]"
) -> None:
    """Forward a channel's deliveries into the merged stream, tagged with the
    source. Backpressure flows back to the channel because ``send`` awaits when
    the merged queue is full. Ends when the channel ends, the merged queue closes,
    or the task is cancelled (on close)."""
    try:
        async for message in sub:
            await out.send(PatternMessage(source, message))
    except Exception:
        # The merged queue closed (pattern subscription dropped) or the channel
        # errored. Either way this forwarder is done. Cancellation propagates as
        # BaseException and ends the task.
        pass


class _TopicGlob:
    """A ``*``-wildcard topic matcher. Mirrors the broker's header-value matcher so
    the discovery glob and the per-subscription filter share one grammar: split on
    ``*``, where each ``*`` matches any run of characters (including empty), no
    regex."""

    def __init__(self, pattern: str) -> None:
        self._segments = pattern.split("*")

    def matches(self, value: str) -> bool:
        segments = self._segments
        if len(segments) == 1:
            return segments[0] == value
        first = segments[0]
        last = segments[-1]
        if not value.startswith(first) or not value.endswith(last):
            return False
        if len(value) < len(first) + len(last):
            return False
        pos = len(first)
        end = len(value) - len(last)
        for mid in segments[1:-1]:
            if not mid:
                continue
            found = value.find(mid, pos)
            if found == -1 or found > end - len(mid):
                return False
            pos = found + len(mid)
        return True
