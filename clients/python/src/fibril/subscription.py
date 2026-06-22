"""Subscription handles: message views, fan-in supervisor, and the builder.

A logical subscription fans in over its partitions: each partition has its own
supervisor feeding a single merged queue the public subscription reads, so
per-partition ordering holds and partitions interleave. A supervised subscription
owns its continuity by re-subscribing to the current owner on a drop or a
graceful owner move, and picks up partitions added by a live grow. This mirrors
the Rust client; the supervisor stays out of the engine reconcile registry.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Awaitable, Callable, Optional

from . import wire
from .engine import Delivered, Engine, Inflight
from .errors import BrokenPipeError, FibrilError, is_transient_error
from .internal.bounded_queue import BoundedQueue
from .internal.retry import (
    PUBLISH_RETRY_INITIAL_BACKOFF_MS,
    PUBLISH_RETRY_MAX_BACKOFF_MS,
    publish_retry_nap_ms,
    sleep_ms,
)
from .message import content_type_header, deserialize_by_content_type
from .publisher import Delay, deadline_from_delay

if TYPE_CHECKING:
    from .client import Client, SubscribeHandle

_Resubscribe = Callable[[wire.Subscribe], Awaitable["SubscribeHandle"]]


def _normalize_group(group: Optional[str]) -> Optional[str]:
    trimmed = group.strip() if group else ""
    if not trimmed or trimmed == "default":
        return None
    return trimmed


class Message:
    """A delivered message in auto-ack mode (server-settled, nothing to ack)."""

    def __init__(self, d: Delivered) -> None:
        self.delivery_tag = d.delivery_tag
        self.content_type_value = d.content_type
        self.headers = d.headers
        self.payload = d.payload
        self.published = d.published
        self.publish_received = d.publish_received
        self.offset = d.offset

    def deserialize(self) -> object:
        return deserialize_by_content_type(self.content_type(), self.payload)

    def content_type(self) -> Optional[str]:
        return content_type_header(self.content_type_value)

    def msgpack(self) -> object:
        return deserialize_by_content_type("application/msgpack", self.payload)

    def json(self) -> object:
        return deserialize_by_content_type("application/json", self.payload)

    def raw(self) -> bytes:
        return self.payload

    def text(self) -> str:
        return self.payload.decode("utf-8")


class InflightMessage:
    """A delivered message in manual-ack mode; settle exactly once.

    Must be settled with ``complete()``, ``fail()``, ``retry()``, or
    ``retry_after()``. Dropping it without settling does not acknowledge it.
    """

    def __init__(self, engine: Engine, d: Inflight) -> None:
        self.delivery_tag = d.delivery_tag
        self.content_type_value = d.content_type
        self.headers = d.headers
        self.payload = d.payload
        self.published = d.published
        self.publish_received = d.publish_received
        self.offset = d.offset
        self._engine = engine
        self._sub_id = d.sub_id
        self._deliver_request_id = d.deliver_request_id
        self._settled = False

    def deserialize(self) -> object:
        return deserialize_by_content_type(self.content_type(), self.payload)

    def content_type(self) -> Optional[str]:
        return content_type_header(self.content_type_value)

    def msgpack(self) -> object:
        return deserialize_by_content_type("application/msgpack", self.payload)

    def json(self) -> object:
        return deserialize_by_content_type("application/json", self.payload)

    def raw(self) -> bytes:
        return self.payload

    def text(self) -> str:
        return self.payload.decode("utf-8")

    async def complete(self) -> "Message":
        self._settle()
        await self._engine.ack(self._sub_id, self.delivery_tag, self._deliver_request_id)
        return self._as_message()

    async def fail(self) -> "Message":
        self._settle()
        await self._engine.nack(
            self._sub_id, self.delivery_tag, False, None, self._deliver_request_id
        )
        return self._as_message()

    async def retry(self) -> "Message":
        self._settle()
        await self._engine.nack(
            self._sub_id, self.delivery_tag, True, None, self._deliver_request_id
        )
        return self._as_message()

    async def retry_after(self, delay: Delay) -> "Message":
        self._settle()
        await self._engine.nack(
            self._sub_id,
            self.delivery_tag,
            True,
            deadline_from_delay(delay),
            self._deliver_request_id,
        )
        return self._as_message()

    def _settle(self) -> None:
        if self._settled:
            raise FibrilError("InflightMessage already settled")
        self._settled = True

    def _as_message(self) -> "Message":
        return Message(
            Delivered(
                delivery_tag=self.delivery_tag,
                payload=self.payload,
                content_type=self.content_type_value,
                headers=self.headers,
                published=self.published,
                publish_received=self.publish_received,
                offset=self.offset,
            )
        )


@dataclass
class _Tagged:
    """A delivery tagged with the engine it arrived on, so settle routes right."""

    engine: Engine
    raw: object


class _PartitionSupervisor:
    """Supervises one partition's stream into the shared merged queue."""

    def __init__(
        self,
        client: "Client",
        req: wire.Subscribe,
        merged: BoundedQueue[_Tagged],
        resubscribe: _Resubscribe,
        first: "SubscribeHandle",
        on_stopped: Callable[[], None],
    ) -> None:
        self._client = client
        self._req = req
        self._merged = merged
        self._resubscribe = resubscribe
        self._engine = first.engine
        self._part_queue: BoundedQueue[object] = first.queue
        self._stopped = False
        self._bound_owner = self._owner_now()
        self._owner_task: Optional[asyncio.Task[None]] = None
        self._run_task = asyncio.ensure_future(self._run_then(on_stopped))
        if client.supervise_subscriptions():
            self._owner_task = asyncio.ensure_future(self._owner_check_loop())

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._owner_task is not None:
            self._owner_task.cancel()
        self._part_queue.close()

    def _owner_now(self) -> Optional[str]:
        return self._client.owner_endpoint(self._req.topic, self._req.partition, self._req.group)

    async def _run_then(self, on_stopped: Callable[[], None]) -> None:
        try:
            await self._run()
        finally:
            on_stopped()

    async def _owner_check_loop(self) -> None:
        interval = max(self._client.supervise_interval_ms(), 1) / 1000
        try:
            while not self._stopped and not self._client.is_shutting_down():
                await asyncio.sleep(interval)
                if self._stopped or self._client.is_shutting_down():
                    return
                await self._client.refresh_topology_throttled()
                current = self._owner_now()
                if current is not None and current != self._bound_owner:
                    self._bound_owner = current
                    self._part_queue.close()
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        while True:
            while True:
                raw = await self._part_queue.recv()
                if raw is None:
                    break
                try:
                    await self._merged.send(_Tagged(self._engine, raw))
                except Exception:
                    self._stopped = True  # merged closed: the consumer is gone
                    break
            if (
                self._stopped
                or self._client.is_shutting_down()
                or not self._client.supervise_subscriptions()
            ):
                if self._owner_task is not None:
                    self._owner_task.cancel()
                return
            if not await self._resubscribe_with_backoff():
                if self._owner_task is not None:
                    self._owner_task.cancel()
                return

    async def _resubscribe_with_backoff(self) -> bool:
        backoff = PUBLISH_RETRY_INITIAL_BACKOFF_MS
        while True:
            if self._stopped or self._client.is_shutting_down():
                return False
            if await self._client.refresh_topology_throttled():
                if self._client.is_topic_missing(self._req.topic, self._req.group):
                    return False
            try:
                handle = await self._resubscribe(self._req)
                if self._stopped or self._client.is_shutting_down():
                    handle.queue.close()
                    return False
                self._engine = handle.engine
                self._part_queue = handle.queue
                self._bound_owner = self._owner_now()
                return True
            except Exception as err:
                if is_transient_error(err):
                    await sleep_ms(publish_retry_nap_ms(backoff))
                    backoff = min(backoff * 2, PUBLISH_RETRY_MAX_BACKOFF_MS)
                    continue
                return False


class _FanIn:
    """Fans one logical subscription in over its partitions into a merged queue."""

    def __init__(
        self,
        client: "Client",
        base_req: wire.Subscribe,
        resubscribe: _Resubscribe,
        initial: list[tuple[int, "SubscribeHandle"]],
        prefetch: int,
    ) -> None:
        self._client = client
        self._base_req = base_req
        self._resubscribe = resubscribe
        cap = max(prefetch, 1) * max(len(initial), 1)
        self._merged: BoundedQueue[_Tagged] = BoundedQueue(cap)
        self._partitions: list[_PartitionSupervisor] = []
        self._covered: set[int] = set()
        self._active = len(initial)
        self._closed = False
        self._growth_task: Optional[asyncio.Task[None]] = None
        for partition, handle in initial:
            self._covered.add(partition)
            self._partitions.append(self._supervise(partition, handle))
        if client.supervise_subscriptions():
            self._growth_task = asyncio.ensure_future(self._growth_loop())

    def _supervise(self, partition: int, handle: "SubscribeHandle") -> _PartitionSupervisor:
        return _PartitionSupervisor(
            self._client,
            replace(self._base_req, partition=partition),
            self._merged,
            self._resubscribe,
            handle,
            self._on_partition_stopped,
        )

    async def recv(self) -> Optional[_Tagged]:
        return await self._merged.recv()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._growth_task is not None:
            self._growth_task.cancel()
        for sup in self._partitions:
            sup.stop()
        self._merged.close()

    async def _growth_loop(self) -> None:
        interval = max(self._client.supervise_interval_ms(), 1) / 1000
        try:
            while not self._closed and not self._client.is_shutting_down():
                await asyncio.sleep(interval)
                if self._closed or self._client.is_shutting_down():
                    return
                await self._pick_up_new_partitions()
        except asyncio.CancelledError:
            pass

    async def _pick_up_new_partitions(self) -> None:
        await self._client.refresh_topology_throttled()
        if self._closed:
            return
        for partition in self._client.partition_set(self._base_req.topic, self._base_req.group):
            if partition in self._covered:
                continue
            try:
                handle = await self._resubscribe(replace(self._base_req, partition=partition))
                if self._closed:
                    handle.queue.close()
                    return
                self._covered.add(partition)
                self._active += 1
                self._partitions.append(self._supervise(partition, handle))
            except Exception:
                pass  # owner not ready yet; retry on the next poll

    def _on_partition_stopped(self) -> None:
        self._active -= 1
        if self._active <= 0 and not self._closed:
            self._closed = True
            if self._growth_task is not None:
                self._growth_task.cancel()
            self._merged.close()


class Subscription:
    """Manual-ack subscription. Async-iterate; settle each ``InflightMessage``."""

    def __init__(self, fan_in: _FanIn) -> None:
        self._fan_in = fan_in

    async def recv(self) -> Optional[InflightMessage]:
        item = await self._fan_in.recv()
        if item is None:
            return None
        assert isinstance(item.raw, Inflight)
        return InflightMessage(item.engine, item.raw)

    def close(self) -> None:
        self._fan_in.close()

    def __aiter__(self) -> "Subscription":
        return self

    async def __anext__(self) -> InflightMessage:
        msg = await self.recv()
        if msg is None:
            raise StopAsyncIteration
        return msg


class AutoAckedSubscription:
    """Auto-ack subscription (server-settled). Async-iterate plain ``Message``s."""

    def __init__(self, fan_in: _FanIn) -> None:
        self._fan_in = fan_in

    async def recv(self) -> Optional[Message]:
        item = await self._fan_in.recv()
        if item is None:
            return None
        assert isinstance(item.raw, Delivered)
        return Message(item.raw)

    def close(self) -> None:
        self._fan_in.close()

    def __aiter__(self) -> "AutoAckedSubscription":
        return self

    async def __anext__(self) -> Message:
        msg = await self.recv()
        if msg is None:
            raise StopAsyncIteration
        return msg


class SubscriptionBuilder:
    """Builder for a subscription. Chain ``group``/``prefetch``/``consumer_group``."""

    def __init__(self, client: "Client", topic: str) -> None:
        self._client = client
        self._topic = topic
        self._group: Optional[str] = None
        self._prefetch = 1
        self._consumer_group: Optional[str] = None
        self._consumer_target: Optional[int] = None

    def group(self, group: str) -> "SubscriptionBuilder":
        self._group = _normalize_group(group)
        return self

    def consumer_group(self, consumer_group: str) -> "SubscriptionBuilder":
        """Join an exclusive cohort: the broker assigns each partition to one
        member, so the cohort consumes the partitioned topic in order with free
        failover. Without this, the subscription is a plain competing consumer."""
        self._consumer_group = consumer_group
        return self

    def consumer_target(self, target: int) -> "SubscriptionBuilder":
        if target < 1:
            raise ValueError("consumer_target must be a positive integer")
        self._consumer_target = target
        return self

    def prefetch(self, prefetch: int) -> "SubscriptionBuilder":
        if prefetch < 1:
            raise ValueError("prefetch must be a positive integer")
        self._prefetch = prefetch
        return self

    async def sub_manual_ack(self) -> Subscription:
        base = self._base_req(auto_ack=False)
        initial = await self._fan_in_initial(base)
        fan_in = _FanIn(self._client, base, self._client.subscribe_once, initial, self._prefetch)
        return Subscription(fan_in)

    async def sub_auto_ack(self) -> AutoAckedSubscription:
        base = self._base_req(auto_ack=True)
        initial = await self._fan_in_initial(base)
        fan_in = _FanIn(self._client, base, self._client.subscribe_once, initial, self._prefetch)
        return AutoAckedSubscription(fan_in)

    def _base_req(self, auto_ack: bool) -> wire.Subscribe:
        return wire.Subscribe(
            topic=self._topic,
            partition=0,
            group=self._group,
            prefetch=self._prefetch,
            auto_ack=auto_ack,
            consumer_group=self._consumer_group,
            consumer_target=self._consumer_target,
        )

    async def _fan_in_initial(
        self, base: wire.Subscribe
    ) -> list[tuple[int, "SubscribeHandle"]]:
        partitions = self._client.partition_set(self._topic, self._group)
        initial: list[tuple[int, "SubscribeHandle"]] = []
        try:
            for partition in partitions:
                handle = await self._client.subscribe_once(replace(base, partition=partition))
                initial.append((partition, handle))
        except Exception as err:
            for _partition, handle in initial:
                handle.queue.close()
            if isinstance(err, FibrilError):
                raise
            raise BrokenPipeError() from err
        return initial
