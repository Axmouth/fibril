"""Publisher handles: fire-and-forget, confirmed, pipelined, delayed, reliable.

Confirmed publishes follow owner redirects (bounded) and retry across a transient
owner failover (refresh topology, jittered backoff, deadline), mirroring the Rust
client. Delays and TTLs are seconds-native (a float of seconds or a
``datetime.timedelta``), matching Python convention and the Rust client's
seconds-or-Duration. The TS client uses milliseconds.
"""

from __future__ import annotations

import time
import uuid as _uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Optional, Union

from . import wire
from .errors import BrokenPipeError, RedirectError, ServerError, is_retryable, is_transient_error
from .internal.retry import (
    PUBLISH_RETRY_INITIAL_BACKOFF_MS,
    bump_backoff,
    new_publish_retry_state,
    now_ms,
    publish_retry_nap_ms,
    sleep_ms,
)
from .internal.topology import Route
from .message import (
    HEADER_PRODUCER_ID,
    HEADER_PRODUCER_SEQ,
    NewMessage,
    Publishable,
    into_message,
)

if TYPE_CHECKING:
    from .client import Client

#: A delay or TTL: seconds as a float, or a ``timedelta``.
Delay = Union[float, timedelta]

_ERR_NOT_FOUND = 404


def _to_ms(value: Delay) -> int:
    seconds = value.total_seconds() if isinstance(value, timedelta) else float(value)
    if seconds < 0:
        raise ValueError("delay/ttl must be non-negative")
    return int(seconds * 1000)


def deadline_from_delay(delay: Delay) -> int:
    """Absolute Unix-millisecond deadline for a relative delay (seconds)."""
    return int(time.time() * 1000) + _to_ms(delay)


def _wall_now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _SendSpec:
    content_type: wire.ContentType
    headers: dict[str, str]
    payload: bytes
    key: Optional[bytes]
    not_before: Optional[int]


class PublishConfirmation:
    """A handle for a pipelined confirmed publish. Await ``confirmed()`` for the offset."""

    def __init__(self, future: "object") -> None:
        self._future = future

    async def confirmed(self) -> int:
        import asyncio

        assert isinstance(self._future, asyncio.Future)
        offset = await self._future
        assert isinstance(offset, int)
        return offset


class Publisher:
    """Publish messages to a topic (and optional group)."""

    def __init__(
        self,
        client: "Client",
        topic: str,
        group: Optional[str],
        default_ttl_ms: Optional[int] = None,
    ) -> None:
        self._client = client
        self._topic = topic
        self._group = group
        self._default_ttl_ms = default_ttl_ms

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def group(self) -> Optional[str]:
        return self._group

    # ---- public publish API -------------------------------------------

    async def publish(self, payload: Publishable) -> None:
        await self._send_unconfirmed(_spec_from_message(into_message(payload), None))

    async def publish_confirmed(self, payload: Publishable) -> int:
        return await self._send_confirmed(_spec_from_message(into_message(payload), None))

    async def publish_with_confirmation(self, payload: Publishable) -> PublishConfirmation:
        return await self._send_with_confirmation(_spec_from_message(into_message(payload), None))

    async def publish_bytes(self, payload: bytes) -> None:
        await self._send_unconfirmed(_raw_spec(payload, None))

    async def publish_bytes_confirmed(self, payload: bytes) -> int:
        return await self._send_confirmed(_raw_spec(payload, None))

    async def publish_delayed(self, payload: Publishable, delay: Delay) -> None:
        await self._send_unconfirmed(
            _spec_from_message(into_message(payload), deadline_from_delay(delay))
        )

    async def publish_delayed_confirmed(self, payload: Publishable, delay: Delay) -> int:
        return await self._send_confirmed(
            _spec_from_message(into_message(payload), deadline_from_delay(delay))
        )

    def expiring(self, ttl: Delay) -> "Publisher":
        """A publisher that stamps a default TTL (seconds or timedelta) on each
        immediate publish, so the broker drops the message if not consumed in
        time. Applies to immediate publishes. Delayed publishes carry no TTL."""
        return Publisher(self._client, self._topic, self._group, _to_ms(ttl))

    def reliable(self) -> "ReliablePublisher":
        """Wrap this publisher to stamp producer dedup ids and retry until durable."""
        return ReliablePublisher(self)

    # ---- routed send paths --------------------------------------------

    async def _send_unconfirmed(self, spec: _SendSpec) -> None:
        route = self._client.route(self._topic, self._group, spec.key)
        engine = await self._client.engine_for(self._topic, route.partition, self._group)
        if spec.not_before is None:
            await engine.publish(self._build_publish(spec, route), confirm=False)
        else:
            await engine.publish_delayed(self._build_delayed(spec, route), confirm=False)

    async def _send_confirmed(self, spec: _SendSpec) -> int:
        state = new_publish_retry_state(self._client.publish_timeout_ms)
        while True:
            try:
                route = self._client.route(self._topic, self._group, spec.key)
                engine = await self._client.engine_for(self._topic, route.partition, self._group)
                if spec.not_before is None:
                    offset = await engine.publish(self._build_publish(spec, route), confirm=True)
                else:
                    offset = await engine.publish_delayed(self._build_delayed(spec, route), confirm=True)
                assert offset is not None
                return offset
            except Exception as err:
                await self._after_publish_failure(err, state)

    async def _after_publish_failure(self, err: Exception, state: object) -> None:
        from .internal.retry import PublishRetryState

        assert isinstance(state, PublishRetryState)
        if isinstance(err, RedirectError):
            if state.redirects >= self._client.max_redirects:
                raise err
            self._client.apply_redirect(err.redirect)
            state.redirects += 1
            return
        if is_transient_error(err):
            if state.deadline_ms is None or now_ms() >= state.deadline_ms:
                raise err
            if await self._client.refresh_topology_throttled():
                if self._client.is_topic_missing(self._topic, self._group):
                    raise ServerError(_ERR_NOT_FOUND, f"{self._topic} is not declared in the cluster")
            remaining = state.deadline_ms - now_ms()
            await sleep_ms(min(publish_retry_nap_ms(state.backoff_ms), max(remaining, 0)))
            bump_backoff(state)
            return
        raise err

    async def _send_with_confirmation(self, spec: _SendSpec) -> PublishConfirmation:
        route = self._client.route(self._topic, self._group, spec.key)
        engine = await self._client.engine_for(self._topic, route.partition, self._group)
        future = await engine.publish_pipelined(self._build_publish(spec, route))
        return PublishConfirmation(future)

    def _build_publish(self, spec: _SendSpec, route: Route) -> wire.Publish:
        return wire.Publish(
            topic=self._topic,
            partition=route.partition,
            group=self._group,
            require_confirm=False,
            content_type=spec.content_type,
            headers=spec.headers,
            payload=spec.payload,
            published=_wall_now_ms(),
            partition_key=spec.key,
            partitioning_version=route.partitioning_version,
            ttl_ms=self._default_ttl_ms,
        )

    def _build_delayed(self, spec: _SendSpec, route: Route) -> wire.PublishDelayed:
        assert spec.not_before is not None
        return wire.PublishDelayed(
            topic=self._topic,
            partition=route.partition,
            group=self._group,
            require_confirm=False,
            not_before=spec.not_before,
            content_type=spec.content_type,
            headers=spec.headers,
            payload=spec.payload,
            published=_wall_now_ms(),
            partition_key=spec.key,
            partitioning_version=route.partitioning_version,
        )


class ReliablePublisher:
    """Retries a confirmed publish until durably confirmed, stamping a stable
    producer id and a monotonic per-producer sequence under ``fibril.client.*``.

    At-least-once today (a retry after a lost confirmation may duplicate). Becomes
    effectively-once with no API change once the broker dedups on those keys.
    """

    def __init__(self, publisher: Publisher, producer_id: Optional[str] = None) -> None:
        self._publisher = publisher
        self._producer_id = producer_id or str(_uuid.uuid4())
        self._seq = 0
        self._max_attempts = 0

    @property
    def producer_id(self) -> str:
        return self._producer_id

    def max_attempts(self, n: int) -> "ReliablePublisher":
        if n < 0:
            raise ValueError("max_attempts must be a non-negative integer")
        self._max_attempts = n
        return self

    async def publish(self, payload: Publishable) -> int:
        seq = self._seq
        self._seq += 1
        message = (
            into_message(payload)
            .system_header(HEADER_PRODUCER_ID, self._producer_id)
            .system_header(HEADER_PRODUCER_SEQ, str(seq))
        )
        attempts = 0
        while True:
            try:
                return await self._publisher.publish_confirmed(message)
            except Exception as err:
                if is_retryable(err):
                    attempts += 1
                    if self._max_attempts != 0 and attempts >= self._max_attempts:
                        raise
                    await sleep_ms(publish_retry_nap_ms(PUBLISH_RETRY_INITIAL_BACKOFF_MS))
                    continue
                raise


def _spec_from_message(message: NewMessage, not_before: Optional[int]) -> _SendSpec:
    return _SendSpec(
        content_type=message.content_type_value,
        headers=message.headers,
        payload=message.payload,
        key=message.partition_key_value,
        not_before=not_before,
    )


def _raw_spec(payload: bytes, not_before: Optional[int]) -> _SendSpec:
    return _SendSpec(content_type=None, headers={}, payload=payload, key=None, not_before=not_before)
