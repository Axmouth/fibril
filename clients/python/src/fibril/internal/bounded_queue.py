"""Bounded async queue with backpressure and error-carrying close.

Behaviorally matches the Rust client's bounded mpsc channels (and the TS client's
BoundedQueue): ``send`` blocks the producer when full, ``recv`` returns ``None``
once the queue is closed and drained, and ``close(error)`` makes pending and
future sends raise while a waiting receiver is released. The error carried by
``close`` is what a subscription iterator raises on a disconnect rather than
ending silently.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Generic, Optional, TypeVar

from ..errors import FibrilError

T = TypeVar("T")


class _QueueClosed(FibrilError):
    """Default close cause when a caller does not supply one."""


class BoundedQueue(Generic[T]):
    __slots__ = (
        "_capacity",
        "_buffer",
        "_pending_senders",
        "_pending_receiver",
        "_closed",
        "_close_error",
    )

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError(f"BoundedQueue capacity must be >= 1, got {capacity}")
        self._capacity = capacity
        self._buffer: deque[T] = deque()
        self._pending_senders: deque[tuple[T, asyncio.Future[None]]] = deque()
        self._pending_receiver: Optional[asyncio.Future[Optional[T]]] = None
        self._closed = False
        self._close_error: Optional[BaseException] = None

    async def send(self, value: T) -> None:
        """Enqueue a value, awaiting space when full. Raises if the queue closes."""
        if self._closed:
            raise self._close_error or _QueueClosed("queue closed")

        receiver = self._pending_receiver
        if receiver is not None and not receiver.done():
            self._pending_receiver = None
            receiver.set_result(value)
            return

        if len(self._buffer) < self._capacity:
            self._buffer.append(value)
            return

        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._pending_senders.append((value, fut))
        await fut

    async def recv(self) -> Optional[T]:
        """Dequeue the next value, or ``None`` when closed and drained."""
        if self._buffer:
            value = self._buffer.popleft()
            self._wake_one_sender()
            return value
        if self._closed:
            return None
        fut: asyncio.Future[Optional[T]] = asyncio.get_running_loop().create_future()
        self._pending_receiver = fut
        return await fut

    def close_error(self) -> Optional[BaseException]:
        """The error the queue was closed with, if any. ``recv`` still returns
        ``None`` on close, so a caller that wants the typed close reason reads
        this after ``recv`` yields ``None``."""
        return self._close_error

    def _wake_one_sender(self) -> None:
        if not self._pending_senders:
            return
        if len(self._buffer) >= self._capacity:
            return
        value, fut = self._pending_senders.popleft()
        self._buffer.append(value)
        if not fut.done():
            fut.set_result(None)

    def close(self, error: Optional[BaseException] = None) -> None:
        """Close the queue. Idempotent. Pending sends raise, and a waiting receiver ends."""
        if self._closed:
            return
        self._closed = True
        self._close_error = error
        err = error or _QueueClosed("queue closed")
        for _value, fut in self._pending_senders:
            if not fut.done():
                fut.set_exception(err)
        self._pending_senders.clear()
        receiver = self._pending_receiver
        if receiver is not None and not self._buffer:
            self._pending_receiver = None
            if not receiver.done():
                receiver.set_result(None)

    def drain(self) -> list[T]:
        """Remove and return all currently buffered values."""
        out = list(self._buffer)
        self._buffer.clear()
        return out

    @property
    def closed(self) -> bool:
        return self._closed

    def __aiter__(self) -> "BoundedQueue[T]":
        return self

    async def __anext__(self) -> T:
        value = await self.recv()
        if value is None:
            raise StopAsyncIteration
        return value
