"""One broker connection: handshake, heartbeat, request/reply, and delivery.

Models the per-connection actor from the Rust client (``crates/client``), which
owns the handshake, optional auth, reconnect reconcile, heartbeats, the
request-id-to-waiter map, and per-subscription delivery queues. The Rust client
funnels everything through a command channel because it spans threads; on a
single asyncio loop that serialization is unnecessary, so the engine exposes
async request methods directly (allocate id, register the waiter, write, await
the reply) with a single read-loop task fanning responses back. There are no
locks: all state lives on one event loop.

Routing and topology are not the engine's concern; the client layer drives it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

from . import wire
from .codec import Frame, build_frame, encode_frame, read_frame
from .errors import (
    BrokenPipeError,
    DisconnectionError,
    EofError,
    FibrilError,
    RedirectError,
    ServerError,
    UnexpectedError,
)
from .frames import decode_body, encode_body
from .protocol import COMPLIANCE_STRING, PROTOCOL_V1, Op

DEFAULT_HEARTBEAT_INTERVAL_S = 5.0

_HANDSHAKE_REQUEST_ID = 1
_AUTH_REQUEST_ID = 2
_RECONCILE_REQUEST_ID = 3
_U64_MASK = 0xFFFFFFFFFFFFFFFF


@dataclass
class EngineOptions:
    """Connection-level settings the engine needs to open and run a session."""

    client_name: str = "fibril-python"
    client_version: str = "0.1.0"
    auth: Optional[wire.Auth] = None
    resume_identity: Optional[wire.ResumeIdentity] = None
    reconnect_reconcile_policy: wire.ReconcilePolicy = "restore_client_subscriptions"
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_S


@dataclass
class Delivered:
    """A delivered message with no client settle action (auto-ack, server-settled)."""

    delivery_tag: wire.DeliveryTag
    payload: bytes
    content_type: wire.ContentType
    headers: dict[str, str]
    published: int
    publish_received: int
    offset: int


@dataclass
class Inflight(Delivered):
    """A delivered message awaiting a manual settle (ack/nack)."""

    deliver_request_id: int = 0
    sub_id: int = 0


@dataclass
class SubscribeResult:
    """Outcome of a subscribe: its delivery queue plus the server-echoed member id."""

    queue: "BoundedQueue[object]"
    member_id: Optional[wire.Uuid]


@dataclass
class _SubState:
    topic: str
    group: Optional[str]
    partition: int
    auto_ack: bool
    queue: "BoundedQueue[object]"


@dataclass
class _Registered:
    """A non-supervised subscription tracked for reconnect reconcile."""

    reconcile: wire.ReconcileSubscription
    queue: "BoundedQueue[object]"
    auto_ack: bool


@dataclass
class _Waiter:
    kind: str
    future: "asyncio.Future[object]"
    supervised: bool = False
    # SubscribeOk does not echo auto_ack, so the subscribe waiter carries the
    # requested value to tag the resulting sub state.
    auto_ack: bool = False


from .internal.bounded_queue import BoundedQueue  # noqa: E402  (after type aliases)

SubscriptionRegistry = dict[int, _Registered]


class Engine:
    """A single live connection to one broker endpoint."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        opts: EngineOptions,
        registry: SubscriptionRegistry,
        resume_identity: wire.ResumeIdentity,
        resume_outcome: wire.ResumeOutcome,
        restored: dict[int, _SubState],
        on_assignment_changed: Optional[object] = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._opts = opts
        self._registry = registry
        self.resume_identity = resume_identity
        self.resume_outcome = resume_outcome
        self._on_assignment_changed = on_assignment_changed

        self._subs: dict[int, _SubState] = dict(restored)
        self._waiters: dict[int, _Waiter] = {}
        self._next_id = 4
        self._closed = False
        self._fatal: Optional[BaseException] = None
        self._preserve_subscriptions = False

        loop = asyncio.get_running_loop()
        self._last_seen = loop.time()
        self._read_task = loop.create_task(self._read_loop())
        self._heartbeat_task = loop.create_task(self._heartbeat_loop())

    # ---- lifecycle -----------------------------------------------------

    @classmethod
    async def start(
        cls,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        opts: EngineOptions,
        registry: Optional[SubscriptionRegistry] = None,
        on_assignment_changed: Optional[object] = None,
    ) -> "Engine":
        registry = registry if registry is not None else {}

        # HELLO
        await _write(writer, build_frame(
            Op.HELLO,
            _HANDSHAKE_REQUEST_ID,
            encode_body(Op.HELLO, wire.Hello(
                client_name=opts.client_name,
                client_version=opts.client_version,
                protocol_version=PROTOCOL_V1,
                resume=opts.resume_identity,
            )),
        ))
        hello_frame = await _next_or_eof(reader)
        if hello_frame.opcode == Op.HELLO_ERR:
            err = decode_body(Op.HELLO_ERR, hello_frame.payload)
            assert isinstance(err, wire.ErrorMsg)
            raise ServerError(err.code, err.message)
        if hello_frame.opcode != Op.HELLO_OK:
            raise UnexpectedError(f"unexpected opcode {hello_frame.opcode} during HELLO")
        hello = decode_body(Op.HELLO_OK, hello_frame.payload)
        assert isinstance(hello, wire.HelloOk)
        if hello.compliance != COMPLIANCE_STRING:
            raise DisconnectionError("protocol compliance marker mismatch")
        if hello.protocol_version != PROTOCOL_V1:
            raise DisconnectionError(
                f"protocol version mismatch: expected {PROTOCOL_V1}, got {hello.protocol_version}"
            )
        resume_identity = wire.ResumeIdentity(
            owner_id=hello.owner_id,
            client_id=hello.client_id,
            resume_token=hello.resume_token,
        )

        # AUTH (optional)
        if opts.auth is not None:
            await _write(writer, build_frame(
                Op.AUTH, _AUTH_REQUEST_ID, encode_body(Op.AUTH, opts.auth)
            ))
            auth_frame = await _next_or_eof(reader)
            if auth_frame.opcode == Op.AUTH_ERR:
                err = decode_body(Op.AUTH_ERR, auth_frame.payload)
                assert isinstance(err, wire.ErrorMsg)
                raise ServerError(err.code, err.message)
            if auth_frame.opcode != Op.AUTH_OK:
                raise UnexpectedError(f"unexpected opcode {auth_frame.opcode} during AUTH")

        # RECONCILE on any reconnect that has subscriptions (see Rust client: a
        # bounced owner reconnects into a fresh session that forgot them).
        restored: dict[int, _SubState] = {}
        reconcile_subs = [r.reconcile for r in registry.values()]
        if reconcile_subs:
            await _write(writer, build_frame(
                Op.RECONCILE_CLIENT,
                _RECONCILE_REQUEST_ID,
                encode_body(Op.RECONCILE_CLIENT, wire.ReconcileClient(
                    policy=opts.reconnect_reconcile_policy,
                    subscriptions=reconcile_subs,
                )),
            ))
            rec_frame = await _next_or_eof(reader)
            if rec_frame.opcode == Op.ERROR:
                err = decode_body(Op.ERROR, rec_frame.payload)
                assert isinstance(err, wire.ErrorMsg)
                raise ServerError(err.code, err.message)
            if rec_frame.opcode != Op.RECONCILE_RESULT:
                raise UnexpectedError(
                    f"unexpected opcode {rec_frame.opcode} during reconciliation"
                )
            result = decode_body(Op.RECONCILE_RESULT, rec_frame.payload)
            assert isinstance(result, wire.ReconcileResult)
            restored = _apply_reconcile_result(registry, result)

        return cls(
            reader,
            writer,
            opts,
            registry,
            resume_identity,
            hello.resume_outcome,
            restored,
            on_assignment_changed,
        )

    def shutdown(self) -> None:
        """Tear the connection down and fail every pending operation."""
        self._mark_dead(self._fatal or BrokenPipeError("engine shutdown"))

    def shutdown_for_reconnect(self) -> None:
        """Tear down but keep registered subscription queues alive for a new engine."""
        self._preserve_subscriptions = True
        self._mark_dead(self._fatal or BrokenPipeError("engine reconnect"))

    async def wait_closed(self) -> None:
        for task in (self._read_task, self._heartbeat_task):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    def is_closed(self) -> bool:
        return self._closed

    # ---- request methods ----------------------------------------------

    async def publish(self, msg: wire.Publish, confirm: bool) -> Optional[int]:
        msg.require_confirm = confirm
        if not confirm:
            await self._send_or_die(build_frame(Op.PUBLISH, self._alloc_id(), encode_body(Op.PUBLISH, msg)))
            return None
        offset = await self._request("publish", Op.PUBLISH, msg)
        assert isinstance(offset, int)
        return offset

    async def publish_pipelined(self, msg: wire.Publish) -> "asyncio.Future[object]":
        """Send a confirmed publish and return its reply future without awaiting it.

        Lets a caller pipeline several confirmed publishes and collect each offset
        later. The frame is written before this returns, so send order is kept.
        """
        msg.require_confirm = True
        if self._closed:
            raise self._fatal or BrokenPipeError()
        rid = self._alloc_id()
        fut: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self._waiters[rid] = _Waiter(kind="publish", future=fut)
        if not await self._send_or_die(build_frame(Op.PUBLISH, rid, encode_body(Op.PUBLISH, msg))):
            self._waiters.pop(rid, None)
            raise self._fatal or BrokenPipeError()
        return fut

    async def publish_delayed(self, msg: wire.PublishDelayed, confirm: bool) -> Optional[int]:
        msg.require_confirm = confirm
        if not confirm:
            await self._send_or_die(
                build_frame(Op.PUBLISH_DELAYED, self._alloc_id(), encode_body(Op.PUBLISH_DELAYED, msg))
            )
            return None
        offset = await self._request("publish", Op.PUBLISH_DELAYED, msg)
        assert isinstance(offset, int)
        return offset

    async def declare_queue(self, req: wire.DeclareQueue) -> None:
        await self._request("declare_queue", Op.DECLARE_QUEUE, req)

    async def fetch_topology(
        self, topic: Optional[str] = None, group: Optional[str] = None
    ) -> wire.TopologyOk:
        result = await self._request(
            "topology", Op.TOPOLOGY, wire.TopologyRequest(topic=topic, group=group)
        )
        assert isinstance(result, wire.TopologyOk)
        return result

    async def subscribe(
        self, req: wire.Subscribe, supervised: bool
    ) -> SubscribeResult:
        result = await self._request(
            "subscribe", Op.SUBSCRIBE, req, supervised=supervised, auto_ack=req.auto_ack
        )
        assert isinstance(result, SubscribeResult)
        return result

    async def ack(self, sub_id: int, tag: wire.DeliveryTag, request_id: int) -> None:
        sub = self._subs.get(sub_id)
        if sub is None:
            return
        msg = wire.Ack(topic=sub.topic, group=sub.group, partition=sub.partition, tags=[tag])
        if not await self._send_or_die(build_frame(Op.ACK, request_id, encode_body(Op.ACK, msg))):
            raise self._fatal or BrokenPipeError()

    async def nack(
        self,
        sub_id: int,
        tag: wire.DeliveryTag,
        requeue: bool,
        not_before: Optional[int],
        request_id: int,
    ) -> None:
        sub = self._subs.get(sub_id)
        if sub is None:
            return
        msg = wire.Nack(
            topic=sub.topic,
            group=sub.group,
            partition=sub.partition,
            tags=[tag],
            requeue=requeue,
            not_before=not_before,
        )
        if not await self._send_or_die(build_frame(Op.NACK, request_id, encode_body(Op.NACK, msg))):
            raise self._fatal or BrokenPipeError()

    # ---- internals -----------------------------------------------------

    def _alloc_id(self) -> int:
        rid = self._next_id
        self._next_id = (self._next_id + 1) & _U64_MASK
        return rid

    async def _request(
        self,
        kind: str,
        op: Op,
        body: object,
        supervised: bool = False,
        auto_ack: bool = False,
    ) -> object:
        if self._closed:
            raise self._fatal or BrokenPipeError()
        rid = self._alloc_id()
        fut: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self._waiters[rid] = _Waiter(
            kind=kind, future=fut, supervised=supervised, auto_ack=auto_ack
        )
        if not await self._send_or_die(build_frame(op, rid, encode_body(op, body))):
            self._waiters.pop(rid, None)
            raise self._fatal or BrokenPipeError()
        return await fut

    async def _send_or_die(self, frame: Frame) -> bool:
        if self._closed:
            return False
        try:
            self._writer.write(encode_frame(frame))
            await self._writer.drain()
            return True
        except (ConnectionError, OSError) as err:
            self._mark_dead(DisconnectionError(f"socket write failed: {err}"))
            return False

    async def _heartbeat_loop(self) -> None:
        interval = max(self._opts.heartbeat_interval_seconds, 0.001)
        timeout = interval * 3
        try:
            while not self._closed:
                await asyncio.sleep(interval)
                if self._closed:
                    return
                if asyncio.get_running_loop().time() - self._last_seen > timeout:
                    self._mark_dead(DisconnectionError("heartbeat timeout"))
                    return
                await self._send_or_die(build_frame(Op.PING, self._alloc_id(), b""))
        except asyncio.CancelledError:
            pass

    async def _read_loop(self) -> None:
        try:
            while True:
                try:
                    frame = await read_frame(self._reader)
                except (ConnectionError, OSError, asyncio.IncompleteReadError) as err:
                    if not self._closed:
                        self._mark_dead(DisconnectionError(f"read failed: {err}"))
                    return
                if frame is None:
                    if not self._closed:
                        self._mark_dead(DisconnectionError("connection closed by peer"))
                    return
                self._last_seen = asyncio.get_running_loop().time()
                await self._handle_frame(frame)
                if self._closed:
                    return
        except asyncio.CancelledError:
            pass

    async def _handle_frame(self, frame: Frame) -> None:
        op = frame.opcode

        if op == Op.PUBLISH_OK:
            ok = decode_body(Op.PUBLISH_OK, frame.payload)
            assert isinstance(ok, wire.PublishOk)
            self._resolve(frame.request_id, "publish", ok.offset)
            return

        if op == Op.SUBSCRIBE_OK:
            self._on_subscribe_ok(frame)
            return

        if op == Op.DECLARE_QUEUE_OK:
            self._resolve(frame.request_id, "declare_queue", None)
            return

        if op == Op.TOPOLOGY_OK:
            topology = decode_body(Op.TOPOLOGY_OK, frame.payload)
            self._resolve(frame.request_id, "topology", topology)
            return

        if op == Op.REDIRECT:
            redirect = decode_body(Op.REDIRECT, frame.payload)
            assert isinstance(redirect, wire.Redirect)
            w = self._waiters.pop(frame.request_id, None)
            if w is not None and not w.future.done():
                w.future.set_exception(RedirectError(redirect))
            return

        if op == Op.DELIVER:
            await self._on_deliver(frame)
            return

        if op == Op.ASSIGNMENT_CHANGED:
            if self._on_assignment_changed is not None:
                msg = decode_body(Op.ASSIGNMENT_CHANGED, frame.payload)
                self._on_assignment_changed(msg)  # type: ignore[operator]
            return

        if op == Op.PING:
            await self._send_or_die(build_frame(Op.PONG, frame.request_id, b""))
            return

        if op == Op.PONG:
            return

        if op in (Op.ERROR, Op.SUBSCRIBE_ERR):
            err = decode_body(Op.ERROR, frame.payload)
            assert isinstance(err, wire.ErrorMsg)
            w = self._waiters.pop(frame.request_id, None)
            if w is not None:
                if not w.future.done():
                    w.future.set_exception(ServerError(err.code, err.message))
            elif op == Op.ERROR:
                # No waiter: a connection-level error is fatal.
                self._mark_dead(
                    DisconnectionError(f"server connection error {err.code}: {err.message}")
                )
            return

        # Unknown opcode: ignore (forward compatibility).

    def _on_subscribe_ok(self, frame: Frame) -> None:
        ok = decode_body(Op.SUBSCRIBE_OK, frame.payload)
        assert isinstance(ok, wire.SubscribeOk)
        w = self._waiters.pop(frame.request_id, None)
        if w is None or w.kind != "subscribe":
            return
        prefetch = max(1, ok.prefetch)
        queue: BoundedQueue[object] = BoundedQueue(prefetch)
        # auto-ack is decided by the request; the broker settles server-side, so
        # the engine carries it only to tag the sub state.
        auto_ack = w.auto_ack
        self._subs[ok.sub_id] = _SubState(
            topic=ok.topic,
            group=ok.group,
            partition=ok.partition,
            auto_ack=auto_ack,
            queue=queue,
        )
        if not w.supervised:
            self._registry[ok.sub_id] = _Registered(
                reconcile=wire.ReconcileSubscription(
                    sub_id=ok.sub_id,
                    topic=ok.topic,
                    partition=ok.partition,
                    group=ok.group,
                    auto_ack=auto_ack,
                    prefetch=ok.prefetch,
                ),
                queue=queue,
                auto_ack=auto_ack,
            )
        if not w.future.done():
            w.future.set_result(SubscribeResult(queue=queue, member_id=ok.member_id))

    async def _on_deliver(self, frame: Frame) -> None:
        d = decode_body(Op.DELIVER, frame.payload)
        assert isinstance(d, wire.Deliver)
        sub = self._subs.get(d.sub_id)
        if sub is None:
            return
        # Auto-ack can be done two ways: server-side, by setting auto_ack on the
        # wire so the broker settles each delivery as it sends it; or client-side,
        # by leaving it false and acking after the consumer yields. This client
        # uses the server-side path (matches the Rust client), so an auto-ack sub
        # yields settled Delivered items with nothing to ack, while a manual sub
        # yields Inflight items the caller settles.
        if sub.auto_ack:
            item: object = Delivered(
                delivery_tag=d.delivery_tag,
                payload=d.payload,
                content_type=d.content_type,
                headers=d.headers,
                published=d.published,
                publish_received=d.publish_received,
                offset=d.offset,
            )
        else:
            item = Inflight(
                delivery_tag=d.delivery_tag,
                payload=d.payload,
                content_type=d.content_type,
                headers=d.headers,
                published=d.published,
                publish_received=d.publish_received,
                offset=d.offset,
                deliver_request_id=frame.request_id,
                sub_id=d.sub_id,
            )
        try:
            await sub.queue.send(item)  # prefetch backpressure
        except FibrilError:
            self._subs.pop(d.sub_id, None)
            self._registry.pop(d.sub_id, None)

    def _resolve(self, request_id: int, kind: str, value: object) -> None:
        w = self._waiters.get(request_id)
        if w is None or w.kind != kind:
            return
        del self._waiters[request_id]
        if not w.future.done():
            w.future.set_result(value)

    def _mark_dead(self, err: BaseException) -> None:
        if self._closed:
            return
        self._closed = True
        self._fatal = err
        for w in self._waiters.values():
            if not w.future.done():
                w.future.set_exception(err)
        self._waiters.clear()
        for sub_id, sub in self._subs.items():
            if self._preserve_subscriptions and sub_id in self._registry:
                continue
            sub.queue.close(err)
        self._subs.clear()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        try:
            self._writer.close()
        except Exception:
            pass


# ---- module helpers ----------------------------------------------------


def _apply_reconcile_result(
    registry: SubscriptionRegistry, result: wire.ReconcileResult
) -> dict[int, _SubState]:
    restored: dict[int, _SubState] = {}
    for item in result.subscriptions:
        client = item.client
        if client is None:
            continue
        registered = registry.get(client.sub_id)
        if item.action != "keep":
            if registered is not None:
                registered.queue.close(
                    DisconnectionError(
                        f"subscription was not kept after reconnect: {item.reason}"
                    )
                )
            registry.pop(client.sub_id, None)
            continue
        if registered is None:
            continue
        server = item.server or client
        registered.reconcile = server
        registry.pop(client.sub_id, None)
        registry[server.sub_id] = registered
        restored[server.sub_id] = _SubState(
            topic=server.topic,
            group=server.group,
            partition=server.partition,
            auto_ack=registered.auto_ack,
            queue=registered.queue,
        )
    return restored


async def _write(writer: asyncio.StreamWriter, frame: Frame) -> None:
    writer.write(encode_frame(frame))
    await writer.drain()


async def _next_or_eof(reader: asyncio.StreamReader) -> Frame:
    try:
        frame = await read_frame(reader)
    except asyncio.IncompleteReadError as err:
        raise EofError("connection closed before expected frame") from err
    if frame is None:
        raise EofError("connection closed before expected frame")
    return frame
