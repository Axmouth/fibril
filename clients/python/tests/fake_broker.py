"""In-process fake broker for engine and client tests.

A loopback asyncio server that speaks the real wire codec (decode requests,
encode responses through ``frames``/``codec``), so tests exercise the actual
bytes. An op it does not handle is dropped, so a client request for it hangs
rather than getting a wrong value, which makes a missing handler obvious.
"""

from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass, field
from typing import Optional

from fibril import wire
from fibril.codec import Frame, build_frame, encode_frame, read_frame
from fibril.frames import decode_body, encode_body
from fibril.protocol import COMPLIANCE_STRING, PROTOCOL_V1, Op


@dataclass
class FakeBroker:
    """A scriptable single-broker stand-in over a real loopback socket."""

    compliance: str = COMPLIANCE_STRING
    # resume_outcome returned in HELLO_OK. Set to "resumed" before a second
    # connection to script a resumed reconnect.
    resume_outcome: wire.ResumeOutcome = "new"
    auth_ok: bool = True
    topology: Optional[wire.TopologyOk] = None
    # When set, PUBLISH gets this response instead of PUBLISH_OK.
    redirect_publish: Optional[wire.Redirect] = None
    error_publish: Optional[tuple[int, str]] = None
    # Payloads delivered (one Deliver frame each) right after a SUBSCRIBE_OK.
    deliver_on_subscribe: list[bytes] = field(default_factory=list)
    deliver_auto_ack: bool = False
    # When set, a TOPOLOGY_UPDATE is pushed right after HELLO_OK.
    push_topology_on_hello: Optional[wire.TopologyOk] = None

    # Records for assertions.
    publishes: list[wire.Publish] = field(default_factory=list)
    delayed_publishes: list[wire.PublishDelayed] = field(default_factory=list)
    acks: list[wire.Ack] = field(default_factory=list)
    nacks: list[wire.Nack] = field(default_factory=list)
    declares: list[wire.DeclareQueue] = field(default_factory=list)
    subscribes: list[wire.Subscribe] = field(default_factory=list)
    stream_subscribes: list[wire.SubscribeStream] = field(default_factory=list)
    reconciles: list[wire.ReconcileClient] = field(default_factory=list)
    topology_acks: list[wire.TopologyUpdateAck] = field(default_factory=list)

    # When set, the broker serves TLS with this server-side context.
    ssl_context: Optional[ssl.SSLContext] = None

    host: str = field(init=False, default="127.0.0.1")
    port: int = field(init=False, default=0)
    _server: Optional[asyncio.AbstractServer] = field(init=False, default=None)
    _offset: int = field(init=False, default=0)
    _sub_id: int = field(init=False, default=0)
    _deliver_rid: int = field(init=False, default=1_000_000)
    _writers: list[asyncio.StreamWriter] = field(init=False, default_factory=list)

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, self.host, 0, ssl=self.ssl_context
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        for w in self._writers:
            try:
                w.close()
            except Exception:
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _send(self, writer: asyncio.StreamWriter, frame: Frame) -> None:
        writer.write(encode_frame(frame))
        await writer.drain()

    async def push(self, frame: Frame) -> None:
        """Push an unsolicited frame to every connected client (server push)."""
        for w in self._writers:
            await self._send(w, frame)

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.append(writer)
        try:
            while True:
                frame = await read_frame(reader)
                if frame is None:
                    return
                await self._dispatch(reader, writer, frame)
        except (asyncio.IncompleteReadError, ConnectionError):
            return

    async def _dispatch(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        frame: Frame,
    ) -> None:
        op = frame.opcode
        rid = frame.request_id

        if op == Op.HELLO:
            ok = wire.HelloOk(
                protocol_version=PROTOCOL_V1,
                owner_id=b"\x01" * 16,
                client_id=b"\x02" * 16,
                resume_token=b"\x03" * 16,
                resume_outcome=self.resume_outcome,
                server_name="fake",
                compliance=self.compliance,
            )
            await self._send(writer, build_frame(Op.HELLO_OK, rid, encode_body(Op.HELLO_OK, ok)))
            if self.push_topology_on_hello is not None:
                await self._send(
                    writer,
                    build_frame(
                        Op.TOPOLOGY_UPDATE,
                        0,
                        encode_body(Op.TOPOLOGY_UPDATE, self.push_topology_on_hello),
                    ),
                )
            return

        if op == Op.TOPOLOGY_UPDATE_ACK:
            ack = decode_body(Op.TOPOLOGY_UPDATE_ACK, frame.payload)
            assert isinstance(ack, wire.TopologyUpdateAck)
            self.topology_acks.append(ack)
            return

        if op == Op.AUTH:
            if self.auth_ok:
                await self._send(writer, build_frame(Op.AUTH_OK, rid, b""))
            else:
                await self._error(writer, Op.AUTH_ERR, rid, 401, "bad credentials")
            return

        if op == Op.PUBLISH:
            msg = decode_body(Op.PUBLISH, frame.payload)
            assert isinstance(msg, wire.Publish)
            self.publishes.append(msg)
            if self.redirect_publish is not None:
                await self._send(
                    writer,
                    build_frame(Op.REDIRECT, rid, encode_body(Op.REDIRECT, self.redirect_publish)),
                )
            elif self.error_publish is not None:
                code, text = self.error_publish
                await self._error(writer, Op.ERROR, rid, code, text)
            elif msg.require_confirm:
                self._offset += 1
                await self._send(
                    writer,
                    build_frame(Op.PUBLISH_OK, rid, encode_body(Op.PUBLISH_OK, wire.PublishOk(self._offset))),
                )
            return

        if op == Op.PUBLISH_DELAYED:
            dmsg = decode_body(Op.PUBLISH_DELAYED, frame.payload)
            assert isinstance(dmsg, wire.PublishDelayed)
            self.delayed_publishes.append(dmsg)
            if dmsg.require_confirm:
                self._offset += 1
                await self._send(
                    writer,
                    build_frame(Op.PUBLISH_OK, rid, encode_body(Op.PUBLISH_OK, wire.PublishOk(self._offset))),
                )
            return

        if op == Op.DECLARE_QUEUE:
            req = decode_body(Op.DECLARE_QUEUE, frame.payload)
            assert isinstance(req, wire.DeclareQueue)
            self.declares.append(req)
            ok = wire.DeclareQueueOk(status="created", partition_count=req.partition_count or 1)
            await self._send(writer, build_frame(Op.DECLARE_QUEUE_OK, rid, encode_body(Op.DECLARE_QUEUE_OK, ok)))
            return

        if op == Op.SUBSCRIBE:
            req = decode_body(Op.SUBSCRIBE, frame.payload)
            assert isinstance(req, wire.Subscribe)
            self.subscribes.append(req)
            self._sub_id += 1
            sub_id = self._sub_id
            ok = wire.SubscribeOk(
                sub_id=sub_id,
                topic=req.topic,
                partition=req.partition,
                group=req.group,
                prefetch=max(1, req.prefetch),
                consumer_group=req.consumer_group,
                consumer_target=req.consumer_target,
                member_id=req.member_id,
            )
            await self._send(writer, build_frame(Op.SUBSCRIBE_OK, rid, encode_body(Op.SUBSCRIBE_OK, ok)))
            for payload in self.deliver_on_subscribe:
                await self.deliver(writer, sub_id, req, payload)
            return

        if op == Op.SUBSCRIBE_STREAM:
            sreq = decode_body(Op.SUBSCRIBE_STREAM, frame.payload)
            assert isinstance(sreq, wire.SubscribeStream)
            self.stream_subscribes.append(sreq)
            self._sub_id += 1
            sub_id = self._sub_id
            # Streams resolve through the shared SUBSCRIBE_OK reply (the engine
            # waiter is keyed by request id, not op).
            ok = wire.SubscribeOk(
                sub_id=sub_id,
                topic=sreq.topic,
                partition=sreq.partition,
                group=None,
                prefetch=max(1, sreq.prefetch),
                consumer_group=None,
                consumer_target=None,
                member_id=None,
            )
            await self._send(writer, build_frame(Op.SUBSCRIBE_OK, rid, encode_body(Op.SUBSCRIBE_OK, ok)))
            shim = wire.Subscribe(
                topic=sreq.topic,
                partition=sreq.partition,
                group=None,
                prefetch=sreq.prefetch,
                auto_ack=sreq.auto_ack,
                consumer_group=None,
                consumer_target=None,
            )
            for payload in self.deliver_on_subscribe:
                await self.deliver(writer, sub_id, shim, payload)
            return

        if op == Op.ACK:
            ack = decode_body(Op.ACK, frame.payload)
            assert isinstance(ack, wire.Ack)
            self.acks.append(ack)
            return

        if op == Op.NACK:
            nack = decode_body(Op.NACK, frame.payload)
            assert isinstance(nack, wire.Nack)
            self.nacks.append(nack)
            return

        if op == Op.TOPOLOGY:
            topology = self.topology or wire.TopologyOk(generation=0, queues=[])
            await self._send(writer, build_frame(Op.TOPOLOGY_OK, rid, encode_body(Op.TOPOLOGY_OK, topology)))
            return

        if op == Op.RECONCILE_CLIENT:
            rc = decode_body(Op.RECONCILE_CLIENT, frame.payload)
            assert isinstance(rc, wire.ReconcileClient)
            self.reconciles.append(rc)
            results = [
                wire.ReconcileSubscriptionResult(client=s, server=s, action="keep", reason="ok")
                for s in rc.subscriptions
            ]
            await self._send(
                writer,
                build_frame(
                    Op.RECONCILE_RESULT, rid, encode_body(Op.RECONCILE_RESULT, wire.ReconcileResult(results))
                ),
            )
            return

        if op == Op.PING:
            await self._send(writer, build_frame(Op.PONG, rid, b""))
            return

        if op == Op.PONG:
            return

        # Unhandled op: drop it (the client request will hang, surfacing the gap).

    async def deliver(
        self,
        writer: asyncio.StreamWriter,
        sub_id: int,
        req: wire.Subscribe,
        payload: bytes,
    ) -> None:
        self._deliver_rid += 1
        self._offset += 1
        d = wire.Deliver(
            sub_id=sub_id,
            topic=req.topic,
            group=req.group,
            partition=req.partition,
            offset=self._offset,
            delivery_tag=wire.DeliveryTag(epoch=self._deliver_rid),
            published=1,
            publish_received=2,
            content_type="text",
            headers={},
            payload=payload,
        )
        await self._send(writer, build_frame(Op.DELIVER, self._deliver_rid, encode_body(Op.DELIVER, d)))

    async def _error(
        self, writer: asyncio.StreamWriter, op: Op, rid: int, code: int, message: str
    ) -> None:
        await self._send(
            writer, build_frame(op, rid, encode_body(op, wire.ErrorMsg(code, message)))
        )
