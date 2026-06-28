"""Custom binary wire format for v1 frame bodies.

Byte-exact with the Rust ``crates/protocol/src/v1/wire.rs`` and the TypeScript
``clients/typescript/src/wire.ts``. The broker encodes frame bodies in this
format (the 20-byte frame header lives in ``codec.py``). This module is the only
one that touches bytes and the only one that must match the broker exactly.

Layout rules (all big-endian):
  - integers: u8 / u16 / u32 / u64 big-endian
  - len-prefixed bytes: u32 length, then the raw bytes
  - strings: len-prefixed UTF-8
  - bool: u8 (0 or 1)
  - uuid: 16 raw bytes (opaque here, the client only echoes them)
  - option[T]: u8 tag (0 = none, 1 = some), then T when some
  - each body starts with a 4-byte ASCII magic
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Literal, Optional, Union

from .errors import WireError

# ---- partition hashing (FNV-1a 64-bit) ----------------------------------

_MASK64 = 0xFFFFFFFFFFFFFFFF
_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3


def fnv1a(data: bytes) -> int:
    """Stable 64-bit FNV-1a hash for partition selection.

    Must stay byte-for-byte identical to the broker and every other client so a
    given key always lands on the same partition (per-key ordering).
    """
    h = _FNV_OFFSET
    for byte in data:
        h = (h ^ byte) & _MASK64
        h = (h * _FNV_PRIME) & _MASK64
    return h


# ---- shared field types -------------------------------------------------

#: Opaque 16-byte identifier (uuid on the wire). The client never interprets it.
Uuid = bytes

Headers = dict[str, str]


@dataclass(frozen=True)
class CustomContentType:
    """An arbitrary MIME content type carried on the wire."""

    value: str


#: Frame content type. ``None`` = unspecified, ``CustomContentType`` = arbitrary.
ContentType = Union[None, Literal["msgpack", "json", "text"], CustomContentType]


@dataclass(frozen=True)
class CustomDlqPolicy:
    """Route dead-lettered messages to an explicit (topic, group)."""

    topic: str
    group: Optional[str]


#: Dead-letter policy on a queue declaration.
QueueDlqPolicy = Union[Literal["discard", "global"], CustomDlqPolicy]

ResumeOutcome = Literal["new", "resumed", "resume_not_found", "resume_rejected"]

_RESUME_OUTCOME_TO_U8: dict[str, int] = {
    "new": 0,
    "resumed": 1,
    "resume_not_found": 2,
    "resume_rejected": 3,
}
_RESUME_OUTCOME_FROM_U8: list[ResumeOutcome] = [
    "new",
    "resumed",
    "resume_not_found",
    "resume_rejected",
]

ReconcilePolicy = Literal["conservative", "restore_client_subscriptions"]
ReconcileAction = Literal[
    "keep", "close_client_side", "close_server_side", "recreate_client_side"
]

_RECONCILE_ACTION_TO_U8: dict[str, int] = {
    "keep": 0,
    "close_client_side": 1,
    "close_server_side": 2,
    "recreate_client_side": 3,
}
_RECONCILE_ACTION_FROM_U8: list[ReconcileAction] = [
    "keep",
    "close_client_side",
    "close_server_side",
    "recreate_client_side",
]


@dataclass(frozen=True)
class DeliveryTag:
    """Broker delivery tag used to ack or nack a delivery."""

    epoch: int


@dataclass(frozen=True)
class ResumeIdentity:
    """Identity returned by the broker and offered on reconnect."""

    owner_id: Uuid
    client_id: Uuid
    resume_token: Uuid


@dataclass(frozen=True)
class ReconcileSubscription:
    sub_id: int
    topic: str
    partition: int
    group: Optional[str]
    auto_ack: bool
    prefetch: int
    consumer_group: Optional[str] = None
    consumer_target: Optional[int] = None
    member_id: Optional[Uuid] = None


@dataclass(frozen=True)
class ReconcileSubscriptionResult:
    client: Optional[ReconcileSubscription]
    server: Optional[ReconcileSubscription]
    action: ReconcileAction
    reason: str


# ---- byte writer / reader ----------------------------------------------


class Writer:
    """Growable big-endian byte writer."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def u8(self, v: int) -> None:
        self._buf += struct.pack(">B", v)

    def u16(self, v: int) -> None:
        self._buf += struct.pack(">H", v)

    def u32(self, v: int) -> None:
        self._buf += struct.pack(">I", v)

    def u64(self, v: int) -> None:
        self._buf += struct.pack(">Q", v)

    def raw(self, b: bytes) -> None:
        self._buf += b

    def write_bytes(self, b: bytes) -> None:
        self.u32(len(b))
        self._buf += b

    def write_str(self, s: str) -> None:
        self.write_bytes(s.encode("utf-8"))

    def write_bool(self, b: bool) -> None:
        self.u8(1 if b else 0)

    def uuid(self, u: Uuid) -> None:
        if len(u) != 16:
            raise WireError("invalid_uuid", f"wire: uuid must be 16 bytes, got {len(u)}")
        self._buf += u

    def optional_str(self, s: Optional[str]) -> None:
        if s is None:
            self.u8(0)
        else:
            self.u8(1)
            self.write_str(s)

    def optional_uuid(self, u: Optional[Uuid]) -> None:
        if u is None:
            self.u8(0)
        else:
            self.u8(1)
            self.uuid(u)

    def optional_bytes(self, b: Optional[bytes]) -> None:
        if b is None:
            self.u8(0)
        else:
            self.u8(1)
            self.write_bytes(b)

    def optional_u32(self, v: Optional[int]) -> None:
        if v is None:
            self.u8(0)
        else:
            self.u8(1)
            self.u32(v)

    def optional_u64(self, v: Optional[int]) -> None:
        if v is None:
            self.u8(0)
        else:
            self.u8(1)
            self.u64(v)

    def resume_outcome(self, outcome: ResumeOutcome) -> None:
        self.u8(_RESUME_OUTCOME_TO_U8[outcome])

    def resume_identity(self, ri: ResumeIdentity) -> None:
        self.uuid(ri.owner_id)
        self.uuid(ri.client_id)
        self.uuid(ri.resume_token)

    def optional_resume_identity(self, ri: Optional[ResumeIdentity]) -> None:
        if ri is None:
            self.u8(0)
        else:
            self.u8(1)
            self.resume_identity(ri)

    def content_type(self, ct: ContentType) -> None:
        if ct is None:
            self.u8(0)
        elif ct == "msgpack":
            self.u8(1)
        elif ct == "json":
            self.u8(2)
        elif ct == "text":
            self.u8(3)
        elif isinstance(ct, CustomContentType):
            self.u8(4)
            self.write_str(ct.value)
        else:  # pragma: no cover - guards an invalid union value
            raise WireError("unknown_content_type", f"wire: bad content type {ct!r}")

    def headers(self, h: Headers) -> None:
        self.u32(len(h))
        for k, v in h.items():
            self.write_str(k)
            self.write_str(v)

    def queue_key(self, topic: str, partition: int, group: Optional[str]) -> None:
        self.write_str(topic)
        self.u32(partition)
        self.optional_str(group)

    def settle_tags(self, tags: list[DeliveryTag]) -> None:
        self.u32(len(tags))
        for t in tags:
            self.u64(t.epoch)

    def dlq_policy(self, p: QueueDlqPolicy) -> None:
        if p == "discard":
            self.u8(0)
        elif p == "global":
            self.u8(1)
        elif isinstance(p, CustomDlqPolicy):
            self.u8(2)
            self.write_str(p.topic)
            self.optional_str(p.group)
        else:  # pragma: no cover - guards an invalid union value
            raise WireError("unknown_tag", f"wire: bad dlq policy {p!r}")

    def optional_dlq_policy(self, p: Optional[QueueDlqPolicy]) -> None:
        if p is None:
            self.u8(0)
        else:
            self.u8(1)
            self.dlq_policy(p)

    def reconcile_policy(self, p: ReconcilePolicy) -> None:
        self.u8(1 if p == "restore_client_subscriptions" else 0)

    def reconcile_action(self, a: ReconcileAction) -> None:
        self.u8(_RECONCILE_ACTION_TO_U8[a])

    def reconcile_subscription(self, s: ReconcileSubscription) -> None:
        self.u64(s.sub_id)
        self.queue_key(s.topic, s.partition, s.group)
        self.write_bool(s.auto_ack)
        self.u32(s.prefetch)
        self.optional_str(s.consumer_group)
        self.optional_u32(s.consumer_target)
        self.optional_uuid(s.member_id)

    def optional_reconcile_subscription(
        self, s: Optional[ReconcileSubscription]
    ) -> None:
        if s is None:
            self.u8(0)
        else:
            self.u8(1)
            self.reconcile_subscription(s)

    def reconcile_subscriptions(self, subs: list[ReconcileSubscription]) -> None:
        self.u32(len(subs))
        for s in subs:
            self.reconcile_subscription(s)

    def partitions(self, parts: list[int]) -> None:
        self.u32(len(parts))
        for p in parts:
            self.u32(p)

    def magic(self, m: str) -> None:
        self.raw(m.encode("ascii"))

    def finish(self) -> bytes:
        return bytes(self._buf)


class Reader:
    """Big-endian byte reader with strict bounds and trailing-byte checks."""

    __slots__ = ("_buf", "_cursor")

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self._cursor = 0

    def _need(self, n: int) -> None:
        if self._cursor + n > len(self._buf):
            raise WireError("unexpected_eof", "wire: unexpected end of input")

    def u8(self) -> int:
        self._need(1)
        v = self._buf[self._cursor]
        self._cursor += 1
        return v

    def u16(self) -> int:
        self._need(2)
        (v,) = struct.unpack_from(">H", self._buf, self._cursor)
        self._cursor += 2
        return int(v)

    def u32(self) -> int:
        self._need(4)
        (v,) = struct.unpack_from(">I", self._buf, self._cursor)
        self._cursor += 4
        return int(v)

    def u64(self) -> int:
        self._need(8)
        (v,) = struct.unpack_from(">Q", self._buf, self._cursor)
        self._cursor += 8
        return int(v)

    def raw(self, n: int) -> bytes:
        self._need(n)
        v = self._buf[self._cursor : self._cursor + n]
        self._cursor += n
        return v

    def read_bytes(self) -> bytes:
        return self.raw(self.u32())

    def read_str(self) -> str:
        return self.read_bytes().decode("utf-8")

    def read_bool(self) -> bool:
        return self.u8() != 0

    def uuid(self) -> Uuid:
        return self.raw(16)

    def optional_str(self) -> Optional[str]:
        return self.read_str() if self.u8() == 1 else None

    def optional_uuid(self) -> Optional[Uuid]:
        return self.uuid() if self.u8() == 1 else None

    def optional_bytes(self) -> Optional[bytes]:
        return self.read_bytes() if self.u8() == 1 else None

    def optional_u32(self) -> Optional[int]:
        return self.u32() if self.u8() == 1 else None

    def optional_u64(self) -> Optional[int]:
        return self.u64() if self.u8() == 1 else None

    def resume_outcome(self) -> ResumeOutcome:
        tag = self.u8()
        if tag >= len(_RESUME_OUTCOME_FROM_U8):
            raise WireError("unknown_tag", f"wire: unknown resume outcome {tag}")
        return _RESUME_OUTCOME_FROM_U8[tag]

    def resume_identity(self) -> ResumeIdentity:
        return ResumeIdentity(
            owner_id=self.uuid(), client_id=self.uuid(), resume_token=self.uuid()
        )

    def optional_resume_identity(self) -> Optional[ResumeIdentity]:
        return self.resume_identity() if self.u8() == 1 else None

    def content_type(self) -> ContentType:
        tag = self.u8()
        if tag == 0:
            return None
        if tag == 1:
            return "msgpack"
        if tag == 2:
            return "json"
        if tag == 3:
            return "text"
        if tag == 4:
            return CustomContentType(self.read_str())
        raise WireError("unknown_content_type", f"wire: unknown content type {tag}")

    def headers(self) -> Headers:
        n = self.u32()
        h: Headers = {}
        for _ in range(n):
            k = self.read_str()
            h[k] = self.read_str()
        return h

    def queue_key(self) -> tuple[str, int, Optional[str]]:
        return (self.read_str(), self.u32(), self.optional_str())

    def settle_tags(self) -> list[DeliveryTag]:
        n = self.u32()
        return [DeliveryTag(epoch=self.u64()) for _ in range(n)]

    def dlq_policy(self) -> QueueDlqPolicy:
        tag = self.u8()
        if tag == 0:
            return "discard"
        if tag == 1:
            return "global"
        if tag == 2:
            return CustomDlqPolicy(topic=self.read_str(), group=self.optional_str())
        raise WireError("unknown_tag", f"wire: unknown dlq policy {tag}")

    def optional_dlq_policy(self) -> Optional[QueueDlqPolicy]:
        return self.dlq_policy() if self.u8() == 1 else None

    def reconcile_policy(self) -> ReconcilePolicy:
        return "restore_client_subscriptions" if self.u8() == 1 else "conservative"

    def reconcile_action(self) -> ReconcileAction:
        tag = self.u8()
        if tag >= len(_RECONCILE_ACTION_FROM_U8):
            raise WireError("unknown_tag", f"wire: unknown reconcile action {tag}")
        return _RECONCILE_ACTION_FROM_U8[tag]

    def reconcile_subscription(self) -> ReconcileSubscription:
        sub_id = self.u64()
        topic, partition, group = self.queue_key()
        return ReconcileSubscription(
            sub_id=sub_id,
            topic=topic,
            partition=partition,
            group=group,
            auto_ack=self.read_bool(),
            prefetch=self.u32(),
            consumer_group=self.optional_str(),
            consumer_target=self.optional_u32(),
            member_id=self.optional_uuid(),
        )

    def optional_reconcile_subscription(self) -> Optional[ReconcileSubscription]:
        return self.reconcile_subscription() if self.u8() == 1 else None

    def reconcile_subscriptions(self) -> list[ReconcileSubscription]:
        n = self.u32()
        return [self.reconcile_subscription() for _ in range(n)]

    def partitions(self) -> list[int]:
        n = self.u32()
        return [self.u32() for _ in range(n)]

    def expect_magic(self, m: str) -> None:
        got = self.raw(4)
        if got != m.encode("ascii"):
            raise WireError("invalid_magic", f"wire: bad magic, expected {m}")

    def remaining(self) -> int:
        return len(self._buf) - self._cursor

    def finish(self) -> None:
        if self._cursor != len(self._buf):
            raise WireError(
                "trailing_bytes", f"wire: {len(self._buf) - self._cursor} trailing byte(s)"
            )


# ---- handshake bodies ---------------------------------------------------


@dataclass
class Hello:
    client_name: str
    client_version: str
    protocol_version: int
    resume: Optional[ResumeIdentity] = None


def encode_hello_body(hello: Hello) -> bytes:
    w = Writer()
    w.magic("FHL1")
    w.write_str(hello.client_name)
    w.write_str(hello.client_version)
    w.u16(hello.protocol_version)
    w.optional_resume_identity(hello.resume)
    return w.finish()


def decode_hello_body(body: bytes) -> Hello:
    r = Reader(body)
    r.expect_magic("FHL1")
    value = Hello(
        client_name=r.read_str(),
        client_version=r.read_str(),
        protocol_version=r.u16(),
        resume=r.optional_resume_identity(),
    )
    r.finish()
    return value


@dataclass
class HelloOk:
    protocol_version: int
    owner_id: Uuid
    client_id: Uuid
    resume_token: Uuid
    resume_outcome: ResumeOutcome
    server_name: str
    compliance: str


def encode_hello_ok_body(hello: HelloOk) -> bytes:
    w = Writer()
    w.magic("FHO1")
    w.u16(hello.protocol_version)
    w.uuid(hello.owner_id)
    w.uuid(hello.client_id)
    w.uuid(hello.resume_token)
    w.resume_outcome(hello.resume_outcome)
    w.write_str(hello.server_name)
    w.write_str(hello.compliance)
    return w.finish()


def decode_hello_ok_body(body: bytes) -> HelloOk:
    r = Reader(body)
    r.expect_magic("FHO1")
    value = HelloOk(
        protocol_version=r.u16(),
        owner_id=r.uuid(),
        client_id=r.uuid(),
        resume_token=r.uuid(),
        resume_outcome=r.resume_outcome(),
        server_name=r.read_str(),
        compliance=r.read_str(),
    )
    r.finish()
    return value


@dataclass
class Auth:
    username: str
    password: str


def encode_auth_body(auth: Auth) -> bytes:
    w = Writer()
    w.magic("FAU1")
    w.write_str(auth.username)
    w.write_str(auth.password)
    return w.finish()


def decode_auth_body(body: bytes) -> Auth:
    r = Reader(body)
    r.expect_magic("FAU1")
    value = Auth(username=r.read_str(), password=r.read_str())
    r.finish()
    return value


@dataclass
class ErrorMsg:
    code: int
    message: str


def encode_error_body(error: ErrorMsg) -> bytes:
    w = Writer()
    w.magic("FER1")
    w.u16(error.code)
    w.write_str(error.message)
    return w.finish()


def decode_error_body(body: bytes) -> ErrorMsg:
    r = Reader(body)
    r.expect_magic("FER1")
    value = ErrorMsg(code=r.u16(), message=r.read_str())
    r.finish()
    return value


# ---- publish ------------------------------------------------------------


@dataclass
class Publish:
    topic: str
    partition: int
    group: Optional[str]
    require_confirm: bool
    content_type: ContentType
    headers: Headers
    payload: bytes
    published: int
    partition_key: Optional[bytes] = None
    partitioning_version: int = 0
    #: Optional message TTL in milliseconds (relative to publish time).
    ttl_ms: Optional[int] = None


def _write_publish_common(w: Writer, p: Publish) -> None:
    w.write_str(p.topic)
    w.optional_str(p.group)
    w.u32(p.partition)
    w.write_bool(p.require_confirm)
    w.content_type(p.content_type)
    w.headers(p.headers)
    w.u64(p.published)
    w.optional_bytes(p.partition_key)
    w.u64(p.partitioning_version)
    w.write_bytes(p.payload)
    # Trailing so a peer that omits it still decodes (read as None).
    w.optional_u64(p.ttl_ms)


def _read_publish_common(r: Reader) -> Publish:
    topic = r.read_str()
    group = r.optional_str()
    partition = r.u32()
    require_confirm = r.read_bool()
    content_type = r.content_type()
    headers = r.headers()
    published = r.u64()
    partition_key = r.optional_bytes()
    partitioning_version = r.u64()
    payload = r.read_bytes()
    # Trailing optional: absent when the peer has not been updated to send it.
    ttl_ms = r.optional_u64() if r.remaining() > 0 else None
    return Publish(
        topic=topic,
        partition=partition,
        group=group,
        require_confirm=require_confirm,
        content_type=content_type,
        headers=headers,
        payload=payload,
        published=published,
        partition_key=partition_key,
        partitioning_version=partitioning_version,
        ttl_ms=ttl_ms,
    )


def encode_publish_body(p: Publish) -> bytes:
    w = Writer()
    w.magic("FPB1")
    _write_publish_common(w, p)
    return w.finish()


def decode_publish_body(body: bytes) -> Publish:
    r = Reader(body)
    r.expect_magic("FPB1")
    value = _read_publish_common(r)
    r.finish()
    return value


@dataclass
class PublishDelayed:
    topic: str
    partition: int
    group: Optional[str]
    require_confirm: bool
    not_before: int
    content_type: ContentType
    headers: Headers
    payload: bytes
    published: int
    partition_key: Optional[bytes] = None
    partitioning_version: int = 0


def encode_publish_delayed_body(p: PublishDelayed) -> bytes:
    w = Writer()
    w.magic("FPD1")
    w.write_str(p.topic)
    w.optional_str(p.group)
    w.u32(p.partition)
    w.write_bool(p.require_confirm)
    w.u64(p.not_before)
    w.content_type(p.content_type)
    w.headers(p.headers)
    w.u64(p.published)
    w.optional_bytes(p.partition_key)
    w.u64(p.partitioning_version)
    w.write_bytes(p.payload)
    return w.finish()


def decode_publish_delayed_body(body: bytes) -> PublishDelayed:
    r = Reader(body)
    r.expect_magic("FPD1")
    value = PublishDelayed(
        topic=r.read_str(),
        group=r.optional_str(),
        partition=r.u32(),
        require_confirm=r.read_bool(),
        not_before=r.u64(),
        content_type=r.content_type(),
        headers=r.headers(),
        published=r.u64(),
        partition_key=r.optional_bytes(),
        partitioning_version=r.u64(),
        payload=r.read_bytes(),
    )
    r.finish()
    return value


@dataclass
class PublishOk:
    offset: int


def encode_publish_ok_body(ok: PublishOk) -> bytes:
    w = Writer()
    w.magic("FPO1")
    w.u64(ok.offset)
    return w.finish()


def decode_publish_ok_body(body: bytes) -> PublishOk:
    r = Reader(body)
    r.expect_magic("FPO1")
    value = PublishOk(offset=r.u64())
    r.finish()
    return value


# ---- deliver ------------------------------------------------------------


@dataclass
class Deliver:
    sub_id: int
    topic: str
    group: Optional[str]
    partition: int
    offset: int
    delivery_tag: DeliveryTag
    published: int
    publish_received: int
    content_type: ContentType
    headers: Headers
    payload: bytes


def encode_deliver_body(d: Deliver) -> bytes:
    w = Writer()
    w.magic("FDL1")
    w.u64(d.sub_id)
    w.write_str(d.topic)
    w.optional_str(d.group)
    w.u32(d.partition)
    w.u64(d.offset)
    w.u64(d.delivery_tag.epoch)
    w.u64(d.published)
    w.u64(d.publish_received)
    w.content_type(d.content_type)
    w.headers(d.headers)
    w.write_bytes(d.payload)
    return w.finish()


def decode_deliver_body(body: bytes) -> Deliver:
    r = Reader(body)
    r.expect_magic("FDL1")
    value = Deliver(
        sub_id=r.u64(),
        topic=r.read_str(),
        group=r.optional_str(),
        partition=r.u32(),
        offset=r.u64(),
        delivery_tag=DeliveryTag(epoch=r.u64()),
        published=r.u64(),
        publish_received=r.u64(),
        content_type=r.content_type(),
        headers=r.headers(),
        payload=r.read_bytes(),
    )
    r.finish()
    return value


# ---- settle (ack / nack) ------------------------------------------------


@dataclass
class Ack:
    topic: str
    group: Optional[str]
    partition: int
    tags: list[DeliveryTag]


def encode_ack_body(ack: Ack) -> bytes:
    w = Writer()
    w.magic("FAK1")
    w.write_str(ack.topic)
    w.optional_str(ack.group)
    w.u32(ack.partition)
    w.settle_tags(ack.tags)
    return w.finish()


def decode_ack_body(body: bytes) -> Ack:
    r = Reader(body)
    r.expect_magic("FAK1")
    value = Ack(
        topic=r.read_str(),
        group=r.optional_str(),
        partition=r.u32(),
        tags=r.settle_tags(),
    )
    r.finish()
    return value


@dataclass
class Nack:
    topic: str
    group: Optional[str]
    partition: int
    tags: list[DeliveryTag]
    requeue: bool
    not_before: Optional[int]


def encode_nack_body(nack: Nack) -> bytes:
    w = Writer()
    w.magic("FNK1")
    w.write_str(nack.topic)
    w.optional_str(nack.group)
    w.u32(nack.partition)
    w.settle_tags(nack.tags)
    w.write_bool(nack.requeue)
    w.optional_u64(nack.not_before)
    return w.finish()


def decode_nack_body(body: bytes) -> Nack:
    r = Reader(body)
    r.expect_magic("FNK1")
    value = Nack(
        topic=r.read_str(),
        group=r.optional_str(),
        partition=r.u32(),
        tags=r.settle_tags(),
        requeue=r.read_bool(),
        not_before=r.optional_u64(),
    )
    r.finish()
    return value


# ---- declare queue ------------------------------------------------------


@dataclass
class DeclareQueue:
    topic: str
    group: Optional[str]
    dlq_policy: Optional[QueueDlqPolicy]
    dlq_max_retries: Optional[int]
    partition_count: Optional[int] = None
    #: Default message TTL (ms) for the queue. Not queue expiration.
    default_message_ttl_ms: Optional[int] = None


def encode_declare_queue_body(d: DeclareQueue) -> bytes:
    w = Writer()
    w.magic("FDQ1")
    w.write_str(d.topic)
    w.optional_str(d.group)
    w.optional_dlq_policy(d.dlq_policy)
    w.optional_u32(d.dlq_max_retries)
    w.optional_u32(d.partition_count)
    # Trailing so a peer that omits it still decodes (read as None).
    w.optional_u64(d.default_message_ttl_ms)
    return w.finish()


def decode_declare_queue_body(body: bytes) -> DeclareQueue:
    r = Reader(body)
    r.expect_magic("FDQ1")
    value = DeclareQueue(
        topic=r.read_str(),
        group=r.optional_str(),
        dlq_policy=r.optional_dlq_policy(),
        dlq_max_retries=r.optional_u32(),
        partition_count=r.optional_u32(),
        default_message_ttl_ms=r.optional_u64() if r.remaining() > 0 else None,
    )
    r.finish()
    return value


@dataclass
class DeclareQueueOk:
    status: str
    partition_count: int


def encode_declare_queue_ok_body(ok: DeclareQueueOk) -> bytes:
    w = Writer()
    w.magic("FDK1")
    w.write_str(ok.status)
    w.u32(ok.partition_count)
    return w.finish()


def decode_declare_queue_ok_body(body: bytes) -> DeclareQueueOk:
    r = Reader(body)
    r.expect_magic("FDK1")
    value = DeclareQueueOk(status=r.read_str(), partition_count=r.u32())
    r.finish()
    return value


# ---- assignment changed (server push to cohort members) -----------------


@dataclass
class AssignmentChanged:
    topic: str
    group: Optional[str]
    consumer_group: str
    generation: int
    assigned: list[int]
    added: list[int]
    revoked: list[int]


def encode_assignment_changed_body(a: AssignmentChanged) -> bytes:
    w = Writer()
    w.magic("FAC1")
    w.write_str(a.topic)
    w.optional_str(a.group)
    w.write_str(a.consumer_group)
    w.u64(a.generation)
    w.partitions(a.assigned)
    w.partitions(a.added)
    w.partitions(a.revoked)
    return w.finish()


def decode_assignment_changed_body(body: bytes) -> AssignmentChanged:
    r = Reader(body)
    r.expect_magic("FAC1")
    value = AssignmentChanged(
        topic=r.read_str(),
        group=r.optional_str(),
        consumer_group=r.read_str(),
        generation=r.u64(),
        assigned=r.partitions(),
        added=r.partitions(),
        revoked=r.partitions(),
    )
    r.finish()
    return value


# ---- subscribe ----------------------------------------------------------


@dataclass
class Subscribe:
    topic: str
    partition: int
    group: Optional[str]
    prefetch: int
    auto_ack: bool
    consumer_group: Optional[str] = None
    consumer_target: Optional[int] = None
    member_id: Optional[Uuid] = None


def encode_subscribe_body(s: Subscribe) -> bytes:
    w = Writer()
    w.magic("FSB1")
    w.queue_key(s.topic, s.partition, s.group)
    w.u32(s.prefetch)
    w.write_bool(s.auto_ack)
    w.optional_str(s.consumer_group)
    w.optional_u32(s.consumer_target)
    w.optional_uuid(s.member_id)
    return w.finish()


def decode_subscribe_body(body: bytes) -> Subscribe:
    r = Reader(body)
    r.expect_magic("FSB1")
    topic, partition, group = r.queue_key()
    value = Subscribe(
        topic=topic,
        partition=partition,
        group=group,
        prefetch=r.u32(),
        auto_ack=r.read_bool(),
        consumer_group=r.optional_str(),
        consumer_target=r.optional_u32(),
        member_id=r.optional_uuid(),
    )
    r.finish()
    return value


@dataclass
class SubscribeOk:
    sub_id: int
    topic: str
    partition: int
    group: Optional[str]
    prefetch: int
    consumer_group: Optional[str] = None
    consumer_target: Optional[int] = None
    member_id: Optional[Uuid] = None


def encode_subscribe_ok_body(s: SubscribeOk) -> bytes:
    w = Writer()
    w.magic("FSO1")
    w.u64(s.sub_id)
    w.queue_key(s.topic, s.partition, s.group)
    w.u32(s.prefetch)
    w.optional_str(s.consumer_group)
    w.optional_u32(s.consumer_target)
    w.optional_uuid(s.member_id)
    return w.finish()


def decode_subscribe_ok_body(body: bytes) -> SubscribeOk:
    r = Reader(body)
    r.expect_magic("FSO1")
    sub_id = r.u64()
    topic, partition, group = r.queue_key()
    value = SubscribeOk(
        sub_id=sub_id,
        topic=topic,
        partition=partition,
        group=group,
        prefetch=r.u32(),
        consumer_group=r.optional_str(),
        consumer_target=r.optional_u32(),
        member_id=r.optional_uuid(),
    )
    r.finish()
    return value


# ---- plexus (fan-out stream) --------------------------------------------

#: Per-channel durability tier: ``"ephemeral"`` | ``"speculative"`` | ``"durable"``.
StreamDurability = str

_DURABILITY_TO_U8 = {"ephemeral": 0, "speculative": 1, "durable": 2}
_U8_TO_DURABILITY = {v: k for k, v in _DURABILITY_TO_U8.items()}


@dataclass
class StreamRetention:
    #: Drop records older than this age in milliseconds.
    max_age_ms: Optional[int] = None
    #: Keep at most this many bytes of retained records.
    max_bytes: Optional[int] = None
    #: Keep at most this many records.
    max_records: Optional[int] = None


@dataclass
class StreamStart:
    """Where a stream subscription begins when it has no durable cursor to resume.

    ``kind`` is one of ``"latest"``, ``"earliest"``, ``"offset"``, ``"nback"``,
    ``"bytime"``. ``value`` carries the offset / count / time for the last three.
    """

    kind: str = "latest"
    value: int = 0


_START_TO_U8 = {"latest": 0, "earliest": 1, "offset": 2, "nback": 3, "bytime": 4}
_U8_TO_START = {v: k for k, v in _START_TO_U8.items()}


def _put_stream_start(w: "Writer", s: StreamStart) -> None:
    tag = _START_TO_U8.get(s.kind)
    if tag is None:  # pragma: no cover - guards an invalid value
        raise WireError("unknown_tag", f"wire: bad stream start {s.kind!r}")
    w.u8(tag)
    if tag >= 2:
        w.u64(s.value)


def _read_stream_start(r: "Reader") -> StreamStart:
    tag = r.u8()
    kind = _U8_TO_START.get(tag)
    if kind is None:
        raise WireError("unknown_tag", f"wire: bad stream start tag {tag}")
    value = r.u64() if tag >= 2 else 0
    return StreamStart(kind=kind, value=value)


@dataclass
class DeclarePlexus:
    topic: str
    partition_count: Optional[int] = None
    durability: StreamDurability = "durable"
    retention: StreamRetention = field(default_factory=StreamRetention)
    replication_factor: Optional[int] = None


def encode_declare_plexus_body(d: DeclarePlexus) -> bytes:
    w = Writer()
    w.magic("FDP1")
    w.write_str(d.topic)
    w.optional_u32(d.partition_count)
    tag = _DURABILITY_TO_U8.get(d.durability)
    if tag is None:  # pragma: no cover - guards an invalid value
        raise WireError("unknown_tag", f"wire: bad durability {d.durability!r}")
    w.u8(tag)
    w.optional_u64(d.retention.max_age_ms)
    w.optional_u64(d.retention.max_bytes)
    w.optional_u64(d.retention.max_records)
    w.optional_u32(d.replication_factor)
    return w.finish()


def decode_declare_plexus_body(body: bytes) -> DeclarePlexus:
    r = Reader(body)
    r.expect_magic("FDP1")
    topic = r.read_str()
    partition_count = r.optional_u32()
    tag = r.u8()
    durability = _U8_TO_DURABILITY.get(tag)
    if durability is None:
        raise WireError("unknown_tag", f"wire: bad durability tag {tag}")
    retention = StreamRetention(
        max_age_ms=r.optional_u64(),
        max_bytes=r.optional_u64(),
        max_records=r.optional_u64(),
    )
    replication_factor = r.optional_u32()
    r.finish()
    return DeclarePlexus(topic, partition_count, durability, retention, replication_factor)


@dataclass
class DeclarePlexusOk:
    status: str
    partition_count: int


def encode_declare_plexus_ok_body(ok: DeclarePlexusOk) -> bytes:
    w = Writer()
    w.magic("FPK1")
    w.write_str(ok.status)
    w.u32(ok.partition_count)
    return w.finish()


def decode_declare_plexus_ok_body(body: bytes) -> DeclarePlexusOk:
    r = Reader(body)
    r.expect_magic("FPK1")
    value = DeclarePlexusOk(status=r.read_str(), partition_count=r.u32())
    r.finish()
    return value


@dataclass
class SubscribeStream:
    topic: str
    partition: int
    durable_name: Optional[str]
    start: StreamStart
    filter: list[tuple[str, str]]
    prefetch: int
    auto_ack: bool


def encode_subscribe_stream_body(s: SubscribeStream) -> bytes:
    w = Writer()
    w.magic("FSP1")
    w.write_str(s.topic)
    w.u32(s.partition)
    w.optional_str(s.durable_name)
    _put_stream_start(w, s.start)
    w.u32(len(s.filter))
    for key, pattern in s.filter:
        w.write_str(key)
        w.write_str(pattern)
    w.u32(s.prefetch)
    w.write_bool(s.auto_ack)
    return w.finish()


def decode_subscribe_stream_body(body: bytes) -> SubscribeStream:
    r = Reader(body)
    r.expect_magic("FSP1")
    topic = r.read_str()
    partition = r.u32()
    durable_name = r.optional_str()
    start = _read_stream_start(r)
    clause_count = r.u32()
    flt: list[tuple[str, str]] = []
    for _ in range(clause_count):
        key = r.read_str()
        pattern = r.read_str()
        flt.append((key, pattern))
    value = SubscribeStream(
        topic=topic,
        partition=partition,
        durable_name=durable_name,
        start=start,
        filter=flt,
        prefetch=r.u32(),
        auto_ack=r.read_bool(),
    )
    r.finish()
    return value


# ---- reconcile ----------------------------------------------------------


@dataclass
class ReconcileClient:
    policy: ReconcilePolicy
    subscriptions: list[ReconcileSubscription]


def encode_reconcile_client_body(rc: ReconcileClient) -> bytes:
    w = Writer()
    w.magic("FRC1")
    w.reconcile_policy(rc.policy)
    w.reconcile_subscriptions(rc.subscriptions)
    return w.finish()


def decode_reconcile_client_body(body: bytes) -> ReconcileClient:
    r = Reader(body)
    r.expect_magic("FRC1")
    value = ReconcileClient(
        policy=r.reconcile_policy(),
        subscriptions=r.reconcile_subscriptions(),
    )
    r.finish()
    return value


@dataclass
class ReconcileServer:
    subscriptions: list[ReconcileSubscription]


def encode_reconcile_server_body(rs: ReconcileServer) -> bytes:
    w = Writer()
    w.magic("FRS1")
    w.reconcile_subscriptions(rs.subscriptions)
    return w.finish()


def decode_reconcile_server_body(body: bytes) -> ReconcileServer:
    r = Reader(body)
    r.expect_magic("FRS1")
    value = ReconcileServer(subscriptions=r.reconcile_subscriptions())
    r.finish()
    return value


@dataclass
class ReconcileResult:
    subscriptions: list[ReconcileSubscriptionResult]


def encode_reconcile_result_body(rr: ReconcileResult) -> bytes:
    w = Writer()
    w.magic("FRR1")
    w.u32(len(rr.subscriptions))
    for s in rr.subscriptions:
        w.optional_reconcile_subscription(s.client)
        w.optional_reconcile_subscription(s.server)
        w.reconcile_action(s.action)
        w.write_str(s.reason)
    return w.finish()


def decode_reconcile_result_body(body: bytes) -> ReconcileResult:
    r = Reader(body)
    r.expect_magic("FRR1")
    n = r.u32()
    subscriptions = [
        ReconcileSubscriptionResult(
            client=r.optional_reconcile_subscription(),
            server=r.optional_reconcile_subscription(),
            action=r.reconcile_action(),
            reason=r.read_str(),
        )
        for _ in range(n)
    ]
    r.finish()
    return ReconcileResult(subscriptions=subscriptions)


# ---- topology + redirect (cluster routing) ------------------------------


@dataclass
class TopologyRequest:
    topic: Optional[str]
    group: Optional[str]


@dataclass
class AdvertisedAddress:
    #: A broker address to advertise to clients: a connectable ``host:port``
    #: (resolved at connect time, so ``host`` may be a service name) with optional
    #: free-form ``tags``. Owners advertise a priority-ordered list and clients use
    #: the first reachable.
    host: str
    port: int
    tags: list[str] = field(default_factory=list)


def write_advertised_addresses(w: "Writer", addrs: list[AdvertisedAddress]) -> None:
    w.u32(len(addrs))
    for a in addrs:
        w.write_str(a.host)
        w.u16(a.port)
        w.u32(len(a.tags))
        for tag in a.tags:
            w.write_str(tag)


def read_advertised_addresses(r: "Reader") -> list[AdvertisedAddress]:
    out: list[AdvertisedAddress] = []
    for _ in range(r.u32()):
        host = r.read_str()
        port = r.u16()
        tags = [r.read_str() for _ in range(r.u32())]
        out.append(AdvertisedAddress(host=host, port=port, tags=tags))
    return out


@dataclass
class QueueTopologyEntry:
    topic: str
    partition: int
    group: Optional[str]
    #: Owner broker endpoints in priority order, empty when the owner is unknown.
    owner_endpoints: list[AdvertisedAddress]
    partitioning_version: int
    #: Authoritative partition count for the queue, used for key routing.
    partition_count: int


@dataclass
class StreamTopologyEntry:
    #: Ownership of one Plexus stream partition, as seen by clients for routing.
    #: Mirrors QueueTopologyEntry without a group. ``owner_endpoints`` is empty
    #: in standalone mode and while an owner is unknown.
    topic: str
    partition: int
    owner_endpoints: list[AdvertisedAddress]
    partitioning_version: int
    #: Authoritative partition count for the stream, used for key routing.
    partition_count: int


@dataclass
class TopologyOk:
    generation: int
    queues: list[QueueTopologyEntry] = field(default_factory=list)
    streams: list[StreamTopologyEntry] = field(default_factory=list)


@dataclass
class TopologyUpdateAck:
    """Client ack of a pushed topology update at a generation.

    Lets the broker fence a repartition cutover until clients have applied the
    new routing.
    """

    generation: int


@dataclass
class Redirect:
    topic: str
    partition: int
    group: Optional[str]
    #: Owner endpoints in priority order; retry against the first reachable.
    owner_endpoints: list[AdvertisedAddress]
    partitioning_version: int


def encode_topology_request_body(req: TopologyRequest) -> bytes:
    w = Writer()
    w.magic("FTP1")
    w.optional_str(req.topic)
    w.optional_str(req.group)
    return w.finish()


def decode_topology_request_body(body: bytes) -> TopologyRequest:
    r = Reader(body)
    r.expect_magic("FTP1")
    value = TopologyRequest(topic=r.optional_str(), group=r.optional_str())
    r.finish()
    return value


def encode_topology_ok_body(topology: TopologyOk) -> bytes:
    w = Writer()
    w.magic("FTO1")
    w.u64(topology.generation)
    w.u32(len(topology.queues))
    for e in topology.queues:
        w.queue_key(e.topic, e.partition, e.group)
        write_advertised_addresses(w, e.owner_endpoints)
        w.u64(e.partitioning_version)
        w.u32(e.partition_count)
    w.u32(len(topology.streams))
    for s in topology.streams:
        w.write_str(s.topic)
        w.u32(s.partition)
        write_advertised_addresses(w, s.owner_endpoints)
        w.u64(s.partitioning_version)
        w.u32(s.partition_count)
    return w.finish()


def decode_topology_ok_body(body: bytes) -> TopologyOk:
    r = Reader(body)
    r.expect_magic("FTO1")
    generation = r.u64()
    n = r.u32()
    queues: list[QueueTopologyEntry] = []
    for _ in range(n):
        topic, partition, group = r.queue_key()
        queues.append(
            QueueTopologyEntry(
                topic=topic,
                partition=partition,
                group=group,
                owner_endpoints=read_advertised_addresses(r),
                partitioning_version=r.u64(),
                partition_count=r.u32(),
            )
        )
    stream_count = r.u32()
    streams: list[StreamTopologyEntry] = []
    for _ in range(stream_count):
        streams.append(
            StreamTopologyEntry(
                topic=r.read_str(),
                partition=r.u32(),
                owner_endpoints=read_advertised_addresses(r),
                partitioning_version=r.u64(),
                partition_count=r.u32(),
            )
        )
    r.finish()
    return TopologyOk(generation=generation, queues=queues, streams=streams)


def encode_topology_update_body(topology: TopologyOk) -> bytes:
    """Unsolicited broker->client topology push.

    Same body as TopologyOk under a distinct magic so a push is distinguishable
    from a request reply.
    """
    w = Writer()
    w.magic("FTU1")
    w.u64(topology.generation)
    w.u32(len(topology.queues))
    for e in topology.queues:
        w.queue_key(e.topic, e.partition, e.group)
        write_advertised_addresses(w, e.owner_endpoints)
        w.u64(e.partitioning_version)
        w.u32(e.partition_count)
    w.u32(len(topology.streams))
    for s in topology.streams:
        w.write_str(s.topic)
        w.u32(s.partition)
        write_advertised_addresses(w, s.owner_endpoints)
        w.u64(s.partitioning_version)
        w.u32(s.partition_count)
    return w.finish()


def decode_topology_update_body(body: bytes) -> TopologyOk:
    r = Reader(body)
    r.expect_magic("FTU1")
    generation = r.u64()
    n = r.u32()
    queues: list[QueueTopologyEntry] = []
    for _ in range(n):
        topic, partition, group = r.queue_key()
        queues.append(
            QueueTopologyEntry(
                topic=topic,
                partition=partition,
                group=group,
                owner_endpoints=read_advertised_addresses(r),
                partitioning_version=r.u64(),
                partition_count=r.u32(),
            )
        )
    stream_count = r.u32()
    streams: list[StreamTopologyEntry] = []
    for _ in range(stream_count):
        streams.append(
            StreamTopologyEntry(
                topic=r.read_str(),
                partition=r.u32(),
                owner_endpoints=read_advertised_addresses(r),
                partitioning_version=r.u64(),
                partition_count=r.u32(),
            )
        )
    r.finish()
    return TopologyOk(generation=generation, queues=queues, streams=streams)


def encode_topology_update_ack_body(ack: TopologyUpdateAck) -> bytes:
    w = Writer()
    w.magic("FTA1")
    w.u64(ack.generation)
    return w.finish()


def decode_topology_update_ack_body(body: bytes) -> TopologyUpdateAck:
    r = Reader(body)
    r.expect_magic("FTA1")
    generation = r.u64()
    r.finish()
    return TopologyUpdateAck(generation=generation)


def encode_redirect_body(redirect: Redirect) -> bytes:
    w = Writer()
    w.magic("FRD1")
    w.queue_key(redirect.topic, redirect.partition, redirect.group)
    write_advertised_addresses(w, redirect.owner_endpoints)
    w.u64(redirect.partitioning_version)
    return w.finish()


def decode_redirect_body(body: bytes) -> Redirect:
    r = Reader(body)
    r.expect_magic("FRD1")
    topic, partition, group = r.queue_key()
    value = Redirect(
        topic=topic,
        partition=partition,
        group=group,
        owner_endpoints=read_advertised_addresses(r),
        partitioning_version=r.u64(),
    )
    r.finish()
    return value
