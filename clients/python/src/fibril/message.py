"""Message construction and payload (de)serialization.

Plain values published through a Publisher are msgpack-encoded by default. Use
``NewMessage`` for JSON, text, raw bytes, custom headers, or a partition key.
Wire bodies are hand-rolled (see ``wire.py``). msgpack here is only the payload
content type.
"""

from __future__ import annotations

from typing import Any, Optional, Union

from .errors import DeserializationError, FibrilError, SerializationError
from .wire import ContentType, CustomContentType


def _load_msgpack() -> Any:
    """Import msgpack on demand. msgpack is an optional dependency - only the
    msgpack payload path needs it, so the client imports and runs without it and
    only this path fails (with a clear message) when it is missing."""
    try:
        import msgpack  # noqa: PLC0415 (lazy: optional dependency)

        return msgpack
    except ImportError as err:
        raise FibrilError(
            "msgpack is not installed; install it (pip install 'fibril[msgpack]' "
            "or pip install msgpack) to use msgpack payloads, or publish/consume "
            "raw bytes, text, or JSON instead"
        ) from err

_MSGPACK = "application/msgpack"
_JSON = "application/json"
_TEXT = "text/plain; charset=utf-8"

# Header key prefixes reserved for system metadata. A publish that sets a
# reserved key is rejected by the broker, so the client guards user code. The
# library-owned carve-out below is the one exception.
RESERVED_HEADER_PREFIXES = ("fibril.", "stroma.")
CLIENT_HEADER_PREFIX = "fibril.client."
HEADER_PRODUCER_ID = "fibril.client.producer_id"
HEADER_PRODUCER_SEQ = "fibril.client.producer_seq"


def is_reserved_header_key(key: str) -> bool:
    return any(key.startswith(p) for p in RESERVED_HEADER_PREFIXES)


def content_type_from_header(value: str) -> ContentType:
    normalized = value.split(";")[0].strip()
    if normalized == _MSGPACK:
        return "msgpack"
    if normalized == _JSON:
        return "json"
    if normalized == "text/plain" and value == _TEXT:
        return "text"
    return CustomContentType(value)


def content_type_header(content_type: ContentType) -> Optional[str]:
    if content_type is None:
        return None
    if content_type == "msgpack":
        return _MSGPACK
    if content_type == "json":
        return _JSON
    if content_type == "text":
        return _TEXT
    if isinstance(content_type, CustomContentType):
        return content_type.value
    return None


class NewMessage:
    """An explicit publish message: payload bytes, content type, headers, key."""

    __slots__ = ("payload", "content_type_value", "headers", "partition_key_value")

    def __init__(
        self,
        payload: bytes,
        content_type_value: ContentType,
        headers: dict[str, str],
        partition_key_value: Optional[bytes] = None,
    ) -> None:
        self.payload = payload
        self.content_type_value = content_type_value
        self.headers = dict(headers)
        self.partition_key_value = partition_key_value

    @staticmethod
    def msgpack(payload: Any) -> "NewMessage":
        mp = _load_msgpack()
        try:
            data = mp.packb(payload, use_bin_type=True)
        except Exception as err:
            raise SerializationError(f"failed to serialize payload: {err}") from err
        return NewMessage(data, "msgpack", {})

    @staticmethod
    def json(payload: Any) -> "NewMessage":
        import json as _json

        try:
            data = _json.dumps(payload).encode("utf-8")
        except Exception as err:
            raise SerializationError(f"failed to serialize payload: {err}") from err
        return NewMessage(data, "json", {})

    @staticmethod
    def raw(payload: bytes) -> "NewMessage":
        return NewMessage(payload, None, {})

    @staticmethod
    def content(payload: str) -> "NewMessage":
        return NewMessage(payload.encode("utf-8"), "text", {})

    def header(self, key: str, value: str) -> "NewMessage":
        if key.lower() == "content-type":
            return NewMessage(
                self.payload, content_type_from_header(value), self.headers, self.partition_key_value
            )
        if is_reserved_header_key(key):
            raise FibrilError(
                f'header key "{key}" is in a reserved namespace (fibril.* / stroma.*)'
            )
        merged = {**self.headers, key: value}
        return NewMessage(self.payload, self.content_type_value, merged, self.partition_key_value)

    def system_header(self, key: str, value: str) -> "NewMessage":
        """Set a library-owned ``fibril.client.*`` header, bypassing the guard.

        Only the client library itself calls this (e.g. ReliablePublisher ids).
        """
        merged = {**self.headers, key: value}
        return NewMessage(self.payload, self.content_type_value, merged, self.partition_key_value)

    def partition_key(self, key: Union[str, bytes]) -> "NewMessage":
        data = key.encode("utf-8") if isinstance(key, str) else key
        return NewMessage(self.payload, self.content_type_value, self.headers, data)

    def content_type(self, content_type: str) -> "NewMessage":
        return self.header("content-type", content_type)

    def content_type_header(self) -> Optional[str]:
        return content_type_header(self.content_type_value)


Publishable = Union[NewMessage, Any]


def into_message(payload: Publishable) -> NewMessage:
    if isinstance(payload, NewMessage):
        return payload
    return NewMessage.msgpack(payload)


def deserialize_by_content_type(content_type: Optional[str], payload: bytes) -> Any:
    """Decode payload bytes per ``content-type``. Missing/empty defaults to msgpack."""
    normalized = content_type.split(";")[0].strip() if content_type else None
    if not normalized or normalized == _MSGPACK:
        mp = _load_msgpack()
        try:
            return mp.unpackb(payload, raw=False)
        except Exception as err:
            raise DeserializationError(f"failed to deserialize payload: {err}") from err
    if normalized == _JSON:
        import json as _json

        try:
            return _json.loads(payload.decode("utf-8"))
        except Exception as err:
            raise DeserializationError(f"failed to deserialize payload: {err}") from err
    raise DeserializationError(f"unsupported content-type `{normalized}`")
