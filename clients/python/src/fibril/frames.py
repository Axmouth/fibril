"""Opcode dispatch between frame bodies and the wire codec.

``codec.py`` carries frame headers. This maps each opcode to the matching
``wire.py`` encode/decode function, in both directions, so the in-process fake
broker used by the tests speaks the same bytes as the real broker. The wire
dataclasses double as the engine's message structs, so there is no field
remapping here, only dispatch.
"""

from __future__ import annotations

from typing import Callable

from . import wire
from .protocol import Op

# Encoders take a concrete wire dataclass. The dispatch table erases that to a
# common signature, so callers are responsible for passing the right body type.
_Encoder = Callable[..., bytes]
_Decoder = Callable[[bytes], object]

# Ops whose body is empty (the magic-less unit frames).
_UNIT_OPS = frozenset({Op.PING, Op.PONG, Op.AUTH_OK})

# Error-shaped ops all share the FER1 body.
_ERROR_OPS = frozenset({Op.ERROR, Op.HELLO_ERR, Op.AUTH_ERR, Op.SUBSCRIBE_ERR})

_ENCODERS: dict[Op, _Encoder] = {
    Op.HELLO: wire.encode_hello_body,
    Op.HELLO_OK: wire.encode_hello_ok_body,
    Op.AUTH: wire.encode_auth_body,
    Op.PUBLISH: wire.encode_publish_body,
    Op.PUBLISH_DELAYED: wire.encode_publish_delayed_body,
    Op.PUBLISH_OK: wire.encode_publish_ok_body,
    Op.SUBSCRIBE: wire.encode_subscribe_body,
    Op.SUBSCRIBE_OK: wire.encode_subscribe_ok_body,
    Op.DELIVER: wire.encode_deliver_body,
    Op.ACK: wire.encode_ack_body,
    Op.NACK: wire.encode_nack_body,
    Op.ASSIGNMENT_CHANGED: wire.encode_assignment_changed_body,
    Op.DECLARE_QUEUE: wire.encode_declare_queue_body,
    Op.DECLARE_QUEUE_OK: wire.encode_declare_queue_ok_body,
    Op.DECLARE_PLEXUS: wire.encode_declare_plexus_body,
    Op.DECLARE_PLEXUS_OK: wire.encode_declare_plexus_ok_body,
    Op.SUBSCRIBE_STREAM: wire.encode_subscribe_stream_body,
    Op.RECONCILE_CLIENT: wire.encode_reconcile_client_body,
    Op.RECONCILE_SERVER: wire.encode_reconcile_server_body,
    Op.RECONCILE_RESULT: wire.encode_reconcile_result_body,
    Op.TOPOLOGY: wire.encode_topology_request_body,
    Op.TOPOLOGY_OK: wire.encode_topology_ok_body,
    Op.REDIRECT: wire.encode_redirect_body,
}

_DECODERS: dict[Op, _Decoder] = {
    Op.HELLO: wire.decode_hello_body,
    Op.HELLO_OK: wire.decode_hello_ok_body,
    Op.AUTH: wire.decode_auth_body,
    Op.PUBLISH: wire.decode_publish_body,
    Op.PUBLISH_DELAYED: wire.decode_publish_delayed_body,
    Op.PUBLISH_OK: wire.decode_publish_ok_body,
    Op.SUBSCRIBE: wire.decode_subscribe_body,
    Op.SUBSCRIBE_OK: wire.decode_subscribe_ok_body,
    Op.DELIVER: wire.decode_deliver_body,
    Op.ACK: wire.decode_ack_body,
    Op.NACK: wire.decode_nack_body,
    Op.ASSIGNMENT_CHANGED: wire.decode_assignment_changed_body,
    Op.DECLARE_QUEUE: wire.decode_declare_queue_body,
    Op.DECLARE_QUEUE_OK: wire.decode_declare_queue_ok_body,
    Op.DECLARE_PLEXUS: wire.decode_declare_plexus_body,
    Op.DECLARE_PLEXUS_OK: wire.decode_declare_plexus_ok_body,
    Op.SUBSCRIBE_STREAM: wire.decode_subscribe_stream_body,
    Op.RECONCILE_CLIENT: wire.decode_reconcile_client_body,
    Op.RECONCILE_SERVER: wire.decode_reconcile_server_body,
    Op.RECONCILE_RESULT: wire.decode_reconcile_result_body,
    Op.TOPOLOGY: wire.decode_topology_request_body,
    Op.TOPOLOGY_OK: wire.decode_topology_ok_body,
    Op.REDIRECT: wire.decode_redirect_body,
}


def encode_body(op: Op, value: object) -> bytes:
    """Encode a frame body for ``op`` from its wire dataclass."""
    if op in _UNIT_OPS:
        return b""
    if op in _ERROR_OPS:
        return wire.encode_error_body(value)  # type: ignore[arg-type]
    encoder = _ENCODERS.get(op)
    if encoder is None:
        raise ValueError(f"frames: no encoder for opcode {int(op)}")
    return encoder(value)


def decode_body(op: Op, payload: bytes) -> object:
    """Decode a frame body for ``op`` into its wire dataclass."""
    if op in _UNIT_OPS:
        return None
    if op in _ERROR_OPS:
        return wire.decode_error_body(payload)
    decoder = _DECODERS.get(op)
    if decoder is None:
        raise ValueError(f"frames: no decoder for opcode {int(op)}")
    return decoder(payload)
