"""Frame header and TCP stream framing.

The fixed 20-byte frame header wraps every body encoded by ``wire.py``:

  u32 payload_len
  u16 version
  u16 opcode
  u32 flags
  u64 request_id
  bytes payload[payload_len]

The header is byte-exact with the Rust ``crates/protocol/src/v1/frame.rs`` and the
TypeScript ``codec.ts``. On asyncio the stream reader uses ``readexactly`` rather
than a hand-rolled accumulator.
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass
from typing import Optional

from .protocol import PROTOCOL_V1, Op

HEADER_SIZE = 20

_HEADER = struct.Struct(">IHHIQ")


@dataclass
class Frame:
    version: int
    opcode: int
    flags: int
    request_id: int
    payload: bytes


def build_frame(op: Op, request_id: int, payload: bytes) -> Frame:
    """Wrap an already-encoded body in a frame for ``op``."""
    return Frame(
        version=PROTOCOL_V1,
        opcode=int(op),
        flags=0,
        request_id=request_id,
        payload=payload,
    )


def encode_frame(frame: Frame) -> bytes:
    """Serialize a frame to its on-wire byte representation."""
    return (
        _HEADER.pack(
            len(frame.payload),
            frame.version,
            frame.opcode,
            frame.flags,
            frame.request_id,
        )
        + frame.payload
    )


def try_decode_frame(buf: bytes) -> Optional[tuple[Frame, int]]:
    """Decode one frame from the head of ``buf``.

    Returns the frame and the number of bytes consumed, or ``None`` when the
    buffer does not yet hold a full frame.
    """
    if len(buf) < HEADER_SIZE:
        return None
    payload_len, version, opcode, flags, request_id = _HEADER.unpack_from(buf, 0)
    total = HEADER_SIZE + payload_len
    if len(buf) < total:
        return None
    payload = bytes(buf[HEADER_SIZE:total])
    return (
        Frame(
            version=version,
            opcode=opcode,
            flags=flags,
            request_id=request_id,
            payload=payload,
        ),
        total,
    )


async def read_frame(reader: asyncio.StreamReader) -> Optional[Frame]:
    """Read one whole frame from a stream, or ``None`` on a clean EOF.

    A partial frame at EOF (a header or body cut short) raises
    ``asyncio.IncompleteReadError``, which the caller treats as a transport
    failure.
    """
    try:
        head = await reader.readexactly(HEADER_SIZE)
    except asyncio.IncompleteReadError as err:
        if not err.partial:
            return None  # clean EOF on a frame boundary
        raise
    payload_len, version, opcode, flags, request_id = _HEADER.unpack(head)
    payload = await reader.readexactly(payload_len) if payload_len else b""
    return Frame(
        version=version,
        opcode=opcode,
        flags=flags,
        request_id=request_id,
        payload=bytes(payload),
    )
