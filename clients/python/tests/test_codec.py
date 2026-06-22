"""Frame header and stream framing tests."""

from __future__ import annotations

import asyncio

from fibril import wire
from fibril.codec import (
    HEADER_SIZE,
    build_frame,
    encode_frame,
    read_frame,
    try_decode_frame,
)
from fibril.protocol import Op


def _publish_frame(request_id: int = 7) -> bytes:
    body = wire.encode_publish_ok_body(wire.PublishOk(42))
    return encode_frame(build_frame(Op.PUBLISH_OK, request_id, body))


def test_header_is_20_bytes() -> None:
    frame = encode_frame(build_frame(Op.PING, 1, b""))
    assert len(frame) == HEADER_SIZE


def test_encode_then_try_decode_round_trips() -> None:
    raw = _publish_frame(request_id=99)
    result = try_decode_frame(raw)
    assert result is not None
    frame, consumed = result
    assert consumed == len(raw)
    assert frame.opcode == Op.PUBLISH_OK
    assert frame.request_id == 99
    assert wire.decode_publish_ok_body(frame.payload).offset == 42


def test_try_decode_returns_none_on_partial_header() -> None:
    raw = _publish_frame()
    assert try_decode_frame(raw[:5]) is None


def test_try_decode_returns_none_on_partial_body() -> None:
    raw = _publish_frame()
    assert try_decode_frame(raw[:-1]) is None


def test_try_decode_consumes_only_first_frame() -> None:
    raw = _publish_frame(1) + _publish_frame(2)
    result = try_decode_frame(raw)
    assert result is not None
    frame, consumed = result
    assert frame.request_id == 1
    assert consumed == len(raw) // 2


async def test_read_frame_streams_then_clean_eof() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(_publish_frame(1) + _publish_frame(2))
    reader.feed_eof()

    first = await read_frame(reader)
    second = await read_frame(reader)
    end = await read_frame(reader)

    assert first is not None and first.request_id == 1
    assert second is not None and second.request_id == 2
    assert end is None


async def test_read_frame_handles_split_chunks() -> None:
    reader = asyncio.StreamReader()
    raw = _publish_frame(5)
    # Deliver the frame in two arbitrary chunks; readexactly stitches them.
    reader.feed_data(raw[:9])
    reader.feed_data(raw[9:])
    reader.feed_eof()

    frame = await read_frame(reader)
    assert frame is not None and frame.request_id == 5
