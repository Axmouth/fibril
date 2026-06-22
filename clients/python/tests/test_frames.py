"""Opcode dispatch tests for frames.py."""

from __future__ import annotations

import pytest

from fibril import wire
from fibril.frames import decode_body, encode_body
from fibril.protocol import Op


def test_publish_ok_round_trips_through_dispatch() -> None:
    body = encode_body(Op.PUBLISH_OK, wire.PublishOk(123))
    decoded = decode_body(Op.PUBLISH_OK, body)
    assert isinstance(decoded, wire.PublishOk)
    assert decoded.offset == 123


def test_deliver_round_trips_through_dispatch() -> None:
    msg = wire.Deliver(
        sub_id=1,
        topic="t",
        group=None,
        partition=0,
        offset=9,
        delivery_tag=wire.DeliveryTag(3),
        published=1,
        publish_received=2,
        content_type="json",
        headers={"a": "b"},
        payload=b"hi",
    )
    decoded = decode_body(Op.DELIVER, encode_body(Op.DELIVER, msg))
    assert decoded == msg


def test_unit_ops_have_empty_body() -> None:
    for op in (Op.PING, Op.PONG, Op.AUTH_OK):
        assert encode_body(op, None) == b""
        assert decode_body(op, b"") is None


def test_error_ops_share_the_error_body() -> None:
    err = wire.ErrorMsg(409, "not owner")
    for op in (Op.ERROR, Op.HELLO_ERR, Op.AUTH_ERR, Op.SUBSCRIBE_ERR):
        decoded = decode_body(op, encode_body(op, err))
        assert decoded == err


def test_unknown_encoder_raises() -> None:
    with pytest.raises(ValueError):
        encode_body(Op.PUBLISH_DELAYED + 1234, wire.PublishOk(1))  # type: ignore[arg-type]


def test_unknown_decoder_raises() -> None:
    with pytest.raises(ValueError):
        decode_body(Op.PUBLISH_DELAYED + 1234, b"")  # type: ignore[arg-type]
