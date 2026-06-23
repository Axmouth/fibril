"""Wire codec tests.

The encoders are cross-checked byte-for-byte against the shared cross-client
wire vectors (``clients/wire_vectors.json``). The Rust protocol crate pins its
own encoders to the same file (``crates/protocol/tests/wire_vectors.rs``), so
all three implementations agree on the bytes. Every op is also round-tripped
(encode then decode) and the FNV-1a partition hash is pinned to canonical
vectors.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fibril import wire
from fibril.errors import WireError

# The shared cross-client fixture lives at clients/wire_vectors.json (two levels
# up from this tests directory).
_VECTORS = json.loads(
    (Path(__file__).parents[2] / "wire_vectors.json").read_text(encoding="utf-8")
)


def _u(byte: int) -> bytes:
    """A deterministic 16-byte uuid filled with ``byte`` (matches the generator)."""
    return bytes([byte]) * 16


# Each case: name -> (encoded message, decode function). Each message mirrors the
# matching entry in clients/wire_vectors.json exactly so the bytes line up.
def _cases() -> dict[str, tuple[bytes, object]]:
    return {
        "hello": (
            wire.encode_hello_body(
                wire.Hello(
                    client_name="py-client",
                    client_version="0.1.0",
                    protocol_version=1,
                    resume=wire.ResumeIdentity(_u(1), _u(2), _u(3)),
                )
            ),
            wire.decode_hello_body,
        ),
        "hello_no_resume": (
            wire.encode_hello_body(
                wire.Hello("c", "v", 1, resume=None)
            ),
            wire.decode_hello_body,
        ),
        "hello_ok": (
            wire.encode_hello_ok_body(
                wire.HelloOk(1, _u(9), _u(8), _u(7), "resumed", "srv", "v=1;x")
            ),
            wire.decode_hello_ok_body,
        ),
        "auth": (
            wire.encode_auth_body(wire.Auth("u", "p")),
            wire.decode_auth_body,
        ),
        "error": (
            wire.encode_error_body(wire.ErrorMsg(409, "not owner")),
            wire.decode_error_body,
        ),
        "publish": (
            wire.encode_publish_body(
                wire.Publish(
                    topic="orders",
                    partition=3,
                    group="g",
                    require_confirm=True,
                    content_type="json",
                    headers={"x-a": "1"},
                    payload=bytes([1, 2, 3, 4]),
                    published=1234567890,
                    partition_key=bytes([9, 9]),
                    partitioning_version=5,
                    ttl_ms=60000,
                )
            ),
            wire.decode_publish_body,
        ),
        "publish_no_ttl": (
            wire.encode_publish_body(
                wire.Publish(
                    topic="t",
                    partition=0,
                    group=None,
                    require_confirm=False,
                    content_type=None,
                    headers={},
                    payload=b"",
                    published=0,
                    partition_key=None,
                    partitioning_version=0,
                    ttl_ms=None,
                )
            ),
            wire.decode_publish_body,
        ),
        "publish_custom_ct": (
            wire.encode_publish_body(
                wire.Publish(
                    topic="t",
                    partition=0,
                    group=None,
                    require_confirm=False,
                    content_type=wire.CustomContentType("application/x-thing"),
                    headers={},
                    payload=bytes([7]),
                    published=1,
                    partition_key=None,
                    partitioning_version=0,
                    ttl_ms=None,
                )
            ),
            wire.decode_publish_body,
        ),
        "publish_delayed": (
            wire.encode_publish_delayed_body(
                wire.PublishDelayed(
                    topic="t",
                    partition=1,
                    group=None,
                    require_confirm=True,
                    not_before=999,
                    content_type="text",
                    headers={"k": "v"},
                    payload=bytes([5, 6]),
                    published=42,
                    partition_key=None,
                    partitioning_version=2,
                )
            ),
            wire.decode_publish_delayed_body,
        ),
        "publish_ok": (
            wire.encode_publish_ok_body(wire.PublishOk(777)),
            wire.decode_publish_ok_body,
        ),
        "deliver": (
            wire.encode_deliver_body(
                wire.Deliver(
                    sub_id=11,
                    topic="t",
                    group="g",
                    partition=2,
                    offset=100,
                    delivery_tag=wire.DeliveryTag(5),
                    published=7,
                    publish_received=8,
                    content_type="msgpack",
                    headers={"h": "1"},
                    payload=bytes([3, 2, 1]),
                )
            ),
            wire.decode_deliver_body,
        ),
        "ack": (
            wire.encode_ack_body(
                wire.Ack("t", None, 0, [wire.DeliveryTag(1), wire.DeliveryTag(2)])
            ),
            wire.decode_ack_body,
        ),
        "nack": (
            wire.encode_nack_body(
                wire.Nack("t", "g", 1, [wire.DeliveryTag(9)], requeue=True, not_before=5000)
            ),
            wire.decode_nack_body,
        ),
        "nack_no_nb": (
            wire.encode_nack_body(
                wire.Nack("t", None, 0, [], requeue=False, not_before=None)
            ),
            wire.decode_nack_body,
        ),
        "declare": (
            wire.encode_declare_queue_body(
                wire.DeclareQueue(
                    topic="t",
                    group="g",
                    dlq_policy=wire.CustomDlqPolicy("dlq", None),
                    dlq_max_retries=3,
                    partition_count=4,
                    default_message_ttl_ms=30000,
                )
            ),
            wire.decode_declare_queue_body,
        ),
        "declare_min": (
            wire.encode_declare_queue_body(
                wire.DeclareQueue("t", None, None, None, None, None)
            ),
            wire.decode_declare_queue_body,
        ),
        "declare_ok": (
            wire.encode_declare_queue_ok_body(wire.DeclareQueueOk("created", 4)),
            wire.decode_declare_queue_ok_body,
        ),
        "assignment": (
            wire.encode_assignment_changed_body(
                wire.AssignmentChanged("t", None, "cg", 6, [0, 1, 2], [2], [])
            ),
            wire.decode_assignment_changed_body,
        ),
        "subscribe": (
            wire.encode_subscribe_body(
                wire.Subscribe(
                    topic="t",
                    partition=1,
                    group="g",
                    prefetch=32,
                    auto_ack=False,
                    consumer_group="cg",
                    consumer_target=2,
                    member_id=_u(4),
                )
            ),
            wire.decode_subscribe_body,
        ),
        "subscribe_min": (
            wire.encode_subscribe_body(
                wire.Subscribe("t", 0, None, 0, auto_ack=True)
            ),
            wire.decode_subscribe_body,
        ),
        "subscribe_ok": (
            wire.encode_subscribe_ok_body(
                wire.SubscribeOk(
                    sub_id=5,
                    topic="t",
                    partition=1,
                    group="g",
                    prefetch=16,
                    consumer_group="cg",
                    consumer_target=None,
                    member_id=_u(4),
                )
            ),
            wire.decode_subscribe_ok_body,
        ),
        "topology_req": (
            wire.encode_topology_request_body(wire.TopologyRequest("t", None)),
            wire.decode_topology_request_body,
        ),
        "topology_ok": (
            wire.encode_topology_ok_body(
                wire.TopologyOk(
                    generation=12,
                    queues=[
                        wire.QueueTopologyEntry("t", 0, None, "127.0.0.1:7000", 1, 2),
                        wire.QueueTopologyEntry("t", 1, None, None, 1, 2),
                    ],
                    streams=[wire.StreamTopologyEntry("s", 3, 4)],
                )
            ),
            wire.decode_topology_ok_body,
        ),
        "redirect": (
            wire.encode_redirect_body(wire.Redirect("t", 1, "g", "h:1", 3)),
            wire.decode_redirect_body,
        ),
        "reconcile_client": (
            wire.encode_reconcile_client_body(
                wire.ReconcileClient(
                    policy="restore_client_subscriptions",
                    subscriptions=[
                        wire.ReconcileSubscription(
                            sub_id=1,
                            topic="t",
                            partition=0,
                            group=None,
                            auto_ack=False,
                            prefetch=8,
                        )
                    ],
                )
            ),
            wire.decode_reconcile_client_body,
        ),
        "declare_plexus": (
            wire.encode_declare_plexus_body(
                wire.DeclarePlexus(
                    topic="t",
                    partition_count=4,
                    durability="speculative",
                    retention=wire.StreamRetention(
                        max_age_ms=60000, max_bytes=None, max_records=1_000_000
                    ),
                )
            ),
            wire.decode_declare_plexus_body,
        ),
        "declare_plexus_min": (
            wire.encode_declare_plexus_body(wire.DeclarePlexus(topic="t")),
            wire.decode_declare_plexus_body,
        ),
        "declare_plexus_ok": (
            wire.encode_declare_plexus_ok_body(
                wire.DeclarePlexusOk(status="created", partition_count=4)
            ),
            wire.decode_declare_plexus_ok_body,
        ),
        "subscribe_stream": (
            wire.encode_subscribe_stream_body(
                wire.SubscribeStream(
                    topic="t",
                    partition=1,
                    durable_name="c1",
                    start=wire.StreamStart(kind="bytime", value=1234),
                    filter=[("region", "eu-*"), ("kind", "order")],
                    prefetch=16,
                    auto_ack=False,
                )
            ),
            wire.decode_subscribe_stream_body,
        ),
        "subscribe_stream_min": (
            wire.encode_subscribe_stream_body(
                wire.SubscribeStream(
                    topic="t",
                    partition=0,
                    durable_name=None,
                    start=wire.StreamStart(kind="latest"),
                    filter=[],
                    prefetch=0,
                    auto_ack=True,
                )
            ),
            wire.decode_subscribe_stream_body,
        ),
    }


@pytest.mark.parametrize("name", sorted(_VECTORS.keys()))
def test_encode_matches_shared_vectors(name: str) -> None:
    encoded, _ = _cases()[name]
    assert encoded.hex() == _VECTORS[name], f"{name} bytes diverge from shared vectors"


@pytest.mark.parametrize("name", sorted(_cases().keys()))
def test_round_trips(name: str) -> None:
    encoded, decode = _cases()[name]
    decoded = decode(encoded)  # type: ignore[operator]
    # Re-encoding the decoded value must reproduce identical bytes.
    reencoded = _reencode(name, decoded)
    assert reencoded == encoded


_ENCODERS = {
    "hello": wire.encode_hello_body,
    "hello_no_resume": wire.encode_hello_body,
    "hello_ok": wire.encode_hello_ok_body,
    "auth": wire.encode_auth_body,
    "error": wire.encode_error_body,
    "publish": wire.encode_publish_body,
    "publish_no_ttl": wire.encode_publish_body,
    "publish_custom_ct": wire.encode_publish_body,
    "publish_delayed": wire.encode_publish_delayed_body,
    "publish_ok": wire.encode_publish_ok_body,
    "deliver": wire.encode_deliver_body,
    "ack": wire.encode_ack_body,
    "nack": wire.encode_nack_body,
    "nack_no_nb": wire.encode_nack_body,
    "declare": wire.encode_declare_queue_body,
    "declare_min": wire.encode_declare_queue_body,
    "declare_ok": wire.encode_declare_queue_ok_body,
    "assignment": wire.encode_assignment_changed_body,
    "subscribe": wire.encode_subscribe_body,
    "subscribe_min": wire.encode_subscribe_body,
    "subscribe_ok": wire.encode_subscribe_ok_body,
    "topology_req": wire.encode_topology_request_body,
    "topology_ok": wire.encode_topology_ok_body,
    "redirect": wire.encode_redirect_body,
    "reconcile_client": wire.encode_reconcile_client_body,
    "declare_plexus": wire.encode_declare_plexus_body,
    "declare_plexus_min": wire.encode_declare_plexus_body,
    "declare_plexus_ok": wire.encode_declare_plexus_ok_body,
    "subscribe_stream": wire.encode_subscribe_stream_body,
    "subscribe_stream_min": wire.encode_subscribe_stream_body,
}


def _reencode(name: str, value: object) -> bytes:
    return _ENCODERS[name](value)  # type: ignore[no-any-return,operator]


# ---- FNV-1a partition hash (canonical vectors from the TS/Rust hash) ----

_FNV_VECTORS = {
    "": 14695981039346656037,
    "a": 12638187200555641996,
    "order-42": 9015620992513762004,
    "partition-key": 11792757095719117019,
    "hello world": 8618312879776256743,
}


@pytest.mark.parametrize("key,expected", list(_FNV_VECTORS.items()))
def test_fnv1a_canonical(key: str, expected: int) -> None:
    assert wire.fnv1a(key.encode("utf-8")) == expected


# ---- typed wire error surface ------------------------------------------


def test_bad_magic_raises_typed() -> None:
    with pytest.raises(WireError) as exc:
        wire.decode_publish_ok_body(b"XXXX\x00\x00\x00\x00\x00\x00\x00\x00")
    assert exc.value.kind == "invalid_magic"


def test_trailing_bytes_raises_typed() -> None:
    body = wire.encode_publish_ok_body(wire.PublishOk(1)) + b"\x00"
    with pytest.raises(WireError) as exc:
        wire.decode_publish_ok_body(body)
    assert exc.value.kind == "trailing_bytes"


def test_truncated_raises_typed() -> None:
    with pytest.raises(WireError) as exc:
        wire.decode_publish_ok_body(b"FPO1\x00")
    assert exc.value.kind == "unexpected_eof"


def test_unknown_content_type_raises_typed() -> None:
    # Build a publish body with an out-of-range content-type tag (99).
    w = wire.Writer()
    w.magic("FPB1")
    w.write_str("t")
    w.optional_str(None)
    w.u32(0)
    w.write_bool(False)
    w.u8(99)  # invalid content type tag
    with pytest.raises(WireError) as exc:
        wire.decode_publish_body(w.finish())
    assert exc.value.kind == "unknown_content_type"


def test_uuid_must_be_16_bytes() -> None:
    with pytest.raises(WireError) as exc:
        wire.encode_hello_ok_body(
            wire.HelloOk(1, b"short", _u(1), _u(2), "new", "s", "c")
        )
    assert exc.value.kind == "invalid_uuid"
