"""msgpack is an optional dependency: raw/text/json work without it, and only the
msgpack path errors (clearly) when it is missing."""

from __future__ import annotations

import sys

import pytest

from fibril import message
from fibril.errors import FibrilError


def test_raw_text_json_need_no_msgpack(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate msgpack not installed (a None entry makes `import msgpack` raise).
    monkeypatch.setitem(sys.modules, "msgpack", None)
    assert message.NewMessage.raw(b"x").payload == b"x"
    assert message.NewMessage.text("hi").payload == b"hi"
    assert message.NewMessage.json({"id": 1}).content_type_value == "json"
    assert message.deserialize_by_content_type("application/json", b'{"id":1}') == {"id": 1}


def test_msgpack_path_errors_clearly_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "msgpack", None)
    with pytest.raises(FibrilError, match="msgpack is not installed"):
        message.NewMessage.msgpack({"id": 1})
    with pytest.raises(FibrilError, match="msgpack is not installed"):
        message.deserialize_by_content_type("application/msgpack", b"\x80")
