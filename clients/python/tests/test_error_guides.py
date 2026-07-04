"""Client-local error guides stay in parity with clients/error_guides.json.

The shared JSON is the single source of truth for the wording all three clients
must carry for errors they raise themselves (connection refused, heartbeat
timeout), the error-message analogue of clients/wire_vectors.json.
"""

from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from fibril.client import ClientOptions

GUIDES = json.loads(
    (Path(__file__).resolve().parent.parent.parent / "error_guides.json").read_text()
)


def _closed_port() -> int:
    """An address with nothing listening: bind an ephemeral port, then free it."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def test_connection_refused_carries_the_shared_guide() -> None:
    port = _closed_port()
    with pytest.raises(Exception) as excinfo:
        await ClientOptions().disable_auto_reconnect().connect(f"127.0.0.1:{port}")
    message = str(excinfo.value).lower()
    for keyword in GUIDES["connection_refused"]["must_contain"]:
        assert keyword.lower() in message, f"missing {keyword}: {message}"
