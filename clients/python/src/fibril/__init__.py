"""Fibril Python client.

Async-first client for the Fibril message broker with a thin blocking facade.
The public surface (``Client``, ``Publisher``, ``Subscription``, ``Message``) is
re-exported here as the higher layers land; for now this exposes the protocol
constants so the package imports cleanly.
"""

from __future__ import annotations

from .protocol import COMPLIANCE_STRING, PROTOCOL_V1, Op

__all__ = ["COMPLIANCE_STRING", "PROTOCOL_V1", "Op"]

__version__ = "0.1.0"
