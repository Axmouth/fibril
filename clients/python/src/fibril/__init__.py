"""Fibril Python client.

Async-first client for the Fibril message broker with a thin blocking facade.
The public surface (``Client``, ``Publisher``, ``Subscription``, ``Message``) is
re-exported from this package root.
"""

from __future__ import annotations

from .protocol import COMPLIANCE_STRING, PROTOCOL_V1, Op

__all__ = ["COMPLIANCE_STRING", "PROTOCOL_V1", "Op"]

__version__ = "0.1.0"
