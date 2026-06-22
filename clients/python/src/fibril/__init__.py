"""Fibril Python client.

Async-first client for the Fibril message broker with a thin blocking facade.
The async surface is re-exported here; the blocking facade lives in
``fibril.blocking``.
"""

from __future__ import annotations

from .client import Client, ClientOptions, QueueConfig, ReconnectOutcome
from .errors import (
    BrokenPipeError,
    DeserializationError,
    DisconnectionError,
    EofError,
    FibrilError,
    RedirectError,
    SerializationError,
    ServerError,
    UnexpectedError,
    WireError,
    is_retryable,
    is_transient_error,
    retry_advice,
)
from .message import NewMessage
from .protocol import COMPLIANCE_STRING, PROTOCOL_V1, Op
from .publisher import Publisher, PublishConfirmation, ReliablePublisher
from .subscription import (
    AutoAckedSubscription,
    InflightMessage,
    Message,
    Subscription,
    SubscriptionBuilder,
)

__all__ = [
    "Client",
    "ClientOptions",
    "QueueConfig",
    "ReconnectOutcome",
    "Publisher",
    "PublishConfirmation",
    "ReliablePublisher",
    "Subscription",
    "AutoAckedSubscription",
    "SubscriptionBuilder",
    "Message",
    "InflightMessage",
    "NewMessage",
    "FibrilError",
    "WireError",
    "ServerError",
    "RedirectError",
    "DisconnectionError",
    "BrokenPipeError",
    "EofError",
    "SerializationError",
    "DeserializationError",
    "UnexpectedError",
    "is_retryable",
    "is_transient_error",
    "retry_advice",
    "COMPLIANCE_STRING",
    "PROTOCOL_V1",
    "Op",
]

__version__ = "0.1.0"
