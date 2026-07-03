"""Fibril Python client.

Async-first client for the Fibril message broker with a thin blocking facade.
The async surface is re-exported here. The blocking facade lives in
``fibril.blocking``.
"""

from __future__ import annotations

from .client import (
    Catalogue,
    Client,
    ClientOptions,
    QueueConfig,
    QueueInfo,
    ReconnectOutcome,
    StreamConfig,
    StreamInfo,
    TlsOptions,
)
from .errors import (
    ERR_TLS_REQUIRED,
    BrokenPipeError,
    DeserializationError,
    DisconnectionError,
    EofError,
    FibrilError,
    RedirectError,
    SerializationError,
    ServerError,
    TlsCertificateUntrustedError,
    TlsConfigError,
    TlsHandshakeError,
    TlsNotSupportedByBrokerError,
    TlsRequiredByBrokerError,
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
    StreamSubscriptionBuilder,
    Subscription,
    SubscriptionBuilder,
)
from .routing import (
    PatternMessage,
    PatternSource,
    PatternSubscribeBuilder,
    PatternSubscription,
    RoutingClient,
    StreamPatternSubscribeBuilder,
)

__all__ = [
    "Client",
    "ClientOptions",
    "QueueConfig",
    "StreamConfig",
    "Catalogue",
    "QueueInfo",
    "StreamInfo",
    "ReconnectOutcome",
    "Publisher",
    "PublishConfirmation",
    "ReliablePublisher",
    "Subscription",
    "AutoAckedSubscription",
    "SubscriptionBuilder",
    "StreamSubscriptionBuilder",
    "RoutingClient",
    "PatternSubscribeBuilder",
    "StreamPatternSubscribeBuilder",
    "PatternSubscription",
    "PatternSource",
    "PatternMessage",
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
    "TlsOptions",
    "ERR_TLS_REQUIRED",
    "TlsRequiredByBrokerError",
    "TlsNotSupportedByBrokerError",
    "TlsCertificateUntrustedError",
    "TlsConfigError",
    "TlsHandshakeError",
    "is_retryable",
    "is_transient_error",
    "retry_advice",
    "COMPLIANCE_STRING",
    "PROTOCOL_V1",
    "Op",
]

__version__ = "0.2.0"
