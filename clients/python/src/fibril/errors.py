"""Typed error taxonomy for the Fibril client.

The Rust client's typed errors are the reference. Callers branch on the failure
kind rather than parsing messages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .wire import Redirect


class FibrilError(Exception):
    """Base class for all errors raised by the Fibril client."""


#: Discriminants for :class:`WireError`, matching the Rust typed wire errors.
WireErrorKind = Literal[
    "unexpected_eof",
    "invalid_magic",
    "trailing_bytes",
    "invalid_uuid",
    "unknown_content_type",
    "unknown_tag",
]


class WireError(FibrilError):
    """A frame body could not be decoded.

    ``kind`` is a stable discriminant (truncation, bad magic, trailing bytes, an
    unknown tag). The message is human-readable.
    """

    def __init__(self, kind: WireErrorKind, message: str) -> None:
        super().__init__(message)
        self.kind: WireErrorKind = kind


class DisconnectionError(FibrilError):
    """The client could not establish or maintain a connection to the server."""


class DeserializationError(FibrilError):
    """Failed to deserialize a payload (typically ``Message.deserialize``)."""


class SerializationError(FibrilError):
    """Failed to serialize a payload before publishing."""


class BrokenPipeError(FibrilError):
    """The internal pipe to the engine is broken.

    Typically means the engine has shut down or is shutting down. Reconnection
    is advised.
    """

    def __init__(self, message: str = "Broken pipe, engine has shut down") -> None:
        super().__init__(message)


class ServerError(FibrilError):
    """The server returned a structured error in response to a request."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"Server error {code}: {message}")
        self.code = code
        self.server_message = message


class RedirectError(FibrilError):
    """The broker told the client to retry this op against a different owner.

    Not a failure: it carries a routing target and must be retried on a
    connection to that owner. The routing layer (not the per-connection engine)
    acts on it, so the engine surfaces it as this typed error.
    """

    def __init__(self, redirect: "Redirect") -> None:
        owners = ", ".join(f"{a.host}:{a.port}" for a in redirect.owner_endpoints)
        super().__init__(
            f"redirected to owner {owners} for {redirect.topic}/{redirect.partition}"
        )
        self.redirect = redirect


class EofError(FibrilError):
    """The connection ended before completing a handshake or expected exchange."""

    def __init__(self, message: str = "Unexpected EOF") -> None:
        super().__init__(message)


class UnexpectedError(FibrilError):
    """Catch-all for protocol violations or unexpected states."""


def is_transient_error(err: BaseException) -> bool:
    """Whether an error is a transient transport failure (connect or severed).

    Narrow on purpose: this is the subset the client retries automatically
    against a refreshed owner during a failover.
    """
    return isinstance(err, (DisconnectionError, BrokenPipeError, EofError))


RetryAdvice = Literal["retry", "do_not_retry"]

# Broker error codes that change the retry decision.
_ERR_INVALID = 400  # malformed request: fix it, do not retry
_ERR_NOT_FOUND = 404  # topic/partition not in the cluster: do not retry
_ERR_NOT_OWNER = 409  # topology conflict: a retry re-routes


def retry_advice(err: BaseException) -> RetryAdvice:
    """How a caller should treat an error when deciding whether to re-issue an op.

    Transport failures, redirects, topology conflicts, and server-transient
    (5xx) errors are worth retrying. Not-found, invalid, and local request
    errors are not. Note the duplicate-publish caveat: a confirmed publish that
    fails after the broker may have accepted it can duplicate on retry until
    owner-side dedup ships.
    """
    if isinstance(err, (DisconnectionError, BrokenPipeError, EofError, RedirectError)):
        return "retry"
    if isinstance(err, ServerError):
        if err.code == _ERR_NOT_OWNER:
            return "retry"
        if err.code in (_ERR_NOT_FOUND, _ERR_INVALID):
            return "do_not_retry"
        if err.code >= 500:
            return "retry"
        return "do_not_retry"
    return "do_not_retry"


def is_retryable(err: BaseException) -> bool:
    """The simple "should I retry this?" check: ``retry_advice(err) == "retry"``."""
    return retry_advice(err) == "retry"
