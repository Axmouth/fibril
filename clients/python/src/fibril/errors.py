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


class SubscriptionClosedError(FibrilError):
    """A subscription ended with a typed reason instead of a silent stop.

    Raised from a subscription's iterator (or ``recv``) when the broker or a
    reconcile verdict closes it - a topic deletion, an ownership move that
    could not be migrated, a broker shutdown, a recreate the client opted out
    of, or a server error. A clean user ``close()`` ends iteration without
    this error. ``code`` is the machine-readable reason (see ``wire.REASON_*``).
    """

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


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


#: Broker error code for a plaintext connection to a TLS listener. The broker
#: replies with this before closing, so the mismatch is definitive.
ERR_TLS_REQUIRED = 426


class TlsRequiredByBrokerError(FibrilError):
    """The broker requires TLS but this client connected plaintext.

    Reported by the broker itself, so it is definitive rather than inferred.
    """

    def __init__(self) -> None:
        super().__init__(
            "the broker requires TLS. Enable client TLS: with_tls_ca_path(...) or "
            "with_tls_ca_fingerprint(...) to trust self-signed broker material, or "
            "bare with_tls() for a publicly issued certificate"
        )


class TlsNotSupportedByBrokerError(FibrilError):
    """TLS is enabled but the handshake ended before completing.

    That usually means the broker listener speaks plaintext.
    """

    def __init__(self, address: str) -> None:
        super().__init__(
            f"TLS handshake with {address} ended early, the broker listener is "
            "probably plaintext. Disable TLS in the client options, or set "
            "tls.enabled = true on the broker"
        )


class TlsCertificateUntrustedError(FibrilError):
    """The broker certificate failed verification.

    A trust configuration problem, distinct from a transport mismatch.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(
            f"broker certificate verification failed: {detail}. Trust the broker "
            "CA via with_tls_ca_path(...) (generated deployments write "
            "<data_dir>/tls/ca.pem) or pin with_tls_ca_fingerprint(...) from the "
            "broker startup log"
        )


class TlsConfigError(FibrilError):
    """Client-side TLS configuration problem.

    An unreadable ca_path, malformed fingerprint, or invalid server name.
    """


class TlsHandshakeError(FibrilError):
    """Any other TLS handshake failure."""


class TlsClientCertificateRequiredError(FibrilError):
    """The broker requires a client certificate (``tls.client_auth =
    require``) and this client presented none. Provide one with
    ``with_tls_client_cert(cert_path, key_path)``; deployment-CA
    certificates are issued with ``fibrilctl cert issue``."""


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
