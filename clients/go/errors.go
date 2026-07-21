package fibril

import (
	"errors"
	"fmt"
)

// ServerError is a structured error the broker returned in response to a request.
type ServerError struct {
	Code    uint16
	Message string
}

func (e *ServerError) Error() string { return fmt.Sprintf("server error %d: %s", e.Code, e.Message) }

// DisconnectionError means the client could not establish or maintain a
// connection to the broker.
type DisconnectionError struct {
	Message string
}

func (e *DisconnectionError) Error() string { return e.Message }

// BrokenPipeError means the engine has shut down (or is shutting down), so the
// operation could not be delivered.
type BrokenPipeError struct {
	Message string
}

func (e *BrokenPipeError) Error() string {
	if e.Message == "" {
		return "broken pipe: engine has shut down"
	}
	return e.Message
}

// StaleDeliveryError means a delivery was settled after a non-resumed reconnect
// (or a broker restart) replaced the session it arrived on. Its delivery tag is
// dead server-side, so no settle frame is sent. The message is not lost: it
// redelivers on the current subscription per at-least-once. Distinct from
// BrokenPipeError (the connection is down, retry) - a stale delivery must NOT be
// retried, since re-settling would only report stale again while the message
// arrives afresh.
type StaleDeliveryError struct{}

func (e *StaleDeliveryError) Error() string {
	return "delivery is stale: its connection was replaced, the message will redeliver"
}

// UnexpectedError is a protocol violation or an otherwise unexpected state.
type UnexpectedError struct {
	Message string
}

func (e *UnexpectedError) Error() string { return e.Message }

// SerializationError means a message could not be prepared for sending (a bad
// value to encode, or a reserved header key).
type SerializationError struct {
	Message string
}

func (e *SerializationError) Error() string { return e.Message }

// DeserializationError means a delivered payload could not be decoded.
type DeserializationError struct {
	Message string
}

func (e *DeserializationError) Error() string { return e.Message }

// errTLSRequired is the broker error code for a plaintext connection to a TLS
// listener: the broker replies with it before closing, so the mismatch is
// definitive.
const errTLSRequired = 426

// TlsRequiredByBrokerError means the broker requires TLS but this client
// connected plaintext. Reported by the broker itself, so it is definitive.
type TlsRequiredByBrokerError struct{}

func (e *TlsRequiredByBrokerError) Error() string {
	return "the broker requires TLS: set ClientOptions.TLS (trust a CA file, pin a fingerprint, or use the OS roots)"
}

// TlsCertificateUntrustedError means the broker certificate failed verification
// (chain or pin). A trust-configuration problem, distinct from a transport error.
type TlsCertificateUntrustedError struct {
	Detail string
}

func (e *TlsCertificateUntrustedError) Error() string {
	return "broker certificate verification failed: " + e.Detail
}

// TlsConfigError is a client-side TLS configuration problem: an unreadable CA
// file, a malformed fingerprint, or missing client-cert material.
type TlsConfigError struct {
	Message string
}

func (e *TlsConfigError) Error() string { return e.Message }

// TlsNotSupportedByBrokerError means TLS is enabled on the client but the
// handshake ended before completing, which usually means the broker listener
// speaks plaintext.
type TlsNotSupportedByBrokerError struct {
	Addr string
}

func (e *TlsNotSupportedByBrokerError) Error() string {
	return "TLS handshake with " + e.Addr + " ended early, the broker listener is probably plaintext. Disable TLS in the client options, or enable TLS on the broker"
}

// TlsHandshakeError is any other TLS handshake failure.
type TlsHandshakeError struct {
	Message string
}

func (e *TlsHandshakeError) Error() string { return e.Message }

// TlsClientCertificateRequiredError means the broker requires a client
// certificate (mTLS) but this client presented none. Set ClientCertFile and
// ClientKeyFile in the TLS options to authenticate.
type TlsClientCertificateRequiredError struct{}

func (e *TlsClientCertificateRequiredError) Error() string {
	return "the broker requires a client certificate (mTLS) but none was configured. Set ClientCertFile and ClientKeyFile in the TLS options"
}

// RedirectError means the broker told the client to retry this op against a
// different owner. It is not a failure: the routing layer applies the target and
// retries, so the per-connection engine surfaces it as this typed error.
type RedirectError struct {
	Redirect Redirect
}

func (e *RedirectError) Error() string {
	return "redirected to a different owner for " + e.Redirect.Topic
}

// WireErrorKind is a stable discriminant for a wire decode failure, matching the
// reference client and the other clients so callers can branch on the kind
// rather than parse the message.
type WireErrorKind string

const (
	WireUnexpectedEOF      WireErrorKind = "unexpected_eof"
	WireInvalidMagic       WireErrorKind = "invalid_magic"
	WireTrailingBytes      WireErrorKind = "trailing_bytes"
	WireInvalidUUID        WireErrorKind = "invalid_uuid"
	WireUnknownContentType WireErrorKind = "unknown_content_type"
	WireUnknownTag         WireErrorKind = "unknown_tag"
)

// WireError is returned when a frame body cannot be decoded: truncated input,
// wrong magic, trailing bytes, or an unknown tag. Kind is a stable discriminant;
// Message is human-readable.
type WireError struct {
	Kind    WireErrorKind
	Message string
}

func (e *WireError) Error() string { return e.Message }

// RetryAdvice tells a caller whether re-issuing an operation is worthwhile.
type RetryAdvice string

const (
	RetryRetry      RetryAdvice = "retry"
	RetryDoNotRetry RetryAdvice = "do_not_retry"
)

// Broker error codes that change the retry decision.
const (
	errCodeInvalid  = 400 // malformed request: fix it, do not retry
	errCodeNotFound = 404 // topic/partition not in the cluster: do not retry
	errCodeNotOwner = 409 // topology conflict: a retry re-routes
)

// AdviseRetry reports how a caller should treat an error when deciding whether to
// re-issue an operation. Transport failures, redirects, topology conflicts, and
// server-transient (5xx) errors are worth retrying; not-found, invalid, and local
// request errors are not. A confirmed publish that fails after the broker may
// have accepted it can duplicate on retry until owner-side dedup ships.
func AdviseRetry(err error) RetryAdvice {
	var d *DisconnectionError
	var b *BrokenPipeError
	var re *RedirectError
	if errors.As(err, &d) || errors.As(err, &b) || errors.As(err, &re) {
		return RetryRetry
	}
	var se *ServerError
	if errors.As(err, &se) {
		switch {
		case se.Code == errCodeNotOwner:
			return RetryRetry
		case se.Code == errCodeNotFound || se.Code == errCodeInvalid:
			return RetryDoNotRetry
		case se.Code >= 500:
			return RetryRetry
		default:
			return RetryDoNotRetry
		}
	}
	// A stale delivery redelivers on its own; retrying the settle is pointless.
	var sd *StaleDeliveryError
	if errors.As(err, &sd) {
		return RetryDoNotRetry
	}
	return RetryDoNotRetry
}

// IsRetryable is the simple "should I retry this?" check.
func IsRetryable(err error) bool {
	return AdviseRetry(err) == RetryRetry
}
