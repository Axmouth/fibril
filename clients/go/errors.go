package fibril

import "fmt"

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

// TlsHandshakeError is any other TLS handshake failure.
type TlsHandshakeError struct {
	Message string
}

func (e *TlsHandshakeError) Error() string { return e.Message }

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
