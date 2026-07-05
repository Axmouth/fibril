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
