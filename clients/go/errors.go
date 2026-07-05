package fibril

// WireErrorKind is a stable discriminant for a wire decode failure, matching the
// Rust reference and the other clients so callers can branch on the kind rather
// than parse the message.
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
