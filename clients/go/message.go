package fibril

// Ergonomic message construction and decoding over the raw Publish/Delivery wire
// types. A Message carries a payload plus a content type, headers, an optional
// partition key, and an optional TTL, and is built fluently. Delivery gains
// content-type-aware decode helpers.

import (
	"encoding/json"
	"strings"
	"time"
)

// Reserved header namespaces. User code cannot set fibril.* or stroma.* headers;
// the library-owned fibril.client.* keys (producer dedup) are set only by the
// client itself (see ReliablePublisher).
const (
	headerProducerID  = "fibril.client.producer_id"
	headerProducerSeq = "fibril.client.producer_seq"
)

func isReservedHeaderKey(k string) bool {
	return strings.HasPrefix(k, "fibril.") || strings.HasPrefix(k, "stroma.")
}

// Message is an outgoing message. Build it with Raw/Text/JSON and the fluent
// With* helpers.
type Message struct {
	Payload      []byte
	ContentType  ContentType
	Headers      Headers
	PartitionKey []byte
	TTL          time.Duration // 0 = no per-message TTL
}

// Raw builds a message from opaque bytes with no content type.
func Raw(payload []byte) Message { return Message{Payload: payload} }

// Text builds a text/plain message.
func Text(s string) Message {
	return Message{Payload: []byte(s), ContentType: ContentType{Kind: ContentText}}
}

// JSON builds a message by JSON-encoding v.
func JSON(v any) (Message, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Message{}, &SerializationError{Message: "json encode: " + err.Error()}
	}
	return Message{Payload: b, ContentType: ContentType{Kind: ContentJSON}}, nil
}

// Keyed sets the partition key (routing hashes it to a partition).
func (m Message) Keyed(key []byte) Message {
	m.PartitionKey = key
	return m
}

// WithTTL sets a per-message time-to-live.
func (m Message) WithTTL(d time.Duration) Message {
	m.TTL = d
	return m
}

// WithHeader returns a copy with a header set. Reserved keys are rejected at
// publish time, not here, so the fluent chain stays error-free.
func (m Message) WithHeader(key, value string) Message {
	h := make(Headers, len(m.Headers)+1)
	for k, v := range m.Headers {
		h[k] = v
	}
	h[key] = value
	m.Headers = h
	return m
}

// toPublish validates user headers and lowers the message onto the wire Publish.
func (m Message) toPublish(topic string, group *string) (Publish, error) {
	for k := range m.Headers {
		if isReservedHeaderKey(k) {
			return Publish{}, &SerializationError{Message: "header key is reserved and cannot be set by user code: " + k}
		}
	}
	var ttl *uint64
	if m.TTL > 0 {
		v := uint64(m.TTL.Milliseconds())
		ttl = &v
	}
	return Publish{
		Topic:        topic,
		Group:        group,
		ContentType:  m.ContentType,
		Headers:      m.Headers,
		Payload:      m.Payload,
		PartitionKey: m.PartitionKey,
		TTLms:        ttl,
	}, nil
}

// ---- delivery decode helpers -------------------------------------------

// Text returns the delivery payload as a string.
func (d Delivery) Text() string { return string(d.Payload) }

// JSON decodes the delivery payload into v.
func (d Delivery) JSON(v any) error {
	if err := json.Unmarshal(d.Payload, v); err != nil {
		return &DeserializationError{Message: "json decode: " + err.Error()}
	}
	return nil
}
