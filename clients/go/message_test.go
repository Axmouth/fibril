package fibril

import (
	"errors"
	"testing"
	"time"
)

func TestMessageBuildersLowerToPublish(t *testing.T) {
	m := Text("hi").Keyed([]byte("k")).WithTTL(2*time.Second).WithHeader("a", "1")
	pub, err := m.toPublish("t", nil)
	if err != nil {
		t.Fatalf("toPublish: %v", err)
	}
	if pub.Topic != "t" || string(pub.Payload) != "hi" || pub.ContentType.Kind != ContentText {
		t.Errorf("unexpected publish: %+v", pub)
	}
	if string(pub.PartitionKey) != "k" {
		t.Errorf("partition key = %q, want k", pub.PartitionKey)
	}
	if pub.TTLms == nil || *pub.TTLms != 2000 {
		t.Errorf("ttl_ms = %v, want 2000", pub.TTLms)
	}
	if pub.Headers["a"] != "1" {
		t.Errorf("header a = %q, want 1", pub.Headers["a"])
	}
}

func TestReservedHeaderKeysRejected(t *testing.T) {
	for _, key := range []string{"fibril.x", "fibril.client.producer_id", "stroma.z"} {
		_, err := Raw(nil).WithHeader(key, "v").toPublish("t", nil)
		if err == nil {
			t.Errorf("%q: expected a reserved-header error", key)
			continue
		}
		var se *SerializationError
		if !errors.As(err, &se) {
			t.Errorf("%q: error = %T, want *SerializationError", key, err)
		}
	}
}

func TestJSONMessageRoundTrips(t *testing.T) {
	type payload struct {
		N int    `json:"n"`
		S string `json:"s"`
	}
	m, err := JSON(payload{N: 7, S: "hi"})
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if m.ContentType.Kind != ContentJSON {
		t.Errorf("content kind = %d, want JSON", m.ContentType.Kind)
	}
	var got payload
	if err := (Delivery{Payload: m.Payload}).JSON(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.N != 7 || got.S != "hi" {
		t.Errorf("decoded = %+v, want {7 hi}", got)
	}
	if s := (Delivery{Payload: []byte("plain")}).Text(); s != "plain" {
		t.Errorf("Text() = %q, want plain", s)
	}
}

func TestReliablePublisherStampsProducerHeaders(t *testing.T) {
	rp := &ReliablePublisher{Publisher: &Publisher{topic: "t"}, producerID: "pid-1"}
	pub, _ := Text("x").toPublish("t", nil)

	rp.stamp(&pub)
	if pub.Headers[headerProducerID] != "pid-1" {
		t.Errorf("producer id = %q, want pid-1", pub.Headers[headerProducerID])
	}
	if pub.Headers[headerProducerSeq] != "0" {
		t.Errorf("first seq = %q, want 0", pub.Headers[headerProducerSeq])
	}
	rp.stamp(&pub)
	if pub.Headers[headerProducerSeq] != "1" {
		t.Errorf("second seq = %q, want 1", pub.Headers[headerProducerSeq])
	}
}
