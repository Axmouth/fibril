package fibril

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Pointer and uuid helpers for building option fields and fixed ids.
func sp(s string) *string   { return &s }
func u32p(v uint32) *uint32 { return &v }
func u64p(v uint64) *uint64 { return &v }
func uup(u UUID) *UUID      { return &u }
func fill(b byte) UUID {
	var u UUID
	for i := range u {
		u[i] = b
	}
	return u
}

// A conformance case: the struct-encoded bytes, and a decode+re-encode of the
// canonical bytes, both of which must equal the shared vector.
type vcase struct {
	name      string
	encoded   []byte
	roundtrip func([]byte) ([]byte, error)
}

func cases() []vcase {
	return []vcase{
		{"hello", EncodeHello(Hello{"py-client", "0.1.0", 1, &ResumeIdentity{fill(1), fill(2), fill(3)}}),
			func(b []byte) ([]byte, error) { v, e := DecodeHello(b); return EncodeHello(v), e }},
		{"hello_no_resume", EncodeHello(Hello{"c", "v", 1, nil}),
			func(b []byte) ([]byte, error) { v, e := DecodeHello(b); return EncodeHello(v), e }},
		{"hello_ok", EncodeHelloOk(HelloOk{1, fill(9), fill(8), fill(7), ResumeResumed, "srv", "v=1;x"}),
			func(b []byte) ([]byte, error) { v, e := DecodeHelloOk(b); return EncodeHelloOk(v), e }},
		{"auth", EncodeAuth(Auth{"u", "p"}),
			func(b []byte) ([]byte, error) { v, e := DecodeAuth(b); return EncodeAuth(v), e }},
		{"error", EncodeError(ErrorMsg{409, "not owner"}),
			func(b []byte) ([]byte, error) { v, e := DecodeError(b); return EncodeError(v), e }},
		{"publish", EncodePublish(Publish{
			Topic: "orders", Partition: 3, Group: sp("g"), RequireConfirm: true,
			ContentType: ContentType{Kind: ContentJSON}, Headers: Headers{"x-a": "1"},
			Payload: []byte{1, 2, 3, 4}, Published: 1234567890, PartitionKey: []byte{9, 9},
			PartitioningVersion: 5, TTLms: u64p(60000)}),
			func(b []byte) ([]byte, error) { v, e := DecodePublish(b); return EncodePublish(v), e }},
		{"publish_no_ttl", EncodePublish(Publish{Topic: "t", ContentType: ContentType{Kind: ContentNone}}),
			func(b []byte) ([]byte, error) { v, e := DecodePublish(b); return EncodePublish(v), e }},
		{"publish_custom_ct", EncodePublish(Publish{
			Topic: "t", ContentType: ContentType{Kind: ContentCustom, Custom: "application/x-thing"},
			Payload: []byte{7}, Published: 1}),
			func(b []byte) ([]byte, error) { v, e := DecodePublish(b); return EncodePublish(v), e }},
		{"publish_delayed", EncodePublishDelayed(PublishDelayed{
			Topic: "t", Partition: 1, RequireConfirm: true, NotBefore: 999,
			ContentType: ContentType{Kind: ContentText}, Headers: Headers{"k": "v"},
			Payload: []byte{5, 6}, Published: 42, PartitioningVersion: 2}),
			func(b []byte) ([]byte, error) { v, e := DecodePublishDelayed(b); return EncodePublishDelayed(v), e }},
		{"publish_ok", EncodePublishOk(PublishOk{777}),
			func(b []byte) ([]byte, error) { v, e := DecodePublishOk(b); return EncodePublishOk(v), e }},
		{"deliver", EncodeDeliver(Deliver{
			SubID: 11, Topic: "t", Group: sp("g"), Partition: 2, Offset: 100,
			DeliveryTag: DeliveryTag{5}, Published: 7, PublishReceived: 8,
			ContentType: ContentType{Kind: ContentMsgpack}, Headers: Headers{"h": "1"},
			Payload: []byte{3, 2, 1}}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeliver(b); return EncodeDeliver(v), e }},
		{"ack", EncodeAck(Ack{Topic: "t", Tags: []DeliveryTag{{1}, {2}}}),
			func(b []byte) ([]byte, error) { v, e := DecodeAck(b); return EncodeAck(v), e }},
		{"nack", EncodeNack(Nack{Topic: "t", Group: sp("g"), Partition: 1, Tags: []DeliveryTag{{9}}, Requeue: true, NotBefore: u64p(5000)}),
			func(b []byte) ([]byte, error) { v, e := DecodeNack(b); return EncodeNack(v), e }},
		{"nack_no_nb", EncodeNack(Nack{Topic: "t", Tags: []DeliveryTag{}}),
			func(b []byte) ([]byte, error) { v, e := DecodeNack(b); return EncodeNack(v), e }},
		{"subscribe", EncodeSubscribe(Subscribe{
			Topic: "t", Partition: 1, Group: sp("g"), Prefetch: 32, AutoAck: false,
			ConsumerGroup: sp("cg"), ConsumerTarget: u32p(2), MemberID: uup(fill(4))}),
			func(b []byte) ([]byte, error) { v, e := DecodeSubscribe(b); return EncodeSubscribe(v), e }},
		{"subscribe_min", EncodeSubscribe(Subscribe{Topic: "t", AutoAck: true}),
			func(b []byte) ([]byte, error) { v, e := DecodeSubscribe(b); return EncodeSubscribe(v), e }},
		{"subscribe_ok", EncodeSubscribeOk(SubscribeOk{
			SubID: 5, Topic: "t", Partition: 1, Group: sp("g"), Prefetch: 16,
			ConsumerGroup: sp("cg"), MemberID: uup(fill(4))}),
			func(b []byte) ([]byte, error) { v, e := DecodeSubscribeOk(b); return EncodeSubscribeOk(v), e }},
		{"declare", EncodeDeclareQueue(DeclareQueue{
			Topic: "t", Group: sp("g"), DlqPolicy: &DlqPolicy{Kind: DlqCustom, Topic: "dlq"},
			DlqMaxRetries: u32p(3), PartitionCount: u32p(4), DefaultMessageTTLms: u64p(30000)}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeclareQueue(b); return EncodeDeclareQueue(v), e }},
		{"declare_min", EncodeDeclareQueue(DeclareQueue{Topic: "t"}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeclareQueue(b); return EncodeDeclareQueue(v), e }},
		{"declare_ok", EncodeDeclareQueueOk(DeclareQueueOk{Status: "created", PartitionCount: 4}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeclareQueueOk(b); return EncodeDeclareQueueOk(v), e }},
		{"declare_plexus", EncodeDeclarePlexus(DeclarePlexus{
			Topic: "t", PartitionCount: u32p(4), Durability: StreamSpeculative,
			Retention:         StreamRetention{MaxAgeMs: u64p(60000), MaxRecords: u64p(1000000)},
			ReplicationFactor: u32p(2)}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeclarePlexus(b); return EncodeDeclarePlexus(v), e }},
		{"declare_plexus_min", EncodeDeclarePlexus(DeclarePlexus{Topic: "t"}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeclarePlexus(b); return EncodeDeclarePlexus(v), e }},
		{"declare_plexus_ok", EncodeDeclarePlexusOk(DeclarePlexusOk{Status: "created", PartitionCount: 4}),
			func(b []byte) ([]byte, error) { v, e := DecodeDeclarePlexusOk(b); return EncodeDeclarePlexusOk(v), e }},
		{"topology_ok", EncodeTopologyOk(TopologyOk{
			Generation: 12,
			Queues: []QueueTopologyEntry{
				{Topic: "t", Partition: 0, OwnerEndpoints: []AdvertisedAddress{{Host: "127.0.0.1", Port: 7000}}, PartitioningVersion: 1, PartitionCount: 2},
				{Topic: "t", Partition: 1, PartitioningVersion: 1, PartitionCount: 2},
			},
			Streams: []StreamTopologyEntry{{Topic: "s", Partition: 2, OwnerEndpoints: []AdvertisedAddress{{Host: "10.0.0.9", Port: 7100}}, PartitioningVersion: 4, PartitionCount: 3}}}),
			func(b []byte) ([]byte, error) { v, e := DecodeTopologyOk(b); return EncodeTopologyOk(v), e }},
		{"topology_req", EncodeTopologyRequest(TopologyRequest{Topic: sp("t")}),
			func(b []byte) ([]byte, error) { v, e := DecodeTopologyRequest(b); return EncodeTopologyRequest(v), e }},
		{"topology_update", EncodeTopologyUpdate(TopologyOk{
			Generation: 12,
			Queues:     []QueueTopologyEntry{{Topic: "t", Partition: 0, OwnerEndpoints: []AdvertisedAddress{{Host: "127.0.0.1", Port: 7000}}, PartitioningVersion: 1, PartitionCount: 2}},
			Streams:    []StreamTopologyEntry{{Topic: "s", Partition: 2, OwnerEndpoints: []AdvertisedAddress{{Host: "10.0.0.9", Port: 7100}}, PartitioningVersion: 4, PartitionCount: 3}}}),
			func(b []byte) ([]byte, error) { v, e := DecodeTopologyUpdate(b); return EncodeTopologyUpdate(v), e }},
		{"topology_update_ack", EncodeTopologyUpdateAck(TopologyUpdateAck{Generation: 12}),
			func(b []byte) ([]byte, error) {
				v, e := DecodeTopologyUpdateAck(b)
				return EncodeTopologyUpdateAck(v), e
			}},
		{"reconcile_client", EncodeReconcileClient(ReconcileClient{
			Policy:        ReconcileRestore,
			Subscriptions: []ReconcileSubscription{{SubID: 1, Topic: "t", Partition: 0, AutoAck: false, Prefetch: 8}}}),
			func(b []byte) ([]byte, error) { v, e := DecodeReconcileClient(b); return EncodeReconcileClient(v), e }},
		{"redirect", EncodeRedirect(Redirect{Topic: "t", Partition: 1, Group: sp("g"), OwnerEndpoints: []AdvertisedAddress{{Host: "h", Port: 1}}, PartitioningVersion: 3}),
			func(b []byte) ([]byte, error) { v, e := DecodeRedirect(b); return EncodeRedirect(v), e }},
		{"assignment", EncodeAssignmentChanged(AssignmentChanged{
			Topic: "t", ConsumerGroup: "cg", Generation: 6, Assigned: []uint32{0, 1, 2}, Added: []uint32{2}, Revoked: []uint32{}}),
			func(b []byte) ([]byte, error) {
				v, e := DecodeAssignmentChanged(b)
				return EncodeAssignmentChanged(v), e
			}},
		{"going_away", EncodeGoingAway(GoingAway{GraceMs: 30000, Message: "broker restarting for upgrade"}),
			func(b []byte) ([]byte, error) { v, e := DecodeGoingAway(b); return EncodeGoingAway(v), e }},
		{"subscribe_stream", EncodeSubscribeStream(SubscribeStream{
			Topic: "t", Partition: 1, DurableName: sp("c1"), Start: StreamStart{Kind: StreamByTime, Value: 1234},
			Filter: []StreamFilter{{"region", "eu-*"}, {"kind", "order"}}, Prefetch: 16, AutoAck: false}),
			func(b []byte) ([]byte, error) { v, e := DecodeSubscribeStream(b); return EncodeSubscribeStream(v), e }},
		{"subscribe_stream_min", EncodeSubscribeStream(SubscribeStream{Topic: "t", Start: StreamStart{Kind: StreamLatest}, AutoAck: true}),
			func(b []byte) ([]byte, error) { v, e := DecodeSubscribeStream(b); return EncodeSubscribeStream(v), e }},
	}
}

func loadVectors(t *testing.T) map[string]string {
	t.Helper()
	// The shared cross-client fixture lives at clients/wire_vectors.json, one
	// level up from clients/go.
	raw, err := os.ReadFile(filepath.Join("..", "wire_vectors.json"))
	if err != nil {
		t.Fatalf("read wire_vectors.json: %v", err)
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse wire_vectors.json: %v", err)
	}
	return m
}

func TestEncodeMatchesSharedVectors(t *testing.T) {
	vectors := loadVectors(t)
	for _, c := range cases() {
		want, ok := vectors[c.name]
		if !ok {
			t.Errorf("%s: no shared vector", c.name)
			continue
		}
		if got := hex.EncodeToString(c.encoded); got != want {
			t.Errorf("%s bytes diverge from shared vector\n got: %s\nwant: %s", c.name, got, want)
		}
	}
}

func TestDecodeRoundTripsToVector(t *testing.T) {
	vectors := loadVectors(t)
	for _, c := range cases() {
		want := vectors[c.name]
		raw, err := hex.DecodeString(want)
		if err != nil {
			t.Fatalf("%s: bad hex vector: %v", c.name, err)
		}
		reencoded, err := c.roundtrip(raw)
		if err != nil {
			t.Errorf("%s: decode failed: %v", c.name, err)
			continue
		}
		if got := hex.EncodeToString(reencoded); got != want {
			t.Errorf("%s does not round-trip\n got: %s\nwant: %s", c.name, got, want)
		}
	}
}

// TestAllVectorsCovered fails if a shared vector has no conformance case, so a
// new op added to wire_vectors.json cannot silently skip the Go codec.
func TestAllVectorsCovered(t *testing.T) {
	vectors := loadVectors(t)
	covered := make(map[string]bool)
	for _, c := range cases() {
		covered[c.name] = true
	}
	for name := range vectors {
		if !covered[name] {
			t.Errorf("shared vector %q has no Go conformance case", name)
		}
	}
}

func TestFNV1aCanonical(t *testing.T) {
	cases := map[string]uint64{
		"":              14695981039346656037,
		"a":             12638187200555641996,
		"order-42":      9015620992513762004,
		"partition-key": 11792757095719117019,
		"hello world":   8618312879776256743,
	}
	for key, want := range cases {
		if got := FNV1a([]byte(key)); got != want {
			t.Errorf("FNV1a(%q) = %d, want %d", key, got, want)
		}
	}
}
