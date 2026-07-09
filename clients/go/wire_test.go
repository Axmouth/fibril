package fibril

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
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
		{"hello", encodeHello(hello{"py-client", "0.1.0", 1, &ResumeIdentity{fill(1), fill(2), fill(3)}}),
			func(b []byte) ([]byte, error) { v, e := decodeHello(b); return encodeHello(v), e }},
		{"hello_no_resume", encodeHello(hello{"c", "v", 1, nil}),
			func(b []byte) ([]byte, error) { v, e := decodeHello(b); return encodeHello(v), e }},
		{"hello_ok", encodeHelloOk(helloOk{1, fill(9), fill(8), fill(7), ResumeResumed, "srv", "v=1;x"}),
			func(b []byte) ([]byte, error) { v, e := decodeHelloOk(b); return encodeHelloOk(v), e }},
		{"auth", encodeAuth(Credentials{"u", "p"}),
			func(b []byte) ([]byte, error) { v, e := decodeAuth(b); return encodeAuth(v), e }},
		{"error", encodeError(errorMsg{409, "not owner"}),
			func(b []byte) ([]byte, error) { v, e := decodeError(b); return encodeError(v), e }},
		{"publish", encodePublish(Publish{
			Topic: "orders", Partition: 3, Group: sp("g"), RequireConfirm: true,
			ContentType: ContentType{Kind: ContentJSON}, Headers: Headers{"x-a": "1"},
			Payload: []byte{1, 2, 3, 4}, Published: 1234567890, PartitionKey: []byte{9, 9},
			PartitioningVersion: 5, TTLms: u64p(60000)}),
			func(b []byte) ([]byte, error) { v, e := decodePublish(b); return encodePublish(v), e }},
		{"publish_no_ttl", encodePublish(Publish{Topic: "t", ContentType: ContentType{Kind: ContentNone}}),
			func(b []byte) ([]byte, error) { v, e := decodePublish(b); return encodePublish(v), e }},
		{"publish_custom_ct", encodePublish(Publish{
			Topic: "t", ContentType: ContentType{Kind: ContentCustom, Custom: "application/x-thing"},
			Payload: []byte{7}, Published: 1}),
			func(b []byte) ([]byte, error) { v, e := decodePublish(b); return encodePublish(v), e }},
		{"publish_delayed", encodePublishDelayed(PublishDelayed{
			Topic: "t", Partition: 1, RequireConfirm: true, NotBefore: 999,
			ContentType: ContentType{Kind: ContentText}, Headers: Headers{"k": "v"},
			Payload: []byte{5, 6}, Published: 42, PartitioningVersion: 2}),
			func(b []byte) ([]byte, error) { v, e := decodePublishDelayed(b); return encodePublishDelayed(v), e }},
		{"publish_ok", encodePublishOk(publishOk{777}),
			func(b []byte) ([]byte, error) { v, e := decodePublishOk(b); return encodePublishOk(v), e }},
		{"deliver", encodeDeliver(deliver{
			SubID: 11, Topic: "t", Group: sp("g"), Partition: 2, Offset: 100,
			DeliveryTag: DeliveryTag{5}, Published: 7, PublishReceived: 8,
			ContentType: ContentType{Kind: ContentMsgpack}, Headers: Headers{"h": "1"},
			Payload: []byte{3, 2, 1}}),
			func(b []byte) ([]byte, error) { v, e := decodeDeliver(b); return encodeDeliver(v), e }},
		{"ack", encodeAck(ackFrame{Topic: "t", Tags: []DeliveryTag{{1}, {2}}}),
			func(b []byte) ([]byte, error) { v, e := decodeAck(b); return encodeAck(v), e }},
		{"nack", encodeNack(nackFrame{Topic: "t", Group: sp("g"), Partition: 1, Tags: []DeliveryTag{{9}}, Requeue: true, NotBefore: u64p(5000)}),
			func(b []byte) ([]byte, error) { v, e := decodeNack(b); return encodeNack(v), e }},
		{"nack_no_nb", encodeNack(nackFrame{Topic: "t", Tags: []DeliveryTag{}}),
			func(b []byte) ([]byte, error) { v, e := decodeNack(b); return encodeNack(v), e }},
		{"subscribe", encodeSubscribe(Subscribe{
			Topic: "t", Partition: 1, Group: sp("g"), Prefetch: 32, AutoAck: false,
			ConsumerGroup: sp("cg"), ConsumerTarget: u32p(2), MemberID: uup(fill(4))}),
			func(b []byte) ([]byte, error) { v, e := decodeSubscribe(b); return encodeSubscribe(v), e }},
		{"subscribe_min", encodeSubscribe(Subscribe{Topic: "t", AutoAck: true}),
			func(b []byte) ([]byte, error) { v, e := decodeSubscribe(b); return encodeSubscribe(v), e }},
		{"subscribe_ok", encodeSubscribeOk(subscribeOk{
			SubID: 5, Topic: "t", Partition: 1, Group: sp("g"), Prefetch: 16,
			ConsumerGroup: sp("cg"), MemberID: uup(fill(4))}),
			func(b []byte) ([]byte, error) { v, e := decodeSubscribeOk(b); return encodeSubscribeOk(v), e }},
		{"declare", encodeDeclareQueue(NewQueueConfig("t").Group("g").
			Dlq(DlqPolicy{Kind: DlqCustom, Topic: "dlq"}).
			DlqMaxRetries(3).PartitionCount(4).DefaultMessageTTL(30 * time.Second)),
			func(b []byte) ([]byte, error) { v, e := decodeDeclareQueue(b); return encodeDeclareQueue(v), e }},
		{"declare_min", encodeDeclareQueue(NewQueueConfig("t")),
			func(b []byte) ([]byte, error) { v, e := decodeDeclareQueue(b); return encodeDeclareQueue(v), e }},
		{"declare_ok", encodeDeclareQueueOk(DeclareQueueOk{Status: "created", PartitionCount: 4}),
			func(b []byte) ([]byte, error) { v, e := decodeDeclareQueueOk(b); return encodeDeclareQueueOk(v), e }},
		{"declare_plexus", encodeDeclarePlexus(NewStreamConfig("t").PartitionCount(4).
			Durability(StreamSpeculative).
			Retention(NewStreamRetention().MaxAge(60 * time.Second).RetainRecords(1000000)).
			ReplicationFactor(2)),
			func(b []byte) ([]byte, error) { v, e := decodeDeclarePlexus(b); return encodeDeclarePlexus(v), e }},
		{"declare_plexus_min", encodeDeclarePlexus(NewStreamConfig("t")),
			func(b []byte) ([]byte, error) { v, e := decodeDeclarePlexus(b); return encodeDeclarePlexus(v), e }},
		{"declare_plexus_ok", encodeDeclarePlexusOk(DeclarePlexusOk{Status: "created", PartitionCount: 4}),
			func(b []byte) ([]byte, error) { v, e := decodeDeclarePlexusOk(b); return encodeDeclarePlexusOk(v), e }},
		{"topology_ok", encodeTopologyOk(TopologyOk{
			Generation: 12,
			Queues: []QueueTopologyEntry{
				{Topic: "t", Partition: 0, OwnerEndpoints: []AdvertisedAddress{{Host: "127.0.0.1", Port: 7000}}, PartitioningVersion: 1, PartitionCount: 2},
				{Topic: "t", Partition: 1, PartitioningVersion: 1, PartitionCount: 2},
			},
			Streams: []StreamTopologyEntry{{Topic: "s", Partition: 2, OwnerEndpoints: []AdvertisedAddress{{Host: "10.0.0.9", Port: 7100}}, PartitioningVersion: 4, PartitionCount: 3}}}),
			func(b []byte) ([]byte, error) { v, e := decodeTopologyOk(b); return encodeTopologyOk(v), e }},
		{"topology_req", encodeTopologyRequest(TopologyRequest{Topic: sp("t")}),
			func(b []byte) ([]byte, error) { v, e := decodeTopologyRequest(b); return encodeTopologyRequest(v), e }},
		{"topology_update", encodeTopologyUpdate(TopologyOk{
			Generation: 12,
			Queues:     []QueueTopologyEntry{{Topic: "t", Partition: 0, OwnerEndpoints: []AdvertisedAddress{{Host: "127.0.0.1", Port: 7000}}, PartitioningVersion: 1, PartitionCount: 2}},
			Streams:    []StreamTopologyEntry{{Topic: "s", Partition: 2, OwnerEndpoints: []AdvertisedAddress{{Host: "10.0.0.9", Port: 7100}}, PartitioningVersion: 4, PartitionCount: 3}}}),
			func(b []byte) ([]byte, error) { v, e := decodeTopologyUpdate(b); return encodeTopologyUpdate(v), e }},
		{"topology_update_ack", encodeTopologyUpdateAck(topologyUpdateAck{Generation: 12}),
			func(b []byte) ([]byte, error) {
				v, e := decodeTopologyUpdateAck(b)
				return encodeTopologyUpdateAck(v), e
			}},
		{"reconcile_client", encodeReconcileClient(reconcileClient{
			Policy:        ReconcileRestore,
			Subscriptions: []reconcileSubscription{{SubID: 1, Topic: "t", Partition: 0, AutoAck: false, Prefetch: 8}}}),
			func(b []byte) ([]byte, error) { v, e := decodeReconcileClient(b); return encodeReconcileClient(v), e }},
		{"redirect", encodeRedirect(Redirect{Topic: "t", Partition: 1, Group: sp("g"), OwnerEndpoints: []AdvertisedAddress{{Host: "h", Port: 1}}, PartitioningVersion: 3}),
			func(b []byte) ([]byte, error) { v, e := decodeRedirect(b); return encodeRedirect(v), e }},
		{"assignment", encodeAssignmentChanged(AssignmentChanged{
			Topic: "t", ConsumerGroup: "cg", Generation: 6, Assigned: []uint32{0, 1, 2}, Added: []uint32{2}, Revoked: []uint32{}}),
			func(b []byte) ([]byte, error) {
				v, e := decodeAssignmentChanged(b)
				return encodeAssignmentChanged(v), e
			}},
		{"going_away", encodeGoingAway(goingAway{GraceMs: 30000, Message: "broker restarting for upgrade"}),
			func(b []byte) ([]byte, error) { v, e := decodeGoingAway(b); return encodeGoingAway(v), e }},
		{"subscribe_stream", encodeSubscribeStream(SubscribeStream{
			Topic: "t", Partition: 1, DurableName: sp("c1"), Start: StreamStart{Kind: StreamByTime, Value: 1234},
			Filter: []StreamFilter{{"region", "eu-*"}, {"kind", "order"}}, Prefetch: 16, AutoAck: false}),
			func(b []byte) ([]byte, error) { v, e := decodeSubscribeStream(b); return encodeSubscribeStream(v), e }},
		{"subscribe_stream_min", encodeSubscribeStream(SubscribeStream{Topic: "t", Start: StreamStart{Kind: StreamLatest}, AutoAck: true}),
			func(b []byte) ([]byte, error) { v, e := decodeSubscribeStream(b); return encodeSubscribeStream(v), e }},
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
