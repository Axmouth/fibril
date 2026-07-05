package fibril

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientFollowsPublishRedirect(t *testing.T) {
	client, server := net.Pipe()
	// The redirect points back to the bootstrap endpoint, so the retry reuses this
	// same connection (no real dial), and the broker sees a second publish.
	const bootEndpoint = "127.0.0.1:9999"
	var publishes atomic.Int32

	go func() {
		br := bufio.NewReader(server)
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			switch f.Opcode {
			case opHello:
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			case opPublish:
				if publishes.Add(1) == 1 {
					rd := Redirect{Topic: "t", Partition: 0, OwnerEndpoints: []AdvertisedAddress{{Host: "127.0.0.1", Port: 9999}}, PartitioningVersion: 1}
					_, _ = server.Write(encodeFrame(buildFrame(opRedirect, f.RequestID, encodeRedirect(rd))))
				} else {
					_, _ = server.Write(encodeFrame(buildFrame(opPublishOk, f.RequestID, encodePublishOk(publishOk{Offset: 99}))))
				}
			case opPing:
				_, _ = server.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith(bootEndpoint, e, ClientOptions{})
	defer c.Shutdown()

	off, err := c.PublishConfirmed(Publish{Topic: "t", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("PublishConfirmed: %v", err)
	}
	if off != 99 {
		t.Errorf("offset = %d, want 99", off)
	}
	if n := publishes.Load(); n != 2 {
		t.Errorf("broker saw %d publishes, want 2 (redirect then ok)", n)
	}
}

func TestClientRoutesByFNVAfterTopology(t *testing.T) {
	c := &Client{topo: newTopologyCache()}
	// A 4-partition topology for "t".
	c.topo.replace(TopologyOk{
		Generation: 1,
		Queues: []QueueTopologyEntry{
			{Topic: "t", Partition: 0, PartitionCount: 4, OwnerEndpoints: []AdvertisedAddress{{Host: "h0", Port: 1}}},
			{Topic: "t", Partition: 1, PartitionCount: 4, OwnerEndpoints: []AdvertisedAddress{{Host: "h1", Port: 1}}},
			{Topic: "t", Partition: 2, PartitionCount: 4, OwnerEndpoints: []AdvertisedAddress{{Host: "h2", Port: 1}}},
			{Topic: "t", Partition: 3, PartitionCount: 4, OwnerEndpoints: []AdvertisedAddress{{Host: "h3", Port: 1}}},
		},
	})
	// A keyed publish must land on FNV(key) % 4, matching every other client.
	key := []byte("order-42")
	want := uint32(FNV1a(key) % 4)
	if got := c.partitionFor("t", nil, key); got != want {
		t.Errorf("partitionFor = %d, want %d", got, want)
	}
	// The owner of that partition resolves from the cache.
	if ep := c.topo.ownerOf("t", want, nil); ep != net.JoinHostPort("h"+string(rune('0'+want)), "1") {
		t.Errorf("ownerOf(%d) = %q", want, ep)
	}
	// An unknown topic is a cold route: partition 0, empty owner (-> bootstrap).
	if got := c.partitionFor("other", nil, key); got != 0 {
		t.Errorf("cold partitionFor = %d, want 0", got)
	}
	if ep := c.topo.ownerOf("other", 0, nil); ep != "" {
		t.Errorf("cold ownerOf = %q, want empty", ep)
	}
}
