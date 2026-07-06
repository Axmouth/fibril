package fibril

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// TestClientAppliesTopologyPush checks a broker-pushed TopologyUpdate warms the
// routing cache and is acked with the generation the client now reflects.
func TestClientAppliesTopologyPush(t *testing.T) {
	client, server := net.Pipe()
	acks := make(chan topologyUpdateAck, 2)
	owner := []AdvertisedAddress{{Host: "127.0.0.1", Port: 9999}}

	go func() {
		br := bufio.NewReader(server)
		f, err := readFrame(br) // HELLO
		if err != nil {
			return
		}
		ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
		_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
		// Push a topology snapshot for "t" with 4 partitions.
		topo := TopologyOk{Generation: 7, Queues: []QueueTopologyEntry{
			{Topic: "t", Partition: 0, PartitionCount: 4, OwnerEndpoints: owner},
		}}
		_, _ = server.Write(encodeFrame(buildFrame(opTopologyUpdate, 0, encodeTopologyUpdate(topo))))
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			if f.Opcode == opTopologyUpdateAck {
				a, _ := decodeTopologyUpdateAck(f.Payload)
				acks <- a
			}
		}
	}()

	cache := newTopologyCache()
	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour, OnTopologyUpdate: cache.applyPush})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith("127.0.0.1:9999", e, ClientOptions{})
	c.topo = cache // engine pushes into this same cache
	defer c.Shutdown()

	select {
	case a := <-acks:
		if a.Generation != 7 {
			t.Errorf("ack generation = %d, want 7", a.Generation)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no topology-update ack received")
	}

	// The ack is written after the cache is applied, so routing now reflects it.
	if pc := c.topo.partitionCount("t", nil); pc != 4 {
		t.Errorf("partition count after push = %d, want 4", pc)
	}
	if ep := c.topo.ownerOf("t", 0, nil); ep != "127.0.0.1:9999" {
		t.Errorf("owner after push = %q, want 127.0.0.1:9999", ep)
	}
}

// TestClientIgnoresStaleTopologyPush checks that a lower-generation topology push
// is ignored (so an out-of-order push from a bounced broker cannot regress
// routing) while the client still acks the generation it currently reflects.
func TestClientIgnoresStaleTopologyPush(t *testing.T) {
	client, server := net.Pipe()
	acks := make(chan topologyUpdateAck, 2)
	fresh := []AdvertisedAddress{{Host: "127.0.0.1", Port: 9999}}
	stale := []AdvertisedAddress{{Host: "127.0.0.1", Port: 8888}}

	go func() {
		br := bufio.NewReader(server)
		f, err := readFrame(br) // HELLO
		if err != nil {
			return
		}
		ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
		_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
		// A newer generation the client adopts.
		topo7 := TopologyOk{Generation: 7, Queues: []QueueTopologyEntry{
			{Topic: "t", Partition: 0, PartitionCount: 4, OwnerEndpoints: fresh},
		}}
		_, _ = server.Write(encodeFrame(buildFrame(opTopologyUpdate, 0, encodeTopologyUpdate(topo7))))
		pushedStale := false
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			if f.Opcode == opTopologyUpdateAck {
				a, _ := decodeTopologyUpdateAck(f.Payload)
				acks <- a
				// After the client acks the fresh push, send a stale one.
				if !pushedStale {
					pushedStale = true
					topo5 := TopologyOk{Generation: 5, Queues: []QueueTopologyEntry{
						{Topic: "t", Partition: 0, PartitionCount: 9, OwnerEndpoints: stale},
					}}
					_, _ = server.Write(encodeFrame(buildFrame(opTopologyUpdate, 0, encodeTopologyUpdate(topo5))))
				}
			}
		}
	}()

	cache := newTopologyCache()
	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour, OnTopologyUpdate: cache.applyPush})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith("127.0.0.1:9999", e, ClientOptions{})
	c.topo = cache
	defer c.Shutdown()

	// Two acks: for the fresh push and the stale one. Both carry the current
	// generation (7): the stale push is ignored, so the client acks the generation
	// it still reflects.
	for i := 0; i < 2; i++ {
		select {
		case a := <-acks:
			if a.Generation != 7 {
				t.Errorf("ack %d generation = %d, want 7", i, a.Generation)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("missing topology-update ack")
		}
	}

	// Routing still reflects the fresh generation; the stale push changed nothing.
	if pc := c.topo.partitionCount("t", nil); pc != 4 {
		t.Errorf("partition count = %d, want 4 (stale push must be ignored)", pc)
	}
	if ep := c.topo.ownerOf("t", 0, nil); ep != "127.0.0.1:9999" {
		t.Errorf("owner = %q, want 127.0.0.1:9999 (stale push must be ignored)", ep)
	}
}
