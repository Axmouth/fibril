package fibril

import (
	"bufio"
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
	e, err := startEngine(client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour, OnTopologyUpdate: cache.applyPush})
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
