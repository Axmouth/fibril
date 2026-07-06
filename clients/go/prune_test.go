package fibril

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// serveHelloOnce answers the HELLO on a fake server connection and then drains
// whatever the client sends, so a test engine handshakes and stays idle.
func serveHelloOnce(server net.Conn) {
	br := bufio.NewReader(server)
	f, err := readFrame(br)
	if err != nil {
		return
	}
	ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
	_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
	for {
		if _, err := readFrame(br); err != nil {
			return
		}
	}
}

func startTestEngine(t *testing.T) *Engine {
	t.Helper()
	client, server := net.Pipe()
	go serveHelloOnce(server)
	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	return e
}

// TestClientPrunesPoolOnFailover checks that a topology refresh which moves a
// partition to a new owner prunes and shuts down the pooled connection to the old
// owner, so a failed-over owner's connection does not linger until shutdown.
func TestClientPrunesPoolOnFailover(t *testing.T) {
	owner := func(port int) []AdvertisedAddress {
		return []AdvertisedAddress{{Host: "127.0.0.1", Port: uint16(port)}}
	}

	c := newClientWith("127.0.0.1:9000", startTestEngine(t), ClientOptions{})
	defer c.Shutdown()

	// A pooled connection to owner A (127.0.0.1:7001).
	pooled := startTestEngine(t)
	c.pool["127.0.0.1:7001"] = pooled

	// The cache says partition 0 is owned by A, so its pooled connection is kept.
	c.topo.replace(TopologyOk{Generation: 1, Queues: []QueueTopologyEntry{
		{Topic: "jobs", Partition: 0, PartitionCount: 1, OwnerEndpoints: owner(7001)},
	}})
	c.prunePool()
	if _, ok := c.pool["127.0.0.1:7001"]; !ok {
		t.Fatal("owner still owns the partition; its pooled connection must be kept")
	}

	// Failover: partition 0 moves to owner B. A owns nothing now, so its pooled
	// connection must be pruned and shut down.
	c.topo.replace(TopologyOk{Generation: 2, Queues: []QueueTopologyEntry{
		{Topic: "jobs", Partition: 0, PartitionCount: 1, OwnerEndpoints: owner(7002)},
	}})
	c.prunePool()
	if _, ok := c.pool["127.0.0.1:7001"]; ok {
		t.Error("failed-over owner's pooled connection was not pruned")
	}
	if !pooled.IsClosed() {
		t.Error("pruned connection was not shut down")
	}
}
