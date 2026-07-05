package fibril

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// A whole-topic fan-in should pick up a partition added by a live repartition
// grow: the broker reports a higher partition count on a later topology fetch,
// and the growth loop subscribes the new partition.
func TestFanInPicksUpGrownPartition(t *testing.T) {
	client, server := net.Pipe()
	var fetches atomic.Uint64
	subscribed := make(chan uint32, 8)

	go func() {
		br := bufio.NewReader(server)
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			switch f.Opcode {
			case OpHello:
				ok := HelloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(OpHelloOk, f.RequestID, encodeHelloOk(ok))))
			case OpTopology:
				// Grow from one partition to two after the first couple of fetches.
				n := fetches.Add(1)
				gen, count := uint64(1), uint32(1)
				if n >= 2 {
					gen, count = 2, 2
				}
				topo := TopologyOk{Generation: gen, Queues: []QueueTopologyEntry{
					{Topic: "t", Partition: 0, PartitionCount: count},
				}}
				_, _ = server.Write(encodeFrame(buildFrame(OpTopologyOk, f.RequestID, encodeTopologyOk(topo))))
			case OpSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				so := SubscribeOk{SubID: uint64(req.Partition) + 1, Topic: req.Topic, Partition: req.Partition, Prefetch: req.Prefetch}
				_, _ = server.Write(encodeFrame(buildFrame(OpSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				subscribed <- req.Partition
			case OpPing:
				_, _ = server.Write(encodeFrame(buildFrame(OpPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith("127.0.0.1:9999", e, ClientOptions{RepartitionPollInterval: 40 * time.Millisecond})
	defer c.Shutdown()

	fi, err := c.SubscribeTopic("t", nil, 4, true)
	if err != nil {
		t.Fatalf("SubscribeTopic: %v", err)
	}
	defer fi.Close()

	seen := map[uint32]bool{}
	deadline := time.After(3 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-subscribed:
			seen[p] = true
		case <-deadline:
			t.Fatalf("only subscribed partitions %v within timeout, want 0 and 1", seen)
		}
	}
	if !seen[0] || !seen[1] {
		t.Errorf("subscribed %v, want partitions 0 and 1", seen)
	}
}
