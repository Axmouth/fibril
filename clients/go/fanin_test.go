package fibril

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// TestClientFanInAcrossPartitions subscribes a 2-partition topic and checks both
// partitions' deliveries arrive on the merged channel and settle correctly.
func TestClientFanInAcrossPartitions(t *testing.T) {
	client, server := net.Pipe()
	const bootEndpoint = "127.0.0.1:9999"
	owner := []AdvertisedAddress{{Host: "127.0.0.1", Port: 9999}}
	acks := make(chan ackFrame, 4)

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
			case opTopology:
				topo := TopologyOk{Generation: 1, Queues: []QueueTopologyEntry{
					{Topic: "t", Partition: 0, PartitionCount: 2, OwnerEndpoints: owner},
					{Topic: "t", Partition: 1, PartitionCount: 2, OwnerEndpoints: owner},
				}}
				_, _ = server.Write(encodeFrame(buildFrame(opTopologyOk, f.RequestID, encodeTopologyOk(topo))))
			case opSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				subID := uint64(100) + uint64(req.Partition)
				so := subscribeOk{SubID: subID, Topic: req.Topic, Partition: req.Partition, Prefetch: 4}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				d := deliver{
					SubID: subID, Topic: req.Topic, Partition: req.Partition, Offset: uint64(req.Partition),
					DeliveryTag: DeliveryTag{Epoch: uint64(req.Partition) + 1}, ContentType: ContentType{Kind: ContentText},
					Payload: []byte{byte('0' + req.Partition)},
				}
				_, _ = server.Write(encodeFrame(buildFrame(opDeliver, 5000+uint64(req.Partition), encodeDeliver(d))))
			case opAck:
				a, _ := decodeAck(f.Payload)
				acks <- a
			case opPing:
				_, _ = server.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith(bootEndpoint, e, ClientOptions{})
	defer c.Shutdown()

	fi, err := c.SubscribeTopic(context.Background(), "t", TopicSubscribeOptions{Prefetch: 4, AutoAck: false})
	if err != nil {
		t.Fatalf("SubscribeTopic: %v", err)
	}

	seen := map[uint32]bool{}
	for len(seen) < 2 {
		select {
		case d := <-fi.Deliveries:
			seen[d.Partition] = true
			if err := d.Complete(); err != nil {
				t.Errorf("ack: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("only saw partitions %v, want 0 and 1", seen)
		}
	}
	if !seen[0] || !seen[1] {
		t.Errorf("partitions seen = %v, want 0 and 1", seen)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-acks:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not receive both acks")
		}
	}
}
