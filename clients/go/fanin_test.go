package fibril

import (
	"bufio"
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
	acks := make(chan Ack, 4)

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
				topo := TopologyOk{Generation: 1, Queues: []QueueTopologyEntry{
					{Topic: "t", Partition: 0, PartitionCount: 2, OwnerEndpoints: owner},
					{Topic: "t", Partition: 1, PartitionCount: 2, OwnerEndpoints: owner},
				}}
				_, _ = server.Write(encodeFrame(buildFrame(OpTopologyOk, f.RequestID, encodeTopologyOk(topo))))
			case OpSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				subID := uint64(100) + uint64(req.Partition)
				so := SubscribeOk{SubID: subID, Topic: req.Topic, Partition: req.Partition, Prefetch: 4}
				_, _ = server.Write(encodeFrame(buildFrame(OpSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				d := Deliver{
					SubID: subID, Topic: req.Topic, Partition: req.Partition, Offset: uint64(req.Partition),
					DeliveryTag: DeliveryTag{Epoch: uint64(req.Partition) + 1}, ContentType: ContentType{Kind: ContentText},
					Payload: []byte{byte('0' + req.Partition)},
				}
				_, _ = server.Write(encodeFrame(buildFrame(OpDeliver, 5000+uint64(req.Partition), encodeDeliver(d))))
			case OpAck:
				a, _ := decodeAck(f.Payload)
				acks <- a
			case OpPing:
				_, _ = server.Write(encodeFrame(buildFrame(OpPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith(bootEndpoint, e, ClientOptions{})
	defer c.Shutdown()

	fi, err := c.SubscribeTopic("t", nil, 4, false)
	if err != nil {
		t.Fatalf("SubscribeTopic: %v", err)
	}

	seen := map[uint32]bool{}
	for len(seen) < 2 {
		select {
		case d := <-fi.Deliveries:
			seen[d.Partition] = true
			if err := d.Ack(); err != nil {
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
