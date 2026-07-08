package fibril

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

func TestClientCohortMemberCaptureCarryAndAssignment(t *testing.T) {
	client, server := net.Pipe()
	minted := fill(7)
	assignments := make(chan AssignmentChanged, 2)
	secondSubMember := make(chan *UUID, 1)

	go func() {
		br := bufio.NewReader(server)
		subCount := 0
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			switch f.Opcode {
			case opHello:
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			case opSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				subCount++
				if subCount == 2 {
					secondSubMember <- req.MemberID // the client should carry the minted id here
				}
				so := subscribeOk{SubID: uint64(subCount), Topic: req.Topic, Partition: req.Partition, ConsumerGroup: req.ConsumerGroup, MemberID: &minted}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				if subCount == 1 {
					a := AssignmentChanged{Topic: req.Topic, ConsumerGroup: "cg", Generation: 1, Assigned: []uint32{0, 1}}
					_, _ = server.Write(encodeFrame(buildFrame(opAssignmentChanged, 0, encodeAssignmentChanged(a))))
				}
			case opPing:
				_, _ = server.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(context.Background(), client, EngineOptions{
		ClientName: "go-test", HeartbeatInterval: time.Hour,
		OnAssignmentChanged: func(a AssignmentChanged) { assignments <- a },
	})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith("127.0.0.1:9999", e, ClientOptions{})
	defer c.Shutdown()

	cg := "cg"
	if _, err := c.Subscribe(context.Background(), Subscribe{Topic: "t", Partition: 0, ConsumerGroup: &cg}); err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	if _, err := c.Subscribe(context.Background(), Subscribe{Topic: "t", Partition: 1, ConsumerGroup: &cg}); err != nil {
		t.Fatalf("second subscribe: %v", err)
	}

	// The second subscribe must carry the member id minted on the first.
	select {
	case mid := <-secondSubMember:
		if mid == nil || *mid != minted {
			t.Errorf("second subscribe carried member id %v, want %v", mid, minted)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not observe the second subscribe")
	}

	select {
	case a := <-assignments:
		if a.ConsumerGroup != "cg" || len(a.Assigned) != 2 {
			t.Errorf("assignment = %+v, want cg with 2 partitions", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no assignment-changed event")
	}
}

func TestSubscribeTopicExclusiveJoinsDefaultCohort(t *testing.T) {
	client, server := net.Pipe()
	gotCG := make(chan string, 4)

	go func() {
		br := bufio.NewReader(server)
		var sid uint64
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
				topo := TopologyOk{Generation: 1, Queues: []QueueTopologyEntry{{Topic: "orders", Partition: 0, PartitionCount: 1}}}
				_, _ = server.Write(encodeFrame(buildFrame(opTopologyOk, f.RequestID, encodeTopologyOk(topo))))
			case opSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				cg := ""
				if req.ConsumerGroup != nil {
					cg = *req.ConsumerGroup
				}
				gotCG <- cg
				sid++
				so := subscribeOk{SubID: sid, Topic: req.Topic, Partition: req.Partition, Prefetch: req.Prefetch, ConsumerGroup: req.ConsumerGroup}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
			case opPing:
				_, _ = server.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	c := newClientWith("127.0.0.1:9999", e, ClientOptions{})
	defer c.Shutdown()

	fan, err := c.SubscribeTopicExclusive(context.Background(), "orders", TopicSubscribeOptions{Prefetch: 8, AutoAck: true})
	if err != nil {
		t.Fatalf("SubscribeTopicExclusive: %v", err)
	}
	defer fan.Close()

	select {
	case cg := <-gotCG:
		if cg != DefaultCohortID {
			t.Errorf("consumer_group = %q, want %q", cg, DefaultCohortID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no subscribe observed")
	}
}
