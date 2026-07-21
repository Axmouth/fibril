package fibril

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// manualAckBroker scripts a broker over server: HELLO with the given resume
// outcome, SUBSCRIBE -> SUBSCRIBE_OK (and one delivery with tag epoch 42 when
// sendDelivery is true), and records acks. Runs until the connection closes.
func manualAckBroker(server net.Conn, outcome ResumeOutcome, acks chan<- ackFrame, sendDelivery bool) {
	br := bufio.NewReader(server)
	for {
		f, err := readFrame(br)
		if err != nil {
			return
		}
		switch f.Opcode {
		case opHello:
			ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: outcome, ServerName: "fake", Compliance: ComplianceString}
			_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
		case opSubscribe:
			req, _ := decodeSubscribe(f.Payload)
			so := subscribeOk{SubID: 100, Topic: req.Topic, Partition: req.Partition, Group: req.Group, Prefetch: 16}
			_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
			if sendDelivery {
				d := deliver{
					SubID: 100, Topic: req.Topic, Group: req.Group, Partition: req.Partition,
					Offset: 7, DeliveryTag: DeliveryTag{Epoch: 42}, Published: 1, PublishReceived: 2,
					ContentType: ContentType{Kind: ContentText}, Payload: []byte("job"),
				}
				_, _ = server.Write(encodeFrame(buildFrame(opDeliver, 5000, encodeDeliver(d))))
			}
		case opAck:
			a, _ := decodeAck(f.Payload)
			acks <- a
		case opPing:
			_, _ = server.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
		}
	}
}

// A delivery held across a NON-resumed reconnect settles to a *StaleDeliveryError
// and sends no frame. The message redelivers per at-least-once.
func TestHeldDeliveryGoesStaleAcrossNonResumedReconnect(t *testing.T) {
	sc := newSettleContext()
	acks := make(chan ackFrame, 4)

	client1, server1 := net.Pipe()
	go manualAckBroker(server1, ResumeNew, acks, true)
	e1, err := startEngine(context.Background(), client1, EngineOptions{ClientName: "t", HeartbeatInterval: time.Hour, settle: sc})
	if err != nil {
		t.Fatalf("startEngine 1: %v", err)
	}
	defer e1.Shutdown()

	sub, err := e1.Subscribe(context.Background(), Subscribe{Topic: "jobs", Partition: 0, Prefetch: 16})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	var held Delivery
	select {
	case held = <-sub.Deliveries:
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery received")
	}

	// A non-resumed reconnect: a fresh engine binds the SAME settle context and
	// claims a new incarnation, so the held delivery goes stale.
	client2, server2 := net.Pipe()
	go manualAckBroker(server2, ResumeNew, acks, false)
	e2, err := startEngine(context.Background(), client2, EngineOptions{ClientName: "t", HeartbeatInterval: time.Hour, settle: sc})
	if err != nil {
		t.Fatalf("startEngine 2: %v", err)
	}
	defer e2.Shutdown()

	err = held.Complete()
	var sd *StaleDeliveryError
	if !errors.As(err, &sd) {
		t.Fatalf("Complete after non-resumed reconnect = %v, want StaleDeliveryError", err)
	}
	select {
	case a := <-acks:
		t.Fatalf("a stale settle must send no ack, got %v", a)
	case <-time.After(100 * time.Millisecond):
	}
}

// A delivery held across a RESUMED reconnect settles to the CURRENT engine.
// Settlement is keyed by (topic, group, partition, tag), not the client sub id.
func TestHeldDeliverySettlesToCurrentEngineAcrossResumedReconnect(t *testing.T) {
	sc := newSettleContext()
	acks1 := make(chan ackFrame, 4)
	acks2 := make(chan ackFrame, 4)

	client1, server1 := net.Pipe()
	go manualAckBroker(server1, ResumeNew, acks1, true)
	e1, err := startEngine(context.Background(), client1, EngineOptions{ClientName: "t", HeartbeatInterval: time.Hour, settle: sc})
	if err != nil {
		t.Fatalf("startEngine 1: %v", err)
	}

	sub, err := e1.Subscribe(context.Background(), Subscribe{Topic: "jobs", Partition: 0, Prefetch: 16})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	var held Delivery
	select {
	case held = <-sub.Deliveries:
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery received")
	}

	// A resumed reconnect keeps the incarnation, so the held delivery still
	// settles - through the new engine.
	client2, server2 := net.Pipe()
	go manualAckBroker(server2, ResumeResumed, acks2, false)
	e2, err := startEngine(context.Background(), client2, EngineOptions{ClientName: "t", HeartbeatInterval: time.Hour, settle: sc})
	if err != nil {
		t.Fatalf("startEngine 2: %v", err)
	}
	defer e2.Shutdown()

	// Drop the ORIGIN engine: settling must route to the current one anyway.
	e1.Shutdown()

	if err := held.Complete(); err != nil {
		t.Fatalf("Complete after resumed reconnect: %v", err)
	}
	select {
	case a := <-acks2:
		if len(a.Tags) != 1 || a.Tags[0].Epoch != 42 {
			t.Errorf("current engine got ack tags %v, want [{42}]", a.Tags)
		}
		if a.Topic != "jobs" {
			t.Errorf("ack topic = %q, want jobs", a.Topic)
		}
	case a := <-acks1:
		t.Fatalf("ack routed to the replaced engine, got %v", a)
	case <-time.After(2 * time.Second):
		t.Fatal("current engine did not receive the ack")
	}
}

// A stale delivery is do-not-retry and not a transient transport failure - the
// message redelivers on its own.
func TestStaleDeliveryIsDoNotRetry(t *testing.T) {
	err := &StaleDeliveryError{}
	if AdviseRetry(err) != RetryDoNotRetry {
		t.Errorf("AdviseRetry(StaleDeliveryError) = %q, want do_not_retry", AdviseRetry(err))
	}
	if IsRetryable(err) {
		t.Error("StaleDeliveryError must not be retryable")
	}
}

func TestEngineSubscribeDeliverAck(t *testing.T) {
	client, server := net.Pipe()
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
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, ServerName: "fake", Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			case opSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				so := subscribeOk{SubID: 100, Topic: req.Topic, Partition: req.Partition, Group: req.Group, Prefetch: 16}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				d := deliver{
					SubID: 100, Topic: req.Topic, Group: req.Group, Partition: req.Partition,
					Offset: 7, DeliveryTag: DeliveryTag{Epoch: 42}, Published: 1, PublishReceived: 2,
					ContentType: ContentType{Kind: ContentText}, Payload: []byte("job"),
				}
				_, _ = server.Write(encodeFrame(buildFrame(opDeliver, 5000, encodeDeliver(d))))
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
	defer e.Shutdown()

	sub, err := e.Subscribe(context.Background(), Subscribe{Topic: "jobs", Partition: 0, Prefetch: 16})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sub.SubID != 100 {
		t.Errorf("sub id = %d, want 100", sub.SubID)
	}

	select {
	case d, ok := <-sub.Deliveries:
		if !ok {
			t.Fatal("delivery channel closed early")
		}
		if string(d.Payload) != "job" {
			t.Errorf("payload = %q, want job", d.Payload)
		}
		if d.DeliveryTag.Epoch != 42 {
			t.Errorf("delivery tag epoch = %d, want 42", d.DeliveryTag.Epoch)
		}
		if d.Offset != 7 {
			t.Errorf("offset = %d, want 7", d.Offset)
		}
		if err := e.Complete(d); err != nil {
			t.Errorf("Ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery received")
	}

	select {
	case a := <-acks:
		if len(a.Tags) != 1 || a.Tags[0].Epoch != 42 {
			t.Errorf("broker got ack tags %v, want [{42}]", a.Tags)
		}
		if a.Topic != "jobs" {
			t.Errorf("ack topic = %q, want jobs", a.Topic)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not receive the ack")
	}
}

func TestSubscriptionClosedFrameSurfacesTypedReason(t *testing.T) {
	client, server := net.Pipe()

	go func() {
		br := bufio.NewReader(server)
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			switch f.Opcode {
			case opHello:
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, ServerName: "fake", Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			case opSubscribe:
				req, _ := decodeSubscribe(f.Payload)
				so := subscribeOk{SubID: 71, Topic: req.Topic, Partition: req.Partition, Group: req.Group, Prefetch: 8}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				// Deliver one message, then close the subscription (topic deleted).
				d := deliver{SubID: 71, Topic: req.Topic, Partition: req.Partition, Payload: []byte("last")}
				_, _ = server.Write(encodeFrame(buildFrame(opDeliver, 5000, encodeDeliver(d))))
				sc := subscriptionClosed{SubID: 71, Code: ReasonTopicDeleted, Message: "the queue was deleted"}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscriptionClosed, 0, encodeSubscriptionClosed(sc))))
			case opPing:
				_, _ = server.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	defer e.Shutdown()

	sub, err := e.Subscribe(context.Background(), Subscribe{Topic: "restart.jobs", Partition: 0, Prefetch: 8})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The buffered delivery arrives first.
	select {
	case d, ok := <-sub.Deliveries:
		if !ok || string(d.Payload) != "last" {
			t.Fatalf("first delivery = %q ok=%v, want last", d.Payload, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery received")
	}

	// Then the channel closes and the typed reason is available.
	select {
	case _, ok := <-sub.Deliveries:
		if ok {
			t.Fatal("expected the delivery channel to close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delivery channel did not close")
	}
	reason := sub.CloseReason()
	if reason == nil || reason.Code != ReasonTopicDeleted {
		t.Fatalf("close reason = %+v, want code %d", reason, ReasonTopicDeleted)
	}
}
