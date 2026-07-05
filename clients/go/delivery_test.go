package fibril

import (
	"bufio"
	"net"
	"testing"
	"time"
)

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

	e, err := startEngine(client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	defer e.Shutdown()

	sub, err := e.Subscribe(Subscribe{Topic: "jobs", Partition: 0, Prefetch: 16})
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
		if err := e.Ack(d); err != nil {
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
