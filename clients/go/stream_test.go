package fibril

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

func TestEngineSubscribeStreamDelivers(t *testing.T) {
	client, server := net.Pipe()
	var gotStart StreamStartKind = 255

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
			case opSubscribeStream:
				req, _ := decodeSubscribeStream(f.Payload)
				gotStart = req.Start.Kind
				so := subscribeOk{SubID: 200, Topic: req.Topic, Partition: req.Partition, Prefetch: 8}
				_, _ = server.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
				d := deliver{SubID: 200, Topic: req.Topic, Partition: req.Partition, ContentType: ContentType{Kind: ContentText}, Payload: []byte("rec")}
				_, _ = server.Write(encodeFrame(buildFrame(opDeliver, 7000, encodeDeliver(d))))
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

	sub, err := e.SubscribeStream(context.Background(), SubscribeStream{Topic: "s", Partition: 0, Prefetch: 8, Start: StreamStart{Kind: StreamEarliest}, AutoAck: true})
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if sub.SubID != 200 {
		t.Errorf("sub id = %d, want 200", sub.SubID)
	}

	select {
	case d := <-sub.Deliveries:
		if string(d.Payload) != "rec" {
			t.Errorf("payload = %q, want rec", d.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no stream delivery")
	}
	if gotStart != StreamEarliest {
		t.Errorf("broker saw start kind %d, want earliest (%d)", gotStart, StreamEarliest)
	}
}
