package fibril

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// fakeBroker speaks just enough of the protocol over conn to exercise the
// engine: it answers HELLO, AUTH, confirmed PUBLISH, and PING. compliance and
// offset let a test steer the handshake reply and the publish offset.
func fakeBroker(conn net.Conn, compliance string, offset uint64) {
	go func() {
		br := bufio.NewReader(conn)
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			switch f.Opcode {
			case opHello:
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, ServerName: "fake", Compliance: compliance}
				_, _ = conn.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			case opAuth:
				_, _ = conn.Write(encodeFrame(buildFrame(opAuthOk, f.RequestID, nil)))
			case opPublish:
				if p, _ := decodePublish(f.Payload); p.RequireConfirm {
					_, _ = conn.Write(encodeFrame(buildFrame(opPublishOk, f.RequestID, encodePublishOk(publishOk{Offset: offset}))))
				}
			case opPing:
				_, _ = conn.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
			}
		}
	}()
}

func TestEngineHandshakeAuthAndPublish(t *testing.T) {
	client, server := net.Pipe()
	fakeBroker(server, ComplianceString, 42)

	e, err := startEngine(context.Background(), client, EngineOptions{
		ClientName:        "go-test",
		Auth:              &Auth{Username: "u", Password: "p"},
		HeartbeatInterval: time.Hour, // keep pings out of this test
	})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	if e.ResumeOutcome != ResumeNew {
		t.Errorf("resume outcome = %q, want new", e.ResumeOutcome)
	}

	off, err := e.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("PublishConfirmed: %v", err)
	}
	if off != 42 {
		t.Errorf("offset = %d, want 42", off)
	}

	if err := e.PublishUnconfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("x")}); err != nil {
		t.Errorf("PublishUnconfirmed: %v", err)
	}

	e.Shutdown()
	if !e.IsClosed() {
		t.Error("engine should be closed after Shutdown")
	}
}

func TestEngineComplianceMismatchFailsHandshake(t *testing.T) {
	client, server := net.Pipe()
	fakeBroker(server, "v=0;wrong", 0)

	_, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err == nil {
		t.Fatal("expected handshake to fail on a compliance mismatch")
	}
	if _, ok := err.(*DisconnectionError); !ok {
		t.Errorf("error = %T, want *DisconnectionError", err)
	}
}

func TestEngineRequestAfterShutdownErrors(t *testing.T) {
	client, server := net.Pipe()
	fakeBroker(server, ComplianceString, 1)

	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	e.Shutdown()

	if _, err := e.PublishConfirmed(context.Background(), Publish{Topic: "t"}); err == nil {
		t.Error("expected an error publishing after shutdown")
	}
}
