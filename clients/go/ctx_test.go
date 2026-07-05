package fibril

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A blocking request must return the context error when the context is cancelled
// or its deadline passes before the broker replies.
func TestRequestHonorsContext(t *testing.T) {
	client, server := net.Pipe()
	go func() {
		br := bufio.NewReader(server)
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			// Answer the handshake, then go silent so requests never resolve.
			if f.Opcode == opHello {
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			}
		}
	}()

	e, err := startEngine(context.Background(), client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	defer e.Shutdown()

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := e.PublishConfirmed(ctx, Publish{Topic: "t", Payload: []byte("x")})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("PublishConfirmed error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(50 * time.Millisecond); cancel() }()
		_, err := e.FetchTopology(ctx, TopologyRequest{})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("FetchTopology error = %v, want Canceled", err)
		}
	})

	t.Run("confirmation-handle", func(t *testing.T) {
		conf, err := e.PublishWithConfirmation(context.Background(), Publish{Topic: "t", Payload: []byte("x")})
		if err != nil {
			t.Fatalf("PublishWithConfirmation: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if _, err := conf.Confirmed(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Confirmed error = %v, want DeadlineExceeded", err)
		}
	})
}
