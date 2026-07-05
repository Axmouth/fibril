package fibril

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnginePublishWithConfirmation(t *testing.T) {
	client, server := net.Pipe()

	go func() {
		br := bufio.NewReader(server)
		var next atomic.Uint64
		for {
			f, err := readFrame(br)
			if err != nil {
				return
			}
			switch f.Opcode {
			case opHello:
				ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
			case opPublish:
				off := next.Add(1) - 1
				_, _ = server.Write(encodeFrame(buildFrame(opPublishOk, f.RequestID, encodePublishOk(publishOk{Offset: off}))))
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

	// Fire several without awaiting each, then collect.
	const n = 5
	handles := make([]PublishConfirmation, 0, n)
	for i := 0; i < n; i++ {
		h, err := e.PublishWithConfirmation(context.Background(), Publish{Topic: "t", Payload: []byte("x")})
		if err != nil {
			t.Fatalf("with-confirmation publish %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	seen := map[uint64]bool{}
	for i, h := range handles {
		offset, err := h.Confirmed(context.Background())
		if err != nil {
			t.Fatalf("confirmation %d: %v", i, err)
		}
		seen[offset] = true
	}
	if len(seen) != n {
		t.Errorf("got %d distinct offsets, want %d", len(seen), n)
	}
}
