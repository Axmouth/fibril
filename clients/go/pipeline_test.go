package fibril

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnginePublishPipelined(t *testing.T) {
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
			case OpHello:
				ok := HelloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
				_, _ = server.Write(encodeFrame(buildFrame(OpHelloOk, f.RequestID, encodeHelloOk(ok))))
			case OpPublish:
				off := next.Add(1) - 1
				_, _ = server.Write(encodeFrame(buildFrame(OpPublishOk, f.RequestID, encodePublishOk(PublishOk{Offset: off}))))
			case OpPing:
				_, _ = server.Write(encodeFrame(buildFrame(OpPong, f.RequestID, nil)))
			}
		}
	}()

	e, err := startEngine(client, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("startEngine: %v", err)
	}
	defer e.Shutdown()

	// Fire several without awaiting each, then collect.
	const n = 5
	handles := make([]<-chan PublishResult, 0, n)
	for i := 0; i < n; i++ {
		h, err := e.PublishPipelined(Publish{Topic: "t", Payload: []byte("x")})
		if err != nil {
			t.Fatalf("pipelined publish %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	seen := map[uint64]bool{}
	for i, h := range handles {
		select {
		case r := <-h:
			if r.Err != nil {
				t.Fatalf("result %d: %v", i, r.Err)
			}
			seen[r.Offset] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("pipelined result %d timed out", i)
		}
	}
	if len(seen) != n {
		t.Errorf("got %d distinct offsets, want %d", len(seen), n)
	}
}
