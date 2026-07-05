package fibril

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientReconnectsBootstrap drives a real TCP fake broker that drops the
// connection after the first publish; the client must reconnect (re-dialing,
// offering its resume identity) and the next publish must succeed on the new
// connection.
func TestClientReconnectsBootstrap(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var conns atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := conns.Add(1)
			go serveOneConn(conn, n)
		}
	}()

	c, err := Dial(context.Background(), ln.Addr().String(), ClientOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Shutdown()

	off1, err := c.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("a")})
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if off1 != 1 {
		t.Errorf("first offset = %d, want 1 (connection 1)", off1)
	}

	// The broker dropped connection 1 after that publish. The next publish must
	// transparently reconnect and land on connection 2.
	off2, err := c.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("b")})
	if err != nil {
		t.Fatalf("second publish (after reconnect): %v", err)
	}
	if off2 != 2 {
		t.Errorf("second offset = %d, want 2 (connection 2)", off2)
	}
	if n := conns.Load(); n != 2 {
		t.Errorf("broker accepted %d connections, want 2", n)
	}
}

// serveOneConn answers HELLO and PUBLISH. Connection 1 drops right after its
// first publish reply, to force a reconnect. The publish offset echoes the
// connection number so the test can tell them apart.
func serveOneConn(conn net.Conn, n int32) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		f, err := readFrame(br)
		if err != nil {
			return
		}
		switch f.Opcode {
		case opHello:
			ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
			_, _ = conn.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
		case opTopology:
			_, _ = conn.Write(encodeFrame(buildFrame(opTopologyOk, f.RequestID, encodeTopologyOk(TopologyOk{Generation: 1}))))
		case opPublish:
			_, _ = conn.Write(encodeFrame(buildFrame(opPublishOk, f.RequestID, encodePublishOk(publishOk{Offset: uint64(n)}))))
			if n == 1 {
				return // drop connection 1 to force a reconnect
			}
		case opPing:
			_, _ = conn.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
		}
	}
}
