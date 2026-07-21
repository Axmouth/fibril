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

// TestClientExplicitReconnectReturnsOutcome drives Reconnect against a broker that
// resumes the second connection, and checks the returned outcome and that a fresh
// connection was actually dialed.
func TestClientExplicitReconnectReturnsOutcome(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var conns atomic.Int32
	go acceptReconnect(ln, &conns, false)

	c, err := Dial(context.Background(), ln.Addr().String(), ClientOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Shutdown()

	outcome, err := c.Reconnect(context.Background())
	if err != nil {
		t.Fatalf("Reconnect: %v", err)
	}
	if outcome != ResumeResumed {
		t.Errorf("resume outcome = %q, want resumed", outcome)
	}
	if n := conns.Load(); n != 2 {
		t.Errorf("broker accepted %d connections, want 2", n)
	}
}

// TestDisableAutoReconnectSurfacesCloseError checks that with auto-reconnect off, a
// dropped connection is not transparently redialed: the next op surfaces an error
// and no new connection is opened.
func TestDisableAutoReconnectSurfacesCloseError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var conns atomic.Int32
	go acceptReconnect(ln, &conns, true)

	c, err := Dial(context.Background(), ln.Addr().String(), ClientOptions{ClientName: "go-test", HeartbeatInterval: time.Hour, DisableAutoReconnect: true})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Shutdown()

	if _, err := c.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("a")}); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	// The broker dropped connection 1. Once the client notices, the next publish
	// must fail rather than reconnect.
	deadline := time.Now().Add(2 * time.Second)
	var pubErr error
	for time.Now().Before(deadline) {
		if _, pubErr = c.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("b")}); pubErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pubErr == nil {
		t.Fatal("expected an error with auto-reconnect disabled")
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("broker accepted %d connections, want 1 (no reconnect)", n)
	}
}

// acceptReconnect accepts connections and serves each with reconnectBroker,
// numbering them so a test can assert how many were opened.
func acceptReconnect(ln net.Listener, conns *atomic.Int32, dropFirstPublish bool) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		n := conns.Add(1)
		go reconnectBroker(conn, n, dropFirstPublish)
	}
}

// reconnectBroker answers HELLO (resuming the second and later connections),
// TOPOLOGY, PUBLISH, and PING. When dropFirstPublish is set, connection 1 closes
// right after its first publish reply so the client sees the loss.
func reconnectBroker(conn net.Conn, n int32, dropFirstPublish bool) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		f, err := readFrame(br)
		if err != nil {
			return
		}
		switch f.Opcode {
		case opHello:
			outcome := ResumeNew
			if n >= 2 {
				outcome = ResumeResumed
			}
			ok := helloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: outcome, Compliance: ComplianceString}
			_, _ = conn.Write(encodeFrame(buildFrame(opHelloOk, f.RequestID, encodeHelloOk(ok))))
		case opTopology:
			_, _ = conn.Write(encodeFrame(buildFrame(opTopologyOk, f.RequestID, encodeTopologyOk(TopologyOk{Generation: 1}))))
		case opPublish:
			_, _ = conn.Write(encodeFrame(buildFrame(opPublishOk, f.RequestID, encodePublishOk(publishOk{Offset: uint64(n)}))))
			if dropFirstPublish && n == 1 {
				return
			}
		case opPing:
			_, _ = conn.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
		}
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
