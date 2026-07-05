package fibril

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestSupervisedSubscriptionSurvivesFailover drops the first connection right
// after one delivery; the supervised subscription must re-attach on a fresh
// connection and keep delivering on the same channel.
func TestSupervisedSubscriptionSurvivesFailover(t *testing.T) {
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
			go serveSupervised(conn, conns.Add(1))
		}
	}()

	c, err := Dial(context.Background(), ln.Addr().String(), ClientOptions{ClientName: "go-test", HeartbeatInterval: time.Hour, SuperviseBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Shutdown()

	ss, err := c.SuperviseSubscribe(context.Background(), Subscribe{Topic: "t", Partition: 0, Prefetch: 8, AutoAck: true})
	if err != nil {
		t.Fatalf("SuperviseSubscribe: %v", err)
	}
	defer ss.Close()

	// First delivery from connection 1, then it drops.
	got1 := recvPayload(t, ss.Deliveries, "first")
	// The supervisor re-attaches on connection 2 and keeps delivering.
	got2 := recvPayload(t, ss.Deliveries, "second (after failover)")

	if got1 != "1" || got2 != "2" {
		t.Errorf("payloads = %q,%q, want 1,2 (one per connection)", got1, got2)
	}
	if n := conns.Load(); n < 2 {
		t.Errorf("broker accepted %d connections, want >= 2", n)
	}
}

func recvPayload(t *testing.T, ch <-chan Delivery, what string) string {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatalf("%s: delivery channel closed", what)
		}
		return string(d.Payload)
	case <-time.After(3 * time.Second):
		t.Fatalf("%s: no delivery", what)
		return ""
	}
}

// serveSupervised answers HELLO, TOPOLOGY, and SUBSCRIBE (pushing one delivery
// whose payload is the connection number). Connection 1 drops after its
// delivery, forcing the supervisor to re-attach.
func serveSupervised(conn net.Conn, n int32) {
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
			// Empty topology: owner stays unknown, so routing uses the bootstrap.
			_, _ = conn.Write(encodeFrame(buildFrame(opTopologyOk, f.RequestID, encodeTopologyOk(TopologyOk{Generation: uint64(n)}))))
		case opSubscribe:
			req, _ := decodeSubscribe(f.Payload)
			so := subscribeOk{SubID: 100, Topic: req.Topic, Partition: req.Partition, Prefetch: 8}
			_, _ = conn.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
			d := deliver{SubID: 100, Topic: req.Topic, Partition: req.Partition, ContentType: ContentType{Kind: ContentText}, Payload: []byte{byte('0' + n)}}
			_, _ = conn.Write(encodeFrame(buildFrame(opDeliver, 5000, encodeDeliver(d))))
			if n == 1 {
				return // drop connection 1 after its delivery
			}
		case opPing:
			_, _ = conn.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
		}
	}
}
