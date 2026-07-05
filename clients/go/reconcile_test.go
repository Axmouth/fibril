package fibril

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// A non-supervised subscription must survive an owner bounce: after the broker
// drops the connection, the client reconnects, re-announces the subscription
// (RECONCILE_CLIENT), and keeps yielding on the same Deliveries channel.
func TestReconcileRestoresSubscriptionAcrossReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	reconcileSent := make(chan ReconcileClient, 1)
	go func() {
		connN := 0
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			connN++
			go serveReconcileBroker(conn, connN, reconcileSent)
		}
	}()

	c, err := Dial(addr, ClientOptions{ClientName: "t", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Shutdown()

	sub, err := c.Subscribe(Subscribe{Topic: "t", Partition: 0, Prefetch: 4, AutoAck: true})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	recvDelivery(t, sub.Deliveries, "first")

	// Wait for the broker's bounce to drop the bootstrap connection.
	for i := 0; i < 200; i++ {
		c.bootstrapMu.Lock()
		closed := c.bootstrap.IsClosed()
		c.bootstrapMu.Unlock()
		if closed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// An operation reconnects, which runs the reconcile handshake.
	_, _ = c.FetchTopology(TopologyRequest{})

	select {
	case rc := <-reconcileSent:
		if len(rc.Subscriptions) != 1 || rc.Subscriptions[0].Topic != "t" {
			t.Errorf("RECONCILE_CLIENT subscriptions = %+v, want one for topic t", rc.Subscriptions)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no RECONCILE_CLIENT sent on reconnect")
	}

	// The restored subscription yields the post-reconnect delivery on the same channel.
	recvDelivery(t, sub.Deliveries, "second")
}

func recvDelivery(t *testing.T, ch <-chan Delivery, want string) {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatalf("Deliveries closed, wanted %q", want)
		}
		if string(d.Payload) != want {
			t.Errorf("payload = %q, want %q", d.Payload, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for delivery %q", want)
	}
}

func serveReconcileBroker(conn net.Conn, connN int, reconcileSent chan ReconcileClient) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		f, err := readFrame(br)
		if err != nil {
			return
		}
		switch f.Opcode {
		case OpHello:
			ok := HelloOk{ProtocolVersion: ProtocolV1, ResumeOutcome: ResumeNew, Compliance: ComplianceString}
			_, _ = conn.Write(encodeFrame(buildFrame(OpHelloOk, f.RequestID, encodeHelloOk(ok))))
		case OpSubscribe:
			req, _ := decodeSubscribe(f.Payload)
			so := SubscribeOk{SubID: 1, Topic: req.Topic, Partition: req.Partition, Prefetch: req.Prefetch}
			_, _ = conn.Write(encodeFrame(buildFrame(OpSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
			d := Deliver{SubID: 1, Topic: req.Topic, Partition: req.Partition, ContentType: ContentType{Kind: ContentText}, Payload: []byte("first")}
			_, _ = conn.Write(encodeFrame(buildFrame(OpDeliver, 9000, encodeDeliver(d))))
			if connN == 1 {
				time.Sleep(30 * time.Millisecond)
				return // bounce conn1 to force reconnect + reconcile
			}
		case OpReconcileClient:
			rc, _ := decodeReconcileClient(f.Payload)
			reconcileSent <- rc
			results := make([]ReconcileSubscriptionResult, 0, len(rc.Subscriptions))
			for i := range rc.Subscriptions {
				s := rc.Subscriptions[i]
				results = append(results, ReconcileSubscriptionResult{Client: &s, Server: &s, Action: ReconcileKeep})
			}
			_, _ = conn.Write(encodeFrame(buildFrame(OpReconcileResult, f.RequestID, encodeReconcileResult(ReconcileResult{Subscriptions: results}))))
			for _, s := range rc.Subscriptions {
				d := Deliver{SubID: s.SubID, Topic: s.Topic, Partition: s.Partition, ContentType: ContentType{Kind: ContentText}, Payload: []byte("second")}
				_, _ = conn.Write(encodeFrame(buildFrame(OpDeliver, 9001, encodeDeliver(d))))
			}
		case OpTopology:
			_, _ = conn.Write(encodeFrame(buildFrame(OpTopologyOk, f.RequestID, encodeTopologyOk(TopologyOk{Generation: 1}))))
		case OpPing:
			_, _ = conn.Write(encodeFrame(buildFrame(OpPong, f.RequestID, nil)))
		}
	}
}

func TestReconcileResultCodecRoundTrip(t *testing.T) {
	client := ReconcileSubscription{SubID: 7, Topic: "t", Partition: 2, Prefetch: 16, AutoAck: true}
	in := ReconcileResult{Subscriptions: []ReconcileSubscriptionResult{
		{Client: &client, Server: &client, Action: ReconcileKeep, Reason: "kept"},
		{Client: &client, Server: nil, Action: ReconcileCloseClientSide, Reason: "gone"},
		{Client: nil, Server: nil, Action: ReconcileRecreateClient, Reason: ""},
	}}
	out, err := decodeReconcileResult(encodeReconcileResult(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Subscriptions) != 3 {
		t.Fatalf("got %d results, want 3", len(out.Subscriptions))
	}
	if out.Subscriptions[0].Action != ReconcileKeep || out.Subscriptions[0].Server == nil || out.Subscriptions[0].Server.SubID != 7 {
		t.Errorf("result 0 = %+v", out.Subscriptions[0])
	}
	if out.Subscriptions[1].Server != nil || out.Subscriptions[1].Action != ReconcileCloseClientSide {
		t.Errorf("result 1 = %+v", out.Subscriptions[1])
	}
	if out.Subscriptions[2].Client != nil || out.Subscriptions[2].Reason != "" {
		t.Errorf("result 2 = %+v", out.Subscriptions[2])
	}
}
