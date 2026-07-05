package fibril

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestTopicGlobMatches(t *testing.T) {
	cases := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"events.*", "events.a", true},
		{"events.*", "events.orders.new", true},
		{"events.*", "events.", true},
		{"events.*", "other.a", false},
		{"events.*", "event", false},
		{"*", "anything.at.all", true},
		{"*", "", true},
		{"exact", "exact", true},
		{"exact", "exacts", false},
		{"a*b", "aXb", true},
		{"a*b", "ab", true},
		{"a*b", "aXbY", false},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "abc", true},
		{"a*b*c", "aXc", false},
	}
	for _, c := range cases {
		if got := newTopicGlob(c.pattern).matches(c.value); got != c.want {
			t.Errorf("glob %q matches %q = %v, want %v", c.pattern, c.value, got, c.want)
		}
	}
}

// A pattern subscription fans in across the queues whose topic matches the glob
// and excludes the ones that do not, driven by the cluster topology (which a
// single-node broker does not advertise, so a synthetic one is served here).
func TestPatternSubscribeFansInMatching(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	// The advertised catalogue: two matching queues and one that does not match.
	all := []string{"events.a", "events.b", "other.x"}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go servePatternBroker(conn, all)
		}
	}()

	c, err := Dial(addr, ClientOptions{ClientName: "t", HeartbeatInterval: time.Hour})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Shutdown()

	ps, err := c.Routing().SubscribePattern("events.*", PatternSubscribeOptions{Prefetch: 4, AutoAck: true})
	if err != nil {
		t.Fatalf("subscribe pattern: %v", err)
	}
	defer ps.Close()

	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case d := <-ps.Deliveries:
			if d.Topic != "events.a" && d.Topic != "events.b" {
				t.Fatalf("delivery from a non-matching topic: %s", d.Topic)
			}
			got[d.Topic] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("only received from %v within timeout, want events.a and events.b", got)
		}
	}
	// The non-matching queue must not be delivered.
	select {
	case d := <-ps.Deliveries:
		t.Fatalf("unexpected extra delivery from %s (non-matching topic should be excluded)", d.Topic)
	case <-time.After(300 * time.Millisecond):
	}
}

func servePatternBroker(conn net.Conn, catalogue []string) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	var nextSub uint64
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
			req, _ := decodeTopologyRequest(f.Payload)
			topo := TopologyOk{Generation: 1}
			for _, topic := range catalogue {
				if req.Topic != nil && *req.Topic != topic {
					continue
				}
				topo.Queues = append(topo.Queues, QueueTopologyEntry{Topic: topic, Partition: 0, PartitionCount: 1})
			}
			_, _ = conn.Write(encodeFrame(buildFrame(opTopologyOk, f.RequestID, encodeTopologyOk(topo))))
		case opSubscribe:
			req, _ := decodeSubscribe(f.Payload)
			nextSub++
			so := subscribeOk{SubID: nextSub, Topic: req.Topic, Partition: req.Partition, Prefetch: req.Prefetch}
			_, _ = conn.Write(encodeFrame(buildFrame(opSubscribeOk, f.RequestID, encodeSubscribeOk(so))))
			d := deliver{SubID: nextSub, Topic: req.Topic, Partition: req.Partition, ContentType: ContentType{Kind: ContentText}, Payload: []byte(req.Topic)}
			_, _ = conn.Write(encodeFrame(buildFrame(opDeliver, 8000+nextSub, encodeDeliver(d))))
		case opPing:
			_, _ = conn.Write(encodeFrame(buildFrame(opPong, f.RequestID, nil)))
		}
	}
}
