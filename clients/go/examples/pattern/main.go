// Demo: pattern (discovery) subscribe. Fan in across every work queue whose topic
// matches a glob, with new matching queues attaching automatically. Discovery is
// driven by the cluster topology, so this needs a multi-node broker that
// advertises topology (a single-node broker advertises none). Run a cluster, then:
//
//	FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/pattern
//
// Pattern subscribe is validated in CI by a fake-broker test (pattern_test.go),
// which serves a synthetic topology a single-node broker would not.
package main

import (
	"context"
	"fmt"

	fibril "github.com/Axmouth/fibril/clients/go"
	"github.com/Axmouth/fibril/clients/go/examples/internal/exharness"
)

func main() {
	c := exharness.Connect("example-pattern")
	defer c.Shutdown()

	ps, err := c.Routing().SubscribePattern(context.Background(), "events.*", fibril.PatternSubscribeOptions{Prefetch: 16, AutoAck: true})
	if err != nil {
		fmt.Println("subscribe pattern:", err)
		return
	}
	defer ps.Close()

	fmt.Println("listening for events.* (new matching queues attach automatically; Ctrl-C to stop)")
	for d := range ps.Deliveries {
		fmt.Printf("%s: %s\n", d.Topic, d.Text())
		_ = d.Ack()
	}
}
