// Example + light test: Plexus fan-out. Declare a stream, attach two independent
// subscribers, publish one record, and verify both subscribers receive it (a
// stream fans out to every consumer, unlike a work queue).
//
//	FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/plexus
package main

import (
	"context"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
	"github.com/Axmouth/fibril/clients/go/examples/internal/exharness"
)

func main() {
	exharness.Run("plexus", func() {
		c := exharness.Connect("example-plexus")
		defer c.Shutdown()

		topic := exharness.UniqueTopic("plexus")
		one := uint32(1)
		_, err := c.DeclarePlexus(context.Background(), fibril.DeclarePlexus{Topic: topic, PartitionCount: &one, Durability: fibril.StreamDurable})
		exharness.Check(err == nil, "declare plexus")

		opts := fibril.StreamSubscribeOptions{Start: fibril.StreamStart{Kind: fibril.StreamLatest}, Prefetch: 16, AutoAck: true}
		subA, err := c.SubscribeStreamTopic(context.Background(), topic, opts)
		exharness.Check(err == nil, "subscribe A")
		defer subA.Close()
		subB, err := c.SubscribeStreamTopic(context.Background(), topic, opts)
		exharness.Check(err == nil, "subscribe B")
		defer subB.Close()

		_, err = c.Publisher(topic).PublishConfirmed(context.Background(), fibril.Text("stream hello"))
		exharness.Check(err == nil, "publish")

		a := exharness.Recv(subA.Deliveries, 5*time.Second, "record on subscriber A")
		b := exharness.Recv(subB.Deliveries, 5*time.Second, "record on subscriber B")
		exharness.AssertEq(a.Text(), "stream hello", "subscriber A payload")
		exharness.AssertEq(b.Text(), "stream hello", "subscriber B payload (fan-out)")
	})
}
