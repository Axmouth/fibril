// Example + light test: a confirmed publish returns an increasing offset, and a
// delayed publish is withheld before its deadline and arrives after it.
//
//	FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/confirmed-delayed
package main

import (
	"context"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
	"github.com/Axmouth/fibril/clients/go/examples/internal/exharness"
)

func main() {
	exharness.Run("confirmed-delayed", func() {
		c := exharness.Connect("example-confirmed-delayed")
		defer c.Shutdown()

		topic := exharness.UniqueTopic("delayed")
		fan, err := c.SubscribeTopic(context.Background(), topic, nil, 8, true)
		exharness.Check(err == nil, "subscribe")
		defer fan.Close()
		pub := c.Publisher(topic)

		o1, err := pub.PublishConfirmed(context.Background(), fibril.Text("now"))
		exharness.Check(err == nil, "first confirmed publish")
		o2, err := pub.PublishConfirmed(context.Background(), fibril.Text("also now"))
		exharness.Check(err == nil, "second confirmed publish")
		exharness.Check(o2 > o1, "offsets increase across confirmed publishes")

		_, err = pub.PublishDelayedConfirmed(context.Background(), fibril.Text("later"), time.Second)
		exharness.Check(err == nil, "delayed confirmed publish")

		exharness.Recv(fan.Deliveries, 5*time.Second, "first immediate message")
		exharness.Recv(fan.Deliveries, 5*time.Second, "second immediate message")

		if _, ok := exharness.TryRecv(fan.Deliveries, 300*time.Millisecond); ok {
			exharness.Check(false, "the delayed message is withheld before its deadline")
		}

		late := exharness.Recv(fan.Deliveries, 5*time.Second, "the delayed message after its deadline")
		exharness.AssertEq(late.Text(), "later", "delayed payload")
	})
}
