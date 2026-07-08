// Example + light test: a manually-acked delivery that is nacked with requeue is
// redelivered, and after it is acked no further copy arrives.
//
//	FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/manual-ack-retry
package main

import (
	"context"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
	"github.com/Axmouth/fibril/clients/go/examples/internal/exharness"
)

func main() {
	exharness.Run("manual-ack-retry", func() {
		c := exharness.Connect("example-manual-ack-retry")
		defer c.Shutdown()

		topic := exharness.UniqueTopic("ackretry")
		fan, err := c.SubscribeTopic(context.Background(), topic, fibril.TopicSubscribeOptions{Prefetch: 4, AutoAck: false}) // manual ack
		exharness.Check(err == nil, "subscribe")
		defer fan.Close()

		_, err = c.Publisher(topic).PublishConfirmed(context.Background(), fibril.Text("work"))
		exharness.Check(err == nil, "publish")

		first := exharness.Recv(fan.Deliveries, 5*time.Second, "first delivery")
		exharness.AssertEq(first.Text(), "work", "first payload")
		exharness.Check(first.Retry() == nil, "retry with requeue")

		second := exharness.Recv(fan.Deliveries, 5*time.Second, "redelivery after nack")
		exharness.AssertEq(second.Text(), "work", "redelivered payload")
		exharness.Check(second.Complete() == nil, "complete")

		if _, ok := exharness.TryRecv(fan.Deliveries, 500*time.Millisecond); ok {
			exharness.Check(false, "no further delivery after ack")
		}
	})
}
