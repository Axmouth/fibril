// Example + light test: publish one JSON message and receive it back, verifying
// the payload round-trips.
//
//	FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/roundtrip
package main

import (
	"context"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
	"github.com/Axmouth/fibril/clients/go/examples/internal/exharness"
)

type payload struct {
	Hello string `json:"hello"`
	N     int    `json:"n"`
}

func main() {
	exharness.Run("roundtrip", func() {
		ctx := context.Background()
		c := exharness.Connect("example-roundtrip")
		defer c.Shutdown()

		topic := exharness.UniqueTopic("roundtrip")
		fan, err := c.SubscribeTopic(ctx, topic, nil, 1, true)
		exharness.Check(err == nil, "subscribe")
		defer fan.Close()

		msg, err := fibril.JSON(payload{Hello: "world", N: 42})
		exharness.Check(err == nil, "encode json")
		offset, err := c.Publisher(topic).PublishConfirmed(ctx, msg)
		exharness.Check(err == nil, "confirmed publish")
		exharness.Check(offset >= 0, "confirmed publish returns a broker offset")

		d := exharness.Recv(fan.Deliveries, 5*time.Second, "the round-trip message")
		var got payload
		exharness.Check(d.JSON(&got) == nil, "decode json")
		exharness.AssertEq(got.Hello, "world", "payload.hello")
		exharness.AssertEq(got.N, 42, "payload.n")
	})
}
