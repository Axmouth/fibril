// Smoke test: connect to a live broker and exercise the full round-trip -
// declare, subscribe, confirmed publish, receive the delivery, and ack.
//
//	go run ./examples/smoke        (broker on 127.0.0.1:9876, fibril/fibril)
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
)

func main() {
	addr := os.Getenv("FIBRIL_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9876"
	}
	c, err := fibril.Dial(context.Background(), addr, fibril.ClientOptions{
		ClientName:  "go-smoke",
		Credentials: &fibril.Credentials{Username: "fibril", Password: "fibril"},
	})
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer c.Shutdown()
	fmt.Println("connected")

	if _, err := c.DeclareQueue(context.Background(), fibril.NewQueueConfig("gosmoke").PartitionCount(1)); err != nil {
		fmt.Println("declare:", err)
		os.Exit(1)
	}
	if _, err := c.FetchTopology(context.Background(), fibril.TopologyRequest{}); err != nil {
		fmt.Println("topology:", err)
		os.Exit(1)
	}

	sub, err := c.Subscribe(context.Background(), fibril.Subscribe{Topic: "gosmoke", Partition: 0, Prefetch: 16})
	if err != nil {
		fmt.Println("subscribe:", err)
		os.Exit(1)
	}

	off, err := c.Publisher("gosmoke").PublishConfirmed(context.Background(), fibril.Text("hello from go"))
	if err != nil {
		fmt.Println("publish:", err)
		os.Exit(1)
	}
	fmt.Printf("confirmed publish, offset=%d\n", off)

	select {
	case d, ok := <-sub.Deliveries:
		if !ok {
			fmt.Println("delivery channel closed")
			os.Exit(1)
		}
		fmt.Printf("delivered: %q offset=%d tag=%d\n", d.Text(), d.Offset, d.DeliveryTag.Epoch)
		if err := d.Complete(); err != nil {
			fmt.Println("ack:", err)
			os.Exit(1)
		}
		fmt.Println("acked")
	case <-time.After(3 * time.Second):
		fmt.Println("no delivery within 3s")
		os.Exit(1)
	}
}
