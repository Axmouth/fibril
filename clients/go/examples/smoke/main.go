// Smoke test: connect to a live broker and exercise the full round-trip -
// declare, subscribe, confirmed publish, receive the delivery, and ack.
//
//	go run ./examples/smoke        (broker on 127.0.0.1:9876, fibril/fibril)
package main

import (
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
	e, err := fibril.Connect(addr, fibril.EngineOptions{
		ClientName: "go-smoke",
		Auth:       &fibril.Auth{Username: "fibril", Password: "fibril"},
	})
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer e.Shutdown()
	fmt.Printf("connected: resume=%s\n", e.ResumeOutcome)

	one := uint32(1)
	if _, err := e.DeclareQueue(fibril.DeclareQueue{Topic: "gosmoke", PartitionCount: &one}); err != nil {
		fmt.Println("declare:", err)
		os.Exit(1)
	}

	sub, err := e.Subscribe(fibril.Subscribe{Topic: "gosmoke", Partition: 0, Prefetch: 16})
	if err != nil {
		fmt.Println("subscribe:", err)
		os.Exit(1)
	}

	off, err := e.PublishConfirmed(fibril.Publish{Topic: "gosmoke", Payload: []byte("hello from go")})
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
		fmt.Printf("delivered: %q offset=%d tag=%d\n", d.Payload, d.Offset, d.DeliveryTag.Epoch)
		if err := e.Ack(d); err != nil {
			fmt.Println("ack:", err)
			os.Exit(1)
		}
		fmt.Println("acked")
	case <-time.After(3 * time.Second):
		fmt.Println("no delivery within 3s")
		os.Exit(1)
	}
}
