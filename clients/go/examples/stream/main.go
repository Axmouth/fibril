// Stream smoke: declare a Plexus stream, subscribe (fan-in across partitions),
// publish a record, and receive it.
//
//	go run ./examples/stream        (broker on 127.0.0.1:9876, fibril/fibril)
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
	c, err := fibril.Dial(addr, fibril.ClientOptions{
		ClientName: "go-stream",
		Auth:       &fibril.Auth{Username: "fibril", Password: "fibril"},
	})
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer c.Shutdown()

	one := uint32(1)
	if _, err := c.DeclarePlexus(fibril.DeclarePlexus{Topic: "gostream", PartitionCount: &one, Durability: fibril.StreamDurable}); err != nil {
		fmt.Println("declare plexus:", err)
		os.Exit(1)
	}

	// Subscribe from the latest position before publishing, so we see the record.
	fi, err := c.SubscribeStreamTopic("gostream", fibril.StreamSubscribeOptions{
		Start:    fibril.StreamStart{Kind: fibril.StreamLatest},
		Prefetch: 16,
		AutoAck:  true,
	})
	if err != nil {
		fmt.Println("subscribe stream:", err)
		os.Exit(1)
	}
	defer fi.Close()

	if _, err := c.Publisher("gostream").PublishConfirmed(fibril.Text("stream hello")); err != nil {
		fmt.Println("publish:", err)
		os.Exit(1)
	}
	fmt.Println("published to stream")

	select {
	case d, ok := <-fi.Deliveries:
		if !ok {
			fmt.Println("stream channel closed")
			os.Exit(1)
		}
		fmt.Printf("stream delivered: %q offset=%d partition=%d\n", d.Text(), d.Offset, d.Partition)
	case <-time.After(3 * time.Second):
		fmt.Println("no stream record within 3s")
		os.Exit(1)
	}
}
