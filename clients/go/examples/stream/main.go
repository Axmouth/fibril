// Example + light test: a continuous producer/consumer over a work queue.
//
// Default: runs forever, printing each message as it flows, so you can watch a
// live stream (Ctrl-C to stop).
//
//	FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/stream
//
// Check mode (--check or FIBRIL_CHECK=1): publishes a fixed burst, asserts it all
// comes back, and exits. This is what run-all.sh and CI use.
//
//	go run ./examples/stream --check
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
	"github.com/Axmouth/fibril/clients/go/examples/internal/exharness"
)

const burst = 50

func main() {
	exharness.Run("stream", func() {
		c := exharness.Connect("example-stream")
		defer c.Shutdown()

		topic := exharness.UniqueTopic("stream")
		fan, err := c.SubscribeTopic(context.Background(), topic, nil, 64, true)
		exharness.Check(err == nil, "subscribe")
		defer fan.Close()
		pub := c.Publisher(topic)

		if exharness.CheckMode() {
			for seq := 0; seq < burst; seq++ {
				m, _ := fibril.JSON(map[string]int{"seq": seq})
				exharness.Check(pub.Publish(context.Background(), m) == nil, "publish")
			}
			received := 0
			for received < burst {
				if _, ok := exharness.TryRecv(fan.Deliveries, 5*time.Second); !ok {
					break
				}
				received++
			}
			exharness.Check(received >= burst, fmt.Sprintf("consumed the full burst (got %d/%d)", received, burst))
			return
		}

		// Continuous: produce steadily and print each delivery until interrupted.
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		go func() {
			for seq := 0; ; seq++ {
				m, _ := fibril.JSON(map[string]int{"seq": seq})
				if pub.Publish(context.Background(), m) != nil {
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()
		for {
			select {
			case <-stop:
				return
			case d, ok := <-fan.Deliveries:
				if !ok {
					return
				}
				fmt.Printf("[stream] received %s\n", d.Text())
			}
		}
	})
}
