// Single-client saturating publish benchmark (unconfirmed), mirroring the
// Python/TS bench_publish. Run a broker on 127.0.0.1:9876, then:
//
//	go run ./examples/bench-publish
//
// Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE (payload bytes), DURATION_S,
// WARMUP_S.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envNum(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func main() {
	addr := env("FIBRIL_ADDR", "127.0.0.1:9876")
	size := int(envNum("SIZE", 1024))
	duration := time.Duration(envNum("DURATION_S", 8) * float64(time.Second))
	warmup := time.Duration(envNum("WARMUP_S", 2) * float64(time.Second))

	c, err := fibril.Dial(context.Background(), addr, fibril.ClientOptions{
		ClientName: "bench",
		Credentials:       &fibril.Credentials{Username: env("FIBRIL_USER", "fibril"), Password: env("FIBRIL_PASS", "fibril")},
	})
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer c.Shutdown()

	pub := c.Publisher("benchtopic")
	msg := fibril.Raw(make([]byte, size))

	loop := func(d time.Duration) int {
		deadline := time.Now().Add(d)
		count := 0
		for time.Now().Before(deadline) {
			for i := 0; i < 512; i++ {
				_ = pub.Publish(context.Background(), msg)
				count++
			}
		}
		return count
	}

	loop(warmup)
	start := time.Now()
	count := loop(duration)
	elapsed := time.Since(start).Seconds()
	fmt.Printf("rate=%.0f msgs/s count=%d elapsed=%.1fs\n", float64(count)/elapsed, count, elapsed)
}
