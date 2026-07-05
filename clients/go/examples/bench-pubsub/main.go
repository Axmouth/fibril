// Combined publish + deliver throughput benchmark, mirroring the Python/TS
// bench_pubsub. A saturating publisher and a consumer run together (MODE=both)
// or alone (MODE=pub / MODE=sub) so they can run as separate processes on their
// own cores.
//
//	go run ./examples/bench-pubsub
//
// Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE, DURATION_S, WARMUP_S, TOPIC,
// GROUP, PREFETCH, MODE (both|pub|sub).
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
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
	topic := env("TOPIC", "benchpubsub")
	prefetch := uint32(envNum("PREFETCH", 4096))
	mode := env("MODE", "both")
	autoAck := env("ACK", "auto") != "manual"
	doPub := mode == "both" || mode == "pub"
	doSub := mode == "both" || mode == "sub"

	auth := &fibril.Auth{Username: env("FIBRIL_USER", "fibril"), Password: env("FIBRIL_PASS", "fibril")}
	dial := func() *fibril.Client {
		c, err := fibril.Dial(context.Background(), addr, fibril.ClientOptions{ClientName: "bench", Auth: auth})
		if err != nil {
			fmt.Println("connect:", err)
			os.Exit(1)
		}
		return c
	}

	var published, delivered atomic.Int64
	running := &atomic.Bool{}
	running.Store(true)

	var producer, consumer *fibril.Client
	if doSub {
		consumer = dial()
		defer consumer.Shutdown()
		fi, err := consumer.SubscribeTopic(context.Background(), topic, nil, prefetch, autoAck)
		if err != nil {
			fmt.Println("subscribe:", err)
			os.Exit(1)
		}
		go func() {
			for d := range fi.Deliveries {
				if !autoAck {
					_ = d.Ack()
				}
				delivered.Add(1)
			}
		}()
	}
	if doPub {
		producer = dial()
		defer producer.Shutdown()
		pub := producer.Publisher(topic)
		msg := fibril.Raw(make([]byte, size))
		go func() {
			for running.Load() {
				_ = pub.Publish(context.Background(), msg)
				published.Add(1)
			}
		}()
	}

	time.Sleep(warmup)
	p0, d0 := published.Load(), delivered.Load()
	start := time.Now()
	time.Sleep(duration)
	elapsed := time.Since(start).Seconds()
	p1, d1 := published.Load(), delivered.Load()
	running.Store(false)

	out := fmt.Sprintf("mode=%s", mode)
	if doPub {
		out += fmt.Sprintf("  publish=%.0f", float64(p1-p0)/elapsed)
	}
	if doSub {
		out += fmt.Sprintf("  deliver=%.0f", float64(d1-d0)/elapsed)
	}
	out += fmt.Sprintf("  msgs/s size=%d elapsed=%.1fs", size, elapsed)
	if doPub && doSub {
		out += fmt.Sprintf("  backlog=%d", p1-d1)
	}
	fmt.Println(out)
}
