// Plexus (fan-out stream) throughput benchmark, mirroring the Python/TS
// bench_plexus. A saturating publisher feeds a stream and CONSUMERS independent
// stream subscribers each receive every record (fan-out), so deliver throughput
// is aggregated across all of them. Run together (MODE=both) or alone
// (MODE=pub / MODE=sub) so producer and consumers can run as separate processes.
//
//	go run ./examples/bench-plexus
//
// Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE, DURATION_S, WARMUP_S, TOPIC,
// PARTITIONS, PREFETCH, MODE (both|pub|sub), CONSUMERS, DURABILITY
// (ephemeral|speculative|durable).
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
	topic := env("TOPIC", "benchplexus")
	partitions := uint32(envNum("PARTITIONS", 1))
	prefetch := uint32(envNum("PREFETCH", 4096))
	mode := env("MODE", "both")
	consumers := int(envNum("CONSUMERS", 1))
	durability := fibril.StreamDurability(env("DURABILITY", "durable"))
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

	// Declare the stream once up front (on a throwaway connection) so both roles
	// can run as separate processes.
	admin := dial()
	if _, err := admin.DeclarePlexus(context.Background(), fibril.DeclarePlexus{Topic: topic, PartitionCount: &partitions, Durability: durability}); err != nil {
		fmt.Println("declare plexus:", err)
		os.Exit(1)
	}
	admin.Shutdown()

	var published, delivered atomic.Int64
	running := &atomic.Bool{}
	running.Store(true)

	if doSub {
		for i := 0; i < consumers; i++ {
			consumer := dial()
			defer consumer.Shutdown()
			fi, err := consumer.SubscribeStreamTopic(context.Background(), topic, fibril.StreamSubscribeOptions{
				Start: fibril.StreamStart{Kind: fibril.StreamLatest}, Prefetch: prefetch, AutoAck: true,
			})
			if err != nil {
				fmt.Println("subscribe stream:", err)
				os.Exit(1)
			}
			go func() {
				for range fi.Deliveries {
					delivered.Add(1)
				}
			}()
		}
	}
	if doPub {
		producer := dial()
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

	out := fmt.Sprintf("mode=%s durability=%s", mode, durability)
	if doPub {
		out += fmt.Sprintf("  publish=%.0f", float64(p1-p0)/elapsed)
	}
	if doSub {
		out += fmt.Sprintf("  deliver=%.0f (fanout=%d, per_consumer=%.0f)", float64(d1-d0)/elapsed, consumers, float64(d1-d0)/elapsed/float64(consumers))
	}
	out += fmt.Sprintf("  msgs/s size=%d elapsed=%.1fs", size, elapsed)
	fmt.Println(out)
}
