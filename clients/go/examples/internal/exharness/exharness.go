// Package exharness holds shared helpers for the runnable examples. Each example
// doubles as a light end-to-end test: it connects to a real broker, exercises one
// feature, and self-validates, exiting non-zero on failure so run-all.sh (and CI)
// can treat the example suite as a smoke test.
//
// Failures are reported by panic, recovered in Run, so example bodies read like
// the feature they demonstrate rather than threading errors through every step.
// This is a harness convenience, not a pattern for library or application code.
package exharness

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	fibril "github.com/Axmouth/fibril/clients/go"
)

// Addr is the broker address, from FIBRIL_ADDR (default 127.0.0.1:9876).
func Addr() string {
	if a := os.Getenv("FIBRIL_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:9876"
}

// Connect dials the broker with the default example credentials.
func Connect(clientName string) *fibril.Client {
	user := envOr("FIBRIL_USER", "fibril")
	pass := envOr("FIBRIL_PASS", "fibril")
	c, err := fibril.Dial(context.Background(), Addr(), fibril.ClientOptions{
		ClientName: clientName,
		Auth:       &fibril.Auth{Username: user, Password: pass},
	})
	if err != nil {
		panic(fmt.Sprintf("connect: %v", err))
	}
	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// UniqueTopic returns a per-run topic so repeated runs do not interfere.
func UniqueTopic(prefix string) string {
	return fmt.Sprintf("example.%s.%d.%d", prefix, time.Now().UnixNano(), rand.Intn(1_000_000))
}

// CheckMode reports whether to run a bounded, self-validating burst (--check or
// FIBRIL_CHECK=1) instead of a continuous run. Bounded examples ignore it.
func CheckMode() bool {
	if os.Getenv("FIBRIL_CHECK") == "1" {
		return true
	}
	for _, a := range os.Args[1:] {
		if a == "--check" {
			return true
		}
	}
	return false
}

// Check panics with a clear message if cond does not hold.
func Check(cond bool, msg string) {
	if !cond {
		panic("check failed: " + msg)
	}
}

// AssertEq panics unless got equals want.
func AssertEq[T comparable](got, want T, msg string) {
	if got != want {
		panic(fmt.Sprintf("%s: got %v, want %v", msg, got, want))
	}
}

// Recv receives from ch within the timeout, panicking on a closed channel or a
// timeout so an example never hangs the runner.
func Recv[T any](ch <-chan T, within time.Duration, what string) T {
	select {
	case v, ok := <-ch:
		if !ok {
			panic("channel closed waiting for " + what)
		}
		return v
	case <-time.After(within):
		panic("timed out waiting for " + what)
	}
}

// TryRecv returns (value, true) if a value arrives within the timeout, otherwise
// (zero, false). Use it to assert that nothing arrives (e.g. a withheld message).
func TryRecv[T any](ch <-chan T, within time.Duration) (T, bool) {
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(within):
		var zero T
		return zero, false
	}
}

// Run executes an example body, printing PASS/FAIL and exiting with the matching
// code. A panic in the body is reported as a failure.
func Run(name string, body func()) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", name, r)
			os.Exit(1)
		}
	}()
	body()
	fmt.Printf("PASS %s\n", name)
}
