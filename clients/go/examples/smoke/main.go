// Smoke test: connect to a live broker, authenticate, and confirm a publish.
//
//	go run ./examples/smoke        (broker on 127.0.0.1:9876, fibril/fibril)
package main

import (
	"fmt"
	"os"

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
	off, err := e.PublishConfirmed(fibril.Publish{Topic: "gosmoke", Payload: []byte("hello from go")})
	if err != nil {
		fmt.Println("publish:", err)
		os.Exit(1)
	}
	fmt.Printf("confirmed publish, offset=%d\n", off)
}
