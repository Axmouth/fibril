# Fibril Go client

Go client for the [Fibril](https://fibril.sh) message broker. It follows the
Rust reference client (`crates/client`) where that fits Go, and uses Go idioms
where they read better: `context.Context` on every blocking call, channels for
deliveries, and plain error returns.

## Status

Pre-alpha. No version has been tagged yet, and the API tracks the source tree.

The module path is `github.com/Axmouth/fibril/clients/go`. It lives in a
subdirectory of the Fibril repo, so its versions are git tags of the form
`clients/go/vX.Y.Z` (for example `clients/go/v0.4.0`), not the repo's plain
`vX.Y.Z` release tags.

Until a tag exists, depend on a commit:

```sh
go get github.com/Axmouth/fibril/clients/go@main
```

or point a `replace` directive at a local checkout:

```
replace github.com/Axmouth/fibril/clients/go => ../fibril/clients/go
```

Once tagged, it will install with:

```sh
go get github.com/Axmouth/fibril/clients/go@latest
```

Requires Go 1.23 or newer. There are no third-party dependencies.

## Quick start

```go
package main

import (
	"context"
	"log"

	fibril "github.com/Axmouth/fibril/clients/go"
)

type Order struct {
	ID   int    `json:"id"`
	Item string `json:"item"`
}

func main() {
	ctx := context.Background()

	client, err := fibril.Dial(ctx, "localhost:9876", fibril.ClientOptions{
		ClientName:  "orders-worker",
		Credentials: &fibril.Credentials{Username: "user", Password: "pass"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Declare a queue. Only the first declare sets its options.
	if _, err := client.DeclareQueue(ctx, fibril.NewQueueConfig("orders").Durable()); err != nil {
		log.Fatal(err)
	}

	// Subscribe with manual ack across every partition of the topic.
	sub, err := client.SubscribeTopic(ctx, "orders", fibril.TopicSubscribeOptions{Prefetch: 100})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Publish and wait for the broker to confirm the offset.
	msg, err := fibril.JSON(Order{ID: 42, Item: "widget"})
	if err != nil {
		log.Fatal(err)
	}
	offset, err := client.Publisher("orders").PublishConfirmed(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published at offset %d", offset)

	for d := range sub.Deliveries {
		var order Order
		if err := d.JSON(&order); err != nil {
			_ = d.Fail() // not requeued: dead-lettered or dropped per queue policy
			continue
		}
		if err := process(order); err != nil {
			_ = d.Retry() // requeue for redelivery
			continue
		}
		_ = d.Complete()
	}
}

func process(Order) error { return nil }
```

Set `AutoAck: true` in `TopicSubscribeOptions` when deliveries need no
settlement. `RetryAfter(delay)` requeues a delivery no sooner than `delay` from
now.

## Queue durability

`QueueConfig` picks how a queue confirms and delivers. `Durable()` waits until
each required copy has synced a message to disk. `Buffered()` waits only until
each required copy has written it to the OS, before fsync, so power loss or a
storage failure on every required node at once can lose recent confirmed
messages. Leaving both out keeps the broker default.

Only the first declare of a queue sets its mode. A later declare naming another
mode fails with error code 409.

## Examples

Runnable examples live in `examples/`. Each connects to a real broker, exercises
one feature, and exits non-zero on failure.

```sh
# Start a broker, run every example, tear it down:
examples/run-all.sh

# Or run one against an already-running broker:
FIBRIL_ADDR=127.0.0.1:9876 go run ./examples/roundtrip
```

## Development

```sh
cd clients/go
go vet ./...
go test ./...
```

## More

- [Client architecture](https://github.com/Axmouth/fibril/blob/main/clients/ARCHITECTURE.md), the layering and invariants shared by every client
- [Feature matrix](https://github.com/Axmouth/fibril/blob/main/clients/FEATURE_MATRIX.md), what each client supports
- [Documentation](https://fibril.sh)

## License

MIT. See [AI_POLICY.md](https://github.com/Axmouth/fibril/blob/main/AI_POLICY.md)
for the text and data mining reservation that comes with this module.
