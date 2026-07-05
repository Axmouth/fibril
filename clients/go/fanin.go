package fibril

// Multi-partition fan-in: subscribing to a whole topic transparently subscribes
// every partition (routed to each owner) and merges their deliveries into one
// channel. Ordering is per-partition only (Kafka-style), as the invariants
// require. Deliveries settle with d.Ack()/d.Nack(), which route to the partition
// they came from.

import "sync"

// FanIn is a subscription across all partitions of a topic. Deliveries yields
// messages from every partition, merged.
type FanIn struct {
	Deliveries <-chan Delivery
	subs       []*Subscription
}

// SubscribeTopic subscribes to every partition of topic and fans the deliveries
// into one channel. It fetches the topology first to learn the partition count,
// then routes each partition to its owner. Prefetch is per partition.
func (c *Client) SubscribeTopic(topic string, group *string, prefetch uint32, autoAck bool) (*FanIn, error) {
	// A whole-topic subscribe needs the real partition count, so warm the cache.
	if _, err := c.FetchTopology(TopologyRequest{Topic: &topic}); err != nil {
		return nil, err
	}
	count := c.topo.partitionCount(topic, group)

	merged := make(chan Delivery, prefetch*count+1)
	fi := &FanIn{Deliveries: merged}
	for p := uint32(0); p < count; p++ {
		sub, err := c.Subscribe(Subscribe{Topic: topic, Partition: p, Group: group, Prefetch: prefetch, AutoAck: autoAck})
		if err != nil {
			// No per-sub unsubscribe yet, so the partitions already subscribed
			// stay until the client is shut down. They are bounded and harmless:
			// the broker stops at prefetch unacked, so their buffers fill and idle.
			return nil, err
		}
		fi.subs = append(fi.subs, sub)
	}

	// One forwarder goroutine per partition; the merged channel closes once every
	// partition's channel has closed.
	var wg sync.WaitGroup
	for _, s := range fi.subs {
		wg.Add(1)
		go func(ch <-chan Delivery) {
			defer wg.Done()
			for d := range ch {
				merged <- d
			}
		}(s.Deliveries)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()

	return fi, nil
}
