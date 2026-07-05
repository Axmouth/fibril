package fibril

// Multi-partition fan-in: subscribing to a whole topic (queue or stream)
// transparently subscribes every partition, supervised so each survives an owner
// failover, and merges their deliveries into one channel. Ordering is
// per-partition only (Kafka-style), as the invariants require. Deliveries settle
// with d.Ack()/d.Nack(), which route to the partition they came from.

import "sync"

// FanIn is a subscription across all partitions of a topic. Deliveries yields
// messages from every partition, merged. Call Close to stop it.
type FanIn struct {
	Deliveries <-chan Delivery
	subs       []*SupervisedSubscription
}

// Close stops every partition subscription; Deliveries closes once they drain.
func (fi *FanIn) Close() {
	for _, s := range fi.subs {
		s.Close()
	}
}

// mergeFanIn merges the supervised partition subscriptions into one channel, one
// forwarder goroutine per partition, closing the merged channel once all close.
func mergeFanIn(subs []*SupervisedSubscription, bufferPerSub uint32) *FanIn {
	merged := make(chan Delivery, bufferPerSub*uint32(len(subs))+1)
	fi := &FanIn{Deliveries: merged, subs: subs}
	var wg sync.WaitGroup
	for _, s := range subs {
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
	return fi
}

func closeAll(subs []*SupervisedSubscription) {
	for _, s := range subs {
		s.Close()
	}
}

// SubscribeTopic subscribes to every partition of a queue topic and fans the
// deliveries into one channel, supervising each partition. Prefetch is per
// partition.
func (c *Client) SubscribeTopic(topic string, group *string, prefetch uint32, autoAck bool) (*FanIn, error) {
	if _, err := c.FetchTopology(TopologyRequest{Topic: &topic}); err != nil {
		return nil, err
	}
	count := c.topo.partitionCount(topic, group)
	subs := make([]*SupervisedSubscription, 0, count)
	for p := uint32(0); p < count; p++ {
		ss, err := c.SuperviseSubscribe(Subscribe{Topic: topic, Partition: p, Group: group, Prefetch: prefetch, AutoAck: autoAck})
		if err != nil {
			closeAll(subs)
			return nil, err
		}
		subs = append(subs, ss)
	}
	return mergeFanIn(subs, prefetch), nil
}

// StreamSubscribeOptions configure a whole-stream (Plexus) fan-in subscription.
type StreamSubscribeOptions struct {
	Start       StreamStart
	Filter      []StreamFilter
	DurableName *string
	Prefetch    uint32
	AutoAck     bool
}

// SubscribeStreamTopic subscribes to every partition of a Plexus stream and fans
// the records into one channel, supervising each partition. Every consumer sees
// every record (fan-out); the client reads all partitions and fans them in.
func (c *Client) SubscribeStreamTopic(topic string, opts StreamSubscribeOptions) (*FanIn, error) {
	if _, err := c.FetchTopology(TopologyRequest{Topic: &topic}); err != nil {
		return nil, err
	}
	count := c.topo.partitionCount(topic, nil) // streams have no group
	subs := make([]*SupervisedSubscription, 0, count)
	for p := uint32(0); p < count; p++ {
		ss, err := c.SuperviseSubscribeStream(SubscribeStream{
			Topic:       topic,
			Partition:   p,
			DurableName: opts.DurableName,
			Start:       opts.Start,
			Filter:      opts.Filter,
			Prefetch:    opts.Prefetch,
			AutoAck:     opts.AutoAck,
		})
		if err != nil {
			closeAll(subs)
			return nil, err
		}
		subs = append(subs, ss)
	}
	return mergeFanIn(subs, opts.Prefetch), nil
}
