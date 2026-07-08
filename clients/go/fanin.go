package fibril

// Multi-partition fan-in: subscribing to a whole topic (queue or stream)
// transparently subscribes every partition, supervised so each survives an owner
// failover, and merges their deliveries into one channel. Ordering is
// per-partition only (Kafka-style), as the invariants require. Deliveries settle
// with d.Ack()/d.Nack(), which route to the partition they came from.
//
// A background growth loop refreshes topology on an interval and attaches any
// partitions added by a live repartition grow, so a fan-in picks up new
// partitions without the caller re-subscribing.

import (
	"context"
	"sync"
	"time"
)

const defaultRepartitionPoll = 2 * time.Second

// FanIn is a subscription across all partitions of a topic. Deliveries yields
// messages from every partition, merged. Call Close to stop it.
type FanIn struct {
	Deliveries <-chan Delivery

	merged chan Delivery
	client *Client
	topic  string
	group  *string // nil for streams
	attach func(ctx context.Context, partition uint32) (*SupervisedSubscription, error)
	cancel chan struct{}

	mu      sync.Mutex
	covered map[uint32]bool
	subs    []*SupervisedSubscription
	closed  bool
	wg      sync.WaitGroup // one per live forwarder
}

// newFanIn subscribes the current partition set (strictly, failing the call if a
// partition cannot be attached) and starts the growth loop.
func (c *Client) newFanIn(ctx context.Context, topic string, group *string, bufferPerSub uint32, attach func(context.Context, uint32) (*SupervisedSubscription, error)) (*FanIn, error) {
	if _, err := c.FetchTopology(ctx, TopologyRequest{Topic: &topic}); err != nil {
		return nil, err
	}
	count := c.topo.partitionCount(topic, group)
	merged := make(chan Delivery, bufferPerSub*count+1)
	fi := &FanIn{
		Deliveries: merged,
		merged:     merged,
		client:     c,
		topic:      topic,
		group:      group,
		attach:     attach,
		cancel:     make(chan struct{}),
		covered:    map[uint32]bool{},
	}
	for p := uint32(0); p < count; p++ {
		if err := fi.attachPartition(ctx, p, true); err != nil {
			fi.Close()
			return nil, err
		}
	}
	interval := c.opts.RepartitionPollInterval
	if interval == 0 {
		interval = defaultRepartitionPoll
	}
	go fi.growthLoop(interval)
	return fi, nil
}

// attachPartition attaches one partition, reserving it first so concurrent polls
// and the initial pass do not double-subscribe. When strict is false a failed
// attach is un-reserved so a later poll retries. Holds no lock across the network
// subscribe.
func (fi *FanIn) attachPartition(ctx context.Context, p uint32, strict bool) error {
	fi.mu.Lock()
	if fi.closed || fi.covered[p] {
		fi.mu.Unlock()
		return nil
	}
	fi.covered[p] = true
	fi.mu.Unlock()

	ss, err := fi.attach(ctx, p)
	if err != nil {
		fi.mu.Lock()
		delete(fi.covered, p)
		fi.mu.Unlock()
		if strict {
			return err
		}
		return nil // best effort: retried on the next poll
	}

	fi.mu.Lock()
	if fi.closed {
		fi.mu.Unlock()
		ss.Close()
		return nil
	}
	fi.subs = append(fi.subs, ss)
	fi.wg.Add(1)
	fi.mu.Unlock()
	go fi.forward(ss.Deliveries)
	return nil
}

func (fi *FanIn) forward(ch <-chan Delivery) {
	defer fi.wg.Done()
	for {
		select {
		case d, ok := <-ch:
			if !ok {
				return
			}
			select {
			case fi.merged <- d:
			case <-fi.cancel:
				return
			}
		case <-fi.cancel:
			return
		}
	}
}

func (fi *FanIn) growthLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-fi.cancel:
			return
		case <-t.C:
			if fi.client.closed.Load() {
				return
			}
			// The growth loop is background work bounded by Close, not the caller's
			// setup context.
			bg := context.Background()
			_, _ = fi.client.FetchTopology(bg, TopologyRequest{Topic: &fi.topic})
			count := fi.client.topo.partitionCount(fi.topic, fi.group)
			for p := uint32(0); p < count; p++ {
				_ = fi.attachPartition(bg, p, false)
			}
		}
	}
}

// Close stops the growth loop and every partition subscription; Deliveries closes
// once the forwarders drain.
func (fi *FanIn) Close() {
	fi.mu.Lock()
	if fi.closed {
		fi.mu.Unlock()
		return
	}
	fi.closed = true
	close(fi.cancel)
	subs := append([]*SupervisedSubscription(nil), fi.subs...)
	fi.mu.Unlock()

	for _, s := range subs {
		s.Close()
	}
	go func() {
		fi.wg.Wait()
		close(fi.merged)
	}()
}

// TopicSubscribeOptions configure a whole-topic (queue) fan-in subscription,
// mirroring StreamSubscribeOptions on the stream side.
type TopicSubscribeOptions struct {
	// Group selects the queue namespace. nil (the zero value) is the default
	// namespace.
	Group    *string
	Prefetch uint32
	AutoAck  bool
}

// SubscribeTopic subscribes to every partition of a queue topic and fans the
// deliveries into one channel, supervising each partition and picking up
// partitions added by a live repartition grow. Prefetch is per partition.
func (c *Client) SubscribeTopic(ctx context.Context, topic string, opts TopicSubscribeOptions) (*FanIn, error) {
	return c.newFanIn(ctx, topic, opts.Group, opts.Prefetch, func(ctx context.Context, p uint32) (*SupervisedSubscription, error) {
		return c.SuperviseSubscribe(ctx, Subscribe{Topic: topic, Partition: p, Group: opts.Group, Prefetch: opts.Prefetch, AutoAck: opts.AutoAck})
	})
}

// DefaultCohortID is the exclusive cohort a queue subscription joins when it asks
// for exclusive consumption without naming one.
const DefaultCohortID = "default"

// SubscribeTopicCohort fans a queue in as a member of the named exclusive cohort:
// the broker assigns each partition to a single member, so several members
// consume the partitioned topic in order with free failover. Prefetch is per
// partition. Leave opts.Group nil for the default queue namespace.
func (c *Client) SubscribeTopicCohort(ctx context.Context, topic, consumerGroup string, opts TopicSubscribeOptions) (*FanIn, error) {
	return c.newFanIn(ctx, topic, opts.Group, opts.Prefetch, func(ctx context.Context, p uint32) (*SupervisedSubscription, error) {
		return c.SuperviseSubscribe(ctx, Subscribe{
			Topic: topic, Partition: p, Group: opts.Group,
			ConsumerGroup: &consumerGroup, Prefetch: opts.Prefetch, AutoAck: opts.AutoAck,
		})
	})
}

// SubscribeTopicExclusive is SubscribeTopicCohort joining the default cohort, for
// the common case of one exclusive cohort per queue. Run several instances that
// all call this on the same queue and they self-organize into the cohort.
func (c *Client) SubscribeTopicExclusive(ctx context.Context, topic string, opts TopicSubscribeOptions) (*FanIn, error) {
	return c.SubscribeTopicCohort(ctx, topic, DefaultCohortID, opts)
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
// the records into one channel, supervising each partition and picking up
// partitions added by a live repartition grow. Every consumer sees every record
// (fan-out); the client reads all partitions and fans them in.
func (c *Client) SubscribeStreamTopic(ctx context.Context, topic string, opts StreamSubscribeOptions) (*FanIn, error) {
	return c.newFanIn(ctx, topic, nil, opts.Prefetch, func(ctx context.Context, p uint32) (*SupervisedSubscription, error) {
		return c.SuperviseSubscribeStream(ctx, SubscribeStream{
			Topic:       topic,
			Partition:   p,
			DurableName: opts.DurableName,
			Start:       opts.Start,
			Filter:      opts.Filter,
			Prefetch:    opts.Prefetch,
			AutoAck:     opts.AutoAck,
		})
	})
}
