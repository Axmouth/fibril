package fibril

// Topic-bound publisher handles over the client's routed publish. A plain
// Publisher sends a Message; a ReliablePublisher additionally stamps
// producer-dedup headers so a retried publish can be deduplicated by the broker.

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"
)

// Publisher publishes to a fixed topic (and optional group).
type Publisher struct {
	client *Client
	topic  string
	group  *string
}

// Publisher returns a publisher bound to topic (default group).
func (c *Client) Publisher(topic string) *Publisher {
	return &Publisher{client: c, topic: topic}
}

// PublisherGrouped returns a publisher bound to topic within a group namespace.
func (c *Client) PublisherGrouped(topic, group string) *Publisher {
	return &Publisher{client: c, topic: topic, group: &group}
}

// Publish sends a fire-and-forget message.
func (p *Publisher) Publish(m Message) error {
	pub, err := m.toPublish(p.topic, p.group)
	if err != nil {
		return err
	}
	return p.client.Publish(pub)
}

// PublishConfirmed sends a message and waits for the broker-assigned offset.
func (p *Publisher) PublishConfirmed(m Message) (uint64, error) {
	pub, err := m.toPublish(p.topic, p.group)
	if err != nil {
		return 0, err
	}
	return p.client.PublishConfirmed(pub)
}

// PublishDelayed sends m fire-and-forget, to become visible after delay from now.
func (p *Publisher) PublishDelayed(m Message, delay time.Duration) error {
	pd, err := p.toDelayed(m, delay)
	if err != nil {
		return err
	}
	return p.client.PublishDelayed(pd)
}

// PublishDelayedConfirmed sends m to become visible after delay and waits for the
// broker-assigned offset.
func (p *Publisher) PublishDelayedConfirmed(m Message, delay time.Duration) (uint64, error) {
	pd, err := p.toDelayed(m, delay)
	if err != nil {
		return 0, err
	}
	return p.client.PublishDelayedConfirmed(pd)
}

// toDelayed builds a delayed publish from a message, turning a relative delay into
// the absolute not-before deadline the wire carries (milliseconds).
func (p *Publisher) toDelayed(m Message, delay time.Duration) (PublishDelayed, error) {
	pub, err := m.toPublish(p.topic, p.group)
	if err != nil {
		return PublishDelayed{}, err
	}
	notBefore := time.Now().UnixMilli() + delay.Milliseconds()
	if notBefore < 0 {
		notBefore = 0
	}
	return PublishDelayed{
		Topic:        pub.Topic,
		Group:        pub.Group,
		ContentType:  pub.ContentType,
		Headers:      pub.Headers,
		Payload:      pub.Payload,
		PartitionKey: pub.PartitionKey,
		NotBefore:    uint64(notBefore),
	}, nil
}

// ReliablePublisher stamps each message with a stable producer id and a
// monotonic sequence, so a publish retried after a transient failure carries the
// same identity and the broker can deduplicate it (owner-side dedup is a broker
// feature; the client always stamps regardless).
type ReliablePublisher struct {
	*Publisher
	producerID string
	seq        atomic.Uint64
}

// ReliablePublisher returns a reliable publisher bound to topic (default group).
func (c *Client) ReliablePublisher(topic string) *ReliablePublisher {
	return &ReliablePublisher{Publisher: c.Publisher(topic), producerID: newProducerID()}
}

// ReliablePublisherGrouped returns a reliable publisher bound to topic within a
// group namespace.
func (c *Client) ReliablePublisherGrouped(topic, group string) *ReliablePublisher {
	return &ReliablePublisher{Publisher: c.PublisherGrouped(topic, group), producerID: newProducerID()}
}

func (p *ReliablePublisher) Publish(m Message) error {
	pub, err := m.toPublish(p.topic, p.group)
	if err != nil {
		return err
	}
	p.stamp(&pub)
	return p.client.Publish(pub)
}

func (p *ReliablePublisher) PublishConfirmed(m Message) (uint64, error) {
	pub, err := m.toPublish(p.topic, p.group)
	if err != nil {
		return 0, err
	}
	p.stamp(&pub)
	return p.client.PublishConfirmed(pub)
}

// stamp adds the library-owned producer-dedup headers directly (bypassing the
// user-header validation, which forbids the reserved fibril.* namespace).
func (p *ReliablePublisher) stamp(pub *Publish) {
	if pub.Headers == nil {
		pub.Headers = Headers{}
	}
	pub.Headers[headerProducerID] = p.producerID
	pub.Headers[headerProducerSeq] = strconv.FormatUint(p.seq.Add(1)-1, 10)
}

func newProducerID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
