package fibril

// A supervised subscription survives an owner failover: when its connection
// drops, a supervisor goroutine refreshes the topology, re-subscribes to the new
// owner, and keeps delivering on the same stable channel - until the caller
// closes it. Because each Delivery carries the connection it arrived on, acks
// keep working across a re-subscribe.

import (
	"sync"
	"time"
)

const defaultSuperviseBackoff = 250 * time.Millisecond

// SupervisedSubscription is a subscription that re-attaches across owner
// failovers. Deliveries yields messages until Close is called (then it closes).
type SupervisedSubscription struct {
	Deliveries <-chan Delivery
	cancel     chan struct{}
	cancelOnce sync.Once
}

// Close stops the supervisor and closes Deliveries.
func (s *SupervisedSubscription) Close() {
	s.cancelOnce.Do(func() { close(s.cancel) })
}

// SuperviseSubscribe subscribes to one partition and keeps it attached across
// owner failovers. The first attach is synchronous (so a bad topic errors here);
// later re-attaches happen in the background on the same Deliveries channel.
func (c *Client) SuperviseSubscribe(req Subscribe) (*SupervisedSubscription, error) {
	sub, err := c.Subscribe(req)
	if err != nil {
		return nil, err
	}
	capHint := int(req.Prefetch)
	if capHint < 1 {
		capHint = 1
	}
	out := make(chan Delivery, capHint)
	ss := &SupervisedSubscription{Deliveries: out, cancel: make(chan struct{})}
	go c.superviseLoop(req, sub, out, ss.cancel)
	return ss, nil
}

func (c *Client) superviseLoop(req Subscribe, sub *Subscription, out chan Delivery, cancel chan struct{}) {
	defer close(out)
	backoff := c.opts.SuperviseBackoff
	if backoff <= 0 {
		backoff = defaultSuperviseBackoff
	}
	for {
		// Forward until this attachment's channel closes (a drop) or we are asked
		// to stop.
		if cancelled := !forwardUntilClosed(sub.Deliveries, out, cancel); cancelled {
			return
		}
		// The connection dropped. Re-attach: back off, refresh the topology so a
		// failed-over owner is picked up, then re-subscribe. Stop if the client
		// is shutting down or a permanent error (e.g. the topic is gone) occurs.
		for {
			if c.closed.Load() {
				return
			}
			if !sleepOrCancel(backoff, cancel) {
				return
			}
			_, _ = c.FetchTopology(TopologyRequest{Topic: &req.Topic}) // best effort
			newSub, err := c.Subscribe(req)
			if err == nil {
				sub = newSub
				break
			}
			if !isTransient(err) || c.closed.Load() {
				return
			}
		}
	}
}

// forwardUntilClosed copies deliveries from in to out. It returns true when in
// closed (a drop, so the caller should re-attach), or false when cancelled.
func forwardUntilClosed(in <-chan Delivery, out chan Delivery, cancel chan struct{}) bool {
	for {
		select {
		case d, ok := <-in:
			if !ok {
				return true // channel closed: connection dropped
			}
			select {
			case out <- d:
			case <-cancel:
				return false
			}
		case <-cancel:
			return false
		}
	}
}

func sleepOrCancel(d time.Duration, cancel <-chan struct{}) bool {
	select {
	case <-time.After(d):
		return true
	case <-cancel:
		return false
	}
}
