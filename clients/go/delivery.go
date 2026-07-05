package fibril

// The delivery path: subscribe, receive pushed messages on a per-subscription
// channel, and settle them with ack/nack. The run goroutine owns the sub-id ->
// channel map and pushes deliveries; the channel is buffered to the effective
// prefetch, so the broker (which never sends beyond prefetch unacked) cannot
// outrun it and the run goroutine never blocks.

import "time"

// Delivery is one message pushed to a subscription.
type Delivery struct {
	Topic           string
	Group           *string
	Partition       uint32
	Payload         []byte
	ContentType     ContentType
	Headers         Headers
	DeliveryTag     DeliveryTag
	Offset          uint64
	Published       uint64
	PublishReceived uint64
	SubID           uint64
	// AutoAck is true when the broker already settled this delivery server-side
	// (an auto-ack subscription). Ack/Nack are then unnecessary.
	AutoAck bool

	reqID  uint64  // the DELIVER frame's request id, reused when settling
	engine *Engine // the connection this arrived on, so Ack/Nack route correctly
}

// Ack settles this delivery as processed, on the connection it arrived on (so it
// stays correct even when deliveries from several partitions are fanned in).
// Unnecessary for an auto-ack delivery, but harmless.
func (d Delivery) Ack() error { return d.engine.Ack(d) }

// Nack returns this delivery unprocessed, optionally requeuing it.
func (d Delivery) Nack(requeue bool, notBefore *uint64) error {
	return d.engine.Nack(d, requeue, notBefore)
}

// Retry requeues this delivery immediately for redelivery.
func (d Delivery) Retry() error { return d.engine.Nack(d, true, nil) }

// Fail settles this delivery as a terminal failure: it is not requeued, so it is
// dead-lettered or dropped per the queue's policy.
func (d Delivery) Fail() error { return d.engine.Nack(d, false, nil) }

// RetryAfter requeues this delivery for redelivery no sooner than delay from now.
func (d Delivery) RetryAfter(delay time.Duration) error {
	notBefore := time.Now().UnixMilli() + delay.Milliseconds()
	if notBefore < 0 {
		notBefore = 0
	}
	nb := uint64(notBefore)
	return d.engine.Nack(d, true, &nb)
}

// Subscription is a live single-partition subscription. Deliveries yields
// messages until the connection closes (the channel is then closed); settle each
// with its Ack/Nack.
type Subscription struct {
	SubID      uint64
	Topic      string
	Partition  uint32
	Group      *string
	MemberID   *UUID
	Deliveries <-chan Delivery

	engine *Engine
}

// Subscribe opens a subscription and returns it once the broker confirms. The
// returned Subscription's Deliveries channel receives pushed messages. A plain
// subscribe is remembered for reconnect reconcile.
func (e *Engine) Subscribe(req Subscribe) (*Subscription, error) {
	return e.subscribe(req, false)
}

// subscribeSupervised opens a subscription that is not remembered for reconcile,
// since its supervisor re-subscribes on a drop instead.
func (e *Engine) subscribeSupervised(req Subscribe) (*Subscription, error) {
	return e.subscribe(req, true)
}

func (e *Engine) subscribe(req Subscribe, noReconcile bool) (*Subscription, error) {
	sr := make(chan subResult, 1)
	reqCopy := req
	select {
	case e.cmdCh <- command{op: opSubscribe, body: encodeSubscribe(req), subReply: sr, autoAck: req.AutoAck, noReconcile: noReconcile, sub: &reqCopy}:
	case <-e.done:
		return nil, e.err()
	}
	select {
	case r := <-sr:
		return r.sub, r.err
	case <-e.done:
		return nil, e.err()
	}
}

// SubscribeStream opens a Plexus (fan-out stream) subscription of one partition.
// The broker confirms it with the shared SUBSCRIBE_OK, so it delivers on a
// per-subscription channel like a queue subscription.
func (e *Engine) SubscribeStream(req SubscribeStream) (*Subscription, error) {
	sr := make(chan subResult, 1)
	// Streams resume by durable cursor, not the reconcile registry.
	select {
	case e.cmdCh <- command{op: opSubscribeStream, body: encodeSubscribeStream(req), subReply: sr, autoAck: req.AutoAck, noReconcile: true}:
	case <-e.done:
		return nil, e.err()
	}
	select {
	case r := <-sr:
		return r.sub, r.err
	case <-e.done:
		return nil, e.err()
	}
}

// Ack settles a delivery as processed. It is a no-op-worthy call for an auto-ack
// delivery (already settled server-side) but harmless. Fire-and-forget.
func (e *Engine) Ack(d Delivery) error {
	body := encodeAck(ackFrame{Topic: d.Topic, Group: d.Group, Partition: d.Partition, Tags: []DeliveryTag{d.DeliveryTag}})
	return e.sendWithID(opAck, d.reqID, body)
}

// Nack returns a delivery unprocessed, optionally requeuing it (notBefore delays
// the requeue). Fire-and-forget.
func (e *Engine) Nack(d Delivery, requeue bool, notBefore *uint64) error {
	body := encodeNack(nackFrame{
		Topic: d.Topic, Group: d.Group, Partition: d.Partition,
		Tags: []DeliveryTag{d.DeliveryTag}, Requeue: requeue, NotBefore: notBefore,
	})
	return e.sendWithID(opNack, d.reqID, body)
}

func (e *Engine) sendWithID(op op, id uint64, body []byte) error {
	select {
	case e.cmdCh <- command{op: op, body: body, id: id}:
		return nil
	case <-e.done:
		return e.err()
	}
}

// ---- run-goroutine handlers --------------------------------------------

func (e *Engine) handleSubscribeOk(f frame) {
	w, ok := e.waiters[f.RequestID]
	if !ok {
		return
	}
	delete(e.waiters, f.RequestID)
	ok2, err := decodeSubscribeOk(f.Payload)
	if err != nil {
		if w.subReply != nil {
			w.subReply <- subResult{err: err}
		}
		return
	}
	prefetch := int(ok2.Prefetch)
	if prefetch < 1 {
		prefetch = 1
	}
	ch := make(chan Delivery, prefetch)
	// A plain subscribe is remembered so a reconnect can restore it on the same
	// channel. The registry then owns the channel across the engine's death.
	preserve := false
	if reg := e.opts.ReconcileRegistry; reg != nil && !w.noReconcile && w.sub != nil {
		reg.register(reconcileSubscription{
			SubID:          ok2.SubID,
			Topic:          ok2.Topic,
			Partition:      ok2.Partition,
			Group:          ok2.Group,
			AutoAck:        w.autoAck,
			Prefetch:       ok2.Prefetch,
			ConsumerGroup:  w.sub.ConsumerGroup,
			ConsumerTarget: w.sub.ConsumerTarget,
			MemberID:       ok2.MemberID,
		}, ch, w.autoAck)
		preserve = true
	}
	e.subs[ok2.SubID] = &subState{ch: ch, autoAck: w.autoAck, preserve: preserve}
	if w.subReply != nil {
		w.subReply <- subResult{sub: &Subscription{
			SubID:      ok2.SubID,
			Topic:      ok2.Topic,
			Partition:  ok2.Partition,
			Group:      ok2.Group,
			MemberID:   ok2.MemberID,
			Deliveries: ch,
			engine:     e,
		}}
	}
}

func (e *Engine) handleDeliver(f frame) {
	d, err := decodeDeliver(f.Payload)
	if err != nil {
		return
	}
	s, ok := e.subs[d.SubID]
	if !ok {
		return // delivery for a sub we no longer track
	}
	deliv := Delivery{
		Topic:           d.Topic,
		Group:           d.Group,
		Partition:       d.Partition,
		Payload:         d.Payload,
		ContentType:     d.ContentType,
		Headers:         d.Headers,
		DeliveryTag:     d.DeliveryTag,
		Offset:          d.Offset,
		Published:       d.Published,
		PublishReceived: d.PublishReceived,
		SubID:           d.SubID,
		AutoAck:         s.autoAck,
		reqID:           f.RequestID,
		engine:          e,
	}
	// The channel is prefetch-sized, so this send has room under normal flow.
	// The stop guard keeps a full channel from wedging shutdown.
	select {
	case s.ch <- deliv:
	case <-e.stop:
	}
}
