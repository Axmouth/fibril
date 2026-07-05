package fibril

// Pattern (discovery) subscribe: fan in across every queue or Plexus stream whose
// topic matches a glob, and keep attaching channels that start matching later.
// The catalogue is derived from topology and kept live by topology pushes, so
// newly declared channels are picked up without a reconnect. Discovery is a
// first-class but opt-in surface, reached via Client.Routing so it stays off the
// default client API.

import (
	"context"
	"strings"
	"sync"
)

// topicGlob is a *-wildcard topic matcher. It mirrors the broker's header-value
// matcher so the discovery glob and the per-subscription filter share one
// grammar: split on *, where each * matches any run of characters (including
// empty), no regex.
type topicGlob struct {
	segments []string
}

func newTopicGlob(pattern string) *topicGlob {
	return &topicGlob{segments: strings.Split(pattern, "*")}
}

func (g *topicGlob) matches(value string) bool {
	segs := g.segments
	if len(segs) == 1 {
		return segs[0] == value
	}
	first, last := segs[0], segs[len(segs)-1]
	if !strings.HasPrefix(value, first) || !strings.HasSuffix(value, last) {
		return false
	}
	if len(value) < len(first)+len(last) {
		return false
	}
	pos := len(first)
	end := len(value) - len(last)
	for _, mid := range segs[1 : len(segs)-1] {
		if mid == "" {
			continue
		}
		found := strings.Index(value[pos:], mid)
		if found < 0 {
			return false
		}
		found += pos
		if found > end-len(mid) {
			return false
		}
		pos = found + len(mid)
	}
	return true
}

// RoutingClient is the opt-in discovery surface over a Client: the catalogue plus
// pattern subscribe. It shares the client's connection and re-exposes the normal
// operations, so routing composes with them rather than replacing them.
type RoutingClient struct {
	client *Client
}

// Routing returns the discovery surface (catalogue + pattern subscribe).
func (c *Client) Routing() *RoutingClient {
	return &RoutingClient{client: c}
}

// Client returns the underlying client, for any operation not surfaced here.
func (r *RoutingClient) Client() *Client { return r.client }

// Catalogue returns the current snapshot of declared channels.
func (r *RoutingClient) Catalogue() Catalogue { return r.client.Catalogue() }

// OnCatalogueChange registers a handler for catalogue changes; the returned
// function unregisters it.
func (r *RoutingClient) OnCatalogueChange(fn func(Catalogue)) (cancel func()) {
	return r.client.OnCatalogueChange(fn)
}

// PatternSubscribeOptions configure a work-queue pattern subscription. Prefetch
// applies per attached queue.
type PatternSubscribeOptions struct {
	Prefetch      uint32
	ConsumerGroup *string
	AutoAck       bool
}

// StreamPatternSubscribeOptions configure a Plexus-stream pattern subscription.
type StreamPatternSubscribeOptions struct {
	Prefetch    uint32
	Start       StreamStart
	Filter      []StreamFilter
	DurableName *string
	AutoAck     bool
}

// SubscribePattern fans in across every work queue whose topic matches pattern (a
// *-wildcard glob, "*" matches all), attaching queues that start matching later.
func (r *RoutingClient) SubscribePattern(ctx context.Context, pattern string, opts PatternSubscribeOptions) (*PatternSubscription, error) {
	prefetch := opts.Prefetch
	if prefetch == 0 {
		prefetch = 1
	}
	attach := func(ctx context.Context, topic string, group *string) (*FanIn, error) {
		return r.client.newFanIn(ctx, topic, group, prefetch, func(ctx context.Context, p uint32) (*SupervisedSubscription, error) {
			return r.client.SuperviseSubscribe(ctx, Subscribe{
				Topic: topic, Partition: p, Group: group,
				ConsumerGroup: opts.ConsumerGroup, Prefetch: prefetch, AutoAck: opts.AutoAck,
			})
		})
	}
	return r.client.subscribePattern(ctx, pattern, false, attach), nil
}

// SubscribeStreamPattern fans in across every Plexus stream whose topic matches
// pattern, attaching streams that start matching later.
func (r *RoutingClient) SubscribeStreamPattern(ctx context.Context, pattern string, opts StreamPatternSubscribeOptions) (*PatternSubscription, error) {
	prefetch := opts.Prefetch
	if prefetch == 0 {
		prefetch = 16
	}
	attach := func(ctx context.Context, topic string, _ *string) (*FanIn, error) {
		return r.client.newFanIn(ctx, topic, nil, prefetch, func(ctx context.Context, p uint32) (*SupervisedSubscription, error) {
			return r.client.SuperviseSubscribeStream(ctx, SubscribeStream{
				Topic: topic, Partition: p, DurableName: opts.DurableName,
				Start: opts.Start, Filter: opts.Filter, Prefetch: prefetch, AutoAck: opts.AutoAck,
			})
		})
	}
	return r.client.subscribePattern(ctx, pattern, true, attach), nil
}

// PatternSubscription is a live fan-in over every channel matching a glob, with
// auto-pickup of channels that start matching later. Deliveries carry their
// source topic (Delivery.Topic). Call Close to stop it.
type PatternSubscription struct {
	Deliveries <-chan Delivery
	fan        *patternFanIn
}

// Close stops the subscription: every attached channel and the catalogue watcher.
func (p *PatternSubscription) Close() { p.fan.close() }

type patternFanIn struct {
	client  *Client
	glob    *topicGlob
	stream  bool // false = queue, true = stream
	attach  func(ctx context.Context, topic string, group *string) (*FanIn, error)
	merged  chan Delivery
	cancel  func() // catalogue listener cancel
	trigger chan struct{}
	done    chan struct{}

	mu     sync.Mutex
	active map[string]*FanIn // matched (topic,group) key -> its whole-topic fan-in
	closed bool
	wg     sync.WaitGroup // one per live forwarder
}

func (c *Client) subscribePattern(ctx context.Context, pattern string, stream bool, attach func(context.Context, string, *string) (*FanIn, error)) *PatternSubscription {
	pf := &patternFanIn{
		client:  c,
		glob:    newTopicGlob(pattern),
		stream:  stream,
		attach:  attach,
		merged:  make(chan Delivery, 64),
		trigger: make(chan struct{}, 1),
		done:    make(chan struct{}),
		active:  map[string]*FanIn{},
	}
	// Warm topology so the first reconcile sees the current catalogue, then watch
	// for changes before reconciling so nothing is missed.
	pf.cancel = c.OnCatalogueChange(func(Catalogue) { pf.signal() })
	_, _ = c.FetchTopology(ctx, TopologyRequest{})
	pf.reconcile(ctx)
	go pf.loop()
	return &PatternSubscription{Deliveries: pf.merged, fan: pf}
}

func (pf *patternFanIn) signal() {
	select {
	case pf.trigger <- struct{}{}:
	default: // a reconcile is already pending; it will read the latest catalogue
	}
}

func (pf *patternFanIn) loop() {
	for {
		select {
		case <-pf.done:
			return
		case <-pf.trigger:
			// Reconcile-driven attaches are background work bounded by Close.
			pf.reconcile(context.Background())
		}
	}
}

// reconcile is only ever run by loop (and once synchronously at start), never
// concurrently with itself, so it can snapshot the active set without racing.
func (pf *patternFanIn) reconcile(ctx context.Context) {
	cat := pf.client.Catalogue()
	type match struct {
		topic string
		group *string
	}
	desired := map[string]match{}
	if pf.stream {
		for _, s := range cat.Streams {
			if pf.glob.matches(s.Topic) {
				desired[queueCacheKey(s.Topic, nil)] = match{s.Topic, nil}
			}
		}
	} else {
		for _, q := range cat.Queues {
			if pf.glob.matches(q.Topic) {
				desired[queueCacheKey(q.Topic, q.Group)] = match{q.Topic, q.Group}
			}
		}
	}

	pf.mu.Lock()
	if pf.closed {
		pf.mu.Unlock()
		return
	}
	current := make(map[string]*FanIn, len(pf.active))
	for k, f := range pf.active {
		current[k] = f
	}
	pf.mu.Unlock()

	// Close channels whose topic stopped matching (or was removed).
	for k, f := range current {
		if _, ok := desired[k]; !ok {
			f.Close()
			pf.mu.Lock()
			delete(pf.active, k)
			pf.mu.Unlock()
		}
	}
	// Attach newly matching topics.
	for k, m := range desired {
		if _, ok := current[k]; ok {
			continue
		}
		fan, err := pf.attach(ctx, m.topic, m.group)
		if err != nil {
			continue // best effort: retried on the next catalogue change
		}
		pf.mu.Lock()
		if pf.closed {
			pf.mu.Unlock()
			fan.Close()
			return
		}
		pf.active[k] = fan
		pf.wg.Add(1)
		pf.mu.Unlock()
		go pf.forward(fan)
	}
}

func (pf *patternFanIn) forward(fan *FanIn) {
	defer pf.wg.Done()
	for {
		select {
		case d, ok := <-fan.Deliveries:
			if !ok {
				return
			}
			select {
			case pf.merged <- d:
			case <-pf.done:
				return
			}
		case <-pf.done:
			return
		}
	}
}

func (pf *patternFanIn) close() {
	pf.mu.Lock()
	if pf.closed {
		pf.mu.Unlock()
		return
	}
	pf.closed = true
	close(pf.done)
	if pf.cancel != nil {
		pf.cancel()
	}
	fans := make([]*FanIn, 0, len(pf.active))
	for _, f := range pf.active {
		fans = append(fans, f)
	}
	pf.active = map[string]*FanIn{}
	pf.mu.Unlock()

	for _, f := range fans {
		f.Close()
	}
	go func() {
		pf.wg.Wait()
		close(pf.merged)
	}()
}
