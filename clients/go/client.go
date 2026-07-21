package fibril

// The client is the cluster-aware layer over the per-connection engine: a
// connection pool (one engine per endpoint), a topology cache, partition
// routing, and bounded redirect-follow. Routing is cache-only and reactive - the
// client never fetches topology on the hot path; it routes from a cache warmed
// by explicit topology fetches and point-updated by redirects, and a cold cache
// routes to partition 0 on the bootstrap connection. A misroute is corrected by
// the broker's redirect, not a pre-flight lookup.
//
// Unlike the engine (a single actor goroutine), the client is shared across the
// caller's goroutines, so the pool and topology cache are guarded by locks.

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultMaxRedirects = 3

// ClientOptions configure a cluster client.
type ClientOptions struct {
	ClientName        string
	ClientVersion     string
	Credentials       *Credentials
	HeartbeatInterval time.Duration
	// MaxRedirects bounds how many owner redirects a single op will follow.
	MaxRedirects int
	// SuperviseBackoff is the pause between re-subscribe attempts after a drop in
	// a supervised subscription (0 uses a sensible default).
	SuperviseBackoff time.Duration
	// RepartitionPollInterval is how often a whole-topic fan-in refreshes topology
	// and picks up partitions added by a live repartition grow (0 uses a sensible
	// default).
	RepartitionPollInterval time.Duration
	// ReconcilePolicy governs how the broker reconciles a client's non-supervised
	// subscriptions after a reconnect ("" defaults to restoring them).
	ReconcilePolicy ReconcilePolicy
	// DisableAutoResubscribe, when set, makes a recreate verdict end a
	// supervised subscription with the typed close reason instead of silently
	// re-subscribing. The zero value keeps auto-resubscribe on (the default).
	DisableAutoResubscribe bool
	// DisableAutoReconnect, when set, stops the client from transparently
	// redialing a dropped bootstrap connection before an operation: a closed
	// connection surfaces its close error instead. The zero value keeps
	// auto-reconnect on (the default). Reconnect always redials regardless.
	DisableAutoReconnect bool
	// RetryBackoff is the pause before retrying an op after a transient failure
	// (0 uses a sensible default).
	RetryBackoff time.Duration
	// TopologyRefreshCooldown throttles how often a transient failure triggers a
	// topology refresh (0 uses a sensible default).
	TopologyRefreshCooldown time.Duration
	// TLS, if set, connects over TLS with these trust settings; nil is plaintext.
	TLS *TLSOptions
	// OnAssignmentChanged, if set, is called for each exclusive-cohort assignment
	// change pushed by the broker.
	OnAssignmentChanged func(AssignmentChanged)
}

func (o ClientOptions) engineOptions() EngineOptions {
	return EngineOptions{
		ClientName:          o.ClientName,
		ClientVersion:       o.ClientVersion,
		Credentials:         o.Credentials,
		HeartbeatInterval:   o.HeartbeatInterval,
		TLS:                 o.TLS,
		OnAssignmentChanged: o.OnAssignmentChanged,
	}
}

// Client is a cluster-aware connection to a Fibril deployment.
type Client struct {
	opts              ClientOptions
	bootstrapEndpoint string
	topo              *topologyCache

	bootstrapMu sync.Mutex // guards bootstrap (reassigned on reconnect)
	bootstrap   *Engine

	poolMu sync.Mutex
	pool   map[string]*Engine

	reconcileMu sync.Mutex
	reconcile   map[string]*reconcileRegistry // per-endpoint, survives engine reconnects

	settleMu sync.Mutex
	settle   map[string]*settleContext // per-endpoint, survives engine reconnects

	rr             atomic.Uint64        // round-robin cursor for keyless publishes
	cohortMemberID atomic.Pointer[UUID] // captured once, carried across cohort subscribes
	closed         atomic.Bool
}

// reconcileFor returns the endpoint's reconcile registry, creating it on first
// use. Shared across the reconnects of that endpoint's engine.
func (c *Client) reconcileFor(endpoint string) *reconcileRegistry {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	reg, ok := c.reconcile[endpoint]
	if !ok {
		reg = newReconcileRegistry(c.opts.ReconcilePolicy)
		c.reconcile[endpoint] = reg
	}
	return reg
}

// settleFor returns the endpoint's settle router, creating it on first use.
// Shared across the reconnects of that endpoint's engine so a held delivery
// settles against the current engine (or goes stale after a non-resumed reconnect).
func (c *Client) settleFor(endpoint string) *settleContext {
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	s, ok := c.settle[endpoint]
	if !ok {
		s = newSettleContext()
		c.settle[endpoint] = s
	}
	return s
}

// applyCohortMember offers the client's captured cohort member id on a
// consumer-group subscribe, so re-subscribes and sibling partitions present the
// same identity to the cohort.
func (c *Client) applyCohortMember(req *Subscribe) {
	if req.ConsumerGroup == nil || req.MemberID != nil {
		return
	}
	if mid := c.cohortMemberID.Load(); mid != nil {
		req.MemberID = mid
	}
}

// captureCohortMember records the broker-minted member id from the first
// consumer-group subscribe.
func (c *Client) captureCohortMember(req Subscribe, sub *Subscription) {
	if req.ConsumerGroup == nil || sub.MemberID == nil {
		return
	}
	c.cohortMemberID.CompareAndSwap(nil, sub.MemberID)
}

// isTransient reports whether err is a transient transport failure worth
// reconnecting and retrying (a severed or shut-down connection).
func isTransient(err error) bool {
	var d *DisconnectionError
	var b *BrokenPipeError
	return errors.As(err, &d) || errors.As(err, &b)
}

const (
	defaultRetryBackoff            = 100 * time.Millisecond
	defaultTopologyRefreshCooldown = time.Second
	// topologyRefreshTimeout bounds the best-effort refresh between retries so a
	// broker that accepts the connection but does not answer cannot wedge the
	// retry loop.
	topologyRefreshTimeout = 5 * time.Second
)

// afterTransient runs between retry attempts: it refreshes topology (throttled
// and time-bounded) so the next attempt re-routes to the current owner, then
// backs off briefly.
func (c *Client) afterTransient(ctx context.Context, topic string, group *string) {
	cooldown := c.opts.TopologyRefreshCooldown
	if cooldown <= 0 {
		cooldown = defaultTopologyRefreshCooldown
	}
	if c.topo.dueForRefresh(cooldown) {
		tctx, cancel := context.WithTimeout(ctx, topologyRefreshTimeout)
		_, _ = c.FetchTopology(tctx, TopologyRequest{Topic: &topic, Group: group})
		cancel()
	}
	backoff := c.opts.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}
	t := time.NewTimer(backoff)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// Dial connects to a broker at addr and returns a cluster client. addr is the
// bootstrap endpoint; other owners are pooled lazily as routing discovers them. A
// deadline on ctx bounds the initial connect.
func Dial(ctx context.Context, addr string, opts ClientOptions) (*Client, error) {
	if opts.MaxRedirects <= 0 {
		opts.MaxRedirects = defaultMaxRedirects
	}
	// Build the client (and its cache) first, so the engine's topology-push
	// callback can point at the cache before the bootstrap connection starts.
	c := &Client{
		opts:              opts,
		bootstrapEndpoint: addr,
		topo:              newTopologyCache(),
		pool:              make(map[string]*Engine),
		reconcile:         make(map[string]*reconcileRegistry),
		settle:            make(map[string]*settleContext),
	}
	boot, err := Connect(ctx, addr, c.engineOpts(addr))
	if err != nil {
		return nil, err
	}
	c.bootstrap = boot
	return c, nil
}

// engineOpts is the per-engine options the client opens connections with,
// including the topology-push callback that keeps the routing cache warm and the
// endpoint's reconcile registry so a reconnect restores its subscriptions.
func (c *Client) engineOpts(endpoint string) EngineOptions {
	o := c.opts.engineOptions()
	o.OnTopologyUpdate = c.applyTopologyPush
	o.ReconcileRegistry = c.reconcileFor(endpoint)
	o.settle = c.settleFor(endpoint)
	return o
}

// applyTopologyPush applies a broker-pushed snapshot and prunes the pool to the
// new owner set, returning the generation the client now reflects (for the ack).
func (c *Client) applyTopologyPush(topo TopologyOk) uint64 {
	gen := c.topo.applyPush(topo)
	c.prunePool()
	return gen
}

// prunePool drops and shuts down pooled connections to endpoints that no longer
// own any partition, so a failed-over owner's stale connection does not linger.
// A full topology view (fetch or push) is authoritative about the live owner set.
func (c *Client) prunePool() {
	live := c.topo.endpoints()
	c.poolMu.Lock()
	for endpoint, e := range c.pool {
		if !live[endpoint] {
			e.Shutdown()
			delete(c.pool, endpoint)
		}
	}
	c.poolMu.Unlock()
}

// newClientWith builds a client around an already-connected bootstrap engine,
// with a given bootstrap endpoint string. Used by tests to drive routing over an
// in-memory connection without dialing.
func newClientWith(endpoint string, boot *Engine, opts ClientOptions) *Client {
	if opts.MaxRedirects <= 0 {
		opts.MaxRedirects = defaultMaxRedirects
	}
	return &Client{
		opts:              opts,
		bootstrapEndpoint: endpoint,
		bootstrap:         boot,
		topo:              newTopologyCache(),
		pool:              make(map[string]*Engine),
		reconcile:         make(map[string]*reconcileRegistry),
		settle:            make(map[string]*settleContext),
	}
}

// ---- topology cache ----------------------------------------------------

type topologyCache struct {
	mu          sync.RWMutex
	generation  uint64
	partitions  map[string]uint32 // (topic,group) -> partition count
	owners      map[string]string // (topic,partition,group) -> endpoint
	lastRefresh time.Time         // last throttled refresh, to rate-limit re-fetches

	catalogue      Catalogue               // declared channels, derived from topology
	catListeners   map[int]func(Catalogue) // change listeners for pattern subscriptions
	nextListenerID int
}

func newTopologyCache() *topologyCache {
	return &topologyCache{
		partitions:   map[string]uint32{},
		owners:       map[string]string{},
		catListeners: map[int]func(Catalogue){},
	}
}

// dueForRefresh reports whether a throttled refresh may run now, recording the
// time when it may. Rate-limits re-fetches under a burst of transient failures.
func (t *topologyCache) dueForRefresh(cooldown time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if !t.lastRefresh.IsZero() && now.Sub(t.lastRefresh) < cooldown {
		return false
	}
	t.lastRefresh = now
	return true
}

func groupStr(group *string) string {
	if group == nil {
		return ""
	}
	return *group
}

func queueCacheKey(topic string, group *string) string {
	return topic + "\x00" + groupStr(group)
}

func partCacheKey(topic string, partition uint32, group *string) string {
	return topic + "\x00" + groupStr(group) + "\x00" + strconv.FormatUint(uint64(partition), 10)
}

func addrString(a AdvertisedAddress) string {
	return net.JoinHostPort(a.Host, strconv.Itoa(int(a.Port)))
}

// replace installs a full topology snapshot, ignoring a stale (older generation)
// one so an out-of-order push cannot regress routing.
func (t *topologyCache) replace(topo TopologyOk) {
	t.mu.Lock()
	if topo.Generation < t.generation && t.generation != 0 {
		t.mu.Unlock()
		return
	}
	t.generation = topo.Generation
	t.partitions = map[string]uint32{}
	t.owners = map[string]string{}
	for _, e := range topo.Queues {
		t.partitions[queueCacheKey(e.Topic, e.Group)] = e.PartitionCount
		if len(e.OwnerEndpoints) > 0 {
			t.owners[partCacheKey(e.Topic, e.Partition, e.Group)] = addrString(e.OwnerEndpoints[0])
		}
	}
	for _, s := range topo.Streams {
		t.partitions[queueCacheKey(s.Topic, nil)] = s.PartitionCount
		if len(s.OwnerEndpoints) > 0 {
			t.owners[partCacheKey(s.Topic, s.Partition, nil)] = addrString(s.OwnerEndpoints[0])
		}
	}
	// Refresh the catalogue only from a newer generation, so a topic-filtered
	// fetch (which returns a subset at the same generation) cannot shrink it.
	// Listeners are collected and fired after unlock so one cannot re-enter the
	// cache under the lock.
	var fire []func(Catalogue)
	if topo.Generation > t.catalogue.Generation || t.catalogue.Generation == 0 {
		next := catalogueFromTopology(topo)
		if !next.sameChannels(t.catalogue) {
			for _, l := range t.catListeners {
				fire = append(fire, l)
			}
		}
		t.catalogue = next
	}
	current := t.catalogue
	t.mu.Unlock()
	for _, l := range fire {
		l(current)
	}
}

// applyPush installs a broker-pushed snapshot and returns the generation the
// cache now reflects (for the ack). Signature matches EngineOptions.OnTopologyUpdate.
func (t *topologyCache) applyPush(topo TopologyOk) uint64 {
	t.replace(topo)
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.generation
}

// endpoints returns the set of endpoints that currently own at least one
// partition. The pool uses it to drop connections to owners that have gone away.
func (t *topologyCache) endpoints() map[string]bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	live := make(map[string]bool, len(t.owners))
	for _, ep := range t.owners {
		live[ep] = true
	}
	return live
}

func (t *topologyCache) applyRedirect(rd Redirect) {
	if len(rd.OwnerEndpoints) == 0 {
		return
	}
	t.mu.Lock()
	t.owners[partCacheKey(rd.Topic, rd.Partition, rd.Group)] = addrString(rd.OwnerEndpoints[0])
	t.mu.Unlock()
}

func (t *topologyCache) partitionCount(topic string, group *string) uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if pc, ok := t.partitions[queueCacheKey(topic, group)]; ok && pc > 0 {
		return pc
	}
	return 1
}

// ownerOf returns the cached owner endpoint for a partition, or "" when unknown
// (route to the bootstrap and let a redirect correct it).
func (t *topologyCache) ownerOf(topic string, partition uint32, group *string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.owners[partCacheKey(topic, partition, group)]
}

// ---- routing -----------------------------------------------------------

// partitionFor chooses a partition: FNV-1a of the key mod the partition count,
// or round-robin across partitions for a keyless publish.
func (c *Client) partitionFor(topic string, group *string, key []byte) uint32 {
	pc := c.topo.partitionCount(topic, group)
	if pc <= 1 {
		return 0
	}
	if key != nil {
		return uint32(FNV1a(key) % uint64(pc))
	}
	return uint32(c.rr.Add(1) % uint64(pc))
}

// bootstrapEngine returns a live bootstrap engine, reconnecting it (offering the
// previous session's resume identity) if the connection has dropped. Serialized
// so concurrent callers reconnect once.
func (c *Client) bootstrapEngine(ctx context.Context) (*Engine, error) {
	c.bootstrapMu.Lock()
	defer c.bootstrapMu.Unlock()
	if !c.bootstrap.IsClosed() {
		return c.bootstrap, nil
	}
	if c.closed.Load() {
		return nil, &BrokenPipeError{Message: "client shut down"}
	}
	// With auto-reconnect off, surface why the connection closed rather than
	// redialing under the caller. Reconnect is the explicit way back.
	if c.opts.DisableAutoReconnect {
		return nil, c.bootstrap.err()
	}
	ne, err := c.dialBootstrap(ctx, c.bootstrap)
	if err != nil {
		return nil, err
	}
	c.bootstrap = ne
	return ne, nil
}

// dialBootstrap opens a fresh bootstrap connection, offering the prior session's
// resume identity so the broker (and the endpoint's reconcile registry and settle
// router) can restore state. The caller holds bootstrapMu.
func (c *Client) dialBootstrap(ctx context.Context, old *Engine) (*Engine, error) {
	opts := c.engineOpts(c.bootstrapEndpoint)
	resume := old.ResumeIdentity
	opts.Resume = &resume
	return Connect(ctx, c.bootstrapEndpoint, opts)
}

// Reconnect forces a fresh bootstrap connection, offering the previous session's
// resume identity, and returns the broker's resume outcome. ResumeResumed means
// the server-side session was reattached; any other outcome means the broker
// treated the connection as fresh. Existing publishers use the new connection;
// active non-supervised subscriptions reconcile, and supervised ones re-subscribe.
func (c *Client) Reconnect(ctx context.Context) (ResumeOutcome, error) {
	c.bootstrapMu.Lock()
	defer c.bootstrapMu.Unlock()
	if c.closed.Load() {
		return "", &BrokenPipeError{Message: "client shut down"}
	}
	old := c.bootstrap
	ne, err := c.dialBootstrap(ctx, old)
	if err != nil {
		return "", err
	}
	c.bootstrap = ne
	old.Shutdown()
	return ne.ResumeOutcome, nil
}

// engineFor returns the engine for an endpoint, opening and pooling one if
// needed. An empty or bootstrap endpoint uses the (reconnecting) bootstrap.
func (c *Client) engineFor(ctx context.Context, endpoint string) (*Engine, error) {
	if endpoint == "" || endpoint == c.bootstrapEndpoint {
		return c.bootstrapEngine(ctx)
	}
	c.poolMu.Lock()
	prev, existed := c.pool[endpoint]
	if existed && !prev.IsClosed() {
		c.poolMu.Unlock()
		return prev, nil
	}
	c.poolMu.Unlock()

	// Dial outside the lock so a slow connect does not block other routing. A
	// reconnect offers the prior session's resume identity so the broker (and the
	// endpoint's reconcile registry) can restore subscriptions.
	opts := c.engineOpts(endpoint)
	if existed {
		resume := prev.ResumeIdentity
		opts.Resume = &resume
	}
	ne, err := Connect(ctx, endpoint, opts)
	if err != nil {
		return nil, err
	}
	c.poolMu.Lock()
	if e, ok := c.pool[endpoint]; ok && !e.IsClosed() {
		c.poolMu.Unlock()
		ne.Shutdown() // lost a race; keep the existing one
		return e, nil
	}
	c.pool[endpoint] = ne
	c.poolMu.Unlock()
	return ne, nil
}

// ---- publish -----------------------------------------------------------

// Publish routes and sends a fire-and-forget publish. Topic, Group, and
// PartitionKey drive routing; Partition is set by the client.
func (c *Client) Publish(ctx context.Context, p Publish) error {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(ctx, c.topo.ownerOf(p.Topic, p.Partition, p.Group))
		if err != nil {
			return err
		}
		if err = eng.PublishUnconfirmed(ctx, p); err == nil {
			return nil
		} else if isTransient(err) {
			c.afterTransient(ctx, p.Topic, p.Group)
			continue
		} else {
			return err
		}
	}
	return &DisconnectionError{Message: "gave up publishing after reconnect attempts"}
}

// routedConfirm routes a confirmed publish-style op to the partition owner,
// following redirects and reconnecting on transient failure, and returns the
// assigned offset.
func (c *Client) routedConfirm(ctx context.Context, topic string, group *string, key []byte, do func(eng *Engine, partition uint32) (uint64, error)) (uint64, error) {
	partition := c.partitionFor(topic, group, key)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(ctx, c.topo.ownerOf(topic, partition, group))
		if err != nil {
			return 0, err
		}
		off, err := do(eng, partition)
		if err == nil {
			return off, nil
		}
		var re *RedirectError
		if errors.As(err, &re) {
			c.topo.applyRedirect(re.Redirect)
			continue
		}
		if isTransient(err) {
			c.afterTransient(ctx, topic, group)
			continue // engineFor reconnects on the next attempt
		}
		return 0, err
	}
	return 0, &DisconnectionError{Message: "gave up after too many redirect/reconnect attempts on publish"}
}

// PublishConfirmed routes and sends a confirmed publish, following owner
// redirects, and returns the assigned offset.
func (c *Client) PublishConfirmed(ctx context.Context, p Publish) (uint64, error) {
	return c.routedConfirm(ctx, p.Topic, p.Group, p.PartitionKey, func(eng *Engine, partition uint32) (uint64, error) {
		p.Partition = partition
		return eng.PublishConfirmed(ctx, p)
	})
}

// PublishWithConfirmation routes a confirmed publish to the partition owner and
// returns a handle for its offset, without blocking on the confirm. Fire several
// and await each handle afterward to pipeline the confirmations. It routes once
// (no redirect-follow), since the confirmation resolves asynchronously; a stale
// route surfaces as a RedirectError from Confirmed.
func (c *Client) PublishWithConfirmation(ctx context.Context, p Publish) (PublishConfirmation, error) {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	eng, err := c.engineFor(ctx, c.topo.ownerOf(p.Topic, p.Partition, p.Group))
	if err != nil {
		return PublishConfirmation{}, err
	}
	return eng.PublishWithConfirmation(ctx, p)
}

// PublishDelayedWithConfirmation is PublishWithConfirmation for a delayed publish.
func (c *Client) PublishDelayedWithConfirmation(ctx context.Context, p PublishDelayed) (PublishConfirmation, error) {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	eng, err := c.engineFor(ctx, c.topo.ownerOf(p.Topic, p.Partition, p.Group))
	if err != nil {
		return PublishConfirmation{}, err
	}
	return eng.PublishDelayedWithConfirmation(ctx, p)
}

// PublishDelayedConfirmed routes and sends a delayed confirmed publish.
func (c *Client) PublishDelayedConfirmed(ctx context.Context, p PublishDelayed) (uint64, error) {
	return c.routedConfirm(ctx, p.Topic, p.Group, p.PartitionKey, func(eng *Engine, partition uint32) (uint64, error) {
		p.Partition = partition
		return eng.PublishDelayedConfirmed(ctx, p)
	})
}

// PublishDelayed routes and sends a delayed fire-and-forget publish.
func (c *Client) PublishDelayed(ctx context.Context, p PublishDelayed) error {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(ctx, c.topo.ownerOf(p.Topic, p.Partition, p.Group))
		if err != nil {
			return err
		}
		if err = eng.PublishDelayedUnconfirmed(ctx, p); err == nil {
			return nil
		} else if isTransient(err) {
			c.afterTransient(ctx, p.Topic, p.Group)
			continue
		} else {
			return err
		}
	}
	return &DisconnectionError{Message: "gave up publishing after reconnect attempts"}
}

// ---- subscribe ---------------------------------------------------------

// Subscribe subscribes to one partition, routed to its owner and following owner
// redirects up to MaxRedirects. The subscription is remembered for reconnect
// reconcile.
func (c *Client) Subscribe(ctx context.Context, req Subscribe) (*Subscription, error) {
	return c.subscribe(ctx, req, false)
}

// subscribeSupervised is Subscribe for a supervised subscription, which recovers
// by re-subscribing and so stays out of the reconcile registry.
func (c *Client) subscribeSupervised(ctx context.Context, req Subscribe) (*Subscription, error) {
	return c.subscribe(ctx, req, true)
}

func (c *Client) subscribe(ctx context.Context, req Subscribe, supervised bool) (*Subscription, error) {
	c.applyCohortMember(&req)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(ctx, c.topo.ownerOf(req.Topic, req.Partition, req.Group))
		if err != nil {
			return nil, err
		}
		var sub *Subscription
		if supervised {
			sub, err = eng.subscribeSupervised(ctx, req)
		} else {
			sub, err = eng.Subscribe(ctx, req)
		}
		if err == nil {
			c.captureCohortMember(req, sub)
			return sub, nil
		}
		var re *RedirectError
		if errors.As(err, &re) {
			c.topo.applyRedirect(re.Redirect)
			continue
		}
		if isTransient(err) {
			c.afterTransient(ctx, req.Topic, req.Group)
			continue // engineFor reconnects on the next attempt
		}
		return nil, err
	}
	return nil, &DisconnectionError{Message: "gave up after too many redirect/reconnect attempts on subscribe"}
}

// SubscribeStream subscribes to one partition of a Plexus (fan-out stream),
// routed to its owner and following owner redirects. Streams have no group.
func (c *Client) SubscribeStream(ctx context.Context, req SubscribeStream) (*Subscription, error) {
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(ctx, c.topo.ownerOf(req.Topic, req.Partition, nil))
		if err != nil {
			return nil, err
		}
		sub, err := eng.SubscribeStream(ctx, req)
		if err == nil {
			return sub, nil
		}
		var re *RedirectError
		if errors.As(err, &re) {
			c.topo.applyRedirect(re.Redirect)
			continue
		}
		if isTransient(err) {
			c.afterTransient(ctx, req.Topic, nil)
			continue
		}
		return nil, err
	}
	return nil, &DisconnectionError{Message: "gave up after too many redirect/reconnect attempts on stream subscribe"}
}

// ---- cluster ops -------------------------------------------------------

// DeclareQueue declares a queue (a cluster op, handled on the bootstrap
// connection), reconnecting once on a transient failure.
func (c *Client) DeclareQueue(ctx context.Context, d QueueConfig) (DeclareQueueOk, error) {
	for attempt := 0; attempt < 2; attempt++ {
		eng, err := c.bootstrapEngine(ctx)
		if err != nil {
			return DeclareQueueOk{}, err
		}
		ok, err := eng.DeclareQueue(ctx, d)
		if err == nil || !isTransient(err) {
			return ok, err
		}
	}
	return DeclareQueueOk{}, &DisconnectionError{Message: "declare failed after reconnect"}
}

// DeclarePlexus declares a Plexus (fan-out stream) channel (a cluster op on the
// bootstrap connection), reconnecting once on a transient failure.
func (c *Client) DeclarePlexus(ctx context.Context, d StreamConfig) (DeclarePlexusOk, error) {
	for attempt := 0; attempt < 2; attempt++ {
		eng, err := c.bootstrapEngine(ctx)
		if err != nil {
			return DeclarePlexusOk{}, err
		}
		ok, err := eng.DeclarePlexus(ctx, d)
		if err == nil || !isTransient(err) {
			return ok, err
		}
	}
	return DeclarePlexusOk{}, &DisconnectionError{Message: "declare plexus failed after reconnect"}
}

// FetchTopology fetches the topology and warms the routing cache, reconnecting
// once on a transient failure.
func (c *Client) FetchTopology(ctx context.Context, req TopologyRequest) (TopologyOk, error) {
	for attempt := 0; attempt < 2; attempt++ {
		eng, err := c.bootstrapEngine(ctx)
		if err != nil {
			return TopologyOk{}, err
		}
		topo, err := eng.FetchTopology(ctx, req)
		if err == nil {
			c.topo.replace(topo)
			c.prunePool()
			return topo, nil
		}
		if !isTransient(err) {
			return TopologyOk{}, err
		}
	}
	return TopologyOk{}, &DisconnectionError{Message: "topology fetch failed after reconnect"}
}

// Close shuts the client down, the idiomatic io.Closer form of Shutdown. It
// always returns nil today, so an error return is reserved for future use.
func (c *Client) Close() error {
	c.Shutdown()
	return nil
}

// Shutdown closes the bootstrap connection and every pooled connection.
func (c *Client) Shutdown() {
	if c.closed.Swap(true) {
		return
	}
	c.bootstrapMu.Lock()
	c.bootstrap.Shutdown()
	c.bootstrapMu.Unlock()
	c.poolMu.Lock()
	for endpoint, e := range c.pool {
		e.Shutdown()
		delete(c.pool, endpoint)
	}
	c.poolMu.Unlock()
	// Close any preserved subscription channels the engines left for a reconnect
	// that will not come now.
	c.reconcileMu.Lock()
	for _, reg := range c.reconcile {
		reg.closeAll()
	}
	c.reconcileMu.Unlock()
}
