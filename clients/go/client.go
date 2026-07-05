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
	Auth              *Auth
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
		Auth:                o.Auth,
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
)

// afterTransient runs between retry attempts: it refreshes topology (throttled)
// so the next attempt re-routes to the current owner, then backs off briefly.
func (c *Client) afterTransient(topic string, group *string) {
	cooldown := c.opts.TopologyRefreshCooldown
	if cooldown <= 0 {
		cooldown = defaultTopologyRefreshCooldown
	}
	if c.topo.dueForRefresh(cooldown) {
		_, _ = c.FetchTopology(TopologyRequest{Topic: &topic, Group: group})
	}
	backoff := c.opts.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}
	time.Sleep(backoff)
}

// Dial connects to a broker at addr and returns a cluster client. addr is the
// bootstrap endpoint; other owners are pooled lazily as routing discovers them.
func Dial(addr string, opts ClientOptions) (*Client, error) {
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
	}
	boot, err := Connect(addr, c.engineOpts(addr))
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
	o.OnTopologyUpdate = c.topo.applyPush
	o.ReconcileRegistry = c.reconcileFor(endpoint)
	return o
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
	}
}

// ---- topology cache ----------------------------------------------------

type topologyCache struct {
	mu          sync.RWMutex
	generation  uint64
	partitions  map[string]uint32 // (topic,group) -> partition count
	owners      map[string]string // (topic,partition,group) -> endpoint
	lastRefresh time.Time         // last throttled refresh, to rate-limit re-fetches
}

func newTopologyCache() *topologyCache {
	return &topologyCache{partitions: map[string]uint32{}, owners: map[string]string{}}
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
	defer t.mu.Unlock()
	if topo.Generation < t.generation && t.generation != 0 {
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
}

// applyPush installs a broker-pushed snapshot and returns the generation the
// cache now reflects (for the ack). Signature matches EngineOptions.OnTopologyUpdate.
func (t *topologyCache) applyPush(topo TopologyOk) uint64 {
	t.replace(topo)
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.generation
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
func (c *Client) bootstrapEngine() (*Engine, error) {
	c.bootstrapMu.Lock()
	defer c.bootstrapMu.Unlock()
	if !c.bootstrap.IsClosed() {
		return c.bootstrap, nil
	}
	if c.closed.Load() {
		return nil, &BrokenPipeError{Message: "client shut down"}
	}
	opts := c.engineOpts(c.bootstrapEndpoint)
	resume := c.bootstrap.ResumeIdentity
	opts.Resume = &resume
	ne, err := Connect(c.bootstrapEndpoint, opts)
	if err != nil {
		return nil, err
	}
	c.bootstrap = ne
	return ne, nil
}

// engineFor returns the engine for an endpoint, opening and pooling one if
// needed. An empty or bootstrap endpoint uses the (reconnecting) bootstrap.
func (c *Client) engineFor(endpoint string) (*Engine, error) {
	if endpoint == "" || endpoint == c.bootstrapEndpoint {
		return c.bootstrapEngine()
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
	ne, err := Connect(endpoint, opts)
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
func (c *Client) Publish(p Publish) error {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(p.Topic, p.Partition, p.Group))
		if err != nil {
			return err
		}
		if err = eng.PublishUnconfirmed(p); err == nil {
			return nil
		} else if isTransient(err) {
			c.afterTransient(p.Topic, p.Group)
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
func (c *Client) routedConfirm(topic string, group *string, key []byte, do func(eng *Engine, partition uint32) (uint64, error)) (uint64, error) {
	partition := c.partitionFor(topic, group, key)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(topic, partition, group))
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
			c.afterTransient(topic, group)
			continue // engineFor reconnects on the next attempt
		}
		return 0, err
	}
	return 0, &DisconnectionError{Message: "gave up after too many redirect/reconnect attempts on publish"}
}

// PublishConfirmed routes and sends a confirmed publish, following owner
// redirects, and returns the assigned offset.
func (c *Client) PublishConfirmed(p Publish) (uint64, error) {
	return c.routedConfirm(p.Topic, p.Group, p.PartitionKey, func(eng *Engine, partition uint32) (uint64, error) {
		p.Partition = partition
		return eng.PublishConfirmed(p)
	})
}

// PublishPipelined routes a confirmed publish to the partition owner and returns
// a handle for its result, without blocking on the confirm. Fire several to
// pipeline confirmations and collect each offset afterward. It routes once (no
// redirect-follow), since the confirmation resolves asynchronously; a stale route
// surfaces as a RedirectError on the returned channel.
func (c *Client) PublishPipelined(p Publish) (<-chan PublishResult, error) {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	eng, err := c.engineFor(c.topo.ownerOf(p.Topic, p.Partition, p.Group))
	if err != nil {
		return nil, err
	}
	return eng.PublishPipelined(p)
}

// PublishDelayedConfirmed routes and sends a delayed confirmed publish.
func (c *Client) PublishDelayedConfirmed(p PublishDelayed) (uint64, error) {
	return c.routedConfirm(p.Topic, p.Group, p.PartitionKey, func(eng *Engine, partition uint32) (uint64, error) {
		p.Partition = partition
		return eng.PublishDelayedConfirmed(p)
	})
}

// PublishDelayed routes and sends a delayed fire-and-forget publish.
func (c *Client) PublishDelayed(p PublishDelayed) error {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(p.Topic, p.Partition, p.Group))
		if err != nil {
			return err
		}
		if err = eng.PublishDelayedUnconfirmed(p); err == nil {
			return nil
		} else if isTransient(err) {
			c.afterTransient(p.Topic, p.Group)
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
func (c *Client) Subscribe(req Subscribe) (*Subscription, error) {
	return c.subscribe(req, false)
}

// subscribeSupervised is Subscribe for a supervised subscription, which recovers
// by re-subscribing and so stays out of the reconcile registry.
func (c *Client) subscribeSupervised(req Subscribe) (*Subscription, error) {
	return c.subscribe(req, true)
}

func (c *Client) subscribe(req Subscribe, supervised bool) (*Subscription, error) {
	c.applyCohortMember(&req)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(req.Topic, req.Partition, req.Group))
		if err != nil {
			return nil, err
		}
		var sub *Subscription
		if supervised {
			sub, err = eng.subscribeSupervised(req)
		} else {
			sub, err = eng.Subscribe(req)
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
			c.afterTransient(req.Topic, req.Group)
			continue // engineFor reconnects on the next attempt
		}
		return nil, err
	}
	return nil, &DisconnectionError{Message: "gave up after too many redirect/reconnect attempts on subscribe"}
}

// SubscribeStream subscribes to one partition of a Plexus (fan-out stream),
// routed to its owner and following owner redirects. Streams have no group.
func (c *Client) SubscribeStream(req SubscribeStream) (*Subscription, error) {
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(req.Topic, req.Partition, nil))
		if err != nil {
			return nil, err
		}
		sub, err := eng.SubscribeStream(req)
		if err == nil {
			return sub, nil
		}
		var re *RedirectError
		if errors.As(err, &re) {
			c.topo.applyRedirect(re.Redirect)
			continue
		}
		if isTransient(err) {
			c.afterTransient(req.Topic, nil)
			continue
		}
		return nil, err
	}
	return nil, &DisconnectionError{Message: "gave up after too many redirect/reconnect attempts on stream subscribe"}
}

// ---- cluster ops -------------------------------------------------------

// DeclareQueue declares a queue (a cluster op, handled on the bootstrap
// connection), reconnecting once on a transient failure.
func (c *Client) DeclareQueue(d DeclareQueue) (DeclareQueueOk, error) {
	for attempt := 0; attempt < 2; attempt++ {
		eng, err := c.bootstrapEngine()
		if err != nil {
			return DeclareQueueOk{}, err
		}
		ok, err := eng.DeclareQueue(d)
		if err == nil || !isTransient(err) {
			return ok, err
		}
	}
	return DeclareQueueOk{}, &DisconnectionError{Message: "declare failed after reconnect"}
}

// DeclarePlexus declares a Plexus (fan-out stream) channel (a cluster op on the
// bootstrap connection), reconnecting once on a transient failure.
func (c *Client) DeclarePlexus(d DeclarePlexus) (DeclarePlexusOk, error) {
	for attempt := 0; attempt < 2; attempt++ {
		eng, err := c.bootstrapEngine()
		if err != nil {
			return DeclarePlexusOk{}, err
		}
		ok, err := eng.DeclarePlexus(d)
		if err == nil || !isTransient(err) {
			return ok, err
		}
	}
	return DeclarePlexusOk{}, &DisconnectionError{Message: "declare plexus failed after reconnect"}
}

// FetchTopology fetches the topology and warms the routing cache, reconnecting
// once on a transient failure.
func (c *Client) FetchTopology(req TopologyRequest) (TopologyOk, error) {
	for attempt := 0; attempt < 2; attempt++ {
		eng, err := c.bootstrapEngine()
		if err != nil {
			return TopologyOk{}, err
		}
		topo, err := eng.FetchTopology(req)
		if err == nil {
			c.topo.replace(topo)
			return topo, nil
		}
		if !isTransient(err) {
			return TopologyOk{}, err
		}
	}
	return TopologyOk{}, &DisconnectionError{Message: "topology fetch failed after reconnect"}
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
