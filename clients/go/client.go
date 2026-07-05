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
}

func (o ClientOptions) engineOptions() EngineOptions {
	return EngineOptions{
		ClientName:        o.ClientName,
		ClientVersion:     o.ClientVersion,
		Auth:              o.Auth,
		HeartbeatInterval: o.HeartbeatInterval,
	}
}

// Client is a cluster-aware connection to a Fibril deployment.
type Client struct {
	opts              ClientOptions
	bootstrapEndpoint string
	bootstrap         *Engine
	topo              *topologyCache

	poolMu sync.Mutex
	pool   map[string]*Engine

	rr     atomic.Uint64 // round-robin cursor for keyless publishes
	closed atomic.Bool
}

// Dial connects to a broker at addr and returns a cluster client. addr is the
// bootstrap endpoint; other owners are pooled lazily as routing discovers them.
func Dial(addr string, opts ClientOptions) (*Client, error) {
	if opts.MaxRedirects <= 0 {
		opts.MaxRedirects = defaultMaxRedirects
	}
	boot, err := Connect(addr, opts.engineOptions())
	if err != nil {
		return nil, err
	}
	return &Client{
		opts:              opts,
		bootstrapEndpoint: addr,
		bootstrap:         boot,
		topo:              newTopologyCache(),
		pool:              make(map[string]*Engine),
	}, nil
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
	}
}

// ---- topology cache ----------------------------------------------------

type topologyCache struct {
	mu         sync.RWMutex
	generation uint64
	partitions map[string]uint32 // (topic,group) -> partition count
	owners     map[string]string // (topic,partition,group) -> endpoint
}

func newTopologyCache() *topologyCache {
	return &topologyCache{partitions: map[string]uint32{}, owners: map[string]string{}}
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

// engineFor returns the engine for an endpoint, opening and pooling one if
// needed. An empty or bootstrap endpoint uses the bootstrap connection.
func (c *Client) engineFor(endpoint string) (*Engine, error) {
	if endpoint == "" || endpoint == c.bootstrapEndpoint {
		return c.bootstrap, nil
	}
	c.poolMu.Lock()
	if e, ok := c.pool[endpoint]; ok && !e.IsClosed() {
		c.poolMu.Unlock()
		return e, nil
	}
	c.poolMu.Unlock()

	// Dial outside the lock so a slow connect does not block other routing.
	ne, err := Connect(endpoint, c.opts.engineOptions())
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
	eng, err := c.engineFor(c.topo.ownerOf(p.Topic, p.Partition, p.Group))
	if err != nil {
		return err
	}
	return eng.PublishUnconfirmed(p)
}

// PublishConfirmed routes and sends a confirmed publish, following owner
// redirects up to MaxRedirects, and returns the assigned offset.
func (c *Client) PublishConfirmed(p Publish) (uint64, error) {
	p.Partition = c.partitionFor(p.Topic, p.Group, p.PartitionKey)
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(p.Topic, p.Partition, p.Group))
		if err != nil {
			return 0, err
		}
		off, err := eng.PublishConfirmed(p)
		if err == nil {
			return off, nil
		}
		var re *RedirectError
		if errors.As(err, &re) {
			c.topo.applyRedirect(re.Redirect)
			continue
		}
		return 0, err
	}
	return 0, &DisconnectionError{Message: "too many redirects following the publish owner"}
}

// ---- subscribe ---------------------------------------------------------

// Subscribe subscribes to one partition, routed to its owner and following owner
// redirects up to MaxRedirects.
func (c *Client) Subscribe(req Subscribe) (*Subscription, error) {
	for attempt := 0; attempt <= c.opts.MaxRedirects; attempt++ {
		eng, err := c.engineFor(c.topo.ownerOf(req.Topic, req.Partition, req.Group))
		if err != nil {
			return nil, err
		}
		sub, err := eng.Subscribe(req)
		if err == nil {
			return sub, nil
		}
		var re *RedirectError
		if errors.As(err, &re) {
			c.topo.applyRedirect(re.Redirect)
			continue
		}
		return nil, err
	}
	return nil, &DisconnectionError{Message: "too many redirects following the subscribe owner"}
}

// ---- cluster ops -------------------------------------------------------

// DeclareQueue declares a queue (a cluster op, handled on the bootstrap
// connection).
func (c *Client) DeclareQueue(d DeclareQueue) (DeclareQueueOk, error) {
	return c.bootstrap.DeclareQueue(d)
}

// FetchTopology fetches the topology and warms the routing cache.
func (c *Client) FetchTopology(req TopologyRequest) (TopologyOk, error) {
	topo, err := c.bootstrap.FetchTopology(req)
	if err != nil {
		return TopologyOk{}, err
	}
	c.topo.replace(topo)
	return topo, nil
}

// Shutdown closes the bootstrap connection and every pooled connection.
func (c *Client) Shutdown() {
	if c.closed.Swap(true) {
		return
	}
	c.bootstrap.Shutdown()
	c.poolMu.Lock()
	for endpoint, e := range c.pool {
		e.Shutdown()
		delete(c.pool, endpoint)
	}
	c.poolMu.Unlock()
}
