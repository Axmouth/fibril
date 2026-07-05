package fibril

import "sort"

// Catalogue is a snapshot of the channels declared in the cluster: every queue
// and Plexus stream the client currently knows about, with partition counts.
// Derived from the topology and kept live by topology pushes, so it needs no
// extra round-trips. Queues and Streams are sorted (by topic, then group) for a
// stable order. Read it with Client.Catalogue, or watch changes with
// Client.OnCatalogueChange.
type Catalogue struct {
	Queues     []QueueInfo
	Streams    []StreamInfo
	Generation uint64
}

// QueueInfo is one declared work queue in the catalogue.
type QueueInfo struct {
	Topic          string
	Group          *string
	PartitionCount uint32
}

// StreamInfo is one declared Plexus stream in the catalogue.
type StreamInfo struct {
	Topic          string
	PartitionCount uint32
}

// catalogueFromTopology derives the catalogue from a topology snapshot. The
// topology lists one entry per partition, so queues dedupe by (topic, group) and
// streams by topic; both come back sorted for a deterministic order.
func catalogueFromTopology(topo TopologyOk) Catalogue {
	queues := map[string]QueueInfo{}
	for _, q := range topo.Queues {
		count := q.PartitionCount
		if count < 1 {
			count = 1
		}
		queues[queueCacheKey(q.Topic, q.Group)] = QueueInfo{Topic: q.Topic, Group: q.Group, PartitionCount: count}
	}
	streams := map[string]StreamInfo{}
	for _, s := range topo.Streams {
		count := s.PartitionCount
		if count < 1 {
			count = 1
		}
		streams[s.Topic] = StreamInfo{Topic: s.Topic, PartitionCount: count}
	}

	c := Catalogue{Generation: topo.Generation}
	for _, q := range queues {
		c.Queues = append(c.Queues, q)
	}
	sort.Slice(c.Queues, func(i, j int) bool {
		if c.Queues[i].Topic != c.Queues[j].Topic {
			return c.Queues[i].Topic < c.Queues[j].Topic
		}
		return groupStr(c.Queues[i].Group) < groupStr(c.Queues[j].Group)
	})
	for _, s := range streams {
		c.Streams = append(c.Streams, s)
	}
	sort.Slice(c.Streams, func(i, j int) bool { return c.Streams[i].Topic < c.Streams[j].Topic })
	return c
}

// sameChannels reports whether two catalogues list the same set of queues and
// streams (ignoring generation), so an owner-only topology churn fires no
// listener.
func (c Catalogue) sameChannels(other Catalogue) bool {
	if len(c.Queues) != len(other.Queues) || len(c.Streams) != len(other.Streams) {
		return false
	}
	for i := range c.Queues {
		if c.Queues[i].Topic != other.Queues[i].Topic ||
			groupStr(c.Queues[i].Group) != groupStr(other.Queues[i].Group) ||
			c.Queues[i].PartitionCount != other.Queues[i].PartitionCount {
			return false
		}
	}
	for i := range c.Streams {
		if c.Streams[i] != other.Streams[i] {
			return false
		}
	}
	return true
}

func (t *topologyCache) snapshotCatalogue() Catalogue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.catalogue
}

func (t *topologyCache) addCatalogueListener(fn func(Catalogue)) (cancel func()) {
	t.mu.Lock()
	id := t.nextListenerID
	t.nextListenerID++
	t.catListeners[id] = fn
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		delete(t.catListeners, id)
		t.mu.Unlock()
	}
}

// Catalogue returns the current snapshot of declared queues and streams, derived
// from topology and kept live by topology pushes.
func (c *Client) Catalogue() Catalogue {
	return c.topo.snapshotCatalogue()
}

// OnCatalogueChange registers a handler fired whenever the set of declared
// queues or streams changes. It returns a cancel function that unregisters it.
func (c *Client) OnCatalogueChange(fn func(Catalogue)) (cancel func()) {
	return c.topo.addCatalogueListener(fn)
}
