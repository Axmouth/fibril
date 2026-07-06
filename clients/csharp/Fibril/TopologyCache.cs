namespace Fibril;

// The routing cache: partition counts and per-partition owner endpoints, warmed
// by explicit topology fetches and point-updated by redirects and broker pushes.
// Routing is cache-only and reactive. The client never fetches topology on the hot
// path. A cold cache routes to partition 0 on the bootstrap connection, and a
// misroute is corrected by the broker's redirect, not a pre-flight lookup. Shared
// across caller threads, so it is lock-guarded.
internal sealed class TopologyCache
{
    private readonly object _lock = new();
    private ulong _generation;
    private Dictionary<string, uint> _partitions = new();  // (topic,group) -> partition count
    private Dictionary<string, string> _owners = new();     // (topic,partition,group) -> endpoint
    private DateTime _lastRefresh;                           // last throttled refresh

    private Catalogue _catalogue = new();
    private readonly Dictionary<int, Action<Catalogue>> _listeners = new();
    private int _nextListenerId;

    private static string GroupStr(string? group) => group ?? "";

    private static string QueueKey(string topic, string? group) => topic + "\0" + GroupStr(group);

    private static string PartKey(string topic, uint partition, string? group) => topic + "\0" + GroupStr(group) + "\0" + partition;

    private static string AddrString(AdvertisedAddress a) => a.Host + ":" + a.Port;

    // Installs a full topology snapshot, ignoring a stale (older-generation) one so
    // an out-of-order push cannot regress routing.
    public void Replace(TopologyOk topo)
    {
        List<Action<Catalogue>>? fire = null;
        Catalogue current;
        lock (_lock)
        {
            if (topo.Generation < _generation && _generation != 0)
            {
                return;
            }
            _generation = topo.Generation;
            var partitions = new Dictionary<string, uint>();
            var owners = new Dictionary<string, string>();
            foreach (var e in topo.Queues)
            {
                partitions[QueueKey(e.Topic, e.Group)] = e.PartitionCount;
                if (e.OwnerEndpoints.Count > 0)
                {
                    owners[PartKey(e.Topic, e.Partition, e.Group)] = AddrString(e.OwnerEndpoints[0]);
                }
            }
            foreach (var s in topo.Streams)
            {
                partitions[QueueKey(s.Topic, null)] = s.PartitionCount;
                if (s.OwnerEndpoints.Count > 0)
                {
                    owners[PartKey(s.Topic, s.Partition, null)] = AddrString(s.OwnerEndpoints[0]);
                }
            }
            _partitions = partitions;
            _owners = owners;

            // Refresh the catalogue only from a newer generation, so a topic-filtered
            // fetch (a subset at the same generation) cannot shrink it. Listeners are
            // collected and fired after unlock so one cannot re-enter under the lock.
            if (topo.Generation > _catalogue.Generation || _catalogue.Generation == 0)
            {
                var next = CatalogueBuilder.FromTopology(topo);
                if (!next.SameChannels(_catalogue))
                {
                    fire = _listeners.Values.ToList();
                }
                _catalogue = next;
            }
            current = _catalogue;
        }
        if (fire is not null)
        {
            foreach (var listener in fire)
            {
                listener(current);
            }
        }
    }

    public Catalogue SnapshotCatalogue()
    {
        lock (_lock)
        {
            return _catalogue;
        }
    }

    public Action AddCatalogueListener(Action<Catalogue> handler)
    {
        int id;
        lock (_lock)
        {
            id = _nextListenerId++;
            _listeners[id] = handler;
        }
        return () =>
        {
            lock (_lock)
            {
                _listeners.Remove(id);
            }
        };
    }

    // Installs a broker-pushed snapshot and returns the generation the cache now
    // reflects, for the engine to ack. Matches EngineOptions.OnTopologyUpdate.
    public ulong ApplyPush(TopologyOk topo)
    {
        Replace(topo);
        lock (_lock)
        {
            return _generation;
        }
    }

    public void ApplyRedirect(Redirect rd)
    {
        if (rd.OwnerEndpoints.Count == 0)
        {
            return;
        }
        lock (_lock)
        {
            _owners[PartKey(rd.Topic, rd.Partition, rd.Group)] = AddrString(rd.OwnerEndpoints[0]);
        }
    }

    public uint PartitionCount(string topic, string? group)
    {
        lock (_lock)
        {
            return _partitions.TryGetValue(QueueKey(topic, group), out var pc) && pc > 0 ? pc : 1;
        }
    }

    // Returns the cached owner endpoint for a partition, or "" when unknown (route
    // to the bootstrap and let a redirect correct it).
    public string OwnerOf(string topic, uint partition, string? group)
    {
        lock (_lock)
        {
            return _owners.TryGetValue(PartKey(topic, partition, group), out var owner) ? owner : "";
        }
    }

    // Reports whether a throttled refresh may run now, recording the time when it
    // may. Rate-limits re-fetches under a burst of transient failures.
    public bool DueForRefresh(TimeSpan cooldown)
    {
        lock (_lock)
        {
            var now = DateTime.UtcNow;
            if (_lastRefresh != default && now - _lastRefresh < cooldown)
            {
                return false;
            }
            _lastRefresh = now;
            return true;
        }
    }
}
