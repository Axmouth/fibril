using System.Threading.Channels;

namespace Fibril;

// Pattern (discovery) subscribe: fan in across every queue or Plexus stream whose
// topic matches a glob, and keep attaching channels that start matching later. The
// catalogue is derived from topology and kept live by topology pushes, so newly
// declared channels are picked up without a reconnect. Discovery is a first-class
// but opt-in surface, reached via Client.Routing so it stays off the default API.

// A *-wildcard topic matcher. Each * matches any run of characters (including
// empty), no regex, mirroring the broker's header-value matcher so the discovery
// glob and the per-subscription filter share one grammar.
internal sealed class TopicGlob
{
    private readonly string[] _segments;

    public TopicGlob(string pattern) => _segments = pattern.Split('*');

    public bool Matches(string value)
    {
        if (_segments.Length == 1)
        {
            return _segments[0] == value;
        }
        var first = _segments[0];
        var last = _segments[^1];
        if (!value.StartsWith(first, StringComparison.Ordinal) || !value.EndsWith(last, StringComparison.Ordinal))
        {
            return false;
        }
        if (value.Length < first.Length + last.Length)
        {
            return false;
        }
        var pos = first.Length;
        var end = value.Length - last.Length;
        for (var i = 1; i < _segments.Length - 1; i++)
        {
            var mid = _segments[i];
            if (mid.Length == 0)
            {
                continue;
            }
            var found = value.IndexOf(mid, pos, StringComparison.Ordinal);
            if (found < 0 || found > end - mid.Length)
            {
                return false;
            }
            pos = found + mid.Length;
        }
        return true;
    }
}

/// <summary>Options for a work-queue pattern subscription. Prefetch applies per attached queue.</summary>
public sealed record PatternSubscribeOptions
{
    public uint Prefetch { get; init; } = 32;
    public string? ConsumerGroup { get; init; }
    public bool AutoAck { get; init; }
}

/// <summary>Options for a Plexus-stream pattern subscription.</summary>
public sealed record StreamPatternSubscribeOptions
{
    public uint Prefetch { get; init; } = 32;
    public StreamStartPosition Start { get; init; } = StreamStartPosition.Latest;
    public IReadOnlyList<StreamHeaderFilter> Filter { get; init; } = Array.Empty<StreamHeaderFilter>();
    public string? DurableName { get; init; }
    public bool AutoAck { get; init; }
}

/// <summary>
/// A live fan-in over every channel matching a glob, with auto-pickup of channels
/// that start matching later. Deliveries carry their source topic. Dispose to stop.
/// </summary>
public sealed class PatternSubscription : IAsyncDisposable
{
    private readonly PatternFanIn _fan;

    internal PatternSubscription(PatternFanIn fan) => _fan = fan;

    /// <summary>The merged stream of deliveries across every matching channel.</summary>
    public IAsyncEnumerable<Delivery> Deliveries(CancellationToken ct = default) => _fan.Reader.ReadAllAsync(ct);

    /// <summary>Stops the subscription: every attached channel and the catalogue watcher.</summary>
    public ValueTask DisposeAsync() => _fan.CloseAsync();
}

// Watches the catalogue and keeps a whole-topic FanIn attached for every matching
// channel, merging them into one stream. Reconcile runs only from the loop (and
// once at start), never concurrently with itself.
internal sealed class PatternFanIn
{
    private readonly Client _client;
    private readonly TopicGlob _glob;
    private readonly bool _stream;
    private readonly Func<string, string?, CancellationToken, Task<FanIn>> _attach;

    private readonly Channel<Delivery> _merged = Channel.CreateUnbounded<Delivery>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Channel<byte> _trigger = Channel.CreateBounded<byte>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
    private readonly CancellationTokenSource _done = new();
    private readonly object _lock = new();
    private readonly Dictionary<string, FanIn> _active = new();
    private readonly List<Task> _forwarders = new();
    private Action _cancelListener = () => { };
    private Task _loop = Task.CompletedTask;
    private bool _closed;

    public PatternFanIn(Client client, TopicGlob glob, bool stream, Func<string, string?, CancellationToken, Task<FanIn>> attach)
    {
        _client = client;
        _glob = glob;
        _stream = stream;
        _attach = attach;
    }

    public ChannelReader<Delivery> Reader => _merged.Reader;

    public async Task StartAsync(CancellationToken ct)
    {
        // Watch for changes before the first reconcile so nothing is missed, then
        // warm topology so that first reconcile sees the current catalogue.
        _cancelListener = _client.AddCatalogueListener(_ => Signal());
        await _client.WarmAllTopologyAsync(ct).ConfigureAwait(false);
        await ReconcileAsync(ct).ConfigureAwait(false);
        _loop = LoopAsync();
    }

    private void Signal() => _trigger.Writer.TryWrite(0);

    private async Task LoopAsync()
    {
        try
        {
            while (await _trigger.Reader.WaitToReadAsync(_done.Token).ConfigureAwait(false))
            {
                while (_trigger.Reader.TryRead(out _))
                {
                }
                await ReconcileAsync(_done.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ReconcileAsync(CancellationToken ct)
    {
        var catalogue = _client.CatalogueSnapshot();
        var desired = new Dictionary<string, (string Topic, string? Group)>();
        if (_stream)
        {
            foreach (var s in catalogue.Streams)
            {
                if (_glob.Matches(s.Topic))
                {
                    desired[s.Topic + "\0"] = (s.Topic, null);
                }
            }
        }
        else
        {
            foreach (var q in catalogue.Queues)
            {
                if (_glob.Matches(q.Topic))
                {
                    desired[q.Topic + "\0" + (q.Group ?? "")] = (q.Topic, q.Group);
                }
            }
        }

        Dictionary<string, FanIn> current;
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }
            current = new Dictionary<string, FanIn>(_active);
        }

        // Close channels whose topic stopped matching (or was removed).
        foreach (var (key, fan) in current)
        {
            if (!desired.ContainsKey(key))
            {
                await fan.DisposeAsync().ConfigureAwait(false);
                lock (_lock)
                {
                    _active.Remove(key);
                }
            }
        }

        // Attach newly matching topics.
        foreach (var (key, match) in desired)
        {
            if (current.ContainsKey(key))
            {
                continue;
            }
            FanIn fan;
            try
            {
                fan = await _attach(match.Topic, match.Group, ct).ConfigureAwait(false);
            }
            catch
            {
                continue; // best effort: retried on the next catalogue change
            }
            lock (_lock)
            {
                if (_closed)
                {
                    _ = fan.DisposeAsync();
                    return;
                }
                _active[key] = fan;
                _forwarders.Add(ForwardAsync(fan));
            }
        }
    }

    private async Task ForwardAsync(FanIn fan)
    {
        try
        {
            await foreach (var d in fan.Deliveries(_done.Token).ConfigureAwait(false))
            {
                await _merged.Writer.WriteAsync(d, _done.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask CloseAsync()
    {
        List<FanIn> fans;
        List<Task> forwarders;
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }
            _closed = true;
            fans = _active.Values.ToList();
            forwarders = _forwarders.ToList();
            _active.Clear();
        }
        _cancelListener();
        _done.Cancel();
        foreach (var fan in fans)
        {
            await fan.DisposeAsync().ConfigureAwait(false);
        }
        try
        {
            await Task.WhenAll(forwarders).ConfigureAwait(false);
            await _loop.ConfigureAwait(false);
        }
        catch
        {
        }
        _merged.Writer.TryComplete();
        _done.Dispose();
    }
}

/// <summary>
/// The opt-in discovery surface over a <see cref="Client"/>: the catalogue plus
/// pattern subscribe. It shares the client's connection.
/// </summary>
public sealed class RoutingClient
{
    private readonly Client _client;

    internal RoutingClient(Client client) => _client = client;

    /// <summary>The current snapshot of declared queues and streams.</summary>
    public Catalogue Catalogue() => _client.CatalogueSnapshot();

    /// <summary>Registers a handler fired when the set of declared channels changes; the return value unregisters it.</summary>
    public Action OnCatalogueChange(Action<Catalogue> handler) => _client.AddCatalogueListener(handler);

    /// <summary>Fans in across every work queue whose topic matches the glob, attaching queues that start matching later.</summary>
    public Task<PatternSubscription> SubscribePatternAsync(string pattern, PatternSubscribeOptions? options = null, CancellationToken ct = default)
        => _client.SubscribePatternAsync(pattern, options ?? new PatternSubscribeOptions(), ct);

    /// <summary>Fans in across every Plexus stream whose topic matches the glob, attaching streams that start matching later.</summary>
    public Task<PatternSubscription> SubscribeStreamPatternAsync(string pattern, StreamPatternSubscribeOptions? options = null, CancellationToken ct = default)
        => _client.SubscribeStreamPatternAsync(pattern, options ?? new StreamPatternSubscribeOptions(), ct);
}

public sealed partial class Client
{
    /// <summary>The opt-in discovery surface: catalogue and pattern subscribe.</summary>
    public RoutingClient Routing => new(this);

    internal Catalogue CatalogueSnapshot() => _topo.SnapshotCatalogue();

    internal Action AddCatalogueListener(Action<Catalogue> handler) => _topo.AddCatalogueListener(handler);

    internal async Task WarmAllTopologyAsync(CancellationToken ct)
    {
        try
        {
            await FetchTopologyInternalAsync(new TopologyRequest(), ct).ConfigureAwait(false);
        }
        catch
        {
            // Best-effort warm before the first reconcile.
        }
    }

    internal async Task<PatternSubscription> SubscribePatternAsync(string pattern, PatternSubscribeOptions options, CancellationToken ct)
    {
        var prefetch = options.Prefetch > 0 ? options.Prefetch : 1u;
        Task<FanIn> Attach(string topic, string? group, CancellationToken c) =>
            NewFanInAsync(topic, group, prefetch, (cc, p) =>
                SuperviseAttachAsync(prefetch, ccc => SubscribeRoutedAsync(new SubscribeFrame
                {
                    Topic = topic,
                    Partition = p,
                    Group = group,
                    ConsumerGroup = options.ConsumerGroup,
                    Prefetch = prefetch,
                    AutoAck = options.AutoAck,
                }, noReconcile: true, ccc), topic, group, cc), c);

        var fan = new PatternFanIn(this, new TopicGlob(pattern), stream: false, Attach);
        await fan.StartAsync(ct).ConfigureAwait(false);
        return new PatternSubscription(fan);
    }

    internal async Task<PatternSubscription> SubscribeStreamPatternAsync(string pattern, StreamPatternSubscribeOptions options, CancellationToken ct)
    {
        var prefetch = options.Prefetch > 0 ? options.Prefetch : 16u;
        Task<FanIn> Attach(string topic, string? group, CancellationToken c) =>
            NewFanInAsync(topic, null, prefetch, (cc, p) =>
                SuperviseAttachAsync(prefetch, ccc => SubscribeStreamRoutedAsync(new SubscribeStream
                {
                    Topic = topic,
                    Partition = p,
                    DurableName = options.DurableName,
                    Start = options.Start.ToWire(),
                    Filter = options.Filter.Select(f => f.ToWire()).ToList(),
                    Prefetch = prefetch,
                    AutoAck = options.AutoAck,
                }, ccc), topic, null, cc), c);

        var fan = new PatternFanIn(this, new TopicGlob(pattern), stream: true, Attach);
        await fan.StartAsync(ct).ConfigureAwait(false);
        return new PatternSubscription(fan);
    }
}
