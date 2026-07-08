namespace Fibril;

// The client is the cluster-aware layer over the per-connection engine: a
// connection pool (one engine per endpoint), a topology cache, partition routing,
// and bounded redirect-follow. Routing is cache-only and reactive. The client
// never fetches topology on the hot path. It routes from a cache warmed by
// explicit fetches and point-updated by redirects, and a cold cache routes to
// partition 0 on the bootstrap connection. A misroute is corrected by the broker's
// redirect, not a pre-flight lookup.
//
// Unlike the engine (a single actor), the client is shared across the caller's
// threads, so the pool and topology cache are lock-guarded and reconnects are
// serialized.

/// <summary>Username and password credentials for broker authentication.</summary>
public sealed record Credentials(string Username, string Password);

/// <summary>Connection-level options for a <see cref="Client"/>.</summary>
public sealed record ClientOptions
{
    public Credentials? Credentials { get; init; }
    public string ClientName { get; init; } = "fibril-csharp";
    public string ClientVersion { get; init; } = "";
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Bounds how many owner redirects a single op will follow. Zero uses a default.</summary>
    public int MaxRedirects { get; init; }

    /// <summary>The pause before retrying an op after a transient failure. Zero uses a default.</summary>
    public TimeSpan RetryBackoff { get; init; }

    /// <summary>Throttles how often a transient failure triggers a topology refresh. Zero uses a default.</summary>
    public TimeSpan TopologyRefreshCooldown { get; init; }

    /// <summary>The pause between re-subscribe attempts after a drop in a supervised subscription. Zero uses a default.</summary>
    public TimeSpan SuperviseBackoff { get; init; }

    /// <summary>How often a whole-topic fan-in refreshes topology to pick up partitions added by a live grow. Zero uses a default.</summary>
    public TimeSpan RepartitionPollInterval { get; init; }

    /// <summary>When set, connects over TLS with these trust settings. Null connects plaintext.</summary>
    public TlsOptions? Tls { get; init; }

    /// <summary>When set, called for each exclusive-cohort assignment change pushed by the broker.</summary>
    public Action<AssignmentChange>? OnAssignmentChanged { get; init; }

    /// <summary>
    /// How the broker reconciles this client's subscriptions after a reconnect. The
    /// conservative default is the safest operational behavior; restore additionally
    /// recreates client-owned subscriptions missing after a resume.
    /// </summary>
    public ReconcilePolicy ReconcilePolicy { get; init; } = ReconcilePolicy.Conservative;
}

/// <summary>Notifies an exclusive-cohort member of its new partition assignment.</summary>
public sealed record AssignmentChange(
    string Topic,
    string ConsumerGroup,
    ulong Generation,
    IReadOnlyList<uint> Assigned,
    IReadOnlyList<uint> Added,
    IReadOnlyList<uint> Revoked);

/// <summary>The outcome of a queue or stream declaration.</summary>
public sealed record DeclareOutcome(string Status, int PartitionCount);

/// <summary>A cluster-aware connection to a Fibril deployment.</summary>
public sealed partial class Client : IAsyncDisposable
{
    /// <summary>The exclusive cohort a queue subscription joins when it asks for exclusive consumption without naming one.</summary>
    public const string DefaultCohortId = "default";

    private const int DefaultMaxRedirects = 3;
    private static readonly TimeSpan DefaultRetryBackoff = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan DefaultTopologyRefreshCooldown = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultSuperviseBackoff = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan DefaultRepartitionPoll = TimeSpan.FromSeconds(2);

    // Bounds the best-effort refresh between retries so a broker that accepts the
    // connection but does not answer cannot wedge the retry loop.
    private static readonly TimeSpan TopologyRefreshTimeout = TimeSpan.FromSeconds(5);

    private readonly ClientOptions _opts;
    private readonly string _bootstrapEndpoint;
    private readonly AuthFrame? _auth;
    private readonly int _maxRedirects;
    private readonly TopologyCache _topo = new();

    private readonly SemaphoreSlim _bootstrapGate = new(1, 1);
    private Engine _bootstrap = null!;

    private readonly object _poolLock = new();
    private readonly Dictionary<string, Engine> _pool = new();

    private long _rr; // round-robin cursor for keyless publishes
    private volatile bool _closed;

    private readonly object _cohortLock = new();
    private Uuid? _cohortMemberId; // captured once, carried across cohort subscribes

    // Per-endpoint reconcile registries, which survive that endpoint's engine reconnects.
    private readonly object _reconcileLock = new();
    private readonly Dictionary<string, ReconcileRegistry> _reconcile = new();

    private Client(string bootstrapEndpoint, ClientOptions opts)
    {
        _opts = opts;
        _bootstrapEndpoint = bootstrapEndpoint;
        _auth = opts.Credentials is null ? null : new AuthFrame(opts.Credentials.Username, opts.Credentials.Password);
        _maxRedirects = opts.MaxRedirects > 0 ? opts.MaxRedirects : DefaultMaxRedirects;
    }

    /// <summary>
    /// Connects to a broker at <paramref name="addr"/> (host:port) and returns a
    /// cluster client. <paramref name="addr"/> is the bootstrap endpoint. Other owners
    /// are pooled lazily as routing discovers them. Cancel <paramref name="ct"/> to
    /// bound the initial connect.
    /// </summary>
    public static async Task<Client> ConnectAsync(string addr, ClientOptions? options = null, CancellationToken ct = default)
    {
        var client = new Client(addr, options ?? new ClientOptions());
        client._bootstrap = await Engine.ConnectAsync(addr, client.EngineOpts(addr), ct).ConfigureAwait(false);
        return client;
    }

    // Per-engine options, including the topology-push callback that keeps the routing
    // cache warm. A reconnect offers the prior session's resume identity.
    private EngineOptions EngineOpts(string endpoint, ResumeIdentity? resume = null) => new()
    {
        ClientName = _opts.ClientName,
        ClientVersion = _opts.ClientVersion,
        Auth = _auth,
        HeartbeatInterval = _opts.HeartbeatInterval,
        Resume = resume,
        Tls = _opts.Tls,
        Reconcile = ReconcileFor(endpoint),
        OnTopologyUpdate = _topo.ApplyPush,
        OnAssignmentChanged = _opts.OnAssignmentChanged is null
            ? null
            : a => _opts.OnAssignmentChanged(new AssignmentChange(a.Topic, a.ConsumerGroup, a.Generation, a.Assigned, a.Added, a.Revoked)),
    };

    // The endpoint's reconcile registry, created on first use and shared across the
    // reconnects of that endpoint's engine.
    private ReconcileRegistry ReconcileFor(string endpoint)
    {
        lock (_reconcileLock)
        {
            if (!_reconcile.TryGetValue(endpoint, out var registry))
            {
                registry = new ReconcileRegistry(_opts.ReconcilePolicy);
                _reconcile[endpoint] = registry;
            }
            return registry;
        }
    }

    // ---- routing ----

    // Chooses a partition: FNV-1a of the key mod the partition count, or round-robin
    // across partitions for a keyless publish.
    private uint PartitionFor(string topic, string? group, byte[]? key)
    {
        var pc = _topo.PartitionCount(topic, group);
        if (pc <= 1)
        {
            return 0;
        }
        if (key is not null)
        {
            return (uint)(Fnv.Hash1a(key) % pc);
        }
        return (uint)((ulong)Interlocked.Increment(ref _rr) % pc);
    }

    // Returns a live bootstrap engine, reconnecting it (offering the previous
    // session's resume identity) if the connection dropped. Serialized so concurrent
    // callers reconnect once.
    private async Task<Engine> BootstrapEngineAsync(CancellationToken ct)
    {
        if (!_bootstrap.IsClosed)
        {
            return _bootstrap;
        }
        await _bootstrapGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_bootstrap.IsClosed)
            {
                return _bootstrap;
            }
            if (_closed)
            {
                throw new BrokenPipeException("client shut down");
            }
            _bootstrap = await Engine.ConnectAsync(_bootstrapEndpoint, EngineOpts(_bootstrapEndpoint, _bootstrap.ResumeIdentity), ct).ConfigureAwait(false);
            return _bootstrap;
        }
        finally
        {
            _bootstrapGate.Release();
        }
    }

    // Returns the engine for an endpoint, opening and pooling one if needed. An empty
    // or bootstrap endpoint uses the (reconnecting) bootstrap.
    private async Task<Engine> EngineForAsync(string endpoint, CancellationToken ct)
    {
        if (endpoint.Length == 0 || endpoint == _bootstrapEndpoint)
        {
            return await BootstrapEngineAsync(ct).ConfigureAwait(false);
        }

        Engine? existing;
        lock (_poolLock)
        {
            _pool.TryGetValue(endpoint, out existing);
        }
        if (existing is not null && !existing.IsClosed)
        {
            return existing;
        }

        // Dial outside the lock so a slow connect does not block other routing. A
        // reconnect offers the prior session's resume identity.
        var opts = existing is not null ? EngineOpts(endpoint, existing.ResumeIdentity) : EngineOpts(endpoint);
        var ne = await Engine.ConnectAsync(endpoint, opts, ct).ConfigureAwait(false);

        Engine? winner = null;
        lock (_poolLock)
        {
            if (_pool.TryGetValue(endpoint, out var current) && !current.IsClosed)
            {
                winner = current;
            }
            else
            {
                _pool[endpoint] = ne;
            }
        }
        if (winner is not null)
        {
            await ne.DisposeAsync().ConfigureAwait(false); // lost a race, keep the existing one
            return winner;
        }
        return ne;
    }

    // Runs between retry attempts: refreshes topology (throttled and time-bounded) so
    // the next attempt re-routes to the current owner, then backs off briefly.
    private async Task AfterTransientAsync(string topic, string? group, CancellationToken ct)
    {
        var cooldown = _opts.TopologyRefreshCooldown > TimeSpan.Zero ? _opts.TopologyRefreshCooldown : DefaultTopologyRefreshCooldown;
        if (_topo.DueForRefresh(cooldown))
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TopologyRefreshTimeout);
            try
            {
                await FetchTopologyInternalAsync(new TopologyRequest { Topic = topic, Group = group }, cts.Token).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort: the next attempt still routes from whatever the cache holds.
            }
        }
        var backoff = _opts.RetryBackoff > TimeSpan.Zero ? _opts.RetryBackoff : DefaultRetryBackoff;
        try
        {
            await Task.Delay(backoff, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static bool IsTransient(Exception ex)
    {
        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            if (e is DisconnectionException or BrokenPipeException)
            {
                return true;
            }
        }
        return false;
    }

    // ---- public surface ----

    /// <summary>Returns a publisher bound to <paramref name="topic"/>.</summary>
    public Publisher Publisher(string topic) => new(this, topic, null);

    /// <summary>
    /// Subscribes to one partition of <paramref name="topic"/>, routed to its owner
    /// and following owner redirects, and returns the live subscription.
    /// </summary>
    public Task<Subscription> SubscribeAsync(
        string topic,
        uint partition = 0,
        string? group = null,
        uint prefetch = 32,
        bool autoAck = false,
        CancellationToken ct = default)
        => SubscribeRoutedAsync(new SubscribeFrame
        {
            Topic = topic,
            Partition = partition,
            Group = group,
            Prefetch = prefetch,
            AutoAck = autoAck,
        }, noReconcile: false, ct);

    internal async Task<Subscription> SubscribeRoutedAsync(SubscribeFrame req, bool noReconcile, CancellationToken ct)
    {
        req = ApplyCohortMember(req);
        for (var attempt = 0; attempt <= _maxRedirects; attempt++)
        {
            var eng = await EngineForAsync(_topo.OwnerOf(req.Topic, req.Partition, req.Group), ct).ConfigureAwait(false);
            try
            {
                var sub = await eng.SubscribeAsync(req, noReconcile, ct).ConfigureAwait(false);
                CaptureCohortMember(req, sub);
                return sub;
            }
            catch (RedirectException re)
            {
                _topo.ApplyRedirect(re.Redirect);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await AfterTransientAsync(req.Topic, req.Group, ct).ConfigureAwait(false);
            }
        }
        throw new DisconnectionException("gave up after too many redirect/reconnect attempts on subscribe");
    }

    internal async Task<Subscription> SubscribeStreamRoutedAsync(SubscribeStream req, CancellationToken ct)
    {
        for (var attempt = 0; attempt <= _maxRedirects; attempt++)
        {
            var eng = await EngineForAsync(_topo.OwnerOf(req.Topic, req.Partition, null), ct).ConfigureAwait(false);
            try
            {
                return await eng.SubscribeStreamAsync(req, ct).ConfigureAwait(false);
            }
            catch (RedirectException re)
            {
                _topo.ApplyRedirect(re.Redirect);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await AfterTransientAsync(req.Topic, null, ct).ConfigureAwait(false);
            }
        }
        throw new DisconnectionException("gave up after too many redirect/reconnect attempts on stream subscribe");
    }

    // Offers the client's captured cohort member id on a consumer-group subscribe,
    // so re-subscribes and sibling partitions present the same identity to the cohort.
    private SubscribeFrame ApplyCohortMember(SubscribeFrame req)
    {
        if (req.ConsumerGroup is null || req.MemberId is not null)
        {
            return req;
        }
        lock (_cohortLock)
        {
            return _cohortMemberId is { } mid ? req with { MemberId = mid } : req;
        }
    }

    // Records the broker-minted member id from the first consumer-group subscribe.
    private void CaptureCohortMember(SubscribeFrame req, Subscription sub)
    {
        if (req.ConsumerGroup is null || sub.MemberId is null)
        {
            return;
        }
        lock (_cohortLock)
        {
            _cohortMemberId ??= sub.MemberId;
        }
    }

    // ---- helpers used by the supervisor and fan-in ----

    internal bool IsClientClosed => _closed;
    internal TimeSpan SuperviseBackoff => _opts.SuperviseBackoff > TimeSpan.Zero ? _opts.SuperviseBackoff : DefaultSuperviseBackoff;
    internal TimeSpan RepartitionPoll => _opts.RepartitionPollInterval > TimeSpan.Zero ? _opts.RepartitionPollInterval : DefaultRepartitionPoll;
    internal uint PartitionCountFor(string topic, string? group) => _topo.PartitionCount(topic, group);

    internal async Task WarmTopologyAsync(string topic, string? group, CancellationToken ct)
    {
        try
        {
            await FetchTopologyInternalAsync(new TopologyRequest { Topic = topic, Group = group }, ct).ConfigureAwait(false);
        }
        catch
        {
            // Best-effort warm: routing falls back to whatever the cache holds.
        }
    }

    /// <summary>Declares a queue with a partition count and waits for the broker's confirmation.</summary>
    public Task<DeclareOutcome> DeclareQueueAsync(string topic, int? partitionCount = null, CancellationToken ct = default)
        => DeclareQueueAsync(topic, new QueueConfig { PartitionCount = partitionCount }, ct);

    /// <summary>Declares a Plexus (fan-out stream) channel with a partition count and waits for confirmation.</summary>
    public Task<DeclareOutcome> DeclarePlexusAsync(string topic, int? partitionCount = null, CancellationToken ct = default)
        => DeclarePlexusAsync(topic, new StreamConfig { PartitionCount = partitionCount }, ct);

    /// <summary>Fetches the topology and warms the routing cache.</summary>
    public async Task<TopologyView> FetchTopologyAsync(string? topic = null, CancellationToken ct = default)
    {
        var topo = await FetchTopologyInternalAsync(new TopologyRequest { Topic = topic }, ct).ConfigureAwait(false);
        return new TopologyView(topo.Generation);
    }

    private async Task<TopologyOk> FetchTopologyInternalAsync(TopologyRequest req, CancellationToken ct)
    {
        var topo = await BootstrapOpAsync(eng => eng.FetchTopologyAsync(req, ct), ct).ConfigureAwait(false);
        _topo.Replace(topo);
        return topo;
    }

    // A cluster op handled on the bootstrap connection, reconnecting once on a
    // transient failure.
    private async Task<T> BootstrapOpAsync<T>(Func<Engine, Task<T>> op, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var eng = await BootstrapEngineAsync(ct).ConfigureAwait(false);
            try
            {
                return await op(eng).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransient(ex) && attempt == 0)
            {
                // Reconnect and retry once.
            }
        }
        throw new DisconnectionException("cluster op failed after reconnect");
    }

    // ---- routed publish (called by Publisher, which builds the frame) ----

    internal async Task PublishUnconfirmedAsync(PublishFrame p, CancellationToken ct)
    {
        var partition = PartitionFor(p.Topic, p.Group, p.PartitionKey);
        p = p with { Partition = partition };
        for (var attempt = 0; attempt <= _maxRedirects; attempt++)
        {
            try
            {
                var eng = await EngineForAsync(_topo.OwnerOf(p.Topic, partition, p.Group), ct).ConfigureAwait(false);
                await eng.PublishUnconfirmedAsync(p, ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await AfterTransientAsync(p.Topic, p.Group, ct).ConfigureAwait(false);
            }
        }
        throw new DisconnectionException("gave up publishing after reconnect attempts");
    }

    internal Task<ulong> PublishConfirmedAsync(PublishFrame p, CancellationToken ct)
        => RoutedConfirmAsync(p.Topic, p.Group, p.PartitionKey, (eng, part) => eng.PublishConfirmedAsync(p with { Partition = part }, ct), ct);

    internal Task<ulong> PublishDelayedConfirmedAsync(PublishDelayedFrame p, CancellationToken ct)
        => RoutedConfirmAsync(p.Topic, p.Group, p.PartitionKey, (eng, part) => eng.PublishDelayedConfirmedAsync(p with { Partition = part }, ct), ct);

    internal async Task PublishDelayedUnconfirmedAsync(PublishDelayedFrame p, CancellationToken ct)
    {
        var partition = PartitionFor(p.Topic, p.Group, p.PartitionKey);
        p = p with { Partition = partition };
        for (var attempt = 0; attempt <= _maxRedirects; attempt++)
        {
            try
            {
                var eng = await EngineForAsync(_topo.OwnerOf(p.Topic, partition, p.Group), ct).ConfigureAwait(false);
                await eng.PublishDelayedUnconfirmedAsync(p, ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await AfterTransientAsync(p.Topic, p.Group, ct).ConfigureAwait(false);
            }
        }
        throw new DisconnectionException("gave up publishing after reconnect attempts");
    }

    // Routes a confirmed publish to the partition owner, following redirects and
    // reconnecting on transient failure, and returns the assigned offset.
    private async Task<ulong> RoutedConfirmAsync(string topic, string? group, byte[]? key, Func<Engine, uint, Task<ulong>> op, CancellationToken ct)
    {
        var partition = PartitionFor(topic, group, key);
        for (var attempt = 0; attempt <= _maxRedirects; attempt++)
        {
            var eng = await EngineForAsync(_topo.OwnerOf(topic, partition, group), ct).ConfigureAwait(false);
            try
            {
                return await op(eng, partition).ConfigureAwait(false);
            }
            catch (RedirectException re)
            {
                _topo.ApplyRedirect(re.Redirect);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await AfterTransientAsync(topic, group, ct).ConfigureAwait(false);
            }
        }
        throw new DisconnectionException("gave up after too many redirect/reconnect attempts on publish");
    }

    // Routes a with-confirmation publish once (no redirect-follow, since the
    // confirmation resolves asynchronously) and returns a handle for its offset. A
    // stale route surfaces as a RedirectException from Confirmed.
    internal async Task<PublishConfirmation> PublishWithConfirmationAsync(PublishFrame p, CancellationToken ct)
    {
        var partition = PartitionFor(p.Topic, p.Group, p.PartitionKey);
        p = p with { Partition = partition };
        var eng = await EngineForAsync(_topo.OwnerOf(p.Topic, partition, p.Group), ct).ConfigureAwait(false);
        return eng.PublishWithConfirmation(p, ct);
    }

    internal async Task<PublishConfirmation> PublishDelayedWithConfirmationAsync(PublishDelayedFrame p, CancellationToken ct)
    {
        var partition = PartitionFor(p.Topic, p.Group, p.PartitionKey);
        p = p with { Partition = partition };
        var eng = await EngineForAsync(_topo.OwnerOf(p.Topic, partition, p.Group), ct).ConfigureAwait(false);
        return eng.PublishDelayedWithConfirmation(p, ct);
    }

    /// <summary>Closes the bootstrap connection and every pooled connection.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_closed)
        {
            return;
        }
        _closed = true;
        await _bootstrap.DisposeAsync().ConfigureAwait(false);
        List<Engine> pooled;
        lock (_poolLock)
        {
            pooled = _pool.Values.ToList();
            _pool.Clear();
        }
        foreach (var e in pooled)
        {
            await e.DisposeAsync().ConfigureAwait(false);
        }
        // Complete any preserved subscription channels the engines left for a reconnect
        // that will not come now.
        List<ReconcileRegistry> registries;
        lock (_reconcileLock)
        {
            registries = _reconcile.Values.ToList();
            _reconcile.Clear();
        }
        foreach (var registry in registries)
        {
            registry.CloseAll();
        }
    }
}

/// <summary>A read-only view of a topology snapshot returned to callers.</summary>
public sealed record TopologyView(ulong Generation);

public sealed partial class Client
{
    /// <summary>Returns a publisher bound to <paramref name="topic"/> within a group namespace.</summary>
    public Publisher PublisherGrouped(string topic, string group) => new(this, topic, group);

    /// <summary>
    /// Returns a reliable publisher bound to <paramref name="topic"/>: it stamps each
    /// message with a stable producer id and a monotonic sequence, so a publish
    /// retried after a transient failure carries the same identity and the broker can
    /// deduplicate it.
    /// </summary>
    public ReliablePublisher ReliablePublisher(string topic) => new(this, topic, null);

    /// <summary>Returns a reliable publisher bound to <paramref name="topic"/> within a group namespace.</summary>
    public ReliablePublisher ReliablePublisherGrouped(string topic, string group) => new(this, topic, group);
}
