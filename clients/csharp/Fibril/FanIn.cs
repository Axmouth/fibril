using System.Threading.Channels;

namespace Fibril;

// Multi-partition fan-in: subscribing to a whole topic transparently subscribes
// every partition, supervised so each survives an owner failover, and merges their
// deliveries into one channel. Ordering is per-partition only (Kafka-style), as the
// invariants require. Deliveries settle with Ack/Nack, which route to the partition
// they came from. A background growth loop refreshes topology on an interval and
// attaches any partitions added by a live repartition grow, so a fan-in picks up
// new partitions without the caller re-subscribing.

/// <summary>
/// A subscription across all partitions of a topic. <see cref="Deliveries"/> yields
/// messages from every partition, merged. Dispose to stop it.
/// </summary>
public sealed class FanIn : IAsyncDisposable
{
    private readonly Channel<Delivery> _merged;
    private readonly CancellationTokenSource _cancel = new();
    private readonly Client _client;
    private readonly string _topic;
    private readonly string? _group;
    private readonly Func<CancellationToken, uint, Task<SupervisedSubscription>> _attach;

    private readonly object _lock = new();
    private readonly HashSet<uint> _covered = new();
    private readonly List<SupervisedSubscription> _subs = new();
    private readonly List<Task> _forwarders = new();
    private Task _growth = Task.CompletedTask;
    private bool _closed;

    internal FanIn(Client client, string topic, string? group, Channel<Delivery> merged, Func<CancellationToken, uint, Task<SupervisedSubscription>> attach)
    {
        _client = client;
        _topic = topic;
        _group = group;
        _merged = merged;
        _attach = attach;
    }

    /// <summary>The merged stream of deliveries from every partition, until disposed.</summary>
    public IAsyncEnumerable<Delivery> Deliveries(CancellationToken ct = default) => _merged.Reader.ReadAllAsync(ct);

    // Attaches one partition, reserving it first so concurrent polls and the initial
    // pass do not double-subscribe. When strict is false a failed attach is
    // un-reserved so a later poll retries. Holds no lock across the network subscribe.
    internal async Task AttachPartitionAsync(uint p, bool strict, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_closed || _covered.Contains(p))
            {
                return;
            }
            _covered.Add(p);
        }

        SupervisedSubscription ss;
        try
        {
            ss = await _attach(ct, p).ConfigureAwait(false);
        }
        catch when (!strict)
        {
            lock (_lock)
            {
                _covered.Remove(p);
            }
            return; // best effort: retried on the next poll
        }
        catch
        {
            lock (_lock)
            {
                _covered.Remove(p);
            }
            throw;
        }

        lock (_lock)
        {
            if (_closed)
            {
                _ = ss.DisposeAsync();
                return;
            }
            _subs.Add(ss);
            _forwarders.Add(ForwardAsync(ss));
        }
    }

    private async Task ForwardAsync(SupervisedSubscription ss)
    {
        try
        {
            await foreach (var d in ss.Deliveries(_cancel.Token).ConfigureAwait(false))
            {
                await _merged.Writer.WriteAsync(d, _cancel.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    internal void StartGrowth(TimeSpan interval) => _growth = GrowthLoopAsync(interval);

    private async Task GrowthLoopAsync(TimeSpan interval)
    {
        try
        {
            using var timer = new PeriodicTimer(interval);
            while (await timer.WaitForNextTickAsync(_cancel.Token).ConfigureAwait(false))
            {
                if (_client.IsClientClosed)
                {
                    return;
                }
                await _client.WarmTopologyAsync(_topic, _group, _cancel.Token).ConfigureAwait(false);
                var count = _client.PartitionCountFor(_topic, _group);
                for (uint p = 0; p < count; p++)
                {
                    await AttachPartitionAsync(p, strict: false, _cancel.Token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>Stops the growth loop and every partition subscription, then completes the merged stream.</summary>
    public async ValueTask DisposeAsync()
    {
        List<SupervisedSubscription> subs;
        List<Task> forwarders;
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }
            _closed = true;
            subs = _subs.ToList();
            forwarders = _forwarders.ToList();
        }
        _cancel.Cancel();
        foreach (var s in subs)
        {
            await s.DisposeAsync().ConfigureAwait(false);
        }
        try
        {
            await Task.WhenAll(forwarders).ConfigureAwait(false);
            await _growth.ConfigureAwait(false);
        }
        catch
        {
            // Loops observe cancellation and end; disposal is best-effort.
        }
        _merged.Writer.TryComplete();
        _cancel.Dispose();
    }
}

public sealed partial class Client
{
    // Subscribes the current partition set (strictly, failing the call if a partition
    // cannot be attached) and starts the growth loop.
    private async Task<FanIn> NewFanInAsync(string topic, string? group, uint bufferPerSub, Func<CancellationToken, uint, Task<SupervisedSubscription>> attach, CancellationToken ct)
    {
        // Strict first fetch: a bad topic errors here rather than in the background.
        await FetchTopologyInternalAsync(new TopologyRequest { Topic = topic, Group = group }, ct).ConfigureAwait(false);
        var count = _topo.PartitionCount(topic, group);
        var merged = Channel.CreateBounded<Delivery>(new BoundedChannelOptions((int)(bufferPerSub * count + 1))
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var fanIn = new FanIn(this, topic, group, merged, attach);
        try
        {
            for (uint p = 0; p < count; p++)
            {
                await fanIn.AttachPartitionAsync(p, strict: true, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            await fanIn.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        fanIn.StartGrowth(RepartitionPoll);
        return fanIn;
    }

    /// <summary>
    /// Subscribes to every partition of a queue topic and fans the deliveries into
    /// one channel, supervising each partition and picking up partitions added by a
    /// live repartition grow. Prefetch is per partition.
    /// </summary>
    public Task<FanIn> SubscribeTopicAsync(string topic, string? group = null, uint prefetch = 32, bool autoAck = false, CancellationToken ct = default)
        => NewFanInAsync(topic, group, prefetch, (c, p) => SuperviseAttachAsync(prefetch, cc => SubscribeRoutedAsync(new SubscribeFrame
        {
            Topic = topic,
            Partition = p,
            Group = group,
            Prefetch = prefetch,
            AutoAck = autoAck,
        }, noReconcile: true, cc), topic, group, c), ct);

    /// <summary>
    /// Fans a queue in as a member of the named exclusive cohort: the broker assigns
    /// each partition to a single member, so several members consume the partitioned
    /// topic in order with free failover. Prefetch is per partition.
    /// </summary>
    public Task<FanIn> SubscribeTopicCohortAsync(string topic, string consumerGroup, string? group = null, uint prefetch = 32, bool autoAck = false, CancellationToken ct = default)
        => NewFanInAsync(topic, group, prefetch, (c, p) => SuperviseAttachAsync(prefetch, cc => SubscribeRoutedAsync(new SubscribeFrame
        {
            Topic = topic,
            Partition = p,
            Group = group,
            ConsumerGroup = consumerGroup,
            Prefetch = prefetch,
            AutoAck = autoAck,
        }, noReconcile: true, cc), topic, group, c), ct);

    /// <summary>
    /// Joins the default cohort, for the common case of one exclusive cohort per
    /// queue. Run several instances that all call this on the same queue and they
    /// self-organize into the cohort.
    /// </summary>
    public Task<FanIn> SubscribeTopicExclusiveAsync(string topic, uint prefetch = 32, bool autoAck = false, CancellationToken ct = default)
        => SubscribeTopicCohortAsync(topic, DefaultCohortId, null, prefetch, autoAck, ct);
}
