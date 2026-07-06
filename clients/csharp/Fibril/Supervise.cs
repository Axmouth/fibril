using System.Threading.Channels;

namespace Fibril;

// A supervised subscription survives an owner failover: when its connection drops,
// a supervisor task refreshes topology, re-subscribes to the new owner, and keeps
// delivering on the same stable channel until the caller disposes it. Because each
// Delivery carries the connection it arrived on, acks keep working across a
// re-subscribe.

/// <summary>
/// A subscription that re-attaches across owner failovers. <see cref="Deliveries"/>
/// yields messages until the subscription is disposed.
/// </summary>
public sealed class SupervisedSubscription : IAsyncDisposable
{
    private readonly Channel<Delivery> _out;
    private readonly CancellationTokenSource _cancel;
    private readonly Task _loop;

    internal SupervisedSubscription(Channel<Delivery> outChannel, CancellationTokenSource cancel, Func<Task> loop)
    {
        _out = outChannel;
        _cancel = cancel;
        _loop = loop();
    }

    /// <summary>The stream of deliveries, merged across re-attaches, until disposed.</summary>
    public IAsyncEnumerable<Delivery> Deliveries(CancellationToken ct = default) => _out.Reader.ReadAllAsync(ct);

    /// <summary>Stops the supervisor and completes <see cref="Deliveries"/>.</summary>
    public async ValueTask DisposeAsync()
    {
        _cancel.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch
        {
            // The loop swallows its own errors; disposal is best-effort.
        }
        _cancel.Dispose();
    }
}

public sealed partial class Client
{
    /// <summary>
    /// Subscribes to one queue partition and keeps it attached across owner
    /// failovers. The first attach is synchronous (so a bad topic errors here) and
    /// later re-attaches happen in the background on the same channel.
    /// </summary>
    public Task<SupervisedSubscription> SuperviseSubscribeAsync(
        string topic,
        uint partition = 0,
        string? group = null,
        uint prefetch = 32,
        bool autoAck = false,
        CancellationToken ct = default)
        => SuperviseAttachAsync(prefetch, c => SubscribeRoutedAsync(new SubscribeFrame
        {
            Topic = topic,
            Partition = partition,
            Group = group,
            Prefetch = prefetch,
            AutoAck = autoAck,
        }, noReconcile: true, c), topic, group, ct);

    // Opens the initial subscription via attach (using the caller's ct) and
    // supervises re-attaches on the same channel. Background re-attaches use the
    // supervisor's own cancellation, bounded by disposal.
    internal async Task<SupervisedSubscription> SuperviseAttachAsync(
        uint prefetch,
        Func<CancellationToken, Task<Subscription>> attach,
        string topic,
        string? group,
        CancellationToken ct)
    {
        var sub = await attach(ct).ConfigureAwait(false);
        var capHint = (int)Math.Max(prefetch, 1);
        var outChannel = Channel.CreateBounded<Delivery>(new BoundedChannelOptions(capHint)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var cancel = new CancellationTokenSource();
        return new SupervisedSubscription(outChannel, cancel, () => SuperviseLoopAsync(topic, group, attach, sub, outChannel, cancel.Token));
    }

    private async Task SuperviseLoopAsync(
        string topic,
        string? group,
        Func<CancellationToken, Task<Subscription>> attach,
        Subscription sub,
        Channel<Delivery> outChannel,
        CancellationToken cancel)
    {
        try
        {
            while (!cancel.IsCancellationRequested)
            {
                // Forward until this attachment's channel closes (a drop) or we are
                // asked to stop.
                try
                {
                    await foreach (var d in sub.Deliveries(cancel).ConfigureAwait(false))
                    {
                        await outChannel.Writer.WriteAsync(d, cancel).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                // The connection dropped. Re-attach: back off, refresh topology so a
                // failed-over owner is picked up, then re-subscribe. Stop if the
                // client is shutting down or a permanent error occurs.
                var reattached = false;
                while (!reattached)
                {
                    if (cancel.IsCancellationRequested || IsClientClosed)
                    {
                        return;
                    }
                    try
                    {
                        await Task.Delay(SuperviseBackoff, cancel).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    await WarmTopologyAsync(topic, group, cancel).ConfigureAwait(false);
                    try
                    {
                        sub = await attach(cancel).ConfigureAwait(false);
                        reattached = true;
                    }
                    catch (Exception ex) when (IsTransient(ex) && !IsClientClosed)
                    {
                        // Transient: retry the re-attach.
                    }
                    catch
                    {
                        return; // permanent error (for example, the topic is gone)
                    }
                }
            }
        }
        finally
        {
            outChannel.Writer.TryComplete();
        }
    }
}
