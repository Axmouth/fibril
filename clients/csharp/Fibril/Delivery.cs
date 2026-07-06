using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Fibril;

// The delivery path: subscribe, receive pushed messages as an IAsyncEnumerable
// per subscription, and settle them with ack/nack. The actor owns the sub-id ->
// channel map and pushes deliveries. Settle is fire-and-forget and stays
// cancellation-token-free, matching the reference client's complete/retry.

/// <summary>One message pushed to a subscription.</summary>
public sealed class Delivery
{
    public string Topic { get; }
    public string? Group { get; }
    public uint Partition { get; }
    public byte[] Payload { get; }
    public ContentType ContentType { get; }
    public IReadOnlyDictionary<string, string> Headers { get; }
    public ulong Offset { get; }
    public ulong Published { get; }
    public ulong PublishReceived { get; }

    /// <summary>
    /// True when the broker already settled this delivery server-side (an auto-ack
    /// subscription). Ack/Nack are then unnecessary.
    /// </summary>
    public bool AutoAck { get; }

    internal DeliveryTag Tag { get; }
    internal ulong SubId { get; }
    private readonly ulong _reqId;   // the DELIVER frame id, reused when settling
    private readonly Engine _engine; // the connection this arrived on

    internal Delivery(Deliver d, ulong reqId, bool autoAck, Engine engine)
    {
        Topic = d.Topic;
        Group = d.Group;
        Partition = d.Partition;
        Payload = d.Payload;
        ContentType = d.ContentType;
        Headers = d.Headers ?? new Dictionary<string, string>();
        Offset = d.Offset;
        Published = d.Published;
        PublishReceived = d.PublishReceived;
        Tag = d.DeliveryTag;
        SubId = d.SubId;
        AutoAck = autoAck;
        _reqId = reqId;
        _engine = engine;
    }

    /// <summary>The delivery payload as a UTF-8 string.</summary>
    public string Text() => System.Text.Encoding.UTF8.GetString(Payload);

    /// <summary>Decodes the delivery payload from JSON into <typeparamref name="T"/>.</summary>
    public T? Json<T>()
    {
        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<T>(Payload);
        }
        catch (System.Text.Json.JsonException ex)
        {
            throw new DeserializationException("json decode: " + ex.Message);
        }
    }

    /// <summary>Settles this delivery as processed, on the connection it arrived on.</summary>
    public void Ack() => _engine.Ack(this);

    /// <summary>Returns this delivery unprocessed, optionally requeuing it.</summary>
    public void Nack(bool requeue, ulong? notBefore = null) => _engine.Nack(this, requeue, notBefore);

    /// <summary>Requeues this delivery immediately for redelivery.</summary>
    public void Retry() => _engine.Nack(this, true, null);

    /// <summary>
    /// Settles this delivery as a terminal failure: not requeued, so it is
    /// dead-lettered or dropped per the queue's policy.
    /// </summary>
    public void Fail() => _engine.Nack(this, false, null);

    /// <summary>Requeues this delivery for redelivery no sooner than <paramref name="delay"/> from now.</summary>
    public void RetryAfter(TimeSpan delay)
    {
        var notBefore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (long)delay.TotalMilliseconds;
        if (notBefore < 0)
        {
            notBefore = 0;
        }
        _engine.Nack(this, true, (ulong)notBefore);
    }
}

/// <summary>
/// A live single-partition subscription. <see cref="Deliveries"/> yields messages
/// until the connection closes. Settle each with its Ack/Nack.
/// </summary>
public sealed class Subscription
{
    public ulong SubId { get; }
    public string Topic { get; }
    public uint Partition { get; }
    public string? Group { get; }
    internal Uuid? MemberId { get; }

    private readonly ChannelReader<Delivery> _reader;

    internal Subscription(SubscribeOk ok, ChannelReader<Delivery> reader)
    {
        SubId = ok.SubId;
        Topic = ok.Topic;
        Partition = ok.Partition;
        Group = ok.Group;
        MemberId = ok.MemberId;
        _reader = reader;
    }

    /// <summary>The stream of deliveries, ending when the connection closes.</summary>
    public IAsyncEnumerable<Delivery> Deliveries(CancellationToken ct = default) => _reader.ReadAllAsync(ct);
}

internal sealed partial class Engine
{
    // ---- publish ----

    internal void PublishUnconfirmed(PublishFrame p, CancellationToken ct)
        => Send(Op.Publish, WireOps.EncodePublish(p with { RequireConfirm = false }), ct);

    internal async Task<ulong> PublishConfirmedAsync(PublishFrame p, CancellationToken ct)
    {
        var f = await RequestAsync(Op.Publish, WireOps.EncodePublish(p with { RequireConfirm = true }), ct).ConfigureAwait(false);
        return WireOps.DecodePublishOk(f.Payload).Offset;
    }

    internal void PublishDelayedUnconfirmed(PublishDelayedFrame p, CancellationToken ct)
        => Send(Op.PublishDelayed, WireOps.EncodePublishDelayed(p with { RequireConfirm = false }), ct);

    internal async Task<ulong> PublishDelayedConfirmedAsync(PublishDelayedFrame p, CancellationToken ct)
    {
        var f = await RequestAsync(Op.PublishDelayed, WireOps.EncodePublishDelayed(p with { RequireConfirm = true }), ct).ConfigureAwait(false);
        return WireOps.DecodePublishOk(f.Payload).Offset;
    }

    // Sends a confirm-required op and returns a handle that resolves to the offset
    // later, without blocking on the confirm. The frame is written before the handle
    // returns, so callers can fire several publishes (in send order) and collect
    // their confirmations afterward to pipeline them.
    internal PublishConfirmation Confirmation(Op op, byte[] body, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var tcs = new TaskCompletionSource<Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_inbox.Writer.TryWrite(new OutgoingCommand(op, body, 0, tcs, null, false)))
        {
            throw Error();
        }

        async Task<ulong> Await()
        {
            var f = await tcs.Task.ConfigureAwait(false);
            return WireOps.DecodePublishOk(f.Payload).Offset;
        }

        return new PublishConfirmation(Await());
    }

    internal PublishConfirmation PublishWithConfirmation(PublishFrame p, CancellationToken ct)
        => Confirmation(Op.Publish, WireOps.EncodePublish(p with { RequireConfirm = true }), ct);

    internal PublishConfirmation PublishDelayedWithConfirmation(PublishDelayedFrame p, CancellationToken ct)
        => Confirmation(Op.PublishDelayed, WireOps.EncodePublishDelayed(p with { RequireConfirm = true }), ct);

    // ---- declare / topology ----

    internal async Task<DeclareQueueOk> DeclareQueueAsync(DeclareQueueFrame d, CancellationToken ct)
    {
        var f = await RequestAsync(Op.DeclareQueue, WireOps.EncodeDeclareQueue(d), ct).ConfigureAwait(false);
        return WireOps.DecodeDeclareQueueOk(f.Payload);
    }

    internal async Task<DeclarePlexusOk> DeclarePlexusAsync(DeclarePlexusFrame d, CancellationToken ct)
    {
        var f = await RequestAsync(Op.DeclarePlexus, WireOps.EncodeDeclarePlexus(d), ct).ConfigureAwait(false);
        return WireOps.DecodeDeclarePlexusOk(f.Payload);
    }

    internal async Task<TopologyOk> FetchTopologyAsync(TopologyRequest req, CancellationToken ct)
    {
        var f = await RequestAsync(Op.Topology, WireOps.EncodeTopologyRequest(req), ct).ConfigureAwait(false);
        return WireOps.DecodeTopologyOk(f.Payload);
    }

    // ---- subscribe ----

    // A plain queue subscribe is remembered for reconnect reconcile; a supervised one
    // (noReconcile) recovers via its supervisor instead. Streams resume by durable
    // cursor, so they carry no reconcile metadata.
    internal Task<Subscription> SubscribeAsync(SubscribeFrame req, bool noReconcile, CancellationToken ct)
        => SubscribeInternalAsync(Op.Subscribe, WireOps.EncodeSubscribe(req), req.AutoAck, new SubscribeMeta(req, noReconcile), ct);

    internal Task<Subscription> SubscribeStreamAsync(SubscribeStream req, CancellationToken ct)
        => SubscribeInternalAsync(Op.SubscribeStream, WireOps.EncodeSubscribeStream(req), req.AutoAck, null, ct);

    private async Task<Subscription> SubscribeInternalAsync(Op op, byte[] body, bool autoAck, SubscribeMeta? meta, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<Subscription>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_inbox.Writer.TryWrite(new OutgoingCommand(op, body, 0, null, tcs, autoAck, meta)))
        {
            throw Error();
        }
        return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
    }

    // ---- settle ----

    internal void Ack(Delivery d)
    {
        var body = WireOps.EncodeAck(new AckFrame { Topic = d.Topic, Group = d.Group, Partition = d.Partition, Tags = new[] { d.Tag } });
        SendWithId(Op.Ack, DeliveryReqId(d), body);
    }

    internal void Nack(Delivery d, bool requeue, ulong? notBefore)
    {
        var body = WireOps.EncodeNack(new NackFrame
        {
            Topic = d.Topic,
            Group = d.Group,
            Partition = d.Partition,
            Tags = new[] { d.Tag },
            Requeue = requeue,
            NotBefore = notBefore,
        });
        SendWithId(Op.Nack, DeliveryReqId(d), body);
    }

    private static ulong DeliveryReqId(Delivery d) => DeliveryReqIds.GetValue(d, _ => new StrongBox<ulong>()).Value;

    // The DELIVER frame id travels with the Delivery so settle reuses it. It is kept
    // in a side table so Delivery stays a clean public type without an internal id field.
    private static readonly ConditionalWeakTable<Delivery, StrongBox<ulong>> DeliveryReqIds = new();

    // ---- actor-side handlers ----

    private void HandleSubscribeOk(Frame f)
    {
        if (!_waiters.Remove(f.RequestId, out var w))
        {
            return;
        }
        SubscribeOk ok;
        try
        {
            ok = WireOps.DecodeSubscribeOk(f.Payload);
        }
        catch (WireException ex)
        {
            w.SubReply?.TrySetException(ex);
            return;
        }
        var channel = Channel.CreateUnbounded<Delivery>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        // A plain subscribe is remembered so a reconnect can restore it on the same
        // channel. The registry then owns the channel across the engine's death.
        var preserve = false;
        if (_opts.Reconcile is { } registry && w.Sub is { NoReconcile: false } meta)
        {
            registry.Register(new ReconcileSubscription
            {
                SubId = ok.SubId,
                Topic = ok.Topic,
                Partition = ok.Partition,
                Group = ok.Group,
                AutoAck = w.AutoAck,
                Prefetch = ok.Prefetch,
                ConsumerGroup = meta.Request.ConsumerGroup,
                ConsumerTarget = meta.Request.ConsumerTarget,
                MemberId = ok.MemberId,
            }, channel, w.AutoAck);
            preserve = true;
        }

        _subs[ok.SubId] = new SubState { Channel = channel, AutoAck = w.AutoAck, Preserve = preserve };
        w.SubReply?.TrySetResult(new Subscription(ok, channel.Reader));
    }

    private void HandleDeliver(Frame f)
    {
        Deliver d;
        try
        {
            d = WireOps.DecodeDeliver(f.Payload);
        }
        catch (WireException)
        {
            return;
        }
        if (!_subs.TryGetValue(d.SubId, out var s))
        {
            return; // delivery for a sub we no longer track
        }
        var delivery = new Delivery(d, f.RequestId, s.AutoAck, this);
        DeliveryReqIds.GetValue(delivery, _ => new StrongBox<ulong>()).Value = f.RequestId;
        s.Channel.Writer.TryWrite(delivery);
    }
}

/// <summary>
/// A handle to a confirmed publish that has been sent but not yet awaited. Call
/// <see cref="Confirmed"/> to get the broker-assigned offset. Because the frame is
/// written before the handle returns, callers can fire several publishes (in send
/// order) and collect their confirmations afterward to pipeline them.
/// </summary>
public sealed class PublishConfirmation
{
    private readonly Task<ulong> _offset;

    internal PublishConfirmation(Task<ulong> offset)
    {
        _offset = offset;
    }

    /// <summary>Waits for the broker-assigned offset of this publish.</summary>
    public Task<ulong> Confirmed(CancellationToken ct = default) => _offset.WaitAsync(ct);
}
