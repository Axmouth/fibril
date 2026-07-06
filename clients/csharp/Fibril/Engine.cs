using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Threading.Channels;

namespace Fibril;

// The engine owns one live connection to one broker: the handshake, optional
// auth, heartbeats, the request-id -> reply map, and the read/write loops. It
// mirrors the reference client's structure: a single actor task owns all mutable
// state and is the sole writer, a read task feeds it decoded frames, and public
// methods hand it commands over one inbox channel and await a per-request reply
// (TaskCompletionSource). No locks guard engine state - only the actor touches it.
//
// The reference client expresses the actor as a select over several channels. C#
// collapses that into a single inbox channel carrying a small message union
// (outgoing command, incoming frame, heartbeat tick, peer close, stop), which the
// actor drains and batches. It knows nothing about routing, topology, or reconnect
// - that is the client layer above it.

/// <summary>
/// Reserved request ids for the fixed handshake exchanges. Regular ops allocate
/// ids after the last one used, so a handshake reply never collides with a live
/// request.
/// </summary>
internal static class HandshakeId
{
    public const ulong Hello = 1;
    public const ulong Auth = 2;
    public const ulong Reconcile = 3;
}

/// <summary>Connection-level settings for one session.</summary>
internal sealed class EngineOptions
{
    public string ClientName { get; init; } = "";
    public string ClientVersion { get; init; } = "";
    public AuthFrame? Auth { get; init; }
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(5);
    public ResumeIdentity? Resume { get; init; }

    /// <summary>When set, connects over TLS with these trust settings. Null connects plaintext.</summary>
    public TlsOptions? Tls { get; init; }

    /// <summary>
    /// When set, remembers non-supervised subscriptions on this endpoint so a
    /// reconnect restores them. On connect the engine sends RECONCILE_CLIENT for any
    /// it holds and adopts the restored delivery channels.
    /// </summary>
    public ReconcileRegistry? Reconcile { get; init; }

    /// <summary>
    /// Called on the actor for each broker topology push. Applies the snapshot and
    /// returns the generation the client now reflects, which the engine acks so the
    /// broker can fence a repartition cutover.
    /// </summary>
    public Func<TopologyOk, ulong>? OnTopologyUpdate { get; init; }

    /// <summary>Called on the actor for each exclusive-cohort assignment push.</summary>
    public Action<AssignmentChanged>? OnAssignmentChanged { get; init; }
}

// ---- inbox message union ----

internal abstract record EngineMessage;

internal sealed record OutgoingCommand(
    Op Op,
    byte[] Body,
    ulong Id,
    TaskCompletionSource<Frame>? Reply,
    TaskCompletionSource<Subscription>? SubReply,
    bool AutoAck,
    SubscribeMeta? Sub = null) : EngineMessage;

// Carries a queue subscribe's original request and reconcile intent, so a
// SUBSCRIBE_OK can register it for reconnect restore without re-decoding.
internal sealed record SubscribeMeta(SubscribeFrame Request, bool NoReconcile);

internal sealed record IncomingFrame(Frame Frame) : EngineMessage;

internal sealed record PeerClosed(Exception? Error) : EngineMessage;

internal sealed record HeartbeatTick : EngineMessage;

internal sealed record StopEngine : EngineMessage;

/// <summary>A pending request awaiting its correlated reply. Exactly one of Reply/SubReply is set.</summary>
internal sealed class Waiter
{
    public TaskCompletionSource<Frame>? Reply { get; init; }
    public TaskCompletionSource<Subscription>? SubReply { get; init; }
    public bool AutoAck { get; init; }
    public SubscribeMeta? Sub { get; init; }
}

/// <summary>A live subscription's delivery channel, owned by the actor.</summary>
internal sealed class SubState
{
    public required Channel<Delivery> Channel { get; init; }
    public bool AutoAck { get; init; }

    // A preserved subscription's channel is not completed when the engine dies: the
    // reconcile registry owns it and carries it onto the reconnected engine.
    public bool Preserve { get; init; }
}

/// <summary>One live broker connection.</summary>
internal sealed partial class Engine : IAsyncDisposable
{
    private const int WriteBatchBytes = 128 * 1024;

    private readonly Stream _stream;
    private readonly TcpClient? _tcp;
    private readonly EngineOptions _opts;

    // Unbounded so producers and the read task never block posting to the actor.
    // Real backpressure lives at the broker (prefetch/confirm windows), not here.
    private readonly Channel<EngineMessage> _inbox =
        Channel.CreateUnbounded<EngineMessage>(new UnboundedChannelOptions { SingleReader = true });

    private readonly CancellationTokenSource _lifetime = new();
    private Task _runTask = Task.CompletedTask;
    private Task _readTask = Task.CompletedTask;
    private Task _heartbeatTask = Task.CompletedTask;

    // Owned exclusively by the actor (no locks).
    private ulong _nextId;
    private readonly Dictionary<ulong, Waiter> _waiters = new();
    private readonly Dictionary<ulong, SubState> _subs = new();
    private DateTime _lastSeen;
    private bool _closed;

    private readonly object _errLock = new();
    private Exception? _closeError;

    public ResumeIdentity ResumeIdentity { get; private set; } = new(Uuid.Zero, Uuid.Zero, Uuid.Zero);
    public ResumeOutcome ResumeOutcome { get; private set; }

    private Engine(Stream stream, TcpClient? tcp, EngineOptions opts, ulong nextId, Dictionary<ulong, SubState>? restoredSubs)
    {
        _stream = stream;
        _tcp = tcp;
        _opts = opts;
        _nextId = nextId;
        _lastSeen = DateTime.UtcNow;
        if (restoredSubs is not null)
        {
            foreach (var (id, state) in restoredSubs)
            {
                _subs[id] = state;
            }
        }
    }

    /// <summary>
    /// Dials addr and performs the handshake (and optional auth), returning a ready
    /// engine. Cancel <paramref name="ct"/> (e.g. with a timeout) to bound the dial
    /// and handshake.
    /// </summary>
    public static async Task<Engine> ConnectAsync(string addr, EngineOptions opts, CancellationToken ct = default)
    {
        var (host, port) = ParseAddress(addr);
        var tcp = new TcpClient();
        try
        {
            await tcp.ConnectAsync(host, port, ct).ConfigureAwait(false);
        }
        catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionRefused)
        {
            tcp.Dispose();
            throw new DisconnectionException(Guides.ConnectionRefused(addr));
        }
        catch
        {
            tcp.Dispose();
            throw;
        }

        Stream stream = tcp.GetStream();
        try
        {
            if (opts.Tls is { } tls)
            {
                stream = await Tls.WrapAsync(stream, addr, host, tls, ct).ConfigureAwait(false);
            }
            return await StartAsync(stream, tcp, opts, ct).ConfigureAwait(false);
        }
        catch
        {
            stream.Dispose();
            tcp.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Runs the handshake over an already-connected stream, then starts the read and
    /// actor tasks. Split from ConnectAsync so tests can supply any stream instead
    /// of dialing.
    /// </summary>
    internal static async Task<Engine> StartAsync(Stream stream, TcpClient? tcp, EngineOptions opts, CancellationToken ct)
    {
        // HELLO.
        var hello = new Hello(opts.ClientName, opts.ClientVersion, Protocol.V1, opts.Resume);
        await WriteFrameAsync(stream, Frame.Build(Op.Hello, HandshakeId.Hello, WireOps.EncodeHello(hello)), ct).ConfigureAwait(false);
        var hf = await ReadFrameAsync(stream, ct).ConfigureAwait(false);
        switch (hf.Opcode)
        {
            case Op.HelloOk:
                break;
            case Op.HelloErr:
            case Op.Error:
                {
                    var em = SafeDecodeError(hf.Payload);
                    // A plaintext connection to a TLS listener draws this definitive code.
                    if (em.Code == BrokerErrorCode.TlsRequired)
                    {
                        throw new TlsRequiredByBrokerException();
                    }
                    throw new ServerException(em.Code, em.Message);
                }
            default:
                throw new UnexpectedException("unexpected frame during HELLO");
        }
        var helloOk = WireOps.DecodeHelloOk(hf.Payload);
        if (helloOk.ProtocolVersion != Protocol.V1)
        {
            throw new DisconnectionException("protocol version mismatch");
        }
        if (helloOk.Compliance != Protocol.ComplianceString)
        {
            throw new DisconnectionException("protocol compliance marker mismatch");
        }

        var nextId = HandshakeId.Hello;

        // AUTH (optional).
        if (opts.Auth is not null)
        {
            await WriteFrameAsync(stream, Frame.Build(Op.Auth, HandshakeId.Auth, WireOps.EncodeAuth(opts.Auth)), ct).ConfigureAwait(false);
            var af = await ReadFrameAsync(stream, ct).ConfigureAwait(false);
            switch (af.Opcode)
            {
                case Op.AuthOk:
                    break;
                case Op.AuthErr:
                case Op.Error:
                    {
                        var em = SafeDecodeError(af.Payload);
                        throw new ServerException(em.Code, em.Message);
                    }
                default:
                    throw new UnexpectedException("unexpected frame during AUTH");
            }
            nextId = HandshakeId.Auth;
        }

        // RECONCILE on a reconnect that has remembered subscriptions: a bounced owner
        // reconnects into a fresh session that forgot them, so the client re-announces
        // them and adopts the restored delivery channels.
        Dictionary<ulong, SubState>? restoredSubs = null;
        if (opts.Reconcile is { } registry && !registry.IsEmpty)
        {
            nextId = HandshakeId.Reconcile;
            var body = WireOps.EncodeReconcileClient(new ReconcileClient { Policy = registry.Policy, Subscriptions = registry.Snapshot() });
            await WriteFrameAsync(stream, Frame.Build(Op.ReconcileClient, HandshakeId.Reconcile, body), ct).ConfigureAwait(false);
            var rf = await ReadFrameAsync(stream, ct).ConfigureAwait(false);
            switch (rf.Opcode)
            {
                case Op.ReconcileResult:
                    restoredSubs = registry.ApplyResult(WireOps.DecodeReconcileResult(rf.Payload));
                    break;
                case Op.Error:
                    {
                        var em = SafeDecodeError(rf.Payload);
                        throw new ServerException(em.Code, em.Message);
                    }
                default:
                    throw new UnexpectedException("unexpected frame during reconciliation");
            }
        }

        var engine = new Engine(stream, tcp, opts, nextId, restoredSubs)
        {
            ResumeIdentity = new ResumeIdentity(helloOk.OwnerId, helloOk.ClientId, helloOk.ResumeToken),
            ResumeOutcome = helloOk.Outcome,
        };
        engine._readTask = engine.ReadLoopAsync(engine._lifetime.Token);
        engine._heartbeatTask = engine.HeartbeatLoopAsync(engine._lifetime.Token);
        engine._runTask = engine.RunAsync();
        return engine;
    }

    // ---- request submission ----

    internal void Send(Op op, byte[] body, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (!_inbox.Writer.TryWrite(new OutgoingCommand(op, body, 0, null, null, false)))
        {
            throw Error();
        }
    }

    // Fire-and-forget with a fixed request id (settle reuses the DELIVER frame id).
    // Best-effort: acking a dead connection is moot.
    internal void SendWithId(Op op, ulong id, byte[] body)
        => _inbox.Writer.TryWrite(new OutgoingCommand(op, body, id, null, null, false));

    internal async Task<Frame> RequestAsync(Op op, byte[] body, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_inbox.Writer.TryWrite(new OutgoingCommand(op, body, 0, tcs, null, false)))
        {
            throw Error();
        }
        return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => ShutdownAsync();

    /// <summary>Tears the connection down gracefully and waits for the loops to exit.</summary>
    public async ValueTask ShutdownAsync()
    {
        _inbox.Writer.TryWrite(new StopEngine());
        try
        {
            await _runTask.ConfigureAwait(false);
        }
        catch
        {
            // The actor task never faults observably. Teardown is best-effort.
        }
    }

    public bool IsClosed => _runTask.IsCompleted;

    // ---- the actor ----

    private async Task RunAsync()
    {
        var writeBuf = new ArrayBufferWriter<byte>();
        Exception? death = null;
        var stopping = false;
        try
        {
            while (await _inbox.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (_inbox.Reader.TryRead(out var msg))
                {
                    switch (msg)
                    {
                        case OutgoingCommand cmd:
                            HandleCommand(cmd, writeBuf);
                            break;
                        case IncomingFrame inc:
                            HandleFrame(inc.Frame, writeBuf, ref death);
                            break;
                        case HeartbeatTick:
                            HandleTick(writeBuf, ref death);
                            break;
                        case PeerClosed pc:
                            death ??= pc.Error as FibrilException ?? new DisconnectionException("connection closed by peer");
                            break;
                        case StopEngine:
                            stopping = true;
                            break;
                    }
                    if (death is not null || stopping)
                    {
                        break;
                    }
                    if (writeBuf.WrittenCount >= WriteBatchBytes)
                    {
                        break;
                    }
                }

                // One socket write for everything buffered this iteration (buffered
                // fire-and-forget frames flush even on a clean stop).
                if (writeBuf.WrittenCount > 0)
                {
                    try
                    {
                        await _stream.WriteAsync(writeBuf.WrittenMemory).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        death ??= new DisconnectionException("socket write failed: " + ex.Message);
                    }
                    writeBuf.Clear();
                }

                if (death is not null)
                {
                    MarkDead(death);
                    break;
                }
                if (stopping)
                {
                    MarkDead(new BrokenPipeException("engine shutdown"));
                    break;
                }
            }
        }
        finally
        {
            if (!_closed)
            {
                MarkDead(new BrokenPipeException("engine shutdown"));
            }
        }
    }

    private void HandleCommand(OutgoingCommand cmd, ArrayBufferWriter<byte> writeBuf)
    {
        if (_closed)
        {
            FailWaiter(new Waiter { Reply = cmd.Reply, SubReply = cmd.SubReply }, Error());
            return;
        }
        var id = cmd.Id;
        if (id == 0)
        {
            id = ++_nextId;
        }
        if (cmd.Reply is not null || cmd.SubReply is not null)
        {
            _waiters[id] = new Waiter { Reply = cmd.Reply, SubReply = cmd.SubReply, AutoAck = cmd.AutoAck, Sub = cmd.Sub };
        }
        AppendFrame(writeBuf, Frame.Build(cmd.Op, id, cmd.Body));
    }

    private void HandleFrame(Frame f, ArrayBufferWriter<byte> writeBuf, ref Exception? death)
    {
        _lastSeen = DateTime.UtcNow;
        switch (f.Opcode)
        {
            case Op.Pong:
                return;
            case Op.Ping:
                AppendFrame(writeBuf, Frame.Build(Op.Pong, f.RequestId, Array.Empty<byte>()));
                return;
            case Op.PublishOk:
            case Op.DeclareQueueOk:
            case Op.DeclarePlexusOk:
            case Op.TopologyOk:
                Resolve(f.RequestId, f);
                return;
            case Op.SubscribeOk:
                HandleSubscribeOk(f);
                return;
            case Op.Deliver:
                HandleDeliver(f);
                return;
            case Op.TopologyUpdate:
                if (_opts.OnTopologyUpdate is { } onTopo)
                {
                    try
                    {
                        var topo = WireOps.DecodeTopologyUpdate(f.Payload);
                        var gen = onTopo(topo);
                        AppendFrame(writeBuf, Frame.Build(Op.TopologyUpdateAck, f.RequestId, WireOps.EncodeTopologyUpdateAck(new TopologyUpdateAck(gen))));
                    }
                    catch (WireException)
                    {
                        // Ignore a malformed push. The next one refreshes routing.
                    }
                }
                return;
            case Op.AssignmentChanged:
                if (_opts.OnAssignmentChanged is { } onAssign)
                {
                    try
                    {
                        onAssign(WireOps.DecodeAssignmentChanged(f.Payload));
                    }
                    catch (WireException)
                    {
                    }
                }
                return;
            case Op.Redirect:
                {
                    if (!_waiters.Remove(f.RequestId, out var w))
                    {
                        return;
                    }
                    try
                    {
                        FailWaiter(w, new RedirectException(WireOps.DecodeRedirect(f.Payload)));
                    }
                    catch (WireException ex)
                    {
                        FailWaiter(w, ex);
                    }
                    return;
                }
            case Op.Error:
            case Op.SubscribeErr:
                {
                    var em = SafeDecodeError(f.Payload);
                    var serr = new ServerException(em.Code, em.Message);
                    if (_waiters.Remove(f.RequestId, out var w))
                    {
                        FailWaiter(w, serr);
                    }
                    else if (f.Opcode == Op.Error)
                    {
                        // A connection-level error with no correlated request is fatal.
                        death ??= new DisconnectionException(serr.Message);
                    }
                    return;
                }
        }
    }

    private void HandleTick(ArrayBufferWriter<byte> writeBuf, ref Exception? death)
    {
        if (_closed)
        {
            return;
        }
        if (DateTime.UtcNow - _lastSeen > 3 * _opts.HeartbeatInterval)
        {
            death ??= new DisconnectionException(Guides.HeartbeatTimeout);
            return;
        }
        AppendFrame(writeBuf, Frame.Build(Op.Ping, ++_nextId, Array.Empty<byte>()));
    }

    private void Resolve(ulong id, Frame f)
    {
        if (_waiters.Remove(id, out var w))
        {
            w.Reply?.TrySetResult(f);
        }
    }

    private void MarkDead(Exception err)
    {
        if (_closed)
        {
            return;
        }
        _closed = true;
        SetError(err);
        foreach (var (id, w) in _waiters)
        {
            FailWaiter(w, err);
        }
        _waiters.Clear();
        foreach (var (id, s) in _subs)
        {
            // A preserved subscription's channel is left open: the reconcile registry
            // owns it and carries it onto the reconnected engine.
            if (!s.Preserve)
            {
                s.Channel.Writer.TryComplete();
            }
        }
        _subs.Clear();
        _inbox.Writer.TryComplete();
        _lifetime.Cancel();
        try
        {
            _stream.Dispose();
        }
        catch
        {
        }
        _tcp?.Dispose();
    }

    private static void FailWaiter(Waiter w, Exception err)
    {
        w.Reply?.TrySetException(err);
        w.SubReply?.TrySetException(err);
    }

    private static void AppendFrame(ArrayBufferWriter<byte> buf, Frame f)
    {
        var bytes = f.Encode();
        buf.Write(bytes);
    }

    // ---- read + heartbeat loops ----

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var f = await ReadFrameAsync(_stream, ct).ConfigureAwait(false);
                if (!_inbox.Writer.TryWrite(new IncomingFrame(f)))
                {
                    return;
                }
            }
        }
        catch (Exception ex)
        {
            _inbox.Writer.TryWrite(new PeerClosed(ex is OperationCanceledException ? null : ex));
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(_opts.HeartbeatInterval);
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                if (!_inbox.Writer.TryWrite(new HeartbeatTick()))
                {
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // ---- error plumbing ----

    private void SetError(Exception err)
    {
        lock (_errLock)
        {
            _closeError ??= err;
        }
    }

    internal Exception Error()
    {
        lock (_errLock)
        {
            return _closeError ?? new BrokenPipeException();
        }
    }

    // ---- framing helpers ----

    private static async Task WriteFrameAsync(Stream s, Frame f, CancellationToken ct)
        => await s.WriteAsync(f.Encode(), ct).ConfigureAwait(false);

    private static async Task<Frame> ReadFrameAsync(Stream s, CancellationToken ct)
    {
        var header = new byte[Frame.HeaderSize];
        await s.ReadExactlyAsync(header, ct).ConfigureAwait(false);
        var payloadLen = BinaryPrimitives.ReadUInt32BigEndian(header);
        var payload = payloadLen > 0 ? new byte[payloadLen] : Array.Empty<byte>();
        if (payloadLen > 0)
        {
            await s.ReadExactlyAsync(payload, ct).ConfigureAwait(false);
        }
        return new Frame
        {
            Version = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(4)),
            Opcode = (Op)BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(6)),
            Flags = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(8)),
            RequestId = BinaryPrimitives.ReadUInt64BigEndian(header.AsSpan(12)),
            Payload = payload,
        };
    }

    private static ErrorMsg SafeDecodeError(byte[] payload)
    {
        try
        {
            return WireOps.DecodeError(payload);
        }
        catch (WireException)
        {
            return new ErrorMsg(0, "unparseable error frame");
        }
    }

    private static (string Host, int Port) ParseAddress(string addr)
    {
        var idx = addr.LastIndexOf(':');
        if (idx <= 0 || idx == addr.Length - 1)
        {
            throw new DisconnectionException("invalid broker address (expected host:port): " + addr);
        }
        if (!int.TryParse(addr.AsSpan(idx + 1), out var port))
        {
            throw new DisconnectionException("invalid broker port in address: " + addr);
        }
        return (addr[..idx], port);
    }
}
