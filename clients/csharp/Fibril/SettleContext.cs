namespace Fibril;

/// <summary>
/// Routes a manual settle to whatever engine is currently live for a connection,
/// keyed by the connection incarnation the delivery arrived on. One per persistent
/// connection (the bootstrap owner and each pooled owner), outliving every engine
/// it swaps through, so a delivery held across a reconnect settles against the
/// current engine rather than the dead one it arrived on. A non-resumed reconnect
/// allocates a fresh incarnation, so a delivery from the replaced session settles
/// to a <see cref="StaleDeliveryException"/>; a resumed reconnect keeps the
/// incarnation, so a held delivery still settles through the new engine. Mirrors
/// the reference client's SettleContext (crates/client).
/// </summary>
internal sealed class SettleContext
{
    // One immutable binding swapped in per (re)connect, so a settle reads both the
    // incarnation and the engine from a single consistent snapshot.
    private sealed record Binding(long Incarnation, Engine Engine);

    private volatile Binding? _current;
    private long _nextIncarnation;

    /// <summary>
    /// Reserves the incarnation a (re)connecting engine stamps on its deliveries,
    /// before its read loop can deliver. A fresh session (a non-resumed reconnect,
    /// or the first connect) claims a new incarnation so deliveries held from the
    /// replaced session go stale; a resumed session reuses the current one so they
    /// still settle. The engine captures the returned value once and never re-reads
    /// it, so a late delivery on a superseded engine keeps its own (now stale) stamp.
    /// </summary>
    public long Reserve(bool freshSession)
    {
        var cur = _current;
        if (freshSession || cur is null)
        {
            return Interlocked.Increment(ref _nextIncarnation) - 1;
        }
        return cur.Incarnation;
    }

    /// <summary>Points settlement at an engine once it exists, under its reserved incarnation.</summary>
    public void Bind(long incarnation, Engine engine) => _current = new Binding(incarnation, engine);

    /// <summary>
    /// The live engine for a delivery stamped at <paramref name="incarnation"/>.
    /// Throws <see cref="StaleDeliveryException"/> if the incarnation has moved on
    /// (the session was replaced) - the caller sends no frame, the message
    /// redelivers - or <see cref="BrokenPipeException"/> if there is no live engine.
    /// </summary>
    public Engine CurrentOrStale(long incarnation)
    {
        var cur = _current;
        if (cur is null)
        {
            throw new BrokenPipeException("no live connection to settle against");
        }
        if (incarnation != cur.Incarnation)
        {
            throw new StaleDeliveryException();
        }
        return cur.Engine;
    }
}
