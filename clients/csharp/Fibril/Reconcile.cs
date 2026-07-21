using System.Threading.Channels;

namespace Fibril;

// The reconcile registry holds the non-supervised subscriptions on one endpoint so
// they survive a reconnect. It outlives the engine it was captured on: the delivery
// channel lives here too, so a restored subscription keeps yielding on the same
// channel the caller already holds. Supervised subscriptions stay out of the
// registry, since their supervisor re-subscribes on a drop instead.
//
// The broker mints fresh sub ids on a new session, so on reconnect the client sends
// its remembered subscriptions (RECONCILE_CLIENT) and the broker replies with a
// per-subscription verdict (RECONCILE_RESULT): keep (re-key to the new server sub id
// and carry the channel over) or close.
internal sealed class ReconcileRegistry
{
    private sealed class Entry
    {
        public required ReconcileSubscription Sub { get; set; }
        public required Channel<Delivery> Channel { get; init; }
        public bool AutoAck { get; init; }
    }

    private readonly object _lock = new();
    private readonly Dictionary<ulong, Entry> _subs = new();

    public ReconcilePolicy Policy { get; }

    public ReconcileRegistry(ReconcilePolicy policy) => Policy = policy;

    public void Register(ReconcileSubscription sub, Channel<Delivery> channel, bool autoAck)
    {
        lock (_lock)
        {
            _subs[sub.SubId] = new Entry { Sub = sub, Channel = channel, AutoAck = autoAck };
        }
    }

    public bool IsEmpty
    {
        get
        {
            lock (_lock)
            {
                return _subs.Count == 0;
            }
        }
    }

    public IReadOnlyList<ReconcileSubscription> Snapshot()
    {
        lock (_lock)
        {
            return _subs.Values.Select(e => e.Sub).ToList();
        }
    }

    // Installs the broker's reconcile verdicts and returns the sub-state map a
    // reconnecting engine should start with, keyed by the new server sub ids.
    // Channels of subscriptions the broker did not keep are completed so their
    // consumers stop.
    public Dictionary<ulong, SubState> ApplyResult(ReconcileResult result)
    {
        lock (_lock)
        {
            var restored = new Dictionary<ulong, SubState>();
            var next = new Dictionary<ulong, Entry>();
            var visited = new HashSet<ulong>();

            foreach (var verdict in result.Subscriptions)
            {
                if (verdict.Client is null)
                {
                    continue;
                }
                var oldId = verdict.Client.Value.SubId;
                visited.Add(oldId);
                if (!_subs.TryGetValue(oldId, out var entry))
                {
                    continue;
                }
                if (verdict.Action is ReconcileAction.Keep)
                {
                    var newId = verdict.Server?.SubId ?? oldId;
                    entry.Sub = entry.Sub with { SubId = newId };
                    restored[newId] = new SubState { Channel = entry.Channel, AutoAck = entry.AutoAck, Preserve = true };
                    next[newId] = entry;
                }
                else
                {
                    // Close verdicts and RecreateClient: the server has no live
                    // subscription behind this channel. Complete it with the
                    // typed reason - a supervised subscription treats a recreate
                    // (and other non-terminal reasons) as a cue to re-subscribe,
                    // a terminal one surfaces to the consumer.
                    entry.Channel.Writer.TryComplete(
                        new SubscriptionClosedException(verdict.Code, verdict.Reason));
                }
            }

            // A subscription the broker did not mention is gone; end its consumer.
            foreach (var (id, entry) in _subs)
            {
                if (!visited.Contains(id))
                {
                    entry.Channel.Writer.TryComplete(
                        new SubscriptionClosedException(
                            ReasonCode.ServerError,
                            "subscription no longer present after reconnect"));
                }
            }

            _subs.Clear();
            foreach (var (id, entry) in next)
            {
                _subs[id] = entry;
            }
            return restored;
        }
    }

    // Completes every remaining channel, for a full client shutdown where no
    // reconnect will reuse them.
    public void CloseAll()
    {
        lock (_lock)
        {
            foreach (var entry in _subs.Values)
            {
                entry.Channel.Writer.TryComplete();
            }
            _subs.Clear();
        }
    }
}
