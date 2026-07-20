package fibril

import "sync"

// reconcileRegistry holds the non-supervised subscriptions on one endpoint so
// they survive a reconnect. It outlives the engine it was captured on: the
// delivery channel lives here too, so a restored subscription keeps yielding on
// the same channel the caller already holds. Supervised subscriptions stay out
// of the registry, since their supervisor re-subscribes on a drop instead.
//
// The broker mints fresh sub ids on a new session, so on reconnect the client
// sends its remembered subscriptions (RECONCILE_CLIENT) and the broker replies
// with a per-subscription verdict (RECONCILE_RESULT): keep (re-key to the new
// server sub id and carry the channel over) or close.
type reconcileRegistry struct {
	policy ReconcilePolicy
	mu     sync.Mutex
	subs   map[uint64]*reconcileEntry
}

type reconcileEntry struct {
	sub     reconcileSubscription
	ch      chan Delivery
	autoAck bool
}

func newReconcileRegistry(policy ReconcilePolicy) *reconcileRegistry {
	if policy == "" {
		policy = ReconcileRestore
	}
	return &reconcileRegistry{policy: policy, subs: map[uint64]*reconcileEntry{}}
}

func (r *reconcileRegistry) register(sub reconcileSubscription, ch chan Delivery, autoAck bool) {
	r.mu.Lock()
	r.subs[sub.SubID] = &reconcileEntry{sub: sub, ch: ch, autoAck: autoAck}
	r.mu.Unlock()
}

func (r *reconcileRegistry) isEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs) == 0
}

func (r *reconcileRegistry) snapshot() []reconcileSubscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]reconcileSubscription, 0, len(r.subs))
	for _, e := range r.subs {
		out = append(out, e.sub)
	}
	return out
}

// applyResult installs the broker's reconcile verdicts and returns the subState
// map a reconnecting engine should start with, keyed by the (new) server sub ids.
// Channels of subscriptions the broker did not keep are closed so their consumers
// stop.
func (r *reconcileRegistry) applyResult(res reconcileResult) map[uint64]*subState {
	r.mu.Lock()
	defer r.mu.Unlock()

	restored := map[uint64]*subState{}
	next := map[uint64]*reconcileEntry{}
	visited := map[uint64]bool{}

	for _, sr := range res.Subscriptions {
		if sr.Client == nil {
			continue
		}
		oldID := sr.Client.SubID
		visited[oldID] = true
		e, ok := r.subs[oldID]
		if !ok {
			continue
		}
		switch sr.Action {
		case reconcileKeep:
			newID := oldID
			if sr.Server != nil {
				newID = sr.Server.SubID
			}
			e.sub.SubID = newID
			restored[newID] = &subState{ch: e.ch, autoAck: e.autoAck, preserve: true}
			next[newID] = e
		default:
			// close_client_side / close_server_side, and for now also
			// recreate_client_side: the server has no live subscription
			// behind this channel, so keeping it open would strand the
			// consumer. Auto-resubscribe on recreate arrives with the typed
			// subscription lifecycle.
			close(e.ch)
		}
	}
	// A subscription the broker did not mention is gone; end its consumer.
	for id, e := range r.subs {
		if !visited[id] {
			close(e.ch)
		}
	}
	r.subs = next
	return restored
}

// closeAll closes every remaining channel, for a full client shutdown where no
// reconnect will reuse them.
func (r *reconcileRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.subs {
		close(e.ch)
	}
	r.subs = map[uint64]*reconcileEntry{}
}
