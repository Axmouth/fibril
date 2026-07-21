package fibril

import "sync"

// CloseReason is the typed reason a subscription ended. A subscription's
// Deliveries channel closes on end (owner move, topic deletion, a reconcile
// verdict, a disconnect); CloseReason, read after the channel closes, says why
// - nil for a clean local close.
type CloseReason struct {
	Code    ReasonCode
	Message string
}

// closeReasonCell is a set-once holder for a subscription's close reason,
// shared between the engine (which stamps it before closing the delivery
// channel) and the reader (which reads it after the channel closes). Go
// channels cannot carry a value on close, so the reason travels beside them.
type closeReasonCell struct {
	mu     sync.Mutex
	reason *CloseReason
}

func newCloseReasonCell() *closeReasonCell { return &closeReasonCell{} }

func (c *closeReasonCell) set(code ReasonCode, message string) {
	c.mu.Lock()
	if c.reason == nil {
		c.reason = &CloseReason{Code: code, Message: message}
	}
	c.mu.Unlock()
}

func (c *closeReasonCell) get() *CloseReason {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// isTerminalClose reports whether a typed close ends the subscription (the
// supervisor stops and surfaces it) rather than triggering a re-subscribe.
// Topic deletion, a server error, and the reserved lag close are terminal; a
// recreate is terminal only when the user opted out of auto-resubscribe.
func isTerminalClose(code ReasonCode, autoResubscribe bool) bool {
	switch code {
	case ReasonTopicDeleted, ReasonServerError, ReasonLagged:
		return true
	case ReasonRecreate:
		return !autoResubscribe
	default:
		return false
	}
}
