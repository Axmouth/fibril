//! Committed metadata wakeups with a bounded fallback and burst coalescing.
use std::time::Duration;
use tokio::{sync::watch, time::Instant};

const RETRY: Duration = Duration::from_secs(1);
const COALESCE: Duration = Duration::from_millis(25);

pub(crate) struct RecoveryWake<T> {
    changes: watch::Receiver<T>,
    last: Option<Instant>,
    closed: bool,
}
impl<T: Clone> RecoveryWake<T> {
    pub(crate) fn new(changes: watch::Receiver<T>) -> Self {
        Self {
            changes,
            last: None,
            closed: false,
        }
    }

    pub(crate) async fn next(&mut self) -> T {
        if let Some(last) = self.last {
            // A stream of unrelated commits must not create a tight scan loop.
            tokio::time::sleep_until(last + COALESCE).await;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(last + RETRY) => break,
                    changed = self.changes.changed(), if !self.closed => {
                        if changed.is_ok() { break; }
                        self.closed = true;
                    }
                }
            }
        }
        self.last = Some(Instant::now());
        // Mark seen at the same time as capture. Commits during processing remain
        // pending for the next pass, including when no waiter is registered.
        self.changes.borrow_and_update().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::poll;

    #[tokio::test(start_paused = true)]
    async fn initial_scan_and_changes_during_work_are_not_lost() {
        let (tx, rx) = watch::channel(0);
        let mut wake = RecoveryWake::new(rx);
        assert_eq!(wake.next().await, 0);
        tx.send_replace(1);
        tx.send_replace(2);
        let next = wake.next();
        tokio::pin!(next);
        assert!(poll!(&mut next).is_pending());
        tokio::time::advance(COALESCE).await;
        assert_eq!(next.await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn change_while_waiting_wakes_before_fallback() {
        let (tx, rx) = watch::channel(0);
        let mut wake = RecoveryWake::new(rx);
        wake.next().await;
        tokio::time::advance(COALESCE).await;
        let next = wake.next();
        tokio::pin!(next);
        assert!(poll!(&mut next).is_pending());
        tx.send_replace(1);
        assert_eq!(next.await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_and_closed_watch_keep_periodic_retry_without_spin() {
        let (tx, rx) = watch::channel(0);
        let mut wake = RecoveryWake::new(rx);
        wake.next().await;
        for closed in [false, true] {
            if closed {
                drop(tx.clone());
                wake.closed = true;
            }
            let start = Instant::now();
            assert_eq!(wake.next().await, 0);
            assert_eq!(start.elapsed(), RETRY);
        }
        drop(tx);
        wake.closed = false; // Exercise the actual Receiver::changed error path.
        let start = Instant::now();
        assert_eq!(wake.next().await, 0);
        assert_eq!(start.elapsed(), RETRY);
        assert!(wake.closed);
    }
}
