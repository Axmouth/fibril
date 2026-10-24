//! Gap coalescing for the publisher sink: after the queued requests are
//! drained, keep gathering until no request arrived for a short gap, so a
//! batch ends as soon as its burst does. Tokio's timer rounds deadlines up to
//! whole milliseconds, too coarse for gaps of tens of microseconds, so the
//! wait uses a Linux timerfd. Other platforms have no gap mode and the caller
//! falls back to the window.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Longest a batch may keep growing, by its size so far. A batch that keeps
/// filling earns a longer wait, so a saturated queue still appends large
/// batches.
const CAPS_US: [(usize, u64); 4] = [(16, 150), (32, 300), (64, 600), (usize::MAX, 1000)];

/// Gap waits run only if the previous append was this recent, so a request
/// after a quiet period goes out at once.
pub(crate) const QUIET: Duration = Duration::from_millis(1);

/// Backstop. The timerfd should always end the wait first, so every flush by
/// this guard is counted and logged as a bug.
const GUARD: Duration = Duration::from_millis(2);

static GUARD_FLUSHES: AtomicU64 = AtomicU64::new(0);

fn cap_us(len: usize) -> u64 {
    CAPS_US.iter().find(|(size, _)| len < *size).map_or(1000, |(_, us)| *us)
}

/// Per sink state. The timer is opened on first use.
#[derive(Default)]
pub(crate) struct GapCoalescer {
    timer: Option<GapTimer>,
    unavailable: bool,
}

impl GapCoalescer {
    /// True once opening the timer failed. The sink then uses the window.
    pub(crate) fn unavailable(&self) -> bool {
        self.unavailable
    }

    /// Gathers requests into `batch` until none arrived for `gap_us`, the
    /// size cap's deadline passes or the batch holds `max`. Returns false
    /// without waiting when no precise timer is available.
    pub(crate) async fn gather<T>(
        &mut self,
        rx: &mut mpsc::Receiver<T>,
        batch: &mut Vec<T>,
        max: usize,
        gap_us: u64,
        shutdown: &CancellationToken,
        owner_shutdown: &CancellationToken,
    ) -> bool {
        if self.unavailable {
            return false;
        }
        if self.timer.is_none() {
            match GapTimer::new() {
                Ok(timer) => self.timer = Some(timer),
                Err(err) => {
                    tracing::warn!("publish gap coalescing needs a Linux timerfd, using the window: {err}");
                    self.unavailable = true;
                    return false;
                }
            }
        }
        let Some(timer) = &self.timer else {
            return false;
        };
        let start = GapTimer::now();
        let guard = tokio::time::sleep(GUARD);
        tokio::pin!(guard);
        let mut armed_us = 0;
        let mut last_arrival_us = 0;
        while batch.len() < max {
            let us = cap_us(batch.len()).min(last_arrival_us + gap_us);
            if us != armed_us {
                if timer.arm(start, us).is_err() {
                    break;
                }
                armed_us = us;
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = owner_shutdown.cancelled() => break,
                req = rx.recv() => {
                    let Some(req) = req else { break };
                    batch.push(req);
                    while batch.len() < max {
                        match rx.try_recv() {
                            Ok(req) => batch.push(req),
                            Err(_) => break,
                        }
                    }
                    last_arrival_us = GapTimer::elapsed_us(start);
                }
                _ = timer.fired() => break,
                _ = &mut guard => {
                    let n = GUARD_FLUSHES.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_power_of_two() {
                        tracing::error!(flushes = n, "publish gap timer missed, guard flushed");
                    }
                    break;
                }
            }
        }
        timer.disarm();
        true
    }
}

#[cfg(target_os = "linux")]
struct GapTimer {
    fd: tokio::io::unix::AsyncFd<rustix::fd::OwnedFd>,
}

#[cfg(target_os = "linux")]
impl GapTimer {
    fn new() -> std::io::Result<Self> {
        use rustix::time::{TimerfdClockId, TimerfdFlags, timerfd_create};
        let fd = timerfd_create(TimerfdClockId::Monotonic, TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC)?;
        Ok(Self {
            fd: tokio::io::unix::AsyncFd::new(fd)?,
        })
    }

    fn now() -> rustix::time::Timespec {
        rustix::time::clock_gettime(rustix::time::ClockId::Monotonic)
    }

    fn elapsed_us(start: rustix::time::Timespec) -> u64 {
        let now = Self::now();
        let ns = (now.tv_sec - start.tv_sec) * 1_000_000_000 + (now.tv_nsec - start.tv_nsec);
        (ns.max(0) / 1000) as u64
    }

    /// Fires at `start` plus `us` microseconds, at once if that has passed.
    fn arm(&self, start: rustix::time::Timespec, us: u64) -> std::io::Result<()> {
        use rustix::time::{Itimerspec, Timespec, TimerfdTimerFlags, timerfd_settime};
        let nsec = start.tv_nsec + (us as i64) * 1000;
        let at = Timespec {
            tv_sec: start.tv_sec + nsec / 1_000_000_000,
            tv_nsec: nsec % 1_000_000_000,
        };
        let spec = Itimerspec {
            it_interval: Timespec { tv_sec: 0, tv_nsec: 0 },
            it_value: at,
        };
        timerfd_settime(self.fd.get_ref(), TimerfdTimerFlags::ABSTIME, &spec)?;
        Ok(())
    }

    /// Stops the timer and clears an expiry nobody read.
    fn disarm(&self) {
        use rustix::time::{Itimerspec, Timespec, TimerfdTimerFlags, timerfd_settime};
        let zero = Timespec { tv_sec: 0, tv_nsec: 0 };
        let spec = Itimerspec {
            it_interval: zero,
            it_value: zero,
        };
        let _ = timerfd_settime(self.fd.get_ref(), TimerfdTimerFlags::empty(), &spec);
    }

    async fn fired(&self) {
        loop {
            let Ok(mut guard) = self.fd.readable().await else {
                return;
            };
            let mut buf = [0u8; 8];
            match guard.try_io(|fd| rustix::io::read(fd.get_ref(), &mut buf).map_err(std::io::Error::from)) {
                Ok(_) => return,
                // Stale readiness from an earlier expiry, wait again.
                Err(_would_block) => continue,
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
struct GapTimer;

#[cfg(not(target_os = "linux"))]
impl GapTimer {
    fn new() -> std::io::Result<Self> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "no timerfd on this platform"))
    }

    fn now() {}

    fn elapsed_us(_start: ()) -> u64 {
        0
    }

    fn arm(&self, _start: (), _us: u64) -> std::io::Result<()> {
        Ok(())
    }

    fn disarm(&self) {}

    async fn fired(&self) {
        std::future::pending().await
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    // Wall clock tests. A loaded machine can preempt any single try past the
    // bound, so each passes if one of TRIES meets it. Tokio's millisecond
    // timer could never meet it at all.
    const TRIES: usize = 20;

    #[tokio::test]
    async fn gap_ends_a_batch_soon_after_the_last_arrival() {
        let mut fastest = Duration::MAX;
        for _ in 0..TRIES {
            let (_tx, mut rx) = mpsc::channel::<u32>(16);
            let mut batch = vec![0u32];
            let mut coalescer = GapCoalescer::default();
            let token = CancellationToken::new();
            let started = std::time::Instant::now();
            assert!(coalescer.gather(&mut rx, &mut batch, 256, 50, &token, &token).await);
            fastest = fastest.min(started.elapsed());
            assert_eq!(batch, vec![0]);
        }
        assert!(fastest < Duration::from_millis(1), "a 50 us gap should not round up to the Tokio tick, fastest {fastest:?}");
    }

    #[tokio::test]
    async fn arrivals_within_the_gap_join_the_batch() {
        let mut joined = false;
        for _ in 0..TRIES {
            let (tx, mut rx) = mpsc::channel(16);
            let mut batch = vec![0u32];
            let mut coalescer = GapCoalescer::default();
            let token = CancellationToken::new();
            let sender = tokio::spawn(async move {
                for i in 1..=3 {
                    tx.send(i).await.unwrap();
                    tokio::task::yield_now().await;
                }
            });
            assert!(coalescer.gather(&mut rx, &mut batch, 256, 500, &token, &token).await);
            sender.await.unwrap();
            // Whatever the timing, the batch keeps arrival order.
            assert_eq!(batch, (0..batch.len() as u32).collect::<Vec<_>>());
            if batch.len() == 4 {
                joined = true;
                break;
            }
        }
        assert!(joined, "arrivals within the gap should join the batch");
    }

    #[test]
    fn caps_grow_with_the_batch() {
        assert_eq!(cap_us(1), 150);
        assert_eq!(cap_us(16), 300);
        assert_eq!(cap_us(63), 600);
        assert_eq!(cap_us(255), 1000);
    }
}
